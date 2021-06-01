package committee

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/oasisprotocol/oasis-core/go/runtime/nodes/grpc"
	storageApi "github.com/oasisprotocol/oasis-core/go/storage/api"
	storageClient "github.com/oasisprotocol/oasis-core/go/storage/client"
	"github.com/oasisprotocol/oasis-core/go/storage/mkvs/checkpoint"
)

const (
	retryInterval = 1 * time.Second
	maxRetries    = 30

	checkpointStatusDone = 0
	checkpointStatusNext = 1
	checkpointStatusBail = 2

	// LogEventCheckpointSyncSuccess is a log event value that signals that checkpoint sync was successful.
	LogEventCheckpointSyncSuccess = "worker/storage/checkpoint-sync-success"
)

// ErrNoUsableCheckpoints is the error returned when none of the checkpoints could be synced.
var ErrNoUsableCheckpoints = errors.New("storage: no checkpoint could be synced")

type restoreResult struct {
	done bool
	err  error
}

type chunkHeap struct {
	array  []*checkpoint.ChunkMetadata
	length int
}

func (h chunkHeap) Len() int           { return h.length }
func (h chunkHeap) Less(i, j int) bool { return h.array[i].Index < h.array[j].Index }
func (h chunkHeap) Swap(i, j int)      { h.array[i], h.array[j] = h.array[j], h.array[i] }

func (h *chunkHeap) Push(x interface{}) {
	h.array[h.length] = x.(*checkpoint.ChunkMetadata)
	h.length++
}

func (h *chunkHeap) Pop() interface{} {
	h.length--
	ret := h.array[h.length]
	h.array[h.length] = nil
	return ret
}

// goWithNodes runs the given operation with all the connections in the provided nodesClient.
func (n *Node) goWithNodes(
	nodesClient grpc.NodesClient,
	fn func(context.Context, *grpc.ConnWithNodeMeta) error,
) (
	context.CancelFunc,
	chan interface{},
	error,
) {
	connCh := make(chan []*grpc.ConnWithNodeMeta)
	connGetter := func() error {
		conns := nodesClient.GetConnectionsWithMeta()
		if len(conns) == 0 {
			return storageClient.ErrStorageNotAvailable
		}
		connCh <- conns
		return nil
	}
	go func() {
		sched := backoff.WithMaxRetries(backoff.NewConstantBackOff(retryInterval), maxRetries)
		_ = backoff.Retry(connGetter, backoff.WithContext(sched, n.ctx))
		close(connCh)
	}()
	conns, ok := <-connCh
	if !ok || len(conns) == 0 {
		return nil, nil, storageClient.ErrStorageNotAvailable
	}

	workerCtx, workerCancel := context.WithCancel(n.ctx)
	var workerGroup sync.WaitGroup
	doneCh := make(chan interface{})

	for _, conn := range conns {
		workerGroup.Add(1)
		go func(conn *grpc.ConnWithNodeMeta) {
			defer workerGroup.Done()
			op := func() error {
				return fn(workerCtx, conn)
			}
			sched := backoff.WithMaxRetries(backoff.NewConstantBackOff(retryInterval), maxRetries)
			_ = backoff.Retry(op, backoff.WithContext(sched, workerCtx))
		}(conn)
	}
	go func() {
		defer close(doneCh)
		workerGroup.Wait()
	}()

	return workerCancel, doneCh, nil
}

func (n *Node) nodeWorker(
	ctx context.Context,
	conn *grpc.ConnWithNodeMeta,
	chunkDispatchCh chan *checkpoint.ChunkMetadata,
	chunkReturnCh chan *checkpoint.ChunkMetadata,
	errorCh chan int,
) error {
	api := storageApi.NewStorageClient(conn.ClientConn)
	for {
		var chunk *checkpoint.ChunkMetadata
		var ok bool
		select {
		case <-ctx.Done():
			return backoff.Permanent(ctx.Err())
		case chunk, ok = <-chunkDispatchCh:
			if !ok {
				return nil
			}
		}

		restoreCh := make(chan *restoreResult)
		rd, wr := io.Pipe()
		go func() {
			done, err := n.localStorage.Checkpointer().RestoreChunk(ctx, chunk.Index, rd)
			restoreCh <- &restoreResult{
				done: done,
				err:  err,
			}
		}()
		err := api.GetCheckpointChunk(ctx, chunk, wr)
		wr.Close()
		result := <-restoreCh

		// GetCheckpointChunk errors.
		// The chunk probably always needs to be returned here
		// (otherwise there's a deadlock risk with one worker's backoff just aborting
		// and another worker then blocking on its chunk).
		switch {
		case err == nil:
			// Fall out of the switch.
		case err != nil:
			n.logger.Error("can't fetch chunk from storage node", "node", conn.Node.ID, "chunk", chunk.Index, "err", err)
			chunkReturnCh <- chunk
			fallthrough
		case errors.Is(err, checkpoint.ErrChunkNotFound):
			return backoff.Permanent(err)
		default:
			return err
		}

		// RestoreChunk errors.
		switch {
		case result.done:
			// Signal to the toplevel handler that we're done.
			chunkReturnCh <- nil
			return nil
		case result.err != nil:
			n.logger.Error("chunk restoration failed",
				"node", conn.Node.ID,
				"chunk", chunk.Index,
				"root", chunk.Root,
				"err", result.err,
			)
			switch {
			case errors.Is(result.err, checkpoint.ErrChunkCorrupted):
				chunkReturnCh <- chunk
				return result.err
			case errors.Is(result.err, checkpoint.ErrChunkProofVerificationFailed):
				errorCh <- checkpointStatusNext
				return backoff.Permanent(result.err)
			default:
				errorCh <- checkpointStatusBail
				return backoff.Permanent(result.err)
			}
		}
	}
}

func (n *Node) handleCheckpoint(check *checkpoint.Metadata, nodesClient grpc.NodesClient, groupSize uint16) (cpStatus int, rerr error) {
	if err := n.localStorage.Checkpointer().StartRestore(n.ctx, check); err != nil {
		// Any previous restores were already aborted by the driver up the call stack, so
		// things should have been going smoothly here; bail.
		return checkpointStatusBail, fmt.Errorf("can't start checkpoint restore: %w", err)
	}
	// This defer has to be here so that we're sure no workers are running anymore during
	// any potential aborts.
	defer func() {
		if cpStatus == checkpointStatusDone {
			return
		}
		// Abort has to succeed even if we were interrupted by context cancellation.
		ctx := context.Background()
		if err := n.localStorage.Checkpointer().AbortRestore(ctx); err != nil {
			cpStatus = checkpointStatusBail
			n.logger.Error("error while aborting checkpoint restore on handler exit, aborting sync",
				"err", err,
			)
		}
	}()

	chunkDispatchCh := make(chan *checkpoint.ChunkMetadata)
	defer close(chunkDispatchCh)

	chunkReturnCh := make(chan *checkpoint.ChunkMetadata, groupSize)
	errorCh := make(chan int, groupSize)

	worker := func(ctx context.Context, conn *grpc.ConnWithNodeMeta) error {
		return n.nodeWorker(ctx, conn, chunkDispatchCh, chunkReturnCh, errorCh)
	}

	cancel, doneCh, err := n.goWithNodes(nodesClient, worker)
	if err != nil {
		return checkpointStatusBail, fmt.Errorf("can't fetch chunks from committee nodes: %w", err)
	}
	// Cancel on exit and wait for the worker pool to drain so that the abort
	// above can proceed safely.
	defer func() {
		cancel()
		<-doneCh
	}()

	// Prepare the heap of chunks.
	chunks := &chunkHeap{
		array:  make([]*checkpoint.ChunkMetadata, len(check.Chunks)),
		length: 0,
	}
	heap.Init(chunks)

	for i, c := range check.Chunks {
		heap.Push(chunks, &checkpoint.ChunkMetadata{
			Version: 1,
			Index:   uint64(i),
			Digest:  c,
			Root:    check.Root,
		})
	}
	n.logger.Debug("checkpoint chunks prepared for dispatch",
		"chunks", len(check.Chunks),
		"checkpoint_root", check.Root,
	)

	// Feed the workers with chunks.
	var next *checkpoint.ChunkMetadata
	var outChan chan *checkpoint.ChunkMetadata

	for {
		if chunks.length == 0 {
			next = nil
			outChan = nil
		} else {
			next = heap.Pop(chunks).(*checkpoint.ChunkMetadata)
			outChan = chunkDispatchCh
		}

		select {
		case <-n.ctx.Done():
			return checkpointStatusBail, n.ctx.Err()

		case returned := <-chunkReturnCh:
			if returned == nil {
				// Restoration completed, no more chunks.
				return checkpointStatusDone, nil
			}
			heap.Push(chunks, returned)

		case status := <-errorCh:
			return status, nil

		// If there's no chunk to send, outChan will be nil here, blocking forever. We still need to wait
		// for other events even if there's no chunk to dispatch, since they may simply all be in processing.
		case outChan <- next:
			next = nil

		case <-doneCh:
			// No usable committee connections left, move on to the next checkpoint.
			return checkpointStatusNext, storageClient.ErrStorageNotAvailable
		}

		if next != nil {
			heap.Push(chunks, next)
		}
	}
}

func (n *Node) getCheckpointList(nodesClient grpc.NodesClient) ([]*checkpoint.Metadata, error) {
	// Get checkpoint list from all current committee members.
	listCh := make(chan []*checkpoint.Metadata)
	req := &checkpoint.GetCheckpointsRequest{
		Version:   1,
		Namespace: n.commonNode.Runtime.ID(),
	}
	getter := func(ctx context.Context, conn *grpc.ConnWithNodeMeta) error {
		api := storageApi.NewStorageClient(conn.ClientConn)
		meta, err := api.GetCheckpoints(ctx, req)
		if err != nil {
			n.logger.Error("error calling GetCheckpoints",
				"err", err,
				"node", conn.Node.ID,
				"this_node", n.commonNode.Identity.NodeSigner.Public,
			)
			return err
		}
		n.logger.Debug("got checkpoint list from a node",
			"length", len(meta),
			"node", conn.Node.ID,
		)
		listCh <- meta
		return nil
	}

	cancel, doneCh, err := n.goWithNodes(nodesClient, getter)
	if err != nil {
		return nil, err
	}
	defer cancel()

	var list []*checkpoint.Metadata
resultLoop:
	for {
		select {
		case <-doneCh:
			break resultLoop
		case <-n.ctx.Done():
			return nil, n.ctx.Err()
		case meta := <-listCh:
			list = append(list, meta...)
		}
	}

	// Prepare the list: sort and deduplicate.
	sort.Slice(list, func(i, j int) bool {
		// Descending!
		if list[j].Root.Version == list[i].Root.Version {
			return bytes.Compare(list[j].Root.Hash[:], list[i].Root.Hash[:]) < 0
		}
		return list[j].Root.Version < list[i].Root.Version
	})
	retList := make([]*checkpoint.Metadata, len(list))
	var prevCheckpoint *checkpoint.Metadata
	cursor := 0
	for i := 0; i < len(list); i++ {
		if prevCheckpoint == nil || !list[i].Root.Equal(&prevCheckpoint.Root) {
			retList[cursor] = list[i]
			cursor++
		}
	}

	return retList[:cursor], nil
}

func (n *Node) checkCheckpointUsable(cp *checkpoint.Metadata, remainingMask outstandingMask) bool {
	namespace := n.commonNode.Runtime.ID()
	if !namespace.Equal(&cp.Root.Namespace) {
		// Not for the right runtime.
		return false
	}
	annBlk, err := n.commonNode.Runtime.History().GetBlock(n.ctx, cp.Root.Version)
	if err != nil {
		n.logger.Error("can't get block information for checkpoint, skipping", "err", err, "root", cp.Root)
		return false
	}
	blk := annBlk.Block

	_, lastIORoot, lastStateRoot := n.GetLastSynced()
	lastVersions := map[storageApi.RootType]uint64{
		storageApi.RootTypeIO:    lastIORoot.Version,
		storageApi.RootTypeState: lastStateRoot.Version,
	}
	if namespace.Equal(&blk.Header.Namespace) {
		for _, root := range blk.Header.StorageRoots() {
			if cp.Root.Type == root.Type && root.Hash.Equal(&cp.Root.Hash) {
				// Do we already have this root?
				if lastVersions[cp.Root.Type] < cp.Root.Version && remainingMask.contains(cp.Root.Type) {
					return true
				}
				return false
			}
		}
	}
	n.logger.Info("checkpoint for unknown root skipped", "root", cp.Root)
	return false
}

func (n *Node) syncCheckpoints(genesisRound uint64) (*blockSummary, error) {
	// Store roots and round info for checkpoints that finished syncing.
	// Round and namespace info will get overwritten as rounds are skipped
	// for errors, driven by remainingRoots.
	var syncState blockSummary

	// Try getting the active descriptor first.
	descriptor, err := n.commonNode.Runtime.ActiveDescriptor(n.ctx)
	if err != nil {
		return nil, fmt.Errorf("can't get runtime descriptor: %w", err)
	}

	// Fetch metadata from the current committee.
	metadata, err := n.getCheckpointList(n.storageNodesGrpc)
	if err != nil {
		return nil, fmt.Errorf("can't get checkpoint list from storage committee: %w", err)
	}

	// Try all the checkpoints now, from most recent backwards.
	var (
		prevVersion      uint64 = ^uint64(0)
		multipartRunning bool
		mask             outstandingMask
	)
	remainingRoots := outstandingMaskFull

	defer func() {
		if !multipartRunning {
			return
		}
		if err := n.localStorage.NodeDB().AbortMultipartInsert(); err != nil {
			n.logger.Error("error aborting multipart restore on exit from syncer",
				"err", err,
			)
		}
	}()

	for _, check := range metadata {
		if check.Root.Version < genesisRound || !n.checkCheckpointUsable(check, remainingRoots) {
			continue
		}

		if check.Root.Version != prevVersion {
			// Starting a new round, so we need to clean up all state from
			// previous retores. Aborting multipart works with no multipart in
			// progress too.
			multipartRunning = false
			if err := n.localStorage.NodeDB().AbortMultipartInsert(); err != nil {
				return nil, fmt.Errorf("error aborting previous multipart restore: %w", err)
			}
			if err := n.localStorage.NodeDB().StartMultipartInsert(check.Root.Version); err != nil {
				return nil, fmt.Errorf("error starting multipart insert for round %d: %w", check.Root.Version, err)
			}
			multipartRunning = true
			remainingRoots = outstandingMaskFull
			prevVersion = check.Root.Version
			syncState.Roots = nil
		}

		status, err := n.handleCheckpoint(check, n.storageNodesGrpc, descriptor.Storage.GroupSize)
		switch status {
		case checkpointStatusDone:
			n.logger.Info("successfully restored from checkpoint", "root", check.Root, "mask", mask)

			syncState.Namespace = check.Root.Namespace
			syncState.Round = check.Root.Version
			syncState.Roots = append(syncState.Roots, check.Root)
			remainingRoots.remove(check.Root.Type)
			if remainingRoots.isEmpty() {
				if err = n.localStorage.NodeDB().Finalize(n.ctx, syncState.Roots); err != nil {
					n.logger.Error("can't finalize version after all checkpoints restored",
						"err", err,
						"version", prevVersion,
						"roots", syncState.Roots,
					)
					// Likely a local problem, so just bail.
					return nil, fmt.Errorf("can't finalize version after checkpoints restored: %w", err)
				}
				multipartRunning = false
				return &syncState, nil
			}
			continue
		case checkpointStatusNext:
			n.logger.Info("error trying to restore from checkpoint, trying next most recent", "root", check.Root, "err", err)
			continue
		case checkpointStatusBail:
			n.logger.Error("error trying to restore from checkpoint, unrecoverable", "root", check.Root, "err", err)
			return nil, fmt.Errorf("error restoring from checkpoints: %w", err)
		}
	}

	return nil, ErrNoUsableCheckpoints
}
