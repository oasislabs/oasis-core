package committee

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/eapache/channels"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/oasisprotocol/oasis-core/go/common/cache/lru"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crash"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	"github.com/oasisprotocol/oasis-core/go/common/pubsub"
	"github.com/oasisprotocol/oasis-core/go/common/tracing"
	"github.com/oasisprotocol/oasis-core/go/common/version"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/block"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/commitment"
	"github.com/oasisprotocol/oasis-core/go/runtime/host"
	"github.com/oasisprotocol/oasis-core/go/runtime/host/protocol"
	"github.com/oasisprotocol/oasis-core/go/runtime/nodes"
	runtimeRegistry "github.com/oasisprotocol/oasis-core/go/runtime/registry"
	"github.com/oasisprotocol/oasis-core/go/runtime/scheduling"
	schedulingAPI "github.com/oasisprotocol/oasis-core/go/runtime/scheduling/api"
	"github.com/oasisprotocol/oasis-core/go/runtime/transaction"
	scheduler "github.com/oasisprotocol/oasis-core/go/scheduler/api"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
	storage "github.com/oasisprotocol/oasis-core/go/storage/api"
	commonWorker "github.com/oasisprotocol/oasis-core/go/worker/common"
	"github.com/oasisprotocol/oasis-core/go/worker/common/committee"
	"github.com/oasisprotocol/oasis-core/go/worker/common/p2p"
	p2pError "github.com/oasisprotocol/oasis-core/go/worker/common/p2p/error"
	"github.com/oasisprotocol/oasis-core/go/worker/registration"
)

var (
	errSeenNewerBlock     = fmt.Errorf("executor: seen newer block")
	errRuntimeAborted     = fmt.Errorf("executor: runtime aborted batch processing")
	errIncompatibleHeader = p2pError.Permanent(fmt.Errorf("executor: incompatible header"))
	errInvalidReceipt     = p2pError.Permanent(fmt.Errorf("executor: invalid storage receipt"))
	errIncorrectRole      = fmt.Errorf("executor: incorrect role")
	errIncorrectState     = fmt.Errorf("executor: incorrect state")
	errMsgFromNonTxnSched = fmt.Errorf("executor: received txn scheduler dispatch msg from non-txn scheduler")

	// Transaction scheduling errors.
	errNoBlocks        = fmt.Errorf("executor: no blocks")
	errNotReady        = fmt.Errorf("executor: runtime not ready")
	errNotTxnScheduler = fmt.Errorf("executor: not transaction scheduler in this round")
	errDuplicateTx     = p2pError.Permanent(p2pError.Relayable(fmt.Errorf("executor: duplicate transaction")))

	// Duration to wait before submitting the propose timeout request.
	proposeTimeoutDelay = 2 * time.Second
)

var (
	discrepancyDetectedCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "oasis_worker_execution_discrepancy_detected_count",
			Help: "Number of detected execute discrepancies.",
		},
		[]string{"runtime"},
	)
	abortedBatchCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "oasis_worker_aborted_batch_count",
			Help: "Number of aborted batches.",
		},
		[]string{"runtime"},
	)
	storageCommitLatency = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "oasis_worker_storage_commit_latency",
			Help: "Latency of storage commit calls (state + outputs) (seconds).",
		},
		[]string{"runtime"},
	)
	batchReadTime = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "oasis_worker_batch_read_time",
			Help: "Time it takes to read a batch from storage (seconds).",
		},
		[]string{"runtime"},
	)
	batchProcessingTime = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "oasis_worker_batch_processing_time",
			Help: "Time it takes for a batch to finalize (seconds).",
		},
		[]string{"runtime"},
	)
	batchRuntimeProcessingTime = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "oasis_worker_batch_runtime_processing_time",
			Help: "Time it takes for a batch to be processed by the runtime (seconds).",
		},
		[]string{"runtime"},
	)
	batchSize = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "oasis_worker_batch_size",
			Help: "Number of transactions in a batch.",
		},
		[]string{"runtime"},
	)
	incomingQueueSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "oasis_worker_incoming_queue_size",
			Help: "Size of the incoming queue (number of entries).",
		},
		[]string{"runtime"},
	)
	nodeCollectors = []prometheus.Collector{
		discrepancyDetectedCount,
		abortedBatchCount,
		storageCommitLatency,
		batchReadTime,
		batchProcessingTime,
		batchRuntimeProcessingTime,
		batchSize,
		incomingQueueSize,
	}

	metricsOnce sync.Once
)

// Node is a committee node.
type Node struct { // nolint: maligned
	*runtimeRegistry.RuntimeHostNode

	runtimeVersion version.Version

	lastScheduledCache    *lru.Cache
	scheduleMaxTxPoolSize uint64
	scheduleCh            *channels.RingChannel

	// The scheduler mutex is here to protect the initialization
	// of the scheduler variable. After initialization the variable
	// will never change though -- so if the variable is non-nil
	// (which must be checked while holding the read lock) it can
	// safely be used without holding the lock.
	schedulerMutex sync.RWMutex
	scheduler      schedulingAPI.Scheduler

	// Guarded by .commonNode.CrossNode.
	proposingTimeout bool
	prevEpochWorker  bool

	commonNode   *committee.Node
	commonCfg    commonWorker.Config
	roleProvider registration.RoleProvider

	storage storage.Backend

	ctx       context.Context
	cancelCtx context.CancelFunc
	stopCh    chan struct{}
	stopOnce  sync.Once
	quitCh    chan struct{}
	initCh    chan struct{}

	// Mutable and shared with common node's worker.
	// Guarded by .commonNode.CrossNode.
	state NodeState
	// Context valid until the next round.
	// Guarded by .commonNode.CrossNode.
	roundCtx       context.Context
	roundCancelCtx context.CancelFunc

	stateTransitions *pubsub.Broker
	// Bump this when we need to change what the worker selects over.
	reselect chan struct{}

	logger *logging.Logger
}

// Name returns the service name.
func (n *Node) Name() string {
	return "committee node"
}

// Start starts the service.
func (n *Node) Start() error {
	go n.worker()
	return nil
}

// Stop halts the service.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}

// Quit returns a channel that will be closed when the service terminates.
func (n *Node) Quit() <-chan struct{} {
	return n.quitCh
}

// Cleanup performs the service specific post-termination cleanup.
func (n *Node) Cleanup() {
}

// Initialized returns a channel that will be closed when the node is
// initialized and ready to service requests.
func (n *Node) Initialized() <-chan struct{} {
	return n.initCh
}

// WatchStateTransitions subscribes to the node's state transitions.
func (n *Node) WatchStateTransitions() (<-chan NodeState, *pubsub.Subscription) {
	sub := n.stateTransitions.Subscribe()
	ch := make(chan NodeState)
	sub.Unwrap(ch)

	return ch, sub
}

func (n *Node) getMetricLabels() prometheus.Labels {
	return prometheus.Labels{
		"runtime": n.commonNode.Runtime.ID().String(),
	}
}

// Assumes scheduler is initialized.
func (n *Node) clearQueuedTxs() {
	n.scheduler.Clear()
	if n.lastScheduledCache != nil {
		n.lastScheduledCache.Clear()
	}
	incomingQueueSize.With(n.getMetricLabels()).Set(0)
}

// HandlePeerMessage implements NodeHooks.
func (n *Node) HandlePeerMessage(ctx context.Context, message *p2p.Message, isOwn bool) (bool, error) {
	n.logger.Debug("received peer message", "message", message, "is_own", isOwn)

	switch {
	case message.Tx != nil:
		rawTx := message.Tx.Data

		// Note: if an epoch transition is just about to happen we can be out of
		// the committee by the time we queue the transaction, but this is fine
		// as scheduling is aware of this.
		if es := n.commonNode.Group.GetEpochSnapshot(); !es.IsExecutorWorker() {
			n.logger.Debug("unable to handle transaction message, not execution worker",
				"current_epoch", es.GetEpochNumber(),
			)
			return true, nil
		}

		// Check transaction before queuing it.
		tx, err := n.checkTx(ctx, rawTx)
		if err != nil {
			return true, err
		}

		n.logger.Debug("queuing transaction",
			"transaction", tx,
		)
		err = n.QueueTx(tx)
		if err != nil {
			n.logger.Error("unable to queue transaction",
				"tx", tx,
				"err", err,
			)
			return true, err
		}
		return true, nil

	case message.ProposedBatch != nil:
		// Ignore own messages as those are handled via handleInternalBatchLocked.
		if isOwn {
			return true, nil
		}
		crash.Here(crashPointBatchReceiveAfter)

		sbd := message.ProposedBatch

		epoch := n.commonNode.Group.GetEpochSnapshot()
		n.commonNode.CrossNode.Lock()
		round := n.commonNode.CurrentBlock.Header.Round
		n.commonNode.CrossNode.Unlock()

		// Before opening the signed dispatch message, verify that it was
		// actually signed by the current transaction scheduler.
		if err := epoch.VerifyTxnSchedulerSigner(sbd.Signature, round); err != nil {
			// Not signed by a current txn scheduler!
			return false, errMsgFromNonTxnSched
		}

		// Transaction scheduler checks out, open the signed dispatch message
		// and add it to the processing queue.
		var bd commitment.ProposedBatch
		if err := sbd.Open(&bd, n.commonNode.Runtime.ID()); err != nil {
			return false, p2pError.Permanent(err)
		}

		err := n.queueBatchBlocking(ctx, bd.IORoot, bd.StorageSignatures, bd.Header, sbd.Signature)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (n *Node) queueBatchBlocking(
	ctx context.Context,
	ioRootHash hash.Hash,
	storageSignatures []signature.Signature,
	hdr block.Header,
	txnSchedSig signature.Signature,
) error {
	// Quick check to see if header is compatible.
	rtID := n.commonNode.Runtime.ID()
	if !bytes.Equal(hdr.Namespace[:], rtID[:]) {
		n.logger.Warn("received incompatible header in external batch",
			"header", hdr,
		)
		return errIncompatibleHeader
	}

	// Verify storage receipt signatures.
	epoch := n.commonNode.Group.GetEpochSnapshot()
	if err := epoch.VerifyCommitteeSignatures(scheduler.KindStorage, storageSignatures); err != nil {
		n.logger.Warn("received bad storage signature",
			"err", err,
		)
		return errInvalidReceipt
	}
	// Make sure there are enough signatures.
	rt, err := n.commonNode.Runtime.ActiveDescriptor(ctx)
	if err != nil {
		n.logger.Warn("failed to fetch active runtime descriptor",
			"err", err,
		)
		return p2pError.Permanent(err)
	}
	if len(storageSignatures) < int(rt.Storage.MinWriteReplication) {
		n.logger.Warn("received external batch with not enough storage receipts",
			"min_write_replication", rt.Storage.MinWriteReplication,
			"num_receipts", len(storageSignatures),
		)
		return errInvalidReceipt
	}

	receiptBody := storage.ReceiptBody{
		Version:   1,
		Namespace: hdr.Namespace,
		Round:     hdr.Round + 1,
		RootTypes: []storage.RootType{storage.RootTypeIO},
		Roots:     []hash.Hash{ioRootHash},
	}
	if !signature.VerifyManyToOne(storage.ReceiptSignatureContext, cbor.Marshal(receiptBody), storageSignatures) {
		n.logger.Warn("received invalid storage receipt signature in external batch")
		return errInvalidReceipt
	}

	// Defer fetching inputs from storage to when we actually start processing a batch.
	batch := &unresolvedBatch{
		ioRoot: storage.Root{
			Namespace: hdr.Namespace,
			Version:   hdr.Round + 1,
			Type:      storage.RootTypeIO,
			Hash:      ioRootHash,
		},
		txnSchedSignature: txnSchedSig,
		storageSignatures: storageSignatures,
		maxBatchSize:      rt.TxnScheduler.MaxBatchSize,
		maxBatchSizeBytes: rt.TxnScheduler.MaxBatchSizeBytes,
	}
	if batchSpan := opentracing.SpanFromContext(ctx); batchSpan != nil {
		batch.spanCtx = batchSpan.Context()
	}

	n.commonNode.CrossNode.Lock()
	defer n.commonNode.CrossNode.Unlock()
	return n.handleExternalBatchLocked(batch, hdr)
}

// handleInternalBatchLocked processes a batch from the internal transaction scheduler.
// Guarded by n.commonNode.CrossNode.
func (n *Node) handleInternalBatchLocked(
	batchSpanCtx opentracing.SpanContext,
	ioRoot hash.Hash,
	batch transaction.RawBatch,
	txnSchedSig signature.Signature,
	inputStorageSigs []signature.Signature,
) {
	n.maybeStartProcessingBatchLocked(
		&unresolvedBatch{
			ioRoot: storage.Root{
				Namespace: n.commonNode.CurrentBlock.Header.Namespace,
				Version:   n.commonNode.CurrentBlock.Header.Round + 1,
				Type:      storage.RootTypeIO,
				Hash:      ioRoot,
			},
			txnSchedSignature: txnSchedSig,
			storageSignatures: inputStorageSigs,
			batch:             batch,
			spanCtx:           batchSpanCtx,
		},
	)
}

func (n *Node) bumpReselect() {
	select {
	case n.reselect <- struct{}{}:
	default:
		// If there's one already queued, we don't need to do anything.
	}
}

// Guarded by n.commonNode.CrossNode.
func (n *Node) transitionLocked(state NodeState) {
	n.logger.Info("state transition",
		"current_state", n.state,
		"new_state", state,
	)

	// Validate state transition.
	dests := validStateTransitions[n.state.Name()]

	var valid bool
	for _, dest := range dests[:] {
		if dest == state.Name() {
			valid = true
			break
		}
	}

	if !valid {
		panic(fmt.Sprintf("invalid state transition: %s -> %s", n.state, state))
	}

	n.state = state
	n.stateTransitions.Broadcast(state)
	// Restart our worker's select in case our state-specific channels have changed.
	n.bumpReselect()
}

// HandleEpochTransitionLocked implements NodeHooks.
// Guarded by n.commonNode.CrossNode.
func (n *Node) HandleEpochTransitionLocked(epoch *committee.EpochSnapshot) {
	n.schedulerMutex.RLock()
	defer n.schedulerMutex.RUnlock()
	if n.scheduler == nil {
		n.logger.Error("scheduling algorithm not available yet")
		return
	}

	switch {
	case epoch.IsExecutorWorker():
		if !n.prevEpochWorker {
			// Clear incoming queue and cache of any stale transactions in case
			// we were not part of the compute committee in previous epoch.
			n.clearQueuedTxs()
		}
		fallthrough
	case epoch.IsExecutorBackupWorker():
		n.transitionLocked(StateWaitingForBatch{})
	default:
		n.transitionLocked(StateNotReady{})
	}
	n.prevEpochWorker = epoch.IsExecutorWorker()
}

// HandleNewBlockEarlyLocked implements NodeHooks.
// Guarded by n.commonNode.CrossNode.
func (n *Node) HandleNewBlockEarlyLocked(blk *block.Block) {
	crash.Here(crashPointRoothashReceiveAfter)
	// If we have seen a new block while a batch was processing, we need to
	// abort it no matter what as any processed state may be invalid.
	n.abortBatchLocked(errSeenNewerBlock)
}

// HandleNewBlockLocked implements NodeHooks.
// Guarded by n.commonNode.CrossNode.
func (n *Node) HandleNewBlockLocked(blk *block.Block) {
	header := blk.Header

	// Cancel old round context, start a new one.
	if n.roundCancelCtx != nil {
		(n.roundCancelCtx)()
	}
	n.roundCtx, n.roundCancelCtx = context.WithCancel(n.ctx)

	// Perform actions based on current state.
	switch state := n.state.(type) {
	case StateWaitingForBlock:
		// Check if this was the block we were waiting for.
		if header.MostlyEqual(state.header) {
			n.logger.Info("received block needed for batch processing")
			n.maybeStartProcessingBatchLocked(state.batch)
			break
		}

		// Check if the new block is for the same or newer round than the
		// one we are waiting for. In this case, we should abort as the
		// block will never be seen.
		curRound := header.Round
		waitRound := state.header.Round
		if curRound >= waitRound {
			n.logger.Warn("seen newer block while waiting for block")
			n.transitionLocked(StateWaitingForBatch{})
			break
		}

		// Continue waiting for block.
		n.logger.Info("still waiting for block",
			"current_round", curRound,
			"wait_round", waitRound,
		)
	case StateWaitingForEvent:
		// Block finalized without the need for a backup worker.
		n.logger.Info("considering the round finalized",
			"round", header.Round,
			"header_hash", header.EncodedHash(),
		)
		n.transitionLocked(StateWaitingForBatch{})
	case StateWaitingForFinalize:
		func() {
			defer n.transitionLocked(StateWaitingForBatch{})

			// A new block means the round has been finalized.
			n.logger.Info("considering the round finalized",
				"round", header.Round,
				"header_hash", header.EncodedHash(),
				"header_type", header.HeaderType,
			)
			if header.HeaderType != block.Normal {
				return
			}
			if !header.IORoot.Equal(&state.proposedIORoot) {
				n.logger.Error("proposed batch was not finalized",
					"header_io_root", header.IORoot,
					"proposed_io_root", state.proposedIORoot,
					"header_type", header.HeaderType,
					"batch", state.raw,
				)
				return
			}

			// Record time taken for successfully processing a batch.
			batchProcessingTime.With(n.getMetricLabels()).Observe(time.Since(state.batchStartTime).Seconds())

			n.logger.Debug("removing processed batch from queue",
				"batch", state.raw,
				"io_root", header.IORoot,
			)
			// Removed processed transactions from queue.
			if err := n.removeTxBatch(state.raw); err != nil {
				n.logger.Warn("failed removing processed batch from queue",
					"err", err,
					"batch", state.raw,
				)
			}
		}()
	}

	// Clear the potentially set "is proposing timeout" flag from the previous round.
	n.proposingTimeout = false

	// Check if we are a proposer and if so try to immediately schedule a new batch.
	if n.commonNode.Group.GetEpochSnapshot().IsTransactionScheduler(blk.Header.Round) {
		n.logger.Info("we are a transaction scheduler",
			"round", blk.Header.Round,
		)

		n.scheduleCh.In() <- struct{}{}
	}
}

// checkTx requests the runtime to check the validity of the given transaction.
func (n *Node) checkTx(ctx context.Context, rawTx []byte) (*transaction.CheckedTransaction, error) {
	n.commonNode.CrossNode.Lock()
	currentBlock := n.commonNode.CurrentBlock
	currentConsensusBlock := n.commonNode.CurrentConsensusBlock
	currentEpoch := n.commonNode.Group.GetEpochSnapshot().GetEpochNumber()
	n.commonNode.CrossNode.Unlock()

	rt := n.GetHostedRuntime()
	if rt == nil {
		n.logger.Error("CheckTx: hosted runtime not initialized")
		return nil, errNotReady
	}

	tx, err := rt.CheckTx(ctx, currentBlock, currentConsensusBlock, currentEpoch, rawTx)
	switch {
	case err == nil:
	case errors.Is(err, host.ErrInvalidArgument):
		return nil, errNotReady
	case errors.Is(err, host.ErrInternal):
		return nil, err
	case errors.Is(err, host.ErrCheckTxFailed):
		return nil, p2pError.Permanent(err)
	default:
		return nil, err
	}
	return tx, nil
}

// QueueTx queues a runtime transaction for scheduling.
func (n *Node) QueueTx(tx *transaction.CheckedTransaction) error {
	n.schedulerMutex.RLock()
	defer n.schedulerMutex.RUnlock()

	if n.scheduler == nil {
		return errNotReady
	}

	// Check if transaction was recently scheduled.
	if n.lastScheduledCache != nil {
		if _, b := n.lastScheduledCache.Get(tx.Hash()); b {
			return errDuplicateTx
		}
	}

	if err := n.scheduler.QueueTx(tx); err != nil {
		return err
	}

	if n.lastScheduledCache != nil {
		if err := n.lastScheduledCache.Put(tx.Hash(), true); err != nil {
			// cache.Put can only error if capacity in bytes is used and the
			// inserted value is too large. This should never happen in here.
			n.logger.Error("cache put error",
				"err", err,
			)
		}
	}
	incomingQueueSize.With(n.getMetricLabels()).Set(float64(n.scheduler.UnscheduledSize()))

	// Notify event loop to attempt to schedule a batch.
	n.scheduleCh.In() <- struct{}{}

	return nil
}

// removeTxBatch removes a batch from scheduling queue.
func (n *Node) removeTxBatch(batch transaction.RawBatch) error {
	hashes := make([]hash.Hash, len(batch))
	for i, b := range batch {
		hashes[i] = hash.NewFromBytes(b)
	}
	// XXX: remove batch can only happen after a batch was already scheduled, meaning
	// the scheduler already exists and there is no need for the scheduler mutex.
	if err := n.scheduler.RemoveTxBatch(hashes); err != nil {
		return err
	}

	incomingQueueSize.With(n.getMetricLabels()).Set(float64(n.scheduler.UnscheduledSize()))

	return nil
}

func (n *Node) proposeTimeoutLocked(roundCtx context.Context) error {
	// Do not propose a timeout if we are already proposing it.
	// The flag will get cleared on the next round or if the propose timeout
	// tx fails.
	if n.proposingTimeout {
		return nil
	}

	if n.commonNode.CurrentBlock == nil {
		return fmt.Errorf("executor: propose timeout error, nil block")
	}
	rt, err := n.commonNode.Runtime.ActiveDescriptor(roundCtx)
	if err != nil {
		return err
	}
	proposerTimeout := rt.TxnScheduler.ProposerTimeout
	currentBlockHeight := n.commonNode.CurrentBlockHeight
	if n.commonNode.Height < currentBlockHeight+proposerTimeout {
		n.logger.Debug("executor: proposer timeout not reached yet",
			"height", n.commonNode.Height,
			"current_block_height", currentBlockHeight,
			"proposer_timeout", proposerTimeout,
		)
		return nil
	}

	n.logger.Debug("executor requesting proposer timeout",
		"height", n.commonNode.Height,
		"current_block_height", currentBlockHeight,
		"proposer_timeout", proposerTimeout,
	)
	n.proposingTimeout = true
	tx := roothash.NewRequestProposerTimeoutTx(0, nil, n.commonNode.Runtime.ID(), n.commonNode.CurrentBlock.Header.Round)
	go func() {
		// Wait a bit before actually proposing a timeout, to give the current
		// scheduler some time to propose a batch in case it just received it.
		//
		// This prevents triggering a timeout when there is a long period
		// of no transactions, as without this artificial delay, the non
		// scheduler nodes would be faster in proposing a timeout than the
		// scheduler node proposing a batch.
		select {
		case <-time.After(proposeTimeoutDelay):
		case <-roundCtx.Done():
			return
		}

		err := consensus.SignAndSubmitTx(roundCtx, n.commonNode.Consensus, n.commonNode.Identity.NodeSigner, tx)
		switch err {
		case nil:
			n.logger.Info("executor timeout request finalized",
				"height", n.commonNode.Height,
				"current_block_height", currentBlockHeight,
				"proposer_timeout", proposerTimeout,
			)
		default:
			n.logger.Error("failed to submit executor timeout request",
				"height", n.commonNode.Height,
				"current_block_height", currentBlockHeight,
				"err", err,
			)
			n.commonNode.CrossNode.Lock()
			n.proposingTimeout = false
			n.commonNode.CrossNode.Unlock()
		}
	}()

	return nil
}

func (n *Node) getRuntimeRoundData(ctx context.Context, height int64, currentRound uint64) (*roothash.RuntimeState, *roothash.RoundResults, []*staking.Event, error) {
	state, err := n.commonNode.Consensus.RootHash().GetRuntimeState(ctx, n.commonNode.Runtime.ID(), height)
	if err != nil {
		n.logger.Error("failed to query runtime state",
			"err", err,
			"height", height,
		)
		return nil, nil, nil, err
	}
	roundResults, err := n.commonNode.Runtime.History().GetRoundResults(ctx, state.LastNormalRound)
	if err != nil {
		n.logger.Error("failed to query round last normal round results",
			"err", err,
			"height", height,
		)
		return nil, nil, nil, err
	}

	// Load all events since last successful round.
	var stakingEvents []*staking.Event
	for r := state.LastNormalRound; r < currentRound; r++ {
		events, err := n.commonNode.Runtime.History().GetRoundEvents(ctx, r)
		if err != nil {
			n.logger.Error("failed to query round events",
				"err", err,
				"height", height,
			)
			return nil, nil, nil, err
		}
		stakingEvents = append(stakingEvents, events...)
	}
	n.logger.Debug("round staking events", "num_events", len(stakingEvents), "events", stakingEvents)

	return state, roundResults, stakingEvents, nil
}

func (n *Node) handleScheduleBatch(force bool) {
	roundCtx, epoch, lastHeader, roundResults, err := func() (context.Context, *committee.EpochSnapshot, *block.Header, *roothash.RoundResults, error) {
		n.commonNode.CrossNode.Lock()
		defer n.commonNode.CrossNode.Unlock()
		roundCtx := n.roundCtx

		// If we are not waiting for a batch, don't do anything.
		if _, ok := n.state.(StateWaitingForBatch); !ok {
			return roundCtx, nil, nil, nil, errIncorrectState
		}
		if n.commonNode.CurrentBlock == nil {
			return roundCtx, nil, nil, nil, errNoBlocks
		}
		header := n.commonNode.CurrentBlock.Header
		epoch := n.commonNode.Group.GetEpochSnapshot()

		// If we are not an executor worker in this epoch, we don't need to do anything.
		if !epoch.IsExecutorWorker() {
			return roundCtx, nil, nil, nil, errNotTxnScheduler
		}

		_, roundResults, _, err := n.getRuntimeRoundData(roundCtx, n.commonNode.CurrentBlockHeight, n.commonNode.CurrentBlock.Header.Round)
		if err != nil {
			return roundCtx, nil, nil, nil, err
		}
		return roundCtx, epoch, &header, roundResults, nil
	}()
	if err != nil {
		n.logger.Debug("not scheduling a batch",
			"err", err,
		)
		return
	}

	// Ask the scheduler to get us a scheduled batch.
	batch := n.scheduler.GetBatch(force)
	if len(batch) == 0 && (!force || len(roundResults.Messages) == 0) {
		return
	}

	// If we are an executor and not a scheduler try proposing a timeout.
	if !epoch.IsTransactionScheduler(lastHeader.Round) {
		n.logger.Debug("proposing a timeout",
			"round", lastHeader.Round,
			"batch", batch,
			"round_results", roundResults,
		)

		err = func() error {
			n.commonNode.CrossNode.Lock()
			defer n.commonNode.CrossNode.Unlock()

			// Make sure we are still in the right state/round.
			if _, ok := n.state.(StateWaitingForBatch); !ok || lastHeader.Round != n.commonNode.CurrentBlock.Header.Round {
				return errIncorrectState
			}
			return n.proposeTimeoutLocked(roundCtx)
		}()
		switch err {
		case nil:
		case errIncorrectState:
			return
		default:
			n.logger.Error("error proposing a timeout",
				"err", err,
			)
		}

		// If we are not a transaction scheduler, we can't really schedule.
		n.logger.Debug("not scheduling a batch as we are not a transaction scheduler")
		return
	}

	n.logger.Debug("scheduling a batch",
		"batch_size", batch,
		"round_results", roundResults,
	)

	// Scheduler node opens a new parent span for batch processing.
	batchSpan := opentracing.StartSpan("ScheduleBatch(batch)")
	defer batchSpan.Finish()
	batchSpanCtx := batchSpan.Context()

	// Generate the initial I/O root containing only the inputs (outputs and
	// tags will be added later by the executor nodes).
	emptyRoot := storage.Root{
		Namespace: lastHeader.Namespace,
		Version:   lastHeader.Round + 1,
		Type:      storage.RootTypeIO,
	}
	emptyRoot.Hash.Empty()

	ioTree := transaction.NewTree(nil, emptyRoot)
	defer ioTree.Close()

	rawBatch := make(transaction.RawBatch, len(batch))
	for idx, tx := range batch {
		if err = ioTree.AddTransaction(roundCtx, transaction.Transaction{Input: tx.Raw(), BatchOrder: uint32(idx)}, nil); err != nil {
			n.logger.Error("failed to create I/O tree",
				"err", err,
			)
			return
		}
		rawBatch[idx] = tx.Raw()
	}

	ioWriteLog, ioRoot, err := ioTree.Commit(roundCtx)
	if err != nil {
		n.logger.Error("failed to create I/O tree",
			"err", err,
		)
		return
	}

	// Commit I/O tree to storage and obtain receipts.
	spanInsert, roundCtx := tracing.StartSpanWithContext(roundCtx, "Apply(ioWriteLog)",
		opentracing.ChildOf(batchSpanCtx),
	)

	ioReceipts, err := n.storage.Apply(roundCtx, &storage.ApplyRequest{
		Namespace: lastHeader.Namespace,
		RootType:  storage.RootTypeIO,
		SrcRound:  lastHeader.Round + 1,
		SrcRoot:   emptyRoot.Hash,
		DstRound:  lastHeader.Round + 1,
		DstRoot:   ioRoot,
		WriteLog:  ioWriteLog,
	})
	if err != nil {
		spanInsert.Finish()
		n.logger.Error("failed to commit I/O tree to storage",
			"err", err,
		)
		return
	}
	spanInsert.Finish()

	// Dispatch batch to group.
	spanPublish := opentracing.StartSpan("PublishScheduledBatch(batchHash, header)",
		opentracing.Tag{Key: "ioRoot", Value: ioRoot},
		opentracing.Tag{Key: "header", Value: lastHeader},
		opentracing.ChildOf(batchSpanCtx),
	)
	ioReceiptSignatures := []signature.Signature{}
	for _, receipt := range ioReceipts {
		ioReceiptSignatures = append(ioReceiptSignatures, receipt.Signature)
	}

	dispatchMsg := &commitment.ProposedBatch{
		IORoot:            ioRoot,
		StorageSignatures: ioReceiptSignatures,
		Header:            *lastHeader,
	}
	signedDispatchMsg, err := commitment.SignProposedBatch(n.commonNode.Identity.NodeSigner, n.commonNode.Runtime.ID(), dispatchMsg)
	if err != nil {
		n.logger.Error("failed to sign txn scheduler batch",
			"err", err,
		)
		return
	}

	n.commonNode.CrossNode.Lock()
	defer n.commonNode.CrossNode.Unlock()

	// Make sure we are still in the right state/round.
	if _, ok := n.state.(StateWaitingForBatch); !ok || lastHeader.Round != n.commonNode.CurrentBlock.Header.Round {
		n.logger.Error("new state or round since started the dispatch",
			"state", n.state,
			"expected_round", lastHeader.Round,
			"round", n.commonNode.CurrentBlock.Header.Round,
		)
		return
	}

	n.logger.Debug("dispatching a new batch proposal",
		"io_root", ioRoot,
		"num_txs", len(batch),
	)

	err = n.commonNode.Group.Publish(
		batchSpanCtx,
		&p2p.Message{
			ProposedBatch: signedDispatchMsg,
		},
	)
	if err != nil {
		spanPublish.Finish()
		n.logger.Error("failed to publish batch to committee",
			"err", err,
		)
		return
	}
	crash.Here(crashPointBatchPublishAfter)
	spanPublish.Finish()

	// Also process the batch locally.
	n.handleInternalBatchLocked(
		batchSpanCtx,
		ioRoot,
		rawBatch,
		signedDispatchMsg.Signature,
		ioReceiptSignatures,
	)
}

// Guarded by n.commonNode.CrossNode.
func (n *Node) maybeStartProcessingBatchLocked(batch *unresolvedBatch) {
	epoch := n.commonNode.Group.GetEpochSnapshot()

	switch {
	case epoch.IsExecutorWorker():
		// Worker, start processing immediately.
		n.startProcessingBatchLocked(batch)
	case epoch.IsExecutorBackupWorker():
		// Backup worker, wait for discrepancy event.
		state, ok := n.state.(StateWaitingForBatch)
		if ok && state.pendingEvent != nil {
			// We have already received a discrepancy event, start processing immediately.
			n.logger.Info("already received a discrepancy event, start processing batch")
			n.startProcessingBatchLocked(batch)
			return
		}

		n.transitionLocked(StateWaitingForEvent{batch: batch})
	default:
		// Currently not a member of an executor committee, log.
		n.logger.Warn("not an executor committee member, ignoring batch")
	}
}

// Guarded by n.commonNode.CrossNode.
func (n *Node) startProcessingBatchLocked(batch *unresolvedBatch) {
	if n.commonNode.CurrentBlock == nil {
		panic("attempted to start processing batch with a nil block")
	}

	n.logger.Debug("processing batch",
		"batch", batch,
	)

	// Create batch processing context and channel for receiving the response.
	ctx, cancel := context.WithCancel(n.roundCtx)
	done := make(chan *processedBatch, 1)

	batchStartTime := time.Now()
	n.transitionLocked(StateProcessingBatch{batch, batchStartTime, cancel, done})

	rt := n.GetHostedRuntime()
	if rt == nil {
		// This should not happen as we only register to be an executor worker
		// once the hosted runtime is ready.
		n.logger.Error("received a batch while hosted runtime is not yet initialized")
		n.abortBatchLocked(errRuntimeAborted)
		return
	}

	// Request the worker host to process a batch. This is done in a separate
	// goroutine so that the committee node can continue processing blocks.
	blk := n.commonNode.CurrentBlock
	consensusBlk := n.commonNode.CurrentConsensusBlock
	height := n.commonNode.CurrentBlockHeight
	epoch := n.commonNode.Group.GetEpochSnapshot()
	go func() {
		defer close(done)

		state, roundResults, stakingEvents, err := n.getRuntimeRoundData(ctx, height, blk.Header.Round)
		if err != nil {
			n.logger.Error("failed to query runtime state and last round results",
				"err", err,
				"height", height,
				"round", blk.Header.Round,
			)
			return
		}

		// Resolve the batch and dispatch it to the runtime.
		readStartTime := time.Now()
		resolvedBatch, err := batch.resolve(ctx, n.storage)
		if err != nil {
			n.logger.Error("failed to resolve batch",
				"err", err,
				"batch", batch,
			)
			return
		}
		rq := &protocol.Body{
			RuntimeExecuteTxBatchRequest: &protocol.RuntimeExecuteTxBatchRequest{
				ConsensusBlock: *consensusBlk,
				StakingEvents:  stakingEvents,
				RoundResults:   roundResults,
				IORoot:         batch.ioRoot.Hash,
				Inputs:         resolvedBatch,
				Block:          *blk,
				Epoch:          epoch.GetEpochNumber(),
				MaxMessages:    state.Runtime.Executor.MaxMessages,
			},
		}
		batchReadTime.With(n.getMetricLabels()).Observe(time.Since(readStartTime).Seconds())
		batchSize.With(n.getMetricLabels()).Observe(float64(len(resolvedBatch)))

		span := opentracing.StartSpan("CallBatch(rq)",
			opentracing.Tag{Key: "rq", Value: rq},
			opentracing.ChildOf(batch.spanCtx),
		)
		ctx = opentracing.ContextWithSpan(ctx, span)
		defer span.Finish()

		rtStartTime := time.Now()
		defer func() {
			batchRuntimeProcessingTime.With(n.getMetricLabels()).Observe(time.Since(rtStartTime).Seconds())
		}()

		rsp, err := rt.Call(ctx, rq)
		switch {
		case err == nil:
		case errors.Is(err, context.Canceled):
			// Context was canceled while the runtime was processing a request.
			n.logger.Error("batch processing aborted by context, restarting runtime")

			// Abort the runtime, so we can start processing the next batch.
			if err = rt.Abort(n.ctx, false); err != nil {
				n.logger.Error("failed to abort the runtime",
					"err", err,
				)
			}
			return
		default:
			n.logger.Error("error while sending batch processing request to runtime",
				"err", err,
			)
			return
		}
		crash.Here(crashPointBatchProcessStartAfter)

		if rsp.RuntimeExecuteTxBatchResponse == nil {
			n.logger.Error("malformed response from runtime",
				"response", rsp,
			)
			return
		}

		// Submit response to the executor worker.
		done <- &processedBatch{
			computed: &rsp.RuntimeExecuteTxBatchResponse.Batch,
			raw:      resolvedBatch,
		}
	}()
}

// Guarded by n.commonNode.CrossNode.
func (n *Node) abortBatchLocked(reason error) {
	state, ok := n.state.(StateProcessingBatch)
	if !ok {
		// We can only abort if a batch is being processed.
		return
	}

	n.logger.Warn("aborting batch",
		"reason", reason,
	)

	// Cancel the batch processing context and wait for it to finish.
	state.cancel()

	crash.Here(crashPointBatchAbortAfter)

	abortedBatchCount.With(n.getMetricLabels()).Inc()
	// After the batch has been aborted, we must wait for the round to be
	// finalized.
	n.transitionLocked(StateWaitingForFinalize{
		batchStartTime: state.batchStartTime,
	})
}

func (n *Node) proposeBatch(
	roundCtx context.Context,
	lastHeader *block.Header,
	unresolved *unresolvedBatch,
	processed *processedBatch,
) {
	crash.Here(crashPointBatchProposeBefore)

	batch := processed.computed
	epoch := n.commonNode.Group.GetEpochSnapshot()

	n.logger.Debug("proposing batch",
		"batch", batch,
	)

	// Generate proposed compute results.
	rakSig := batch.RakSig
	proposedResults := &commitment.ComputeBody{
		Header:           batch.Header,
		RakSig:           &rakSig,
		TxnSchedSig:      unresolved.txnSchedSignature,
		InputRoot:        unresolved.ioRoot.Hash,
		InputStorageSigs: unresolved.storageSignatures,
	}
	// If we are the transaction scheduler also include all the emitted messages.
	if epoch.IsTransactionScheduler(lastHeader.Round) {
		proposedResults.Messages = batch.Messages
	}

	// Commit I/O and state write logs to storage.
	storageErr := func() error {
		start := time.Now()
		defer storageCommitLatency.With(n.getMetricLabels()).Observe(time.Since(start).Seconds())

		span, ctx := tracing.StartSpanWithContext(roundCtx, "Apply(io, state)",
			opentracing.ChildOf(unresolved.spanCtx),
		)
		defer span.Finish()

		ctx, cancel := context.WithTimeout(ctx, n.commonCfg.StorageCommitTimeout)
		defer cancel()

		// NOTE: Order is important for verifying the receipt.
		applyOps := []storage.ApplyOp{
			// I/O root.
			{
				RootType: storage.RootTypeIO,
				SrcRound: lastHeader.Round + 1,
				SrcRoot:  unresolved.ioRoot.Hash,
				DstRoot:  *batch.Header.IORoot,
				WriteLog: batch.IOWriteLog,
			},
			// State root.
			{
				RootType: storage.RootTypeState,
				SrcRound: lastHeader.Round,
				SrcRoot:  lastHeader.StateRoot,
				DstRoot:  *batch.Header.StateRoot,
				WriteLog: batch.StateWriteLog,
			},
		}

		receipts, err := n.storage.ApplyBatch(ctx, &storage.ApplyBatchRequest{
			Namespace: lastHeader.Namespace,
			DstRound:  lastHeader.Round + 1,
			Ops:       applyOps,
		})
		if err != nil {
			n.logger.Error("failed to apply to storage",
				"err", err,
			)
			return err
		}

		// Verify storage receipts.
		signatures := []signature.Signature{}
		for _, receipt := range receipts {
			var receiptBody storage.ReceiptBody
			if err = receipt.Open(&receiptBody); err != nil {
				n.logger.Error("failed to open receipt",
					"receipt", receipt,
					"err", err,
				)
				return err
			}
			if err = proposedResults.VerifyStorageReceipt(lastHeader.Namespace, &receiptBody); err != nil {
				n.logger.Error("failed to validate receipt body",
					"receipt body", receiptBody,
					"err", err,
				)
				return err
			}
			signatures = append(signatures, receipt.Signature)
		}
		if err := epoch.VerifyCommitteeSignatures(scheduler.KindStorage, signatures); err != nil {
			n.logger.Error("failed to validate receipt signer",
				"err", err,
			)
			return err
		}
		proposedResults.StorageSignatures = signatures

		return nil
	}()
	if storageErr != nil {
		n.logger.Error("storage failure, submitting failure indicating commitment",
			"err", storageErr,
		)
		proposedResults.SetFailure(commitment.FailureStorageUnavailable)
	}

	// Submit commitment.
	n.commonNode.Group.Lock()
	defer n.commonNode.Group.Unlock()

	// Make sure we are still in the right state/round.
	state, ok := n.state.(StateProcessingBatch)
	if !ok || lastHeader.Round != n.commonNode.CurrentBlock.Header.Round {
		n.logger.Error("new state or round since started proposing batch",
			"state", state,
			"round", n.commonNode.CurrentBlock.Header.Round,
			"expected_round", lastHeader.Round,
		)
		return
	}

	if err := n.signAndSubmitCommitment(roundCtx, proposedResults); err != nil {
		n.logger.Error("failed to sign and submit the commitment",
			"commit", proposedResults,
			"err", err,
		)
		n.abortBatchLocked(err)
		return
	}

	switch storageErr {
	case nil:
		n.transitionLocked(StateWaitingForFinalize{
			batchStartTime: state.batchStartTime,
			raw:            processed.raw,
			proposedIORoot: *proposedResults.Header.IORoot,
		})
	default:
		n.abortBatchLocked(storageErr)
	}

	crash.Here(crashPointBatchProposeAfter)
}

func (n *Node) signAndSubmitCommitment(roundCtx context.Context, body *commitment.ComputeBody) error {
	commit, err := commitment.SignExecutorCommitment(n.commonNode.Identity.NodeSigner, n.commonNode.Runtime.ID(), body)
	if err != nil {
		n.logger.Error("failed to sign commitment",
			"commit", body,
			"err", err,
		)
		return err
	}

	tx := roothash.NewExecutorCommitTx(0, nil, n.commonNode.Runtime.ID(), []commitment.ExecutorCommitment{*commit})
	go func() {
		commitErr := consensus.SignAndSubmitTx(roundCtx, n.commonNode.Consensus, n.commonNode.Identity.NodeSigner, tx)
		switch commitErr {
		case nil:
			n.logger.Info("executor commit finalized")
		default:
			n.logger.Error("failed to submit executor commit",
				"commit", body,
				"err", commitErr,
			)
		}
	}()

	return nil
}

// HandleNewEventLocked implements NodeHooks.
// Guarded by n.commonNode.CrossNode.
func (n *Node) HandleNewEventLocked(ev *roothash.Event) {
	switch {
	case ev.ExecutionDiscrepancyDetected != nil:
		n.logger.Warn("execution discrepancy detected")

		crash.Here(crashPointDiscrepancyDetectedAfter)

		discrepancyDetectedCount.With(n.getMetricLabels()).Inc()

		// If the node is not a backup worker in this epoch, no need to do anything. Also if the
		// node is an executor worker in this epoch, then it has already processed and submitted
		// a commitment, so no need to do anything.
		epoch := n.commonNode.Group.GetEpochSnapshot()
		if !epoch.IsExecutorBackupWorker() || epoch.IsExecutorWorker() {
			return
		}

		var state StateWaitingForEvent
		switch s := n.state.(type) {
		case StateWaitingForBatch:
			// Discrepancy detected event received before the batch. We need to
			// record the received event and keep waiting for the batch.
			s.pendingEvent = ev.ExecutionDiscrepancyDetected
			n.transitionLocked(s)
			return
		case StateWaitingForEvent:
			state = s
		default:
			n.logger.Warn("ignoring received discrepancy event in incorrect state",
				"state", s,
			)
			return
		}

		// Backup worker, start processing a batch.
		n.logger.Info("backup worker activating and processing batch")
		n.startProcessingBatchLocked(state.batch)
	}
}

// HandleNodeUpdateLocked implements NodeHooks.
// Guarded by n.commonNode.CrossNode.
func (n *Node) HandleNodeUpdateLocked(update *nodes.NodeUpdate, snapshot *committee.EpochSnapshot) {
	// Nothing to do here.
}

// Guarded by n.commonNode.CrossNode.
func (n *Node) handleExternalBatchLocked(batch *unresolvedBatch, hdr block.Header) error {
	// If we are not waiting for a batch, don't do anything.
	if _, ok := n.state.(StateWaitingForBatch); !ok {
		return errIncorrectState
	}

	epoch := n.commonNode.Group.GetEpochSnapshot()

	// We can only receive external batches if we are an executor member.
	if !epoch.IsExecutorMember() {
		n.logger.Error("got external batch while in incorrect role")
		return errIncorrectRole
	}

	// Check if we have the correct block -- in this case, start processing the batch.
	if n.commonNode.CurrentBlock.Header.MostlyEqual(&hdr) {
		n.maybeStartProcessingBatchLocked(batch)
		return nil
	}

	// Check if the current block is older than what is expected we base our batch
	// on. In case it is equal or newer, but different, discard the batch.
	curRound := n.commonNode.CurrentBlock.Header.Round
	waitRound := hdr.Round
	if curRound >= waitRound {
		n.logger.Warn("got external batch based on incompatible header",
			"header", hdr,
		)
		return errIncompatibleHeader
	}

	// Wait for the correct block to arrive.
	n.transitionLocked(StateWaitingForBlock{
		batch:  batch,
		header: &hdr,
	})

	return nil
}

func (n *Node) handleRuntimeHostEvent(ev *host.Event) {
	switch {
	case ev.Started != nil:
		// We are now able to service requests for this runtime.
		n.runtimeVersion = ev.Started.Version

		n.roleProvider.SetAvailable(func(nd *node.Node) error {
			rt := nd.AddOrUpdateRuntime(n.commonNode.Runtime.ID())
			rt.Version = n.runtimeVersion
			rt.Capabilities.TEE = ev.Started.CapabilityTEE
			return nil
		})
	case ev.Updated != nil:
		// Update runtime capabilities.
		n.roleProvider.SetAvailable(func(nd *node.Node) error {
			rt := nd.AddOrUpdateRuntime(n.commonNode.Runtime.ID())
			rt.Version = n.runtimeVersion
			rt.Capabilities.TEE = ev.Updated.CapabilityTEE
			return nil
		})
	case ev.FailedToStart != nil, ev.Stopped != nil:
		// Runtime failed to start or was stopped -- we can no longer service requests.
		n.roleProvider.SetUnavailable()
	default:
		// Unknown event.
		n.logger.Warn("unknown worker event",
			"ev", ev,
		)
	}
}

func (n *Node) handleProcessedBatch(batch *processedBatch, processingCh chan *processedBatch) {
	n.commonNode.CrossNode.Lock()

	// To avoid stale events, check if the stored state is still valid.
	// XXX: processingCh not changing ensures we are in the same state and not
	// in a "new" processing batch state. This also ensures that the round did
	// not change.
	state, ok := n.state.(StateProcessingBatch)
	if !ok || state.done != processingCh {
		n.commonNode.CrossNode.Unlock()
		return
	}
	roundCtx := n.roundCtx
	lastHeader := n.commonNode.CurrentBlock.Header

	// Successfully processed a batch.
	if batch != nil && batch.computed != nil {
		stateBatch := state.batch
		n.commonNode.CrossNode.Unlock()
		n.logger.Info("worker has finished processing a batch")
		n.proposeBatch(roundCtx, &lastHeader, stateBatch, batch)
		return
	}

	defer n.commonNode.CrossNode.Unlock()

	// Unsuccessful batch processing.
	n.logger.Warn("worker has aborted batch processing")
	commit := &commitment.ComputeBody{
		Header: commitment.ComputeResultsHeader{
			Round:        lastHeader.Round + 1,
			PreviousHash: lastHeader.EncodedHash(),
		},
		TxnSchedSig:      state.batch.txnSchedSignature,
		InputRoot:        state.batch.ioRoot.Hash,
		InputStorageSigs: state.batch.storageSignatures,
	}
	commit.SetFailure(commitment.FailureUnknown)

	n.logger.Debug("submitting failure indicating commitment",
		"commitment", commit,
	)
	if err := n.signAndSubmitCommitment(roundCtx, commit); err != nil {
		n.logger.Error("failed to sign and submit the commitment",
			"commit", commit,
			"err", err,
		)
	}

	n.abortBatchLocked(errRuntimeAborted)
}

func (n *Node) worker() {
	defer close(n.quitCh)
	defer (n.cancelCtx)()

	// Wait for the common node to be initialized.
	select {
	case <-n.commonNode.Initialized():
	case <-n.stopCh:
		close(n.initCh)
		return
	}

	n.logger.Info("starting committee node")

	// Provision the hosted runtime.
	hrt, hrtNotifier, err := n.ProvisionHostedRuntime(n.ctx)
	if err != nil {
		n.logger.Error("failed to provision hosted runtime",
			"err", err,
		)
		return
	}

	hrtEventCh, hrtSub, err := hrt.WatchEvents(n.ctx)
	if err != nil {
		n.logger.Error("failed to subscribe to hosted runtime events",
			"err", err,
		)
		return
	}
	defer hrtSub.Close()

	if err = hrt.Start(); err != nil {
		n.logger.Error("failed to start hosted runtime",
			"err", err,
		)
		return
	}
	defer hrt.Stop()

	if err = hrtNotifier.Start(); err != nil {
		n.logger.Error("failed to start runtime notifier",
			"err", err,
		)
		return
	}
	defer hrtNotifier.Stop()

	// Initialize transaction scheduling algorithm with latest registry desctiptor.
	// Note: in case the runtime is already running, the correct active descriptor
	// is the version at the last epoch transition.
	// This case will be handled by the first tick from the descriptor updates
	// channel below, which will update the parameters.
	runtime, err := n.commonNode.Runtime.ActiveDescriptor(n.ctx)
	if err != nil {
		n.logger.Error("failed to fetch runtime registry descriptor",
			"err", err,
		)
		return
	}
	scheduler, err := scheduling.New(
		n.scheduleMaxTxPoolSize,
		runtime.TxnScheduler.Algorithm,
		make(map[string]uint64),
	)
	if err != nil {
		n.logger.Error("failed to create new transaction scheduler algorithm",
			"err", err,
		)
		return
	}

	n.schedulerMutex.Lock()
	n.scheduler = scheduler
	n.schedulerMutex.Unlock()
	// Check incoming queue every FlushTimeout.
	txnScheduleTicker := time.NewTicker(runtime.TxnScheduler.BatchFlushTimeout)
	defer txnScheduleTicker.Stop()

	// Watch runtime descriptor updates.
	rtCh, rtSub, err := n.commonNode.Runtime.WatchActiveDescriptor()
	if err != nil {
		n.logger.Error("failed to watch runtimes",
			"err", err,
		)
		return
	}
	defer rtSub.Close()

	// We are initialized.
	close(n.initCh)

	for {
		// Check if we are currently processing a batch. In this case, we also
		// need to select over the result channel.
		var processingDoneCh chan *processedBatch

		func() {
			n.commonNode.CrossNode.Lock()
			defer n.commonNode.CrossNode.Unlock()

			if stateProcessing, ok := n.state.(StateProcessingBatch); ok {
				processingDoneCh = stateProcessing.done
			}
		}()

		select {
		case <-n.stopCh:
			n.logger.Info("termination requested")
			return
		case ev := <-hrtEventCh:
			n.handleRuntimeHostEvent(ev)
		case batch := <-processingDoneCh:
			// Batch processing has finished.
			n.handleProcessedBatch(batch, processingDoneCh)
		case runtime := <-rtCh:
			// XXX: Once there is more than one scheduling algorithm available
			// this might need to recreate the scheduler and reinsert
			// the transactions.
			// At that point also update the schedulerMutex usage across the
			// code, as it will be no longer be true that the scheduler
			// variable never gets updated.

			// Set per round weight limits.
			weightLimits := map[string]uint64{
				transaction.WeightConsensusMessages: uint64(runtime.Executor.MaxMessages),
				transaction.WeightSizeBytes:         runtime.TxnScheduler.MaxBatchSizeBytes,
				transaction.WeightCount:             runtime.TxnScheduler.MaxBatchSize,
			}
			if err = n.scheduler.UpdateParameters(runtime.TxnScheduler.Algorithm, weightLimits); err != nil {
				n.logger.Error("error updating scheduler parameters",
					"err", err,
				)
				return
			}
			// Update batch flush timeout ticker.
			txnScheduleTicker.Reset(runtime.TxnScheduler.BatchFlushTimeout)
		case <-txnScheduleTicker.C:
			// Force scheduling a batch if possible.
			n.handleScheduleBatch(true)
		case <-n.scheduleCh.Out():
			// Regular scheduling of a batch.
			n.handleScheduleBatch(false)
		case <-n.reselect:
			// Recalculate select set.
		}
	}
}

// NewNode initializes a new executor node.
//
// NOTE: If the node is also a storage node, the executor node will use the
// local storage backend in addition to the committee connections, so the storage
// worker needs to be initialized before constructing the executor node.
func NewNode(
	commonNode *committee.Node,
	commonCfg commonWorker.Config,
	roleProvider registration.RoleProvider,
	scheduleMaxTxPoolSize uint64,
	lastScheduledCacheSize uint64,
) (*Node, error) {
	metricsOnce.Do(func() {
		prometheus.MustRegister(nodeCollectors...)
	})

	// Prepare the runtime host node helpers.
	rhn, err := runtimeRegistry.NewRuntimeHostNode(commonNode)
	if err != nil {
		return nil, err
	}

	var cache *lru.Cache
	if lastScheduledCacheSize > 0 {
		cache, err = lru.New(lru.Capacity(lastScheduledCacheSize, false))
		if err != nil {
			return nil, fmt.Errorf("error creating cache: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	var storageMux storage.Backend
	if lsb, ok := commonNode.Runtime.Storage().(storage.LocalBackend); ok && commonNode.Runtime.HasRoles(node.RoleStorageWorker) {
		storageMux = storage.NewStorageMux(
			storage.MuxReadOpFinishEarly(storage.MuxIterateIgnoringErrors()),
			lsb,
			commonNode.Group.Storage(),
		)
	} else {
		storageMux = commonNode.Group.Storage()
	}

	n := &Node{
		RuntimeHostNode:       rhn,
		commonNode:            commonNode,
		commonCfg:             commonCfg,
		roleProvider:          roleProvider,
		scheduleMaxTxPoolSize: scheduleMaxTxPoolSize,
		lastScheduledCache:    cache,
		scheduleCh:            channels.NewRingChannel(1),
		storage:               storageMux,
		ctx:                   ctx,
		cancelCtx:             cancel,
		stopCh:                make(chan struct{}),
		quitCh:                make(chan struct{}),
		initCh:                make(chan struct{}),
		state:                 StateNotReady{},
		stateTransitions:      pubsub.NewBroker(false),
		reselect:              make(chan struct{}, 1),
		logger:                logging.GetLogger("worker/executor/committee").With("runtime_id", commonNode.Runtime.ID()),
	}

	// Register prune handler.
	commonNode.Runtime.History().Pruner().RegisterHandler(&pruneHandler{commonNode: commonNode})

	return n, nil
}
