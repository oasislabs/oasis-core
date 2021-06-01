package client

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/block"
	"github.com/oasisprotocol/oasis-core/go/runtime/client/api"
	"github.com/oasisprotocol/oasis-core/go/runtime/transaction"
	storage "github.com/oasisprotocol/oasis-core/go/storage/api"
	"github.com/oasisprotocol/oasis-core/go/worker/common/p2p"
	executor "github.com/oasisprotocol/oasis-core/go/worker/compute/executor/api"
)

type txRequest struct {
	id     hash.Hash
	ctx    context.Context
	req    *api.SubmitTxRequest
	height int64

	respCh chan<- *txResult
}

func (w *txRequest) result(res *txResult) {
	select {
	case <-w.ctx.Done():
	case w.respCh <- res:
	}
}

type txResult struct {
	err    error
	result []byte
}

type txSubmitter struct {
	logger *logging.Logger

	common *clientCommon
	id     common.Namespace

	transactions map[hash.Hash]*txRequest
	newCh        chan *txRequest

	maxTransactionAge int64
	toBeChecked       []*block.Block
	recheckTicker     *backoff.Ticker

	stopCh chan struct{}
	quitCh chan struct{}
}

func (w *txSubmitter) publishTx(tx *txRequest, groupVersion int64) {
	w.common.p2p.Publish(w.common.ctx, w.id, &p2p.Message{
		Tx: &executor.Tx{
			Data: tx.req.Data,
		},
		GroupVersion: groupVersion,
	})
}

func (w *txSubmitter) checkBlock(blk *block.Block) error {
	if blk.Header.IORoot.IsEmpty() {
		return nil
	}

	// If there's no pending transactions, we can skip the check.
	if len(w.transactions) == 0 {
		return nil
	}

	ctx := w.common.ctx
	ioRoot := storage.Root{
		Namespace: blk.Header.Namespace,
		Version:   blk.Header.Round,
		Type:      storage.RootTypeIO,
		Hash:      blk.Header.IORoot,
	}

	tree := transaction.NewTree(w.common.storage, ioRoot)
	defer tree.Close()

	// Check if there's anything interesting in this block.
	var txHashes []hash.Hash
	for txHash := range w.transactions {
		txHashes = append(txHashes, txHash)
	}

	matches, err := tree.GetTransactionMultiple(ctx, txHashes)
	if err != nil {
		return fmt.Errorf("error getting block I/O from storage: %w", err)
	}

	for txHash, tx := range matches {
		txReq := w.transactions[txHash]
		txReq.result(&txResult{
			result: tx.Output,
		})
		close(txReq.respCh)
		delete(w.transactions, txHash)
	}

	return nil
}

func (w *txSubmitter) checkBlocks() {
	if len(w.toBeChecked) == 0 {
		return
	}

	var failedBlocks []*block.Block
	for _, b := range w.toBeChecked {
		if err := w.checkBlock(b); err != nil {
			w.logger.Error("error checking block",
				"err", err,
				"round", b.Header.Round,
			)
			failedBlocks = append(failedBlocks, b)
		}
	}
	if len(failedBlocks) > 0 {
		w.logger.Warn("failed roothash blocks",
			"num_failed_blocks", len(failedBlocks),
		)

		// Start recheck ticker.
		if w.recheckTicker == nil {
			boff := backoff.NewExponentialBackOff()
			boff.InitialInterval = 5 * time.Second
			w.recheckTicker = backoff.NewTicker(boff)
		}
	} else if w.recheckTicker != nil {
		w.recheckTicker.Stop()
		w.recheckTicker = nil
	}
	w.toBeChecked = failedBlocks
}

func (w *txSubmitter) getGroupVersion(height int64) (int64, error) {
	epoch, err := w.common.consensus.Beacon().GetEpoch(w.common.ctx, height)
	if err != nil {
		return 0, fmt.Errorf("failed querying for epoch: %w", err)
	}
	return w.common.consensus.Beacon().GetEpochBlock(w.common.ctx, epoch)
}

func (w *txSubmitter) start() {
	defer func() {
		for _, txReq := range w.transactions {
			close(txReq.respCh)
		}
		close(w.quitCh)
	}()

	rt, err := w.common.runtimeRegistry.GetRuntime(w.id)
	if err != nil {
		w.logger.Error("failed to get runtime from runtime registry", "err", err)
		return
	}
	// Start watching roothash blocks.
	blocks, blocksSub, err := rt.History().WatchBlocks(w.common.ctx)
	if err != nil {
		w.logger.Error("failed to subscribe to roothash blocks",
			"err", err,
		)
		return
	}
	defer blocksSub.Close()

	// Start watching consensus blocks.
	consensusBlocks, consensusBlocksSub, err := w.common.consensus.WatchBlocks(w.common.ctx)
	if err != nil {
		w.logger.Error("failed to subscribe to consensus blocks",
			"err", err,
		)
		return
	}
	defer consensusBlocksSub.Close()

	// latestHeight contains the latest known consensus block height.
	var latestHeight int64
	// latestGroupVersion contains the latest known committee group version.
	var latestGroupVersion int64
	// Wait for first consensus block before proceeding.
	select {
	case <-w.stopCh:
		return
	case <-w.common.ctx.Done():
		return
	case blk := <-consensusBlocks:
		latestHeight = blk.Height
		latestGroupVersion, err = w.getGroupVersion(blk.Height)
		if err != nil {
			w.logger.Error("failed querying for latest group version",
				"err", err,
			)
			return
		}
	}

	for {
		var recheckCh <-chan time.Time
		if w.recheckTicker != nil {
			recheckCh = w.recheckTicker.C
		}

		// Wait for stuff to happen.
		select {
		case blk := <-blocks:
			w.toBeChecked = append(w.toBeChecked, blk.Block)
			w.checkBlocks()

			// If this is an epoch transition block, update latest known group
			// version and resend all transactions.
			if blk.Block.Header.HeaderType != block.EpochTransition {
				continue
			}

			// Get group version.
			latestGroupVersion, err = w.getGroupVersion(blk.Height)
			if err != nil {
				w.logger.Error("failed querying for latest group version",
					"err", err,
				)
				continue
			}

			// Republish all transactions as messages with old groupVersion will
			// be discarded.
			for _, req := range w.transactions {
				w.publishTx(req, latestGroupVersion)
			}
		case <-recheckCh:
			// Recheck blocks if needed.
			w.checkBlocks()
		case blk := <-consensusBlocks:
			if blk == nil {
				break
			}
			latestHeight = blk.Height

			// Check if any transaction is considered expired.
			for key, req := range w.transactions {
				if (latestHeight - w.maxTransactionAge) < req.height {
					continue
				}
				w.logger.Debug("expired transaction",
					"key", key,
					"latest_height", latestHeight,
					"max_transaction_age", w.maxTransactionAge,
					"initial_height", req.height,
				)
				req.result(&txResult{
					err: api.ErrTransactionExpired,
				})
				close(req.respCh)
				delete(w.transactions, key)
			}
		case newRequest := <-w.newCh:
			w.transactions[newRequest.id] = newRequest
			newRequest.height = latestHeight
			w.publishTx(newRequest, latestGroupVersion)
		case <-w.stopCh:
			w.logger.Info("stop requested, aborting watcher")
			return
		case <-w.common.ctx.Done():
			w.logger.Info("context cancelled, aborting watcher")
			return
		}
	}
}

func (w *txSubmitter) Quit() <-chan struct{} {
	return w.quitCh
}

func (w *txSubmitter) Start() {
	go w.start()
}

func (w *txSubmitter) Stop() {
	close(w.stopCh)
}

func newTxSubmitter(common *clientCommon, id common.Namespace, p2pSvc *p2p.P2P, maxTransactionAge int64) *txSubmitter {
	// Register handler.
	p2pSvc.RegisterHandler(id, &p2p.BaseHandler{})

	txSubmitter := &txSubmitter{
		logger:            logging.GetLogger("client/txsubmitter").With("runtime_id", id),
		common:            common,
		id:                id,
		maxTransactionAge: maxTransactionAge,
		transactions:      make(map[hash.Hash]*txRequest),
		newCh:             make(chan *txRequest),
		stopCh:            make(chan struct{}),
		quitCh:            make(chan struct{}),
	}
	return txSubmitter
}
