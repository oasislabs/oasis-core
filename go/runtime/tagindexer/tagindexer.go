// Package tagindexer implements the runtime transaction tag indexer.
package tagindexer

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/common/service"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	"github.com/oasisprotocol/oasis-core/go/runtime/history"
	"github.com/oasisprotocol/oasis-core/go/runtime/transaction"
	storage "github.com/oasisprotocol/oasis-core/go/storage/api"
)

const (
	storageRequestTimeout = 5 * time.Second
	storageRetryTimeout   = 120 * time.Second
)

var _ history.PruneHandler = (*pruneHandler)(nil)

// Service is an indexer service.
type Service struct {
	service.BaseBackgroundService
	QueryableBackend

	runtimeID common.Namespace
	backend   Backend
	roothash  roothash.Backend
	history   history.History

	ctx       context.Context
	cancelCtx context.CancelFunc

	stopCh chan struct{}
}

func (s *Service) worker(storageBackend storage.Backend) {
	defer s.BaseBackgroundService.Stop()

	s.Logger.Info("started indexer for runtime")

	// Start watching roothash blocks.
	blocksCh, blocksSub, err := s.history.WatchBlocks(s.ctx)
	if err != nil {
		s.Logger.Error("failed to subscribe to roothash blocks",
			"err", err,
		)
		return
	}
	defer blocksSub.Close()

	for {
		select {
		case <-s.stopCh:
			s.Logger.Info("stop requested, terminating indexer")
			return
		case annBlk := <-blocksCh:
			// New blocks to index.
			blk := annBlk.Block

			// Fetch transactions from storage.
			//
			// NOTE: Currently the indexer requires all transactions as well since it needs to
			//       expose a notion of a "transaction index within a block" which is hard to
			//       provide as batches can be merged in arbitrary order and the sequence can
			//       only be known after the fact.
			var txs []*transaction.Transaction
			var tags transaction.Tags
			if !blk.Header.IORoot.IsEmpty() {
				off := backoff.NewExponentialBackOff()
				off.MaxElapsedTime = storageRetryTimeout

				err = backoff.Retry(func() error {
					bctx, cancel := context.WithTimeout(s.ctx, storageRequestTimeout)
					defer cancel()

					// Prioritize nodes that signed the storage receipt.
					bctx = storage.WithNodePriorityHintFromSignatures(bctx, blk.Header.StorageSignatures)

					ioRoot := storage.Root{
						Namespace: blk.Header.Namespace,
						Version:   blk.Header.Round,
						Type:      storage.RootTypeIO,
						Hash:      blk.Header.IORoot,
					}

					tree := transaction.NewTree(storageBackend, ioRoot)
					defer tree.Close()

					txs, err = tree.GetTransactions(bctx)
					if err != nil {
						return err
					}

					tags, err = tree.GetTags(bctx)
					if err != nil {
						return err
					}

					return nil
				}, off)

				if err != nil {
					s.Logger.Error("can't get I/O root from storage",
						"err", err,
						"round", blk.Header.Round,
					)
					continue
				}
			}

			if err = s.backend.Index(s.ctx, blk.Header.Round, blk.Header.EncodedHash(), txs, tags); err != nil {
				s.Logger.Error("failed to index tags",
					"err", err,
					"round", blk.Header.Round,
				)
				continue
			}
		}
	}
}

func (s *Service) Start(storage storage.Backend) error {
	if _, ok := s.backend.(*nopBackend); ok {
		// In case this is a nopBackend (which doesn't index anything) avoid the overhead of having
		// a tag indexer service follow blocks and index them.
		s.Logger.Info("not starting tag indexer as it is disabled")
		return nil
	}

	go s.worker(storage)
	return nil
}

func (s *Service) Stop() {
	s.cancelCtx()
	close(s.stopCh)

	if _, ok := s.backend.(*nopBackend); ok {
		// Since we didn't start a worker for the nopBackend, propagate stop immediately.
		s.BaseBackgroundService.Stop()
	}
}

// New creates a new tag indexer service.
func New(
	dataDir string,
	backendFactory BackendFactory,
	history history.History,
	roothash roothash.Backend,
) (*Service, error) {
	runtimeID := history.RuntimeID()
	backend, err := backendFactory(dataDir, runtimeID)
	if err != nil {
		return nil, err
	}

	ctx, cancelCtx := context.WithCancel(context.Background())

	s := &Service{
		BaseBackgroundService: *service.NewBaseBackgroundService("runtime/history/tagindexer"),
		QueryableBackend:      backend,
		runtimeID:             runtimeID,
		backend:               backend,
		roothash:              roothash,
		history:               history,
		ctx:                   ctx,
		cancelCtx:             cancelCtx,
		stopCh:                make(chan struct{}),
	}
	s.Logger = s.Logger.With("runtime_id", s.runtimeID.String())

	// Register prune handler.
	history.Pruner().RegisterHandler(&pruneHandler{
		logger:  s.Logger,
		backend: s.backend,
	})

	return s, nil
}

type pruneHandler struct {
	logger  *logging.Logger
	backend Backend
}

func (p *pruneHandler) Prune(ctx context.Context, rounds []uint64) error {
	// New blocks to prune from the index.
	for _, round := range rounds {
		if err := p.backend.Prune(ctx, round); err != nil {
			p.logger.Error("failed to prune index",
				"err", err,
				"round", round,
			)
			return err
		}
	}

	return nil
}
