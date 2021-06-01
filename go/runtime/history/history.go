// Package history implements the runtime block history and pruning policy.
package history

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/eapache/channels"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/common/pubsub"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	"github.com/oasisprotocol/oasis-core/go/runtime/history/api"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
)

// DbFilename is the filename of the history database.
const DbFilename = "history.db"

var (
	errNopHistory = errors.New("runtime/history: not supported")

	_ History = (*runtimeHistory)(nil)
)

// Config is runtime history keeper configuration.
type Config struct {
	// Pruner configures the pruner to use.
	Pruner PrunerFactory

	// PruneInterval configures the pruning interval.
	PruneInterval time.Duration
}

// NewDefaultConfig returns the default runtime history keeper config.
func NewDefaultConfig() *Config {
	return &Config{
		Pruner:        NewNonePruner(),
		PruneInterval: 10 * time.Second,
	}
}

// History is the runtime history interface.
type History interface {
	api.BlockHistory

	// Pruner returns the history pruner.
	Pruner() Pruner

	// Close closes the history keeper.
	Close()
}

type nopHistory struct {
	runtimeID common.Namespace
}

func (h *nopHistory) RuntimeID() common.Namespace {
	return h.runtimeID
}

func (h *nopHistory) Commit(blk *roothash.AnnotatedBlock, roundResults *roothash.RoundResults) error {
	return errNopHistory
}

func (h *nopHistory) CommitPendingConsensusEvents(height int64, stakingEvents []*staking.Event) error {
	return errNopHistory
}

func (h *nopHistory) ConsensusCheckpoint(height int64) error {
	return errNopHistory
}

func (h *nopHistory) LastConsensusHeight() (int64, error) {
	return 0, errNopHistory
}

func (h *nopHistory) GetBlock(ctx context.Context, round uint64) (*roothash.AnnotatedBlock, error) {
	return nil, errNopHistory
}

func (h *nopHistory) GetLatestBlock(ctx context.Context) (*roothash.AnnotatedBlock, error) {
	return nil, errNopHistory
}

func (h *nopHistory) GetRoundResults(ctx context.Context, round uint64) (*roothash.RoundResults, error) {
	return nil, errNopHistory
}

func (h *nopHistory) GetRoundEvents(ctx context.Context, round uint64) ([]*staking.Event, error) {
	return nil, errNopHistory
}

func (h *nopHistory) WatchBlocks(ctx context.Context) (<-chan *roothash.AnnotatedBlock, *pubsub.Subscription, error) {
	return nil, nil, errNopHistory
}

func (h *nopHistory) Pruner() Pruner {
	pruner, _ := NewNonePruner()(nil)
	return pruner
}

func (h *nopHistory) Close() {
}

// NewNop creates a new no-op runtime history keeper.
func NewNop(runtimeID common.Namespace) History {
	return &nopHistory{runtimeID: runtimeID}
}

type runtimeHistory struct {
	runtimeID common.Namespace

	logger *logging.Logger

	ctx       context.Context
	cancelCtx context.CancelFunc

	db            *DB
	blockNotifier *pubsub.Broker

	pruner        Pruner
	pruneInterval time.Duration
	pruneCh       *channels.RingChannel
	stopCh        chan struct{}
	quitCh        chan struct{}
}

func (h *runtimeHistory) RuntimeID() common.Namespace {
	return h.runtimeID
}

func (h *runtimeHistory) Commit(blk *roothash.AnnotatedBlock, roundResults *roothash.RoundResults) error {
	if err := h.db.commit(blk, roundResults); err != nil {
		return err
	}

	// Broadcast the new block.
	h.blockNotifier.Broadcast(blk)
	// Notify the pruner what the new round is.
	h.pruneCh.In() <- blk.Block.Header.Round

	return nil
}

func (h *runtimeHistory) CommitPendingConsensusEvents(height int64, stakingEvents []*staking.Event) error {
	return h.db.commitPendingConsensusEvents(height, stakingEvents)
}

func (h *runtimeHistory) ConsensusCheckpoint(height int64) error {
	return h.db.consensusCheckpoint(height)
}

func (h *runtimeHistory) LastConsensusHeight() (int64, error) {
	meta, err := h.db.metadata()
	if err != nil {
		return 0, err
	}

	return meta.LastConsensusHeight, nil
}

func (h *runtimeHistory) GetBlock(ctx context.Context, round uint64) (*roothash.AnnotatedBlock, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	annBlk, err := h.db.getBlock(round)
	if err != nil {
		return nil, err
	}

	return annBlk, nil
}

func (h *runtimeHistory) GetLatestBlock(ctx context.Context) (*roothash.AnnotatedBlock, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	meta, err := h.db.metadata()
	if err != nil {
		return nil, err
	}
	annBlk, err := h.db.getBlock(meta.LastRound)
	if err != nil {
		return nil, err
	}

	return annBlk, nil
}

func (h *runtimeHistory) GetRoundResults(ctx context.Context, round uint64) (*roothash.RoundResults, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return h.db.getRoundResults(round)
}

func (h *runtimeHistory) GetRoundEvents(ctx context.Context, round uint64) ([]*staking.Event, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return h.db.getStakingEvents(round)
}

func (h *runtimeHistory) WatchBlocks(ctx context.Context) (<-chan *roothash.AnnotatedBlock, *pubsub.Subscription, error) {
	typedCh := make(chan *roothash.AnnotatedBlock)

	// Load latest block and send it on subscribe hook.
	latestBlock, err := h.GetLatestBlock(ctx)
	if err != nil {
		h.logger.Error("error getting latest block", "err", err)
	}
	h.logger.Info("gooooot block from histoery latest", "latest_block", latestBlock, "err", err)
	sub := h.blockNotifier.SubscribeEx(-1, func(ch channels.Channel) {
		if latestBlock != nil {
			ch.In() <- latestBlock
		}
	})
	sub.Unwrap(typedCh)

	return typedCh, sub, nil
}

func (h *runtimeHistory) Pruner() Pruner {
	return h.pruner
}

func (h *runtimeHistory) Close() {
	h.cancelCtx()
	close(h.stopCh)
	<-h.quitCh

	h.db.close()
}

func (h *runtimeHistory) pruneWorker() {
	defer close(h.quitCh)

	ticker := time.NewTicker(h.pruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var round interface{}
			select {
			case round = <-h.pruneCh.Out():
			case <-h.stopCh:
				h.logger.Info("prune worker is terminating")
				return
			}

			h.logger.Debug("pruning runtime history",
				"round", round.(uint64),
			)

			if err := h.pruner.Prune(h.ctx, round.(uint64)); err != nil {
				h.logger.Error("failed to prune",
					"err", err,
				)
				continue
			}
		case <-h.stopCh:
			h.logger.Info("prune worker is terminating")
			return
		}
	}
}

// New creates a new runtime history keeper.
func New(dataDir string, runtimeID common.Namespace, cfg *Config) (History, error) {
	db, err := newDB(filepath.Join(dataDir, DbFilename), runtimeID)
	if err != nil {
		return nil, err
	}

	if cfg == nil {
		cfg = NewDefaultConfig()
	}
	if cfg.Pruner == nil {
		cfg.Pruner = NewNonePruner()
	}
	pruner, err := cfg.Pruner(db)
	if err != nil {
		return nil, err
	}

	ctx, cancelCtx := context.WithCancel(context.Background())

	h := &runtimeHistory{
		runtimeID:     runtimeID,
		logger:        logging.GetLogger("roothash/history").With("runtime_id", runtimeID),
		ctx:           ctx,
		cancelCtx:     cancelCtx,
		db:            db,
		blockNotifier: pubsub.NewBroker(false),
		pruner:        pruner,
		pruneInterval: cfg.PruneInterval,
		pruneCh:       channels.NewRingChannel(1),
		stopCh:        make(chan struct{}),
		quitCh:        make(chan struct{}),
	}
	go h.pruneWorker()

	return h, nil
}
