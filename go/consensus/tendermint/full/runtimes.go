package full

import (
	"fmt"

	tmtypes "github.com/tendermint/tendermint/types"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/crash"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	genesisAPI "github.com/oasisprotocol/oasis-core/go/genesis/api"
	"github.com/oasisprotocol/oasis-core/go/roothash/api"
	runtimes "github.com/oasisprotocol/oasis-core/go/runtime/history/api"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
)

const (
	crashPointBlockBeforeIndex               = "runtimes.before_index"
	crashPointBlockAfterConsensusEventsIndex = "runtimes.after_consensus_events_index"
)

type trackedRuntime struct {
	logger *logging.Logger

	runtimeID common.Namespace

	blockHistory runtimes.BlockHistory
}

func (t *fullService) TrackRuntime(history runtimes.BlockHistory) error {
	if history == nil {
		return fmt.Errorf("missing runtime history")
	}
	return t.trackRuntime(history.RuntimeID(), history)
}

func (t *fullService) trackRuntime(id common.Namespace, history runtimes.BlockHistory) error {
	if _, tracked := t.trackedRuntimes.Load(id); tracked {
		return nil
	}

	t.Logger.Debug("tracking new runtime",
		"runtime_id", id,
	)
	tr := &trackedRuntime{
		logger:       logging.GetLogger("runtimes/indexer").With("runtime_id", id),
		runtimeID:    id,
		blockHistory: history,
	}
	t.trackedRuntimes.Store(id, tr)

	return nil
}

func (t *fullService) getTrackedRuntime(rtID common.Namespace) *trackedRuntime {
	rt, exists := t.trackedRuntimes.Load(rtID)
	if !exists {
		return nil
	}
	return rt.(*trackedRuntime)
}

func (t *fullService) allTrackedRuntimes() []*trackedRuntime {
	var trackedRuntimes []*trackedRuntime
	t.trackedRuntimes.Range(func(k, v interface{}) bool {
		trackedRuntimes = append(trackedRuntimes, v.(*trackedRuntime))
		return true
	})
	return trackedRuntimes
}

func (t *fullService) indexRuntime(tr *trackedRuntime, indexHeight int64) error {
	var err error
	runtimeID := tr.runtimeID
	logger := tr.logger.With("index_height", indexHeight)

	crash.Here(crashPointBlockBeforeIndex)

	// Load last indexed height.
	var lastHeight int64
	if lastHeight, err = tr.blockHistory.LastConsensusHeight(); err != nil {
		return fmt.Errorf("failed to get last indexed height: %w", err)
	}
	lastNonSeenHeight := lastHeight + 1

	switch {
	case lastNonSeenHeight > indexHeight:
		// Already indexed, skip.
		logger.Debug("height already indexed, skipping", "last_height", lastHeight)
		return nil
	case lastNonSeenHeight == indexHeight:
		// Nothing to reindex, just index the incoming height below.
	default:
		// We need to reindex. Adjust last non seen height to last available height.
		var lastRetainedHeight int64
		lastRetainedHeight, err = t.GetLastRetainedVersion(t.ctx)
		if err != nil {
			return fmt.Errorf("failed to get last retained height: %w", err)
		}
		if lastNonSeenHeight < lastRetainedHeight {
			logger.Debug("last non seen height pruned, skipping until last retained",
				"last_retained_height", lastRetainedHeight,
				"last_height", lastNonSeenHeight,
			)
			lastNonSeenHeight = lastRetainedHeight
		}

		// Take initial genesis height into account.
		var genesisDoc *genesisAPI.Document
		genesisDoc, err = t.GetGenesisDocument(t.ctx)
		if err != nil {
			return fmt.Errorf("failed to get genesis document: %w", err)
		}
		if lastNonSeenHeight < genesisDoc.Height {
			logger.Debug("genesis height greater than last non seen height, skip until genesis height",
				"genesis_height", genesisDoc.HaltEpoch,
				"last_height", lastNonSeenHeight,
			)
			lastNonSeenHeight = genesisDoc.Height
		}

	}

	// Index all blocks between last available height and current height.
	logger.Debug("indexing blocks",
		"last_non_seen_height", lastNonSeenHeight,
		logging.LogEvent, api.LogEventHistoryReindexing,
	)

	for height := lastNonSeenHeight; height <= indexHeight; height++ {
		// Index runtime events.
		var stakingEvs []*staking.Event
		stakingEvs, err = t.getRuntimeStakingEvents(runtimeID, height)
		if err != nil {
			logger.Error("querying runtime staking events",
				"height", height,
				"err", err,
			)
			return fmt.Errorf("failed to get staking events: %w", err)
		}
		if err = tr.blockHistory.CommitPendingConsensusEvents(height, stakingEvs); err != nil {
			logger.Error("committing pending consensus events", "err", err, "num_evens", len(stakingEvs))
			return fmt.Errorf("failed to set consensus events: %w", err)
		}

		crash.Here(crashPointBlockAfterConsensusEventsIndex)

		var annBlk *api.AnnotatedBlock
		var roundResults *api.RoundResults
		annBlk, roundResults, err = t.getRuntimeRoothashEvents(runtimeID, height)
		if err != nil {
			return fmt.Errorf("failed to query roothash events: %w", err)
		}
		if annBlk == nil {
			// No runtime round on this height, commit consensus height.
			logger.Debug("no finalized roothash round",
				"block", annBlk,
				"round_results", roundResults,
			)
			if err = tr.blockHistory.ConsensusCheckpoint(height); err != nil {
				return fmt.Errorf("failed to checkpoint consensus height: %w", err)
			}
			continue
		}

		// Commit runtime round.
		logger.Debug("commit runtime block",
			"runtime_id", runtimeID,
			"round_results", roundResults,
			"block", annBlk,
		)
		err = tr.blockHistory.Commit(annBlk, roundResults)
		if err != nil {
			logger.Error("failed to commit block to history keeper",
				"err", err,
				"block", annBlk,
				"round_results", roundResults,
			)
			return fmt.Errorf("failed to commit block to history keeper: %w", err)
		}
	}

	logger.Debug("indexing done")
	return nil
}

// Returns runtime relevant roothash event results at the provided height.
func (t *fullService) getRuntimeRoothashEvents(runtimeID common.Namespace, height int64) (*api.AnnotatedBlock, *api.RoundResults, error) {
	events, err := t.RootHash().GetEvents(t.ctx, height)
	if err != nil {
		return nil, nil, err
	}
	for _, ev := range events {
		// Skip non finalized events.
		if ev.Finalized == nil {
			continue
		}

		// Skip non tracked runtimes.
		if !runtimeID.Equal(&ev.RuntimeID) {
			continue
		}

		blk, err := t.RootHash().GetLatestBlock(t.ctx, runtimeID, height)
		if err != nil {
			return nil, nil, fmt.Errorf("roothash: failed to process finalized event: %w", err)
		}
		if blk.Header.Round != ev.Finalized.Round {
			t.Logger.Error("finalized event/query round mismatch",
				"block_round", blk.Header.Round,
				"event_round", ev.Finalized.Round,
			)
			return nil, nil, fmt.Errorf("roothash: finalized event/query round mismatch")
		}
		annBlk := &api.AnnotatedBlock{
			Height: height,
			Block:  blk,
		}
		var rtState *api.RuntimeState
		rtState, err = t.RootHash().GetRuntimeState(t.ctx, runtimeID, height)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to query runtime state: %w", err)
		}
		roundResults, err := t.RootHash().GetRoundResults(t.ctx, runtimeID, rtState.LastNormalHeight)
		if err != nil {
			return nil, nil, fmt.Errorf("roothash: failed to get round results: %w", err)
		}
		return annBlk, roundResults, err
	}

	return nil, nil, nil
}

// Returns runtime relevant staking events at the provided height.
func (t *fullService) getRuntimeStakingEvents(runtimeID common.Namespace, height int64) ([]*staking.Event, error) {
	events, err := t.Staking().GetEvents(t.ctx, height)
	if err != nil {
		return nil, fmt.Errorf("failed to query staking events: %w", err)
	}

	results := []*staking.Event{}
	rtAddress := staking.NewRuntimeAddress(runtimeID)

	// Filter events relevant to the provided runtime.
	for _, ev := range events {
		switch {
		case ev.Escrow != nil && ev.Escrow.Add != nil:
			if ev.Escrow.Add.Escrow.Equal(rtAddress) {
				results = append(results, ev)
				continue
			}
			if ev.Escrow.Add.Owner.Equal(rtAddress) {
				results = append(results, ev)
				continue
			}
		case ev.Escrow != nil && ev.Escrow.Reclaim != nil:
			if ev.Escrow.Reclaim.Escrow.Equal(rtAddress) {
				results = append(results, ev)
				continue
			}
			if ev.Escrow.Reclaim.Owner.Equal(rtAddress) {
				results = append(results, ev)
				continue
			}
		case ev.Escrow != nil && ev.Escrow.Take != nil:
			if ev.Escrow.Take.Owner.Equal(rtAddress) {
				results = append(results, ev)
				continue
			}
		case ev.AllowanceChange != nil:
			if ev.AllowanceChange.Beneficiary.Equal(rtAddress) {
				results = append(results, ev)
				continue
			}
			if ev.AllowanceChange.Owner.Equal(rtAddress) {
				results = append(results, ev)
				continue
			}
		case ev.Transfer != nil:
			if ev.Transfer.From.Equal(rtAddress) {
				results = append(results, ev)
				continue
			}
			if ev.Transfer.To.Equal(rtAddress) {
				results = append(results, ev)
				continue
			}
		case ev.Burn != nil:
			if ev.Burn.Owner.Equal(rtAddress) {
				results = append(results, ev)
			}
		}
	}
	return results, nil
}

func (t *fullService) runtimeIndexer() {
	ch, sub := t.WatchTendermintBlocks()
	defer sub.Close()

	for {
		var blk *tmtypes.Block
		select {
		case <-t.node.Quit():
			return
		case blk = <-ch:
		}

		// XXX: could do concurrently.
		for _, tr := range t.allTrackedRuntimes() {
			if err := t.indexRuntime(tr, blk.Height); err != nil {
				t.Logger.Error("runtime block indexer failure", "err", err)
				return
			}
		}
	}
}

func (t *fullService) GetLatestIndexedRuntimeBlock(id common.Namespace) (*api.AnnotatedBlock, error) {
	rt := t.getTrackedRuntime(id)
	if rt == nil {
		return nil, fmt.Errorf("runtime not tracked: %s", id.String())
	}

	return rt.blockHistory.GetLatestBlock(t.ctx)
}

func init() {
	crash.RegisterCrashPoints(
		crashPointBlockBeforeIndex,
		crashPointBlockAfterConsensusEventsIndex,
	)
}
