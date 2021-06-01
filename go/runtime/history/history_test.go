package history

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/quantity"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/block"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
)

const recvTimeout = 1 * time.Second

func TestHistory(t *testing.T) {
	require := require.New(t)

	// Create a new random temporary directory under /tmp.
	dataDir, err := ioutil.TempDir("", "oasis-runtime-history-test_")
	require.NoError(err, "TempDir")
	defer os.RemoveAll(dataDir)

	runtimeID := common.NewTestNamespaceFromSeed([]byte("history test ns 1"), 0)
	runtimeID2 := common.NewTestNamespaceFromSeed([]byte("history test ns 2"), 0)

	history, err := New(dataDir, runtimeID, NewDefaultConfig())
	require.NoError(err, "New")

	require.Equal(runtimeID, history.RuntimeID())

	lastHeight, err := history.LastConsensusHeight()
	require.NoError(err, "LastConsensusHeight")
	require.EqualValues(0, lastHeight)

	_, err = history.GetBlock(context.Background(), 10)
	require.Error(err, "GetBlock should fail for non-indexed block")
	require.Equal(roothash.ErrNotFound, err)

	_, err = history.GetLatestBlock(context.Background())
	require.Error(err, "GetLatestBlock should fail for no indexed block")
	require.Equal(roothash.ErrNotFound, err)

	err = history.ConsensusCheckpoint(42)
	require.NoError(err, "ConsensusCheckpoint")
	err = history.ConsensusCheckpoint(40)
	require.Error(err, "ConsensusCheckpoint should fail for lower height")

	lastHeight, err = history.LastConsensusHeight()
	require.NoError(err, "LastConsensusHeight")
	require.EqualValues(42, lastHeight)

	blk := roothash.AnnotatedBlock{
		Height: 40,
		Block:  block.NewGenesisBlock(runtimeID, 0),
	}
	blk.Block.Header.Round = 10

	roundResults := &roothash.RoundResults{
		Messages: []*roothash.MessageEvent{
			{Module: "", Code: 0, Index: 0},
			{Module: "", Code: 0, Index: 1},
		},
	}

	stakingEvents := []*staking.Event{
		{Transfer: &staking.TransferEvent{
			To:     staking.NewRuntimeAddress(runtimeID),
			Amount: *quantity.NewFromUint64(100),
		}},
	}
	err = history.CommitPendingConsensusEvents(blk.Height, stakingEvents)
	require.Error(err, "CommitPendingConsensusEvents should fail for lower consensus height")

	err = history.Commit(&blk, roundResults)
	require.Error(err, "Commit should fail for lower consensus height")

	blk.Height = 50
	copy(blk.Block.Header.Namespace[:], runtimeID2[:])
	err = history.Commit(&blk, roundResults)
	require.Error(err, "Commit should fail for different runtime")

	err = history.CommitPendingConsensusEvents(blk.Height, stakingEvents)
	require.NoError(err, "CommitPendingConsensusEvents")
	copy(blk.Block.Header.Namespace[:], runtimeID[:])
	err = history.Commit(&blk, roundResults)
	require.NoError(err, "Commit")
	putBlk := *blk.Block
	err = history.Commit(&blk, roundResults)
	require.Error(err, "Commit should fail for the same round")
	blk.Block.Header.Round = 5
	err = history.Commit(&blk, roundResults)
	require.Error(err, "Commit should fail for a lower round")

	lastHeight, err = history.LastConsensusHeight()
	require.NoError(err, "LastConsensusHeight")
	require.EqualValues(50, lastHeight)

	gotBlk, err := history.GetBlock(context.Background(), 10)
	require.NoError(err, "GetBlock")
	require.Equal(&putBlk, gotBlk.Block, "GetBlock should return the correct block")

	gotLatestBlk, err := history.GetLatestBlock(context.Background())
	require.NoError(err, "GetLatestBlock")
	require.Equal(&putBlk, gotLatestBlk.Block, "GetLatestBlock should return the correct block")

	gotResults, err := history.GetRoundResults(context.Background(), 10)
	require.NoError(err, "GetRoundResults")
	require.Equal(roundResults, gotResults, "GetRoundResults should return the correct results")

	gotEvents, err := history.GetRoundEvents(context.Background(), 10)
	require.NoError(err, "GetRoundEvents")
	require.Equal(stakingEvents, gotEvents, "GetRoundEvents should return the correct results")

	// Close history and try to reopen and continue.
	history.Close()

	// Try to manually load the block index database with incorrect runtime ID.
	// Use path from the first runtime.
	_, err = New(dataDir, runtimeID2, NewDefaultConfig())
	require.Error(err, "New should return an error on runtime mismatch")

	history, err = New(dataDir, runtimeID, NewDefaultConfig())
	require.NoError(err, "New")

	require.Equal(runtimeID, history.RuntimeID())

	lastHeight, err = history.LastConsensusHeight()
	require.NoError(err, "LastConsensusHeight")
	require.EqualValues(50, lastHeight)

	gotBlk, err = history.GetBlock(context.Background(), 10)
	require.NoError(err, "GetBlock")
	require.Equal(&putBlk, gotBlk.Block, "GetBlock should return the correct block")

	gotLatestBlk, err = history.GetLatestBlock(context.Background())
	require.NoError(err, "GetLatestBlock")
	require.Equal(&putBlk, gotLatestBlk.Block, "GetLatestBlock should return the correct block")

	gotResults, err = history.GetRoundResults(context.Background(), 10)
	require.NoError(err, "GetRoundResults")
	require.Equal(roundResults, gotResults, "GetRoundResults should return the correct results")
	// TODO: some more consensus events tests.
}

type testPruneHandler struct {
	done         bool
	doneCh       chan struct{}
	waitRounds   int
	prunedRounds []uint64
	batches      []int
}

func (h *testPruneHandler) Prune(ctx context.Context, rounds []uint64) error {
	// NOTE: Users must ensure that accessing prunedRounds is safe (e.g., that
	//       no more pruning happens using this handler before prunedRounds is
	//       accessed from a different goroutine).
	if h.done {
		panic("pruned more rounds than specified in waitRounds")
	}

	h.batches = append(h.batches, len(rounds))
	h.prunedRounds = append(h.prunedRounds, rounds...)
	if len(h.prunedRounds) >= h.waitRounds {
		close(h.doneCh)
		h.done = true
	}
	return nil
}

func TestHistoryPrune(t *testing.T) {
	require := require.New(t)

	// Create a new random temporary directory under /tmp.
	dataDir, err := ioutil.TempDir("", "oasis-runtime-history-test_")
	require.NoError(err, "TempDir")
	defer os.RemoveAll(dataDir)

	runtimeID := common.NewTestNamespaceFromSeed([]byte("history prune test ns"), 0)

	history, err := New(dataDir, runtimeID, &Config{
		Pruner:        NewKeepLastPruner(10),
		PruneInterval: 100 * time.Millisecond,
	})
	require.NoError(err, "New")
	defer history.Close()

	ph := testPruneHandler{
		doneCh:     make(chan struct{}),
		waitRounds: 41,
	}
	history.Pruner().RegisterHandler(&ph)

	// Create some blocks.
	for i := 0; i <= 50; i++ {
		blk := roothash.AnnotatedBlock{
			Height: int64(i),
			Block:  block.NewGenesisBlock(runtimeID, 0),
		}
		blk.Block.Header.Round = uint64(i)

		var msgResults []*roothash.MessageEvent
		if i%5 == 0 {
			msgResults = []*roothash.MessageEvent{
				{Module: "", Code: 0, Index: 0},
				{Module: "", Code: 0, Index: 1},
			}
		}
		var stakingEvents []*staking.Event
		if i%3 == 0 {
			stakingEvents = []*staking.Event{
				{Transfer: &staking.TransferEvent{
					To:     staking.NewRuntimeAddress(runtimeID),
					Amount: *quantity.NewFromUint64(100),
				}},
			}
		}
		err = history.CommitPendingConsensusEvents(blk.Height, stakingEvents)
		require.NoError(err, "CommitPendingConsensusEvents")

		roundResults := &roothash.RoundResults{
			Messages: msgResults,
		}

		err = history.Commit(&blk, roundResults)
		require.NoError(err, "Commit")
	}

	// No more blocks after this point.

	// Wait for pruning to complete.
	select {
	case <-ph.doneCh:
	case <-time.After(recvTimeout):
		t.Fatalf("failed to wait for prune to complete")
	}

	// Wait until the pruning transaction has been committed. This is needed because doneCh may be
	// closed before the database transaction commits so the pruning may not yet be visible.
	ctx, cancel := context.WithTimeout(context.Background(), recvTimeout)
	defer cancel()
	for {
		_, err = history.GetBlock(ctx, 0)
		if err == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		require.Error(err, "GetBlock should fail for pruned block 0")
		require.Equal(roothash.ErrNotFound, err)
		break
	}

	// Ensure we can only lookup the last 10 blocks.
	for i := 0; i <= 50; i++ {
		_, err = history.GetBlock(context.Background(), uint64(i))
		if i <= 40 {
			require.Error(err, "GetBlock should fail for pruned block %d", i)
			require.Equal(roothash.ErrNotFound, err)
		} else {
			require.NoError(err, "GetBlock(%d)", i)
		}

		roundResults, err := history.GetRoundResults(context.Background(), uint64(i))
		if i <= 40 {
			require.Error(err, "GetRoundResults(%d)", i)
			require.Equal(roothash.ErrNotFound, err)
		} else if i%5 == 0 {
			require.NoError(err, "GetRoundResults(%d)", i)
			require.NotEmpty(roundResults.Messages, "GetRoundResults should return correct results for block %d", i)
		}
	}

	// Ensure the prune handler was called.
	require.Len(ph.prunedRounds, 41)
	for i := 0; i <= 40; i++ {
		require.EqualValues(ph.prunedRounds[i], i)
	}
}

type testPruneFailingHandler struct {
}

func (h *testPruneFailingHandler) Prune(ctx context.Context, rounds []uint64) error {
	return fmt.Errorf("thou shall not pass")
}

func TestHistoryPruneError(t *testing.T) {
	require := require.New(t)

	// Create a new random temporary directory under /tmp.
	dataDir, err := ioutil.TempDir("", "oasis-runtime-history-test_")
	require.NoError(err, "TempDir")
	defer os.RemoveAll(dataDir)

	runtimeID := common.NewTestNamespaceFromSeed([]byte("history prune error test ns"), 0)

	history, err := New(dataDir, runtimeID, &Config{
		Pruner:        NewKeepLastPruner(10),
		PruneInterval: 100 * time.Millisecond,
	})
	require.NoError(err, "New")
	defer history.Close()

	var ph testPruneFailingHandler
	history.Pruner().RegisterHandler(&ph)

	// Create some blocks.
	for i := 0; i <= 50; i++ {
		blk := roothash.AnnotatedBlock{
			Height: int64(i),
			Block:  block.NewGenesisBlock(runtimeID, 0),
		}
		blk.Block.Header.Round = uint64(i)

		err = history.Commit(&blk, nil)
		require.NoError(err, "Commit")
	}

	// Wait for some pruning.
	time.Sleep(200 * time.Millisecond)

	// Ensure nothing was pruned.
	for i := 0; i <= 50; i++ {
		_, err = history.GetBlock(context.Background(), uint64(i))
		require.NoError(err, "GetBlock(%d)", i)
	}
}
