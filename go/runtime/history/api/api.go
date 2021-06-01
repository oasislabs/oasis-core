package api

import (
	"context"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/pubsub"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
)

// BlockHistory is the root hash block history keeper interface.
//
// All methods operate on a specific runtime.
type BlockHistory interface {
	// RuntimeID returns the runtime ID of the runtime this block history is for.
	RuntimeID() common.Namespace

	// Commit commits an indexed block into history.
	//
	// Automatically checkpoints the consensus height on the provided annotated
	// block.
	//
	// Must be called in order, sorted by round.
	Commit(blk *roothash.AnnotatedBlock, roundResults *roothash.RoundResults) error

	// CommitPendingConsensusEvents commits pending runtime consensus events into history.
	//
	// On next roothash block Commit all pending consensus events will get committed
	// as round events, and removed from pending queue.
	//
	// The consensus height is not automatically checkpointed, this should be done
	// separately by invoking either Commit or ConsensusCheckpoint methods.
	//
	// Cannot be called for height <= last committed/checkpointed height.
	CommitPendingConsensusEvents(height int64, stakingEvents []*staking.Event) error

	// ConsensusCheckpoint records the last consensus height which was processed
	// by the roothash backend.
	//
	// This method can only be called once all roothash blocks and consensus events
	// for consensus heights <= height have been committed.
	ConsensusCheckpoint(height int64) error

	// LastConsensusHeight returns the last consensus height which was seen
	// by block history.
	LastConsensusHeight() (int64, error)

	// GetBlock returns the block at a specific round.
	GetBlock(ctx context.Context, round uint64) (*roothash.AnnotatedBlock, error)

	// GetLatestBlock returns the block at latest round.
	GetLatestBlock(ctx context.Context) (*roothash.AnnotatedBlock, error)

	// GetRoundResults returns the round results for the given round.
	GetRoundResults(ctx context.Context, round uint64) (*roothash.RoundResults, error)

	// GetRoundEvents returns the events for the given round.
	GetRoundEvents(ctx context.Context, round uint64) ([]*staking.Event, error)

	// WatchBlocks returns a channel that produces a stream of indexed blocks.
	//
	// The latest block if any will get pushed to the stream immediately.
	// Subsequent blocks will be pushed into the stream as they are indexed.
	WatchBlocks(ctx context.Context) (<-chan *roothash.AnnotatedBlock, *pubsub.Subscription, error)
}
