// Package api implements the Oasis timekeeping API and common types.
package api

import (
	"context"
	"fmt"

	fuzz "github.com/google/gofuzz"

	"github.com/oasislabs/oasis-core/go/common/pubsub"
	"github.com/oasislabs/oasis-core/go/oasis-node/cmd/common/flags"
)

// EpochTime is the number of intervals (epochs) since a fixed instant
// in time (epoch date).
type EpochTime uint64

func (et *EpochTime) Fuzz(c fuzz.Continue) {
	*et = EpochTime(c.Uint64())
}

// EpochInvalid is the placeholder invalid epoch.
const EpochInvalid EpochTime = 0xffffffffffffffff // ~50 quadrillion years away.

// Backend is a timekeeping implementation.
type Backend interface {
	// GetBaseEpoch returns the base epoch.
	GetBaseEpoch(context.Context) (EpochTime, error)

	// GetEpoch returns the epoch number at the specified block height.
	// Calling this method with height `0`, should return the
	// epoch of latest known block.
	GetEpoch(context.Context, int64) (EpochTime, error)

	// GetEpochBlock returns the block height at the start of the said
	// epoch.
	GetEpochBlock(context.Context, EpochTime) (int64, error)

	// WatchEpochs returns a channel that produces a stream of messages
	// on epoch transitions.
	//
	// Upon subscription the current epoch is sent immediately.
	WatchEpochs() (<-chan EpochTime, *pubsub.Subscription)

	// StateToGenesis returns the genesis state at the specified block height.
	StateToGenesis(ctx context.Context, height int64) (*Genesis, error)
}

// SetableBackend is a Backend that supports setting the current epoch.
type SetableBackend interface {
	Backend

	// SetEpoch sets the current epoch.
	SetEpoch(context.Context, EpochTime) error
}

// Genesis is the initial genesis state for allowing configurable timekeeping.
type Genesis struct {
	// Parameters are the epochtime consensus parameters.
	Parameters ConsensusParameters `json:"params"`

	// Base is the starting epoch.
	Base EpochTime `json:"base"`
}

// ConsensusParameters are the epochtime consensus parameters.
type ConsensusParameters struct {
	// Interval is the epoch interval (in blocks).
	Interval int64 `json:"interval"`

	// DebugMockBackend is flag for enabling mock epochtime backend.
	DebugMockBackend bool `json:"debug_mock_backend"`
}

// SanityCheck does basic sanity checking on the genesis state.
func (g *Genesis) SanityCheck() error {
	unsafeFlags := g.Parameters.DebugMockBackend
	if unsafeFlags && !flags.DebugDontBlameOasis() {
		return fmt.Errorf("epochtime: sanity check failed: one or more unsafe debug flags set")
	}

	if g.Parameters.Interval <= 0 && !g.Parameters.DebugMockBackend {
		return fmt.Errorf("epochtime: sanity check failed: epoch interval must be > 0")
	}

	if g.Base == EpochInvalid {
		return fmt.Errorf("epochtime: sanity check failed: starting epoch is invalid")
	}

	return nil
}
