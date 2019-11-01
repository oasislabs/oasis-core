// Package api defines the committee scheduler API.
package api

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/oasislabs/oasis-core/go/common/crypto/hash"
	"github.com/oasislabs/oasis-core/go/common/crypto/signature"
	"github.com/oasislabs/oasis-core/go/common/pubsub"
	epochtime "github.com/oasislabs/oasis-core/go/epochtime/api"
	staking "github.com/oasislabs/oasis-core/go/staking/api"
)

var (
	// ErrNilProtobuf is the error returned when a protobuf is nil.
	ErrNilProtobuf = errors.New("scheduler: protobuf is nil")

	// ErrInvalidRole is the error returned when a role is invalid.
	ErrInvalidRole = errors.New("scheduler: invalid role")
)

// Role is the role a given node plays in a committee.
type Role uint8

// TODO: Rename these to include the Role prefix.
const (
	// Invalid is an invalid role (should never appear on the wire).
	Invalid Role = 0

	// Worker indicates the node is a worker.
	Worker Role = 1

	// BackupWorker indicates the node is a backup worker.
	BackupWorker Role = 2

	// Leader indicates the node is a group leader.
	Leader Role = 3
)

// RewardFactorEpochElectionAny is the factor for a reward
// distributed per epoch to entities that have any node considered
// in any election.
var RewardFactorEpochElectionAny *staking.Quantity

// String returns a string representation of a Role.
func (r Role) String() string {
	switch r {
	case Invalid:
		return "invalid"
	case Worker:
		return "worker"
	case BackupWorker:
		return "backup worker"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("unknown role: %d", r)
	}
}

// CommitteeNode is a node participating in a committee.
type CommitteeNode struct {
	// Role is the node's role in a committee.
	Role Role `json:"role"`

	// PublicKey is the node's public key.
	PublicKey signature.PublicKey `json:"public_key"`
}

// CommitteeKind is the functionality a committee exists to provide.
type CommitteeKind uint8

const (
	// KindCompute is a compute committee.
	KindCompute CommitteeKind = 0

	// KindStorage is a storage committee.
	KindStorage CommitteeKind = 1

	// KindTransactionScheduler is a transaction scheduler committee.
	KindTransactionScheduler CommitteeKind = 2

	// KindMerge is a merge committee.
	KindMerge CommitteeKind = 3

	// MaxCommitteeKind is a dummy value used for iterating all committee kinds.
	MaxCommitteeKind = 4
)

// NeedsLeader returns if committee kind needs leader role.
func (k CommitteeKind) NeedsLeader() bool {
	switch k {
	case KindCompute:
		return false
	case KindMerge:
		return false
	case KindStorage:
		return false
	default:
		return true
	}
}

// String returns a string representation of a CommitteeKind.
func (k CommitteeKind) String() string {
	switch k {
	case KindCompute:
		return "compute"
	case KindStorage:
		return "storage"
	case KindTransactionScheduler:
		return "transaction"
	case KindMerge:
		return "merge"
	default:
		return fmt.Sprintf("unknown kind: %d", k)
	}
}

// Committee is a per-runtime (instance) committee.
type Committee struct {
	// Kind is the functionality a committee exists to provide.
	Kind CommitteeKind `json:"kind"`

	// Members is the committee members.
	Members []*CommitteeNode `json:"members"`

	// RuntimeID is the runtime ID that this committee is for.
	RuntimeID signature.PublicKey `json:"runtime_id"`

	// ValidFor is the epoch for which the committee is valid.
	ValidFor epochtime.EpochTime `json:"valid_for"`
}

// String returns a string representation of a Committee.
func (c Committee) String() string {
	members := make([]string, len(c.Members))
	for i, m := range c.Members {
		members[i] = fmt.Sprintf("%+v", m)
	}
	return fmt.Sprintf("&{Kind:%v Members:[%v] RuntimeID:%v ValidFor:%v}", c.Kind, strings.Join(members, " "), c.RuntimeID, c.ValidFor)
}

// EncodedMembersHash returns the encoded cryptographic hash of the committee members.
func (c *Committee) EncodedMembersHash() hash.Hash {
	var hh hash.Hash

	hh.From(c.Members)

	return hh
}

// Backend is a scheduler implementation.
type Backend interface {
	// GetCommittees returns the vector of committees for a given
	// runtime ID, at the specified block height, and optional callback
	// for querying the beacon for a given epoch/block height.
	//
	// Iff the callback is nil, `beacon.GetBlockBeacon` will be used.
	GetCommittees(context.Context, signature.PublicKey, int64) ([]*Committee, error)

	// WatchCommittees returns a channel that produces a stream of
	// Committee.
	//
	// Upon subscription, all committees for the current epoch will
	// be sent immediately.
	WatchCommittees() (<-chan *Committee, *pubsub.Subscription)

	// ToGenesis returns the genesis state at specified block height.
	ToGenesis(context.Context, int64) (*Genesis, error)

	// Cleanup cleans up the scheduler backend.
	Cleanup()
}

// Genesis is the committee scheduler genesis state.
type Genesis struct {
	// Parameters are the scheduler consensus parameters.
	Parameters ConsensusParameters `json:"params"`
}

// ConsensusParameters are the scheduler consensus parameters.
type ConsensusParameters struct {
	// DebugBypassStake is true iff the scheduler should bypass all of
	// the staking related checks and operations.
	DebugBypassStake bool `json:"debug_bypass_stake"`

	// DebugStaticValidators is true iff the scheduler should use
	// a static validator set instead of electing anything.
	DebugStaticValidators bool `json:"debug_static_validators"`
}

func init() {
	RewardFactorEpochElectionAny = staking.NewQuantity()
	err := RewardFactorEpochElectionAny.FromBigInt(big.NewInt(1))
	if err != nil {
		panic(err)
	}
}
