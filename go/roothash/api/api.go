// Package api implements the root hash backend API and common datastructures.
package api

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/errors"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/common/pubsub"
	"github.com/oasisprotocol/oasis-core/go/consensus/api/transaction"
	"github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common/flags"
	registry "github.com/oasisprotocol/oasis-core/go/registry/api"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/block"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/commitment"
)

const (
	// ModuleName is a unique module name for the roothash module.
	ModuleName = "roothash"

	// RoundInvalid is a special round number that refers to an invalid round.
	RoundInvalid uint64 = math.MaxUint64

	// LogEventExecutionDiscrepancyDetected is a log event value that signals
	// an execution discrepancy has been detected.
	LogEventExecutionDiscrepancyDetected = "roothash/execution_discrepancy_detected"
	// LogEventTimerFired is a log event value that signals a timer has fired.
	LogEventTimerFired = "roothash/timer_fired"
	// LogEventRoundFailed is a log event value that signals a round has failed.
	LogEventRoundFailed = "roothash/round_failed"
	// LogEventMessageUnsat is a log event value that signals a roothash message was not satisfactory.
	LogEventMessageUnsat = "roothash/message_unsat"
	// LogEventHistoryReindexing is a log event value that signals a roothash runtime reindexing
	// was run.
	LogEventHistoryReindexing = "roothash/history_reindexing"
)

var (
	// ErrInvalidArgument is the error returned on malformed argument(s).
	ErrInvalidArgument = errors.New(ModuleName, 1, "roothash: invalid argument")

	// ErrNotFound is the error returned when a block is not found.
	ErrNotFound = errors.New(ModuleName, 2, "roothash: block not found")

	// ErrInvalidRuntime is the error returned when the passed runtime is invalid.
	ErrInvalidRuntime = errors.New(ModuleName, 3, "roothash: invalid runtime")

	// ErrNoExecutorPool is the error returned when there is no executor pool.
	ErrNoExecutorPool = errors.New(ModuleName, 4, "roothash: no executor pool")

	// ErrRuntimeSuspended is the error returned when the passed runtime is suspended.
	ErrRuntimeSuspended = errors.New(ModuleName, 5, "roothash: runtime is suspended")

	// ErrProposerTimeoutNotAllowed is the error returned when proposer timeout is not allowed.
	ErrProposerTimeoutNotAllowed = errors.New(ModuleName, 6, "roothash: proposer timeout not allowed")

	// ErrMaxMessagesTooBig is the error returned when the MaxMessages parameter is set to a value
	// larger than the MaxRuntimeMessages specified in consensus parameters.
	ErrMaxMessagesTooBig = errors.New(ModuleName, 7, "roothash: max runtime messages is too big")

	// ErrRuntimeDoesNotSlash is the error returned when misbehaviour evidence is submitted for a
	// runtime that does not slash.
	ErrRuntimeDoesNotSlash = errors.New(ModuleName, 8, "roothash: runtime does not slash")

	// ErrDuplicateEvidence is the error returned when submitting already existing evidence.
	ErrDuplicateEvidence = errors.New(ModuleName, 9, "roothash: duplicate evidence")

	// ErrInvalidEvidence is the error return when an invalid evidence is submitted.
	ErrInvalidEvidence = errors.New(ModuleName, 10, "roothash: invalid evidence")

	// MethodExecutorCommit is the method name for executor commit submission.
	MethodExecutorCommit = transaction.NewMethodName(ModuleName, "ExecutorCommit", ExecutorCommit{})

	// MethodExecutorProposerTimeout is the method name for executor proposer timeout.
	MethodExecutorProposerTimeout = transaction.NewMethodName(ModuleName, "ExecutorProposerTimeout", ExecutorProposerTimeoutRequest{})

	// MethodEvidence is the method name for submitting evidence of node misbehavior.
	MethodEvidence = transaction.NewMethodName(ModuleName, "Evidence", Evidence{})

	// Methods is a list of all methods supported by the roothash backend.
	Methods = []transaction.MethodName{
		MethodExecutorCommit,
		MethodExecutorProposerTimeout,
		MethodEvidence,
	}
)

// Backend is a root hash implementation.
type Backend interface {
	// GetGenesisBlock returns the genesis block.
	GetGenesisBlock(ctx context.Context, request *RuntimeRequest) (*block.Block, error)

	// GetLatestBlock returns the latest block.
	//
	// The metadata contained in this block can be further used to get
	// the latest state from the storage backend.
	GetLatestBlock(ctx context.Context, request *RuntimeRequest) (*block.Block, error)

	// GetRuntimeState returns the given runtime's state.
	GetRuntimeState(ctx context.Context, request *RuntimeRequest) (*RuntimeState, error)

	// WatchBlocks returns a channel that produces a stream of
	// annotated blocks.
	//
	// The latest block if any will get pushed to the stream immediately.
	// Subsequent blocks will be pushed into the stream as they are
	// confirmed.
	WatchBlocks(ctx context.Context, runtimeID common.Namespace) (<-chan *AnnotatedBlock, pubsub.ClosableSubscription, error)

	// WatchEvents returns a stream of protocol events.
	WatchEvents(ctx context.Context, runtimeID common.Namespace) (<-chan *Event, pubsub.ClosableSubscription, error)

	// TrackRuntime adds a runtime the history of which should be tracked.
	TrackRuntime(ctx context.Context, history BlockHistory) error

	// StateToGenesis returns the genesis state at specified block height.
	StateToGenesis(ctx context.Context, height int64) (*Genesis, error)

	// GetEvents returns the events at specified block height.
	GetEvents(ctx context.Context, height int64) ([]*Event, error)

	// Cleanup cleans up the roothash backend.
	Cleanup()
}

// RuntimeRequest is a generic roothash get request for a specific runtime.
type RuntimeRequest struct {
	RuntimeID common.Namespace `json:"runtime_id"`
	Height    int64            `json:"height"`
}

// ExecutorCommit is the argument set for the ExecutorCommit method.
type ExecutorCommit struct {
	ID      common.Namespace                `json:"id"`
	Commits []commitment.ExecutorCommitment `json:"commits"`
}

// NewExecutorCommitTx creates a new executor commit transaction.
func NewExecutorCommitTx(nonce uint64, fee *transaction.Fee, runtimeID common.Namespace, commits []commitment.ExecutorCommitment) *transaction.Transaction {
	return transaction.NewTransaction(nonce, fee, MethodExecutorCommit, &ExecutorCommit{
		ID:      runtimeID,
		Commits: commits,
	})
}

// ExecutorProposerTimeoutRequest is an executor proposer timeout request.
type ExecutorProposerTimeoutRequest struct {
	ID    common.Namespace `json:"id"`
	Round uint64           `json:"round"`
}

// NewRequestProposerTimeoutTx creates a new request proposer timeout transaction.
func NewRequestProposerTimeoutTx(nonce uint64, fee *transaction.Fee, runtimeID common.Namespace, round uint64) *transaction.Transaction {
	return transaction.NewTransaction(nonce, fee, MethodExecutorProposerTimeout, &ExecutorProposerTimeoutRequest{
		ID:    runtimeID,
		Round: round,
	})
}

// EvidenceKind is the evidence kind.
type EvidenceKind uint8

const (
	// EvidenceKindEquivocation is the evidence kind for equivocation.
	EvidenceKindEquivocation = 1
)

// Evidence is an evidence of node misbehaviour.
type Evidence struct {
	ID common.Namespace `json:"id"`

	EquivocationExecutor *EquivocationExecutorEvidence `json:"equivocation_executor,omitempty"`
	EquivocationBatch    *EquivocationBatchEvidence    `json:"equivocation_batch,omitempty"`
}

// Hash computes the evidence hash.
//
// Hash is derived by hashing the evidence kind and the public key of the signer.
// Assumes evidence has been validated.
func (ev *Evidence) Hash() (hash.Hash, error) {
	switch {
	case ev.EquivocationBatch != nil:
		return hash.NewFromBytes([]byte{EvidenceKindEquivocation}, ev.EquivocationBatch.BatchA.Signature.PublicKey[:]), nil
	case ev.EquivocationExecutor != nil:
		return hash.NewFromBytes([]byte{EvidenceKindEquivocation}, ev.EquivocationExecutor.CommitA.Signature.PublicKey[:]), nil
	default:
		return hash.Hash{}, fmt.Errorf("cannot compute hash, invalid evidence")
	}
}

// ValidateBasic performs basic evidence validity checks.
func (ev *Evidence) ValidateBasic() error {
	switch {
	case ev.EquivocationExecutor != nil && ev.EquivocationBatch != nil:
		return fmt.Errorf("evidence has multiple fields set")
	case ev.EquivocationExecutor != nil:
		return ev.EquivocationExecutor.ValidateBasic(ev.ID)
	case ev.EquivocationBatch != nil:
		return ev.EquivocationBatch.ValidateBasic(ev.ID)
	default:
		return fmt.Errorf("evidence content has no fields set")
	}
}

// EquivocationExecutorEvidence is evidence of executor commitment equivocation.
type EquivocationExecutorEvidence struct {
	CommitA commitment.ExecutorCommitment `json:"commit_a"`
	CommitB commitment.ExecutorCommitment `json:"commit_b"`
}

// ValidateBasic performs stateless executor evidence validation checks.
//
// Particularly evidence is not verified to not be expired as this requires stateful checks.
func (ev *EquivocationExecutorEvidence) ValidateBasic(id common.Namespace) error {
	if ev.CommitA.Equal(&ev.CommitB) {
		return fmt.Errorf("commits are equal, no sign of equivocation")
	}

	if !ev.CommitA.Signature.PublicKey.Equal(ev.CommitB.Signature.PublicKey) {
		return fmt.Errorf("equivocation executor evidence signature public keys don't match")
	}

	a, err := ev.CommitA.Open(id)
	if err != nil {
		return fmt.Errorf("opening CommitA: %w", err)
	}
	b, err := ev.CommitB.Open(id)
	if err != nil {
		return fmt.Errorf("opening CommitB: %w", err)
	}

	if a.Body.Header.Round != b.Body.Header.Round {
		return fmt.Errorf("equivocation evidence commit headers not for same round")
	}

	if err := a.Body.ValidateBasic(); err != nil {
		return fmt.Errorf("equivocation evidence commit A not valid: %w", err)
	}
	if err := b.Body.ValidateBasic(); err != nil {
		return fmt.Errorf("equivocation evidence commit B not valid: %w", err)
	}

	switch {
	// Note: ValidBasics checks above ensure that none of these fields are nil.
	case a.Body.Failure == commitment.FailureNone && b.Body.Failure == commitment.FailureNone:
		if a.Body.Header.PreviousHash.Equal(&b.Body.Header.PreviousHash) &&
			a.Body.Header.IORoot.Equal(b.Body.Header.IORoot) &&
			a.Body.Header.StateRoot.Equal(b.Body.Header.StateRoot) &&
			a.Body.Header.MessagesHash.Equal(b.Body.Header.MessagesHash) {
			return fmt.Errorf("equivocation evidence commit headers match, no sign of equivocation")
		}
	default:
		if a.Body.Failure == b.Body.Failure {
			return fmt.Errorf("equivocation evidence failure indication fields match, no sign of equivocation")
		}
	}

	return nil
}

// EquivocationBatchEvidence is evidence of executor proposed batch equivocation.
type EquivocationBatchEvidence struct {
	BatchA commitment.SignedProposedBatch `json:"batch_a"`
	BatchB commitment.SignedProposedBatch `json:"batch_b"`
}

// ValidateBasic performs stateless batch evidence validation checks.
//
// Particularly evidence is not verified to not be expired as this requires stateful checks.
func (ev *EquivocationBatchEvidence) ValidateBasic(id common.Namespace) error {
	if ev.BatchA.Equal(&ev.BatchB) {
		return fmt.Errorf("batches are equal, no sign of equivocation")
	}

	if !ev.BatchA.Signature.PublicKey.Equal(ev.BatchB.Signature.PublicKey) {
		return fmt.Errorf("equivocation batch evidence signature public keys don't match")
	}

	var a, b commitment.ProposedBatch
	if err := ev.BatchA.Open(&a, id); err != nil {
		return fmt.Errorf("opening BatchA: %w", err)
	}
	if err := ev.BatchB.Open(&b, id); err != nil {
		return fmt.Errorf("opening BatchB: %w", err)
	}

	if a.Header.Round != b.Header.Round {
		return fmt.Errorf("equivocation evidence batch header rounds don't match")
	}

	if a.IORoot.Equal(&b.IORoot) && a.Header.MostlyEqual(&b.Header) {
		return fmt.Errorf("equivocation evidence batch io roots match, no sign of equivocation")
	}

	return nil
}

// NewEvidenceTx creates a new evidence transaction.
func NewEvidenceTx(nonce uint64, fee *transaction.Fee, evidence *Evidence) *transaction.Transaction {
	return transaction.NewTransaction(nonce, fee, MethodEvidence, evidence)
}

// RuntimeState is the per-runtime state.
type RuntimeState struct {
	Runtime   *registry.Runtime `json:"runtime"`
	Suspended bool              `json:"suspended,omitempty"`

	GenesisBlock *block.Block `json:"genesis_block"`

	CurrentBlock       *block.Block `json:"current_block"`
	CurrentBlockHeight int64        `json:"current_block_height"`

	// LastNormalRound is the runtime round which was normally processed by the runtime. This is
	// also the round that contains the message results for the last processed runtime messages.
	LastNormalRound uint64 `json:"last_normal_round"`
	// LastNormalHeight is the consensus block height corresponding to LastNormalRound.
	LastNormalHeight int64 `json:"last_normal_height"`

	ExecutorPool *commitment.Pool `json:"executor_pool"`
}

// AnnotatedBlock is an annotated roothash block.
type AnnotatedBlock struct {
	// Height is the underlying roothash backend's block height that
	// generated this block.
	Height int64 `json:"consensus_height"`

	// Block is the roothash block.
	Block *block.Block `json:"block"`
}

// ExecutorCommittedEvent is an event emitted each time an executor node commits.
type ExecutorCommittedEvent struct {
	// Commit is the executor commitment.
	Commit commitment.ExecutorCommitment `json:"commit"`
}

// ExecutionDiscrepancyDetectedEvent is an execute discrepancy detected event.
type ExecutionDiscrepancyDetectedEvent struct {
	// Timeout signals whether the discrepancy was due to a timeout.
	Timeout bool `json:"timeout"`
}

// FinalizedEvent is a finalized event.
type FinalizedEvent struct {
	// Round is the round that was finalized.
	Round uint64 `json:"round"`

	// GoodComputeNodes are the public keys of compute nodes that positively contributed to the
	// round by replicating the computation correctly.
	GoodComputeNodes []signature.PublicKey `json:"good_compute_nodes,omitempty"`

	// BadComputeNodes are the public keys of compute nodes that negatively contributed to the round
	// by causing discrepancies.
	BadComputeNodes []signature.PublicKey `json:"bad_compute_nodes,omitempty"`
}

// MessageEvent is a runtime message processed event.
type MessageEvent struct {
	Module string `json:"module,omitempty"`
	Code   uint32 `json:"code,omitempty"`
	Index  uint32 `json:"index,omitempty"`
}

// IsSuccess returns true if the event indicates that the message was successfully processed.
func (me *MessageEvent) IsSuccess() bool {
	return me.Code == errors.CodeNoError
}

// Event is a roothash event.
type Event struct {
	Height int64     `json:"height,omitempty"`
	TxHash hash.Hash `json:"tx_hash,omitempty"`

	RuntimeID common.Namespace `json:"runtime_id"`

	ExecutorCommitted            *ExecutorCommittedEvent            `json:"executor_committed,omitempty"`
	ExecutionDiscrepancyDetected *ExecutionDiscrepancyDetectedEvent `json:"execution_discrepancy,omitempty"`
	Finalized                    *FinalizedEvent                    `json:"finalized,omitempty"`
	Message                      *MessageEvent                      `json:"message,omitempty"`
}

// MetricsMonitorable is the interface exposed by backends capable of
// providing metrics data.
type MetricsMonitorable interface {
	// WatchAllBlocks returns a channel that produces a stream of blocks.
	//
	// All blocks from all tracked runtimes will be pushed into the stream
	// immediately as they are finalized.
	WatchAllBlocks() (<-chan *block.Block, *pubsub.Subscription)
}

// GenesisRuntimeState contains state for runtimes that are restored in a genesis block.
type GenesisRuntimeState struct {
	registry.RuntimeGenesis

	// MessageResults are the message results emitted at the last processed round.
	MessageResults []*MessageEvent `json:"message_results,omitempty"`
}

// Genesis is the roothash genesis state.
type Genesis struct {
	// Parameters are the roothash consensus parameters.
	Parameters ConsensusParameters `json:"params"`

	// RuntimeStates are the runtime states at genesis.
	RuntimeStates map[common.Namespace]*GenesisRuntimeState `json:"runtime_states,omitempty"`
}

// ConsensusParameters are the roothash consensus parameters.
type ConsensusParameters struct {
	// GasCosts are the roothash transaction gas costs.
	GasCosts transaction.Costs `json:"gas_costs,omitempty"`

	// DebugDoNotSuspendRuntimes is true iff runtimes should not be suspended
	// for lack of paying maintenance fees.
	DebugDoNotSuspendRuntimes bool `json:"debug_do_not_suspend_runtimes,omitempty"`

	// DebugBypassStake is true iff the roothash should bypass all of the staking
	// related checks and operations.
	DebugBypassStake bool `json:"debug_bypass_stake,omitempty"`

	// MaxRuntimeMessages is the maximum number of allowed messages that can be emitted by a runtime
	// in a single round.
	MaxRuntimeMessages uint32 `json:"max_runtime_messages"`

	// MaxEvidenceAge is the maximum age of submitted evidence in the number of rounds.
	MaxEvidenceAge uint64 `json:"max_evidence_age"`
}

const (
	// GasOpComputeCommit is the gas operation identifier for compute commits.
	GasOpComputeCommit transaction.Op = "compute_commit"

	// GasOpProposerTimeout is the gas operation identifier for executor propose timeout cost.
	GasOpProposerTimeout transaction.Op = "proposer_timeout"

	// GasOpEvidence is the gas operation identifier for evidence submission transaction cost.
	GasOpEvidence transaction.Op = "evidence"
)

// XXX: Define reasonable default gas costs.

// DefaultGasCosts are the "default" gas costs for operations.
var DefaultGasCosts = transaction.Costs{
	GasOpComputeCommit:   1000,
	GasOpProposerTimeout: 1000,
	GasOpEvidence:        1000,
}

// SanityCheckBlocks examines the blocks table.
func SanityCheckBlocks(blocks map[common.Namespace]*block.Block) error {
	for _, blk := range blocks {
		hdr := blk.Header

		if uint64(hdr.Timestamp) > uint64(time.Now().Unix()+61*60) {
			return fmt.Errorf("roothash: sanity check failed: block header timestamp is more than 1h1m in the future")
		}
	}
	return nil
}

// SanityCheck does basic sanity checking on the genesis state.
func (g *Genesis) SanityCheck() error {
	unsafeFlags := g.Parameters.DebugDoNotSuspendRuntimes || g.Parameters.DebugBypassStake
	if unsafeFlags && !flags.DebugDontBlameOasis() {
		return fmt.Errorf("roothash: sanity check failed: one or more unsafe debug flags set")
	}

	// Check blocks.
	for _, rtg := range g.RuntimeStates {
		if err := rtg.SanityCheck(true); err != nil {
			return err
		}
	}
	return nil
}

// VerifyRuntimeParameters verifies whether the runtime parameters are valid in the context of the
// roothash service.
func VerifyRuntimeParameters(logger *logging.Logger, rt *registry.Runtime, params *ConsensusParameters) error {
	if rt.Executor.MaxMessages > params.MaxRuntimeMessages {
		return ErrMaxMessagesTooBig
	}
	return nil
}
