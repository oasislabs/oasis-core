// Package consensus provides the implementation agnostic consensus
// backend.
package api

import (
	"context"
	"strings"
	"time"

	beacon "github.com/oasisprotocol/oasis-core/go/beacon/api"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/errors"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	"github.com/oasisprotocol/oasis-core/go/common/pubsub"
	"github.com/oasisprotocol/oasis-core/go/common/service"
	"github.com/oasisprotocol/oasis-core/go/common/version"
	"github.com/oasisprotocol/oasis-core/go/consensus/api/transaction"
	"github.com/oasisprotocol/oasis-core/go/consensus/api/transaction/results"
	genesis "github.com/oasisprotocol/oasis-core/go/genesis/api"
	governance "github.com/oasisprotocol/oasis-core/go/governance/api"
	keymanager "github.com/oasisprotocol/oasis-core/go/keymanager/api"
	registry "github.com/oasisprotocol/oasis-core/go/registry/api"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	runtimeHistory "github.com/oasisprotocol/oasis-core/go/runtime/history/api"
	scheduler "github.com/oasisprotocol/oasis-core/go/scheduler/api"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
	mkvsNode "github.com/oasisprotocol/oasis-core/go/storage/mkvs/node"
)

const (
	// moduleName is the module name used for error definitions.
	moduleName = "consensus"

	// HeightLatest is the height that represents the most recent block height.
	HeightLatest int64 = 0
)

var (
	// ErrNoCommittedBlocks is the error returned when there are no committed
	// blocks and as such no state can be queried.
	ErrNoCommittedBlocks = errors.New(moduleName, 1, "consensus: no committed blocks")

	// ErrOversizedTx is the error returned when the given transaction is too big to be processed.
	ErrOversizedTx = errors.New(moduleName, 2, "consensus: oversized transaction")

	// ErrVersionNotFound is the error returned when the given version (height) cannot be found,
	// possibly because it was pruned.
	ErrVersionNotFound = errors.New(moduleName, 3, "consensus: version not found")

	// ErrUnsupported is the error returned when the given method is not supported by the consensus
	// backend.
	ErrUnsupported = errors.New(moduleName, 4, "consensus: method not supported")

	// ErrDuplicateTx is the error returned when the transaction already exists in the mempool.
	ErrDuplicateTx = errors.New(moduleName, 5, "consensus: duplicate transaction")

	// ErrInvalidArgument is the error returned when the request contains an invalid argument.
	ErrInvalidArgument = errors.New(moduleName, 6, "consensus: invalid argument")
)

// FeatureMask is the consensus backend feature bitmask.
type FeatureMask uint8

const (
	// FeatureServices indicates support for communicating with consensus services.
	FeatureServices FeatureMask = 1 << 0

	// FeatureFullNode indicates that the consensus backend is independently fully verifying all
	// consensus-layer blocks.
	FeatureFullNode FeatureMask = 1 << 1
)

// String returns a string representation of the consensus backend feature bitmask.
func (m FeatureMask) String() string {
	var ret []string
	if m&FeatureServices != 0 {
		ret = append(ret, "consensus services")
	}
	if m&FeatureFullNode != 0 {
		ret = append(ret, "full node")
	}

	return strings.Join(ret, ",")
}

// Has checks whether the feature bitmask includes specific features.
func (m FeatureMask) Has(f FeatureMask) bool {
	return m&f != 0
}

// ClientBackend is a limited consensus interface used by clients that connect to the local full
// node. This is separate from light clients which use the LightClientBackend interface.
type ClientBackend interface {
	LightClientBackend
	TransactionAuthHandler

	// SubmitTx submits a signed consensus transaction and waits for the transaction to be included
	// in a block. Use SubmitTxNoWait if you only need to broadcast the transaction.
	SubmitTx(ctx context.Context, tx *transaction.SignedTransaction) error

	// StateToGenesis returns the genesis state at the specified block height.
	StateToGenesis(ctx context.Context, height int64) (*genesis.Document, error)

	// EstimateGas calculates the amount of gas required to execute the given transaction.
	EstimateGas(ctx context.Context, req *EstimateGasRequest) (transaction.Gas, error)

	// GetBlock returns a consensus block at a specific height.
	GetBlock(ctx context.Context, height int64) (*Block, error)

	// GetTransactions returns a list of all transactions contained within a
	// consensus block at a specific height.
	//
	// NOTE: Any of these transactions could be invalid.
	GetTransactions(ctx context.Context, height int64) ([][]byte, error)

	// GetTransactionsWithResults returns a list of transactions and their
	// execution results, contained within a consensus block at a specific
	// height.
	GetTransactionsWithResults(ctx context.Context, height int64) (*TransactionsWithResults, error)

	// GetUnconfirmedTransactions returns a list of transactions currently in the local node's
	// mempool. These have not yet been included in a block.
	GetUnconfirmedTransactions(ctx context.Context) ([][]byte, error)

	// WatchBlocks returns a channel that produces a stream of consensus
	// blocks as they are being finalized.
	WatchBlocks(ctx context.Context) (<-chan *Block, pubsub.ClosableSubscription, error)

	// GetGenesisDocument returns the original genesis document.
	GetGenesisDocument(ctx context.Context) (*genesis.Document, error)

	// GetChainContext returns the chain domain separation context.
	GetChainContext(ctx context.Context) (string, error)

	// GetStatus returns the current status overview.
	GetStatus(ctx context.Context) (*Status, error)

	// Beacon returns the beacon backend.
	Beacon() beacon.Backend

	// Registry returns the registry backend.
	Registry() registry.Backend

	// Staking returns the staking backend.
	Staking() staking.Backend

	// Scheduler returns the scheduler backend.
	Scheduler() scheduler.Backend

	// Governance returns the governance backend.
	Governance() governance.Backend
}

// Block is a consensus block.
//
// While some common fields are provided, most of the structure is dependent on
// the actual backend implementation.
type Block struct {
	// Height contains the block height.
	Height int64 `json:"height"`
	// Hash contains the block header hash.
	Hash []byte `json:"hash"`
	// Time is the second-granular consensus time.
	Time time.Time `json:"time"`
	// StateRoot is the Merkle root of the consensus state tree.
	StateRoot mkvsNode.Root `json:"state_root"`
	// Meta contains the consensus backend specific block metadata.
	Meta cbor.RawMessage `json:"meta"`
}

// Status is the current status overview.
type Status struct { // nolint: maligned
	// Version is the version of the consensus protocol that the node is using.
	Version version.Version `json:"version"`
	// Backend is the consensus backend identifier.
	Backend string `json:"backend"`
	// Features are the indicated consensus backend features.
	Features FeatureMask `json:"features"`

	// NodePeers is a list of node's peers.
	NodePeers []string `json:"node_peers"`

	// LatestHeight is the height of the latest block.
	LatestHeight int64 `json:"latest_height"`
	// LatestHash is the hash of the latest block.
	LatestHash []byte `json:"latest_hash"`
	// LatestTime is the timestamp of the latest block.
	LatestTime time.Time `json:"latest_time"`
	// LatestEpoch is the epoch of the latest block.
	LatestEpoch beacon.EpochTime `json:"latest_epoch"`
	// LatestStateRoot is the Merkle root of the consensus state tree.
	LatestStateRoot mkvsNode.Root `json:"latest_state_root"`

	// GenesisHeight is the height of the genesis block.
	GenesisHeight int64 `json:"genesis_height"`
	// GenesisHash is the hash of the genesis block.
	GenesisHash []byte `json:"genesis_hash"`

	// LastRetainedHeight is the height of the oldest retained block.
	LastRetainedHeight int64 `json:"last_retained_height"`
	// LastRetainedHash is the hash of the oldest retained block.
	LastRetainedHash []byte `json:"last_retained_hash"`

	// ChainContext is the chain domain separation context.
	ChainContext string `json:"chain_context"`

	// IsValidator returns whether the current node is part of the validator set.
	IsValidator bool `json:"is_validator"`
}

// Backend is an interface that a consensus backend must provide.
type Backend interface {
	service.BackgroundService
	ServicesBackend

	// SupportedFeatures returns the features supported by this consensus backend.
	SupportedFeatures() FeatureMask

	// Synced returns a channel that is closed once synchronization is
	// complete.
	Synced() <-chan struct{}

	// ConsensusKey returns the consensus signing key.
	ConsensusKey() signature.PublicKey

	// GetAddresses returns the consensus backend addresses.
	GetAddresses() ([]node.ConsensusAddress, error)
}

// HaltHook is a function that gets called when consensus needs to halt for some reason.
type HaltHook func(ctx context.Context, blockHeight int64, epoch beacon.EpochTime, err error)

// ServicesBackend is an interface for consensus backends which indicate support for
// communicating with consensus services.
//
// In case the feature is absent, these methods may return nil or ErrUnsupported.
type ServicesBackend interface {
	ClientBackend

	// RegisterHaltHook registers a function to be called when the consensus needs to halt.
	RegisterHaltHook(hook HaltHook)

	// SubmissionManager returns the transaction submission manager.
	SubmissionManager() SubmissionManager

	// KeyManager returns the keymanager backend.
	KeyManager() keymanager.Backend

	// RootHash returns the roothash backend.
	RootHash() roothash.Backend

	// TrackRuntime adds a runtime the history of which should be tracked.
	TrackRuntime(history runtimeHistory.BlockHistory) error
}

// TransactionAuthHandler is the interface for handling transaction authentication
// (checking nonces and fees).
type TransactionAuthHandler interface {
	// GetSignerNonce returns the nonce that should be used by the given
	// signer for transmitting the next transaction.
	GetSignerNonce(ctx context.Context, req *GetSignerNonceRequest) (uint64, error)
}

// EstimateGasRequest is a EstimateGas request.
type EstimateGasRequest struct {
	Signer      signature.PublicKey      `json:"signer"`
	Transaction *transaction.Transaction `json:"transaction"`
}

// GetSignerNonceRequest is a GetSignerNonce request.
type GetSignerNonceRequest struct {
	AccountAddress staking.Address `json:"account_address"`
	Height         int64           `json:"height"`
}

// TransactionsWithResults is GetTransactionsWithResults response.
//
// Results[i] are the results of executing Transactions[i].
type TransactionsWithResults struct {
	Transactions [][]byte          `json:"transactions"`
	Results      []*results.Result `json:"results"`
}
