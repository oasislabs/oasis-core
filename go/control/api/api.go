// Package api implements the node control API.
package api

import (
	"context"
	"time"

	beacon "github.com/oasisprotocol/oasis-core/go/beacon/api"
	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/errors"
	"github.com/oasisprotocol/oasis-core/go/common/identity"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	registry "github.com/oasisprotocol/oasis-core/go/registry/api"
	block "github.com/oasisprotocol/oasis-core/go/roothash/api/block"
	storage "github.com/oasisprotocol/oasis-core/go/storage/api"
	upgrade "github.com/oasisprotocol/oasis-core/go/upgrade/api"
	commonWorker "github.com/oasisprotocol/oasis-core/go/worker/common/api"
	storageWorker "github.com/oasisprotocol/oasis-core/go/worker/storage/api"
)

// NodeController is a node controller interface.
type NodeController interface {
	// RequestShutdown requests the node to shut down gracefully.
	//
	// If the wait argument is true then the method will also wait for the
	// shutdown to complete.
	RequestShutdown(ctx context.Context, wait bool) error

	// WaitSync waits for the node to finish syncing.
	WaitSync(ctx context.Context) error

	// IsSynced checks whether the node has finished syncing.
	IsSynced(ctx context.Context) (bool, error)

	// WaitReady waits for the node to accept runtime work.
	WaitReady(ctx context.Context) error

	// IsReady checks whether the node is ready to accept runtime work.
	IsReady(ctx context.Context) (bool, error)

	// UpgradeBinary submits an upgrade descriptor to a running node.
	// The node will wait for the appropriate epoch, then update its binaries
	// and shut down.
	UpgradeBinary(ctx context.Context, descriptor *upgrade.Descriptor) error

	// CancelUpgrade cancels the specific pending upgrade, unless it is already in progress.
	CancelUpgrade(ctx context.Context, descriptor *upgrade.Descriptor) error

	// GetStatus returns the current status overview of the node.
	GetStatus(ctx context.Context) (*Status, error)
}

// Status is the current status overview.
type Status struct {
	// SoftwareVersion is the oasis-node software version.
	SoftwareVersion string `json:"software_version"`

	// Identity is the identity of the node.
	Identity IdentityStatus `json:"identity"`

	// Consensus is the status overview of the consensus layer.
	Consensus consensus.Status `json:"consensus"`

	// Runtimes is the status overview for each runtime supported by the node.
	Runtimes map[common.Namespace]RuntimeStatus `json:"runtimes"`

	// Registration is the node's registration status.
	Registration RegistrationStatus `json:"registration"`

	// PendingUpgrades are the node's pending upgrades.
	PendingUpgrades []*upgrade.PendingUpgrade `json:"pending_upgrades"`
}

// IdentityStatus is the current node identity status, listing all the public keys that identify
// this node in different contexts.
type IdentityStatus struct {
	// Node is the node identity public key.
	Node signature.PublicKey `json:"node"`

	// P2P is the public key used for p2p communication.
	P2P signature.PublicKey `json:"p2p"`

	// Consensus is the consensus public key.
	Consensus signature.PublicKey `json:"consensus"`

	// TLS are the public keys used for TLS connections.
	TLS []signature.PublicKey `json:"tls"`
}

// RegistrationStatus is the node registration status.
type RegistrationStatus struct {
	// LastRegistration is the time of the last successful registration with the consensus registry
	// service. In case the node did not successfully register yet, it will be the zero timestamp.
	LastRegistration time.Time `json:"last_registration"`

	// Descriptor is the node descriptor that the node successfully registered with. In case the
	// node did not successfully register yet, it will be nil.
	Descriptor *node.Node `json:"descriptor,omitempty"`

	// NodeStatus is the registry live status of the node.
	NodeStatus *registry.NodeStatus `json:"node_status,omitempty"`
}

// RuntimeStatus is the per-runtime status overview.
type RuntimeStatus struct {
	// Descriptor is the runtime registration descriptor.
	Descriptor *registry.Runtime `json:"descriptor"`

	// LatestRound is the round of the latest runtime block.
	LatestRound uint64 `json:"latest_round"`
	// LatestHash is the hash of the latest runtime block.
	LatestHash hash.Hash `json:"latest_hash"`
	// LatestTime is the timestamp of the latest runtime block.
	LatestTime block.TimestampType `json:"latest_time"`
	// LatestStateRoot is the Merkle root of the runtime state tree.
	LatestStateRoot storage.Root `json:"latest_state_root"`

	// GenesisRound is the round of the genesis runtime block.
	GenesisRound uint64 `json:"genesis_round"`
	// GenesisHash is the hash of the genesis runtime block.
	GenesisHash hash.Hash `json:"genesis_hash"`

	// Committee contains the runtime worker status in case this node is a (candidate) member of a
	// runtime committee (e.g., compute or storage).
	Committee *commonWorker.Status `json:"committee"`
	// Storage contains the storage worker status in case this node is a storage node.
	Storage *storageWorker.Status `json:"storage"`
}

// ControlledNode is an internal interface that the controlled oasis-node must provide.
type ControlledNode interface {
	// RequestShutdown is the method called by the control server to trigger node shutdown.
	RequestShutdown() (<-chan struct{}, error)

	// Ready returns a channel that is closed once node is ready.
	Ready() <-chan struct{}

	// GetIdentity returns the node's identity.
	GetIdentity() *identity.Identity

	// GetRegistrationStatus returns the node's current registration status.
	GetRegistrationStatus(ctx context.Context) (*RegistrationStatus, error)

	// GetRuntimeStatus returns the node's current per-runtime status.
	GetRuntimeStatus(ctx context.Context) (map[common.Namespace]RuntimeStatus, error)

	// GetPendingUpgrade returns the node's pending upgrades.
	GetPendingUpgrades(ctx context.Context) ([]*upgrade.PendingUpgrade, error)
}

// DebugModuleName is the module name for the debug controller service.
const DebugModuleName = "control/debug"

// ErrIncompatibleBackend is the error raised when the current beacon
// backend does not support manually setting the current epoch.
var ErrIncompatibleBackend = errors.New(DebugModuleName, 1, "debug: incompatible backend")

// DebugController is a debug-only controller useful during tests.
type DebugController interface {
	// SetEpoch manually sets the current epoch to the given epoch.
	//
	// NOTE: This only works with a mock beacon backend and will otherwise
	//       return an error.
	SetEpoch(ctx context.Context, epoch beacon.EpochTime) error

	// WaitNodesRegistered waits for the given number of nodes to register.
	WaitNodesRegistered(ctx context.Context, count int) error
}
