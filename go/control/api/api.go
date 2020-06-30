// Package api implements the node control API.
package api

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/errors"
	"github.com/oasisprotocol/oasis-core/go/common/identity"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	"github.com/oasisprotocol/oasis-core/go/common/prettyprint"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	epochtime "github.com/oasisprotocol/oasis-core/go/epochtime/api"
	upgrade "github.com/oasisprotocol/oasis-core/go/upgrade/api"
)

var (
	_ prettyprint.PrettyPrinter = (*Status)(nil)
	_ prettyprint.PrettyPrinter = (*IdentityStatus)(nil)
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

	// CancelUpgrade cancels a pending upgrade, unless it is already in progress.
	CancelUpgrade(ctx context.Context) error

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

	// Registration is the node's registration status.
	Registration RegistrationStatus `json:"registration"`
}

func (s Status) PrettyPrint(prefix string, w io.Writer) {
	fmt.Fprintf(w, "%sSoftware version: %s\n", prefix, s.SoftwareVersion)
	fmt.Fprintf(w, "%sIdentity:\n", prefix)
	s.Identity.PrettyPrint(prefix+"  ", w)
	fmt.Fprintf(w, "%sConsensus:\n", prefix)
	s.Consensus.PrettyPrint(prefix+"  ", w)
	fmt.Fprintf(w, "%sRegistration: %s\n", prefix, s.Registration)
}

func (s Status) PrettyType() (interface{}, error) {
	return s, nil
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

func (s IdentityStatus) PrettyPrint(prefix string, w io.Writer) {
	fmt.Fprintf(w, "%sNode: %s\n", prefix, s.Node)
	fmt.Fprintf(w, "%sP2P: %s\n", prefix, s.P2P)
	fmt.Fprintf(w, "%sConsensus: %s\n", prefix, s.Consensus)
	fmt.Fprintf(w, "%sTLS: %s\n", prefix, s.TLS)
}

func (s IdentityStatus) PrettyType() (interface{}, error) {
	return s, nil
}

// RegistrationStatus is the node registration status.
type RegistrationStatus struct {
	// LastRegistration is the time of the last successful registration with the consensus registry
	// service. In case the node did not successfully register yet, it will be the zero timestamp.
	LastRegistration time.Time `json:"last_registration"`

	// Descriptor is the node descriptor that the node successfully registered with. In case the
	// node did not successfully register yet, it will be nil.
	Descriptor *node.Node `json:"descriptor,omitempty"`
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
}

// DebugModuleName is the module name for the debug controller service.
const DebugModuleName = "control/debug"

// ErrIncompatibleBackend is the error raised when the current epochtime
// backend does not support manually setting the current epoch.
var ErrIncompatibleBackend = errors.New(DebugModuleName, 1, "debug: incompatible backend")

// DebugController is a debug-only controller useful during tests.
type DebugController interface {
	// SetEpoch manually sets the current epoch to the given epoch.
	//
	// NOTE: This only works with a mock epochtime backend and will otherwise
	//       return an error.
	SetEpoch(ctx context.Context, epoch epochtime.EpochTime) error

	// WaitNodesRegistered waits for the given number of nodes to register.
	WaitNodesRegistered(ctx context.Context, count int) error
}
