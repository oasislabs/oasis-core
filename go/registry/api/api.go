// Package api implements the runtime and entity registry APIs.
package api

import (
	"bytes"
	"errors"
	"sort"
	"time"

	"golang.org/x/net/context"

	"github.com/oasislabs/ekiden/go/common/cbor"
	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	"github.com/oasislabs/ekiden/go/common/entity"
	"github.com/oasislabs/ekiden/go/common/logging"
	"github.com/oasislabs/ekiden/go/common/node"
	"github.com/oasislabs/ekiden/go/common/pubsub"
	epochtime "github.com/oasislabs/ekiden/go/epochtime/api"
)

const (
	// TimestampValidFor is the number of seconds that a timestamp in a
	// register or deregister call is considered valid.
	// Default is 15 minutes.
	TimestampValidFor = uint64(15 * 60)
)

var (
	// RegisterEntitySignatureContext is the context used for entity
	// registration.
	RegisterEntitySignatureContext = []byte("EkEntReg")

	// DeregisterEntitySignatureContext is the context used for entity
	// deregistration.
	DeregisterEntitySignatureContext = []byte("EkEDeReg")

	// RegisterNodeSignatureContext is the context used for node
	// registration.
	RegisterNodeSignatureContext = []byte("EkNodReg")

	// RegisterRuntimeSignatureContext is the context used for runtime
	// registration.
	RegisterRuntimeSignatureContext = []byte("EkRunReg")

	// ErrInvalidArgument is the error returned on malformed argument(s).
	ErrInvalidArgument = errors.New("registry: invalid argument")

	// ErrInvalidSignature is the error returned on an invalid signature.
	ErrInvalidSignature = errors.New("registry: invalid signature")

	// ErrBadEntityForNode is the error returned when a node registration
	// with an unknown entity is attempted.
	ErrBadEntityForNode = errors.New("registry: unknown entity in node registration")

	// ErrNoSuchEntity is the error returned when an entity does not exist.
	ErrNoSuchEntity = errors.New("registry: no such entity")

	// ErrNoSuchNode is the error returned when an node does not exist.
	ErrNoSuchNode = errors.New("registry: no such node")

	// ErrNoSuchRuntime is the error returned when an runtime does not exist.
	ErrNoSuchRuntime = errors.New("registry: no such runtime")

	// ErrInvalidTimestamp is the error returned when a timestamp is invalid.
	ErrInvalidTimestamp = errors.New("registry: invalid timestamp")
)

// Backend is a registry implementation.
type Backend interface {
	// RegisterEntity registers and or updates an entity with the registry.
	//
	// The signature should be made using RegisterEntitySignatureContext.
	RegisterEntity(context.Context, *entity.SignedEntity) error

	// DeregisterEntity deregisters an entity.
	//
	// The signature should be made using DeregisterEntitySignatureContext.
	DeregisterEntity(context.Context, *signature.Signed) error

	// GetEntity gets an entity by ID.
	GetEntity(context.Context, signature.PublicKey) (*entity.Entity, error)

	// GetEntities gets a list of all registered entities.
	GetEntities(context.Context) ([]*entity.Entity, error)

	// WatchEntities returns a channel that produces a stream of
	// EntityEvent on entity registration changes.
	WatchEntities() (<-chan *EntityEvent, *pubsub.Subscription)

	// RegisterNode registers and or updates a node with the registry.
	//
	// The signature should be made using RegisterNodeSignatureContext.
	RegisterNode(context.Context, *node.SignedNode) error

	// GetNode gets a node by ID.
	GetNode(context.Context, signature.PublicKey) (*node.Node, error)

	// GetNodes gets a list of all registered nodes.
	GetNodes(context.Context) ([]*node.Node, error)

	// GetNodesForEntity gets a list of nodes registered to an entity ID.
	GetNodesForEntity(context.Context, signature.PublicKey) []*node.Node

	// WatchNodes returns a channel that produces a stream of
	// NodeEvent on node registration changes.
	WatchNodes() (<-chan *NodeEvent, *pubsub.Subscription)

	// WatchNodeList returns a channel that produces a stream of NodeList.
	// Upon subscription, the node list for the current epoch will be sent
	// immediately if available.
	//
	// Each node list will be sorted by node ID in lexographically ascending
	// order.
	WatchNodeList() (<-chan *NodeList, *pubsub.Subscription)

	// RegisterRuntime registers a runtime.
	RegisterRuntime(context.Context, *SignedRuntime) error

	// GetRuntime gets a runtime by ID.
	GetRuntime(context.Context, signature.PublicKey) (*Runtime, error)

	// GetRuntimes gets a list of all registered runtimes.
	GetRuntimes(context.Context) ([]*Runtime, error)

	// WatchRuntimes returns a stream of Runtime.  Upon subscription,
	// all runtimes will be sent immediately.
	WatchRuntimes() (<-chan *Runtime, *pubsub.Subscription)

	// Cleanup cleans up the regsitry backend.
	Cleanup()
}

// EntityEvent is the event that is returned via WatchEntities to signify
// entity registration changes and updates.
type EntityEvent struct {
	Entity         *entity.Entity
	IsRegistration bool
}

// NodeEvent is the event that is returned via WatchNodes to signify node
// registration changes and updates.
type NodeEvent struct {
	Node           *node.Node
	IsRegistration bool
}

// NodeList is a per-epoch immutable node list.
type NodeList struct {
	Epoch epochtime.EpochTime
	Nodes []*node.Node
}

// BlockBackend is a Backend that is backed by a blockchain.
type BlockBackend interface {
	Backend

	// GetBlockNodeList returns the NodeList at the specified block height.
	GetBlockNodeList(context.Context, int64) (*NodeList, error)
}

type Timestamp uint64

// MarshalCBOR serializes the Timestamp type into a CBOR byte vector.
func (t *Timestamp) MarshalCBOR() []byte {
	return cbor.Marshal(t)
}

// UnmarshalCBOR deserializes a CBOR byte vector into a Timestamp.
func (t *Timestamp) UnmarshalCBOR(data []byte) error {
	return cbor.Unmarshal(data, t)
}

// VerifyRegisterEntityArgs verifies arguments for RegisterEntity.
func VerifyRegisterEntityArgs(logger *logging.Logger, sigEnt *entity.SignedEntity) (*entity.Entity, error) {
	// XXX: Ensure ent is well-formed.
	var ent entity.Entity
	if sigEnt == nil {
		return nil, ErrInvalidArgument
	}
	if err := sigEnt.Open(RegisterEntitySignatureContext, &ent); err != nil {
		logger.Error("RegisterEntity: invalid signature",
			"signed_entity", sigEnt,
		)
		return nil, ErrInvalidSignature
	}
	if sigEnt.Signed.Signature.SanityCheck(ent.ID) != nil {
		logger.Error("RegisterEntity: invalid argument(s)",
			"signed_entity", sigEnt,
			"entity", ent,
		)
		return nil, ErrInvalidArgument
	}

	return &ent, nil
}

// VerifyDeregisterEntityArgs verifies arguments for DeregisterEntity.
func VerifyDeregisterEntityArgs(logger *logging.Logger, sigTimestamp *signature.Signed) (signature.PublicKey, uint64, error) {
	var id signature.PublicKey
	var timestamp Timestamp
	if sigTimestamp == nil {
		return nil, 0, ErrInvalidArgument
	}
	if err := sigTimestamp.Open(DeregisterEntitySignatureContext, &timestamp); err != nil {
		logger.Error("DeregisterEntity: invalid signature",
			"signed_timestamp", sigTimestamp,
		)
		return nil, 0, ErrInvalidSignature
	}
	id = sigTimestamp.Signature.PublicKey

	return id, uint64(timestamp), nil
}

// VerifyRegisterNodeArgs verifies arguments for RegisterNode.
func VerifyRegisterNodeArgs(logger *logging.Logger, sigNode *node.SignedNode, now time.Time) (*node.Node, error) {
	// XXX: Ensure node is well-formed.
	var node node.Node
	if sigNode == nil {
		return nil, ErrInvalidArgument
	}
	if err := sigNode.Open(RegisterNodeSignatureContext, &node); err != nil {
		logger.Error("RegisterNode: invalid signature",
			"signed_node", sigNode,
		)
		return nil, ErrInvalidSignature
	}
	if sigNode.Signed.Signature.SanityCheck(node.EntityID) != nil {
		logger.Error("RegisterNode: not signed by entity",
			"signed_node", sigNode,
			"node", node,
		)
		return nil, ErrInvalidArgument
	}

	switch len(node.Runtimes) {
	case 0:
		logger.Warn("RegisterNode: no runtimes in registration",
			"node", node,
		)
	default:
		// If the node indicates TEE support for any of it's runtimes,
		// validate the attestation evidence.
		for _, rt := range node.Runtimes {
			tee := rt.Capabilities.TEE
			if tee == nil {
				continue
			}

			if err := tee.Verify(now); err != nil {
				logger.Error("RegisterNode: failed to validate attestation",
					"node", node,
					"runtime", rt.ID,
					"err", err,
				)
				return nil, err
			}
		}
	}

	return &node, nil
}

// VerifyRegisterRuntimeArgs verifies arguments for RegisterRuntime.
func VerifyRegisterRuntimeArgs(logger *logging.Logger, sigCon *SignedRuntime) (*Runtime, error) {
	// XXX: Ensure contact is well-formed.
	var con Runtime
	if sigCon == nil {
		return nil, ErrInvalidArgument
	}
	if err := sigCon.Open(RegisterRuntimeSignatureContext, &con); err != nil {
		logger.Error("RegisterRuntime: invalid signature",
			"signed_runtime", sigCon,
		)
		return nil, ErrInvalidSignature
	}

	// TODO: Who should sign the runtime? Current compute node assumes an entity (deployer).

	return &con, nil
}

// SortNodeList sorts the given node list to ensure a canonical order.
func SortNodeList(nodes []*node.Node) {
	sort.Slice(nodes, func(i, j int) bool {
		return bytes.Compare(nodes[i].ID, nodes[j].ID) == -1
	})
}

// VerifyTimestamp verifies that the given timestamp is valid.
func VerifyTimestamp(timestamp uint64, now uint64) error {
	// For now, we check that it's new enough and not too far in the future.
	// We allow the timestamp to be up to 1 minute in the future to account
	// for network latency, leap seconds, and real-time clock inaccuracies
	// and drift.
	if timestamp < now-TimestampValidFor || timestamp > now+60 {
		return ErrInvalidTimestamp
	}

	return nil
}
