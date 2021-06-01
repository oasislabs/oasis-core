// Package api implements the API between Oasis ABCI application and Oasis core.
package api

import (
	"context"
	"fmt"
	"strings"

	"github.com/tendermint/tendermint/abci/types"
	tmpubsub "github.com/tendermint/tendermint/libs/pubsub"
	tmquery "github.com/tendermint/tendermint/libs/pubsub/query"
	tmp2p "github.com/tendermint/tendermint/p2p"
	tmcrypto "github.com/tendermint/tendermint/proto/tendermint/crypto"
	tmrpctypes "github.com/tendermint/tendermint/rpc/core/types"
	tmtypes "github.com/tendermint/tendermint/types"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	"github.com/oasisprotocol/oasis-core/go/common/pubsub"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	"github.com/oasisprotocol/oasis-core/go/consensus/api/transaction"
	"github.com/oasisprotocol/oasis-core/go/consensus/tendermint/crypto"
	"github.com/oasisprotocol/oasis-core/go/roothash/api"
	mkvsNode "github.com/oasisprotocol/oasis-core/go/storage/mkvs/node"
)

// BackendName is the consensus backend name.
const BackendName = "tendermint"

const (
	// LogEventPeerExchangeDisable is a log event that indicates that
	// Tendermint's peer exchange has been disabled.
	LogEventPeerExchangeDisabled = "tendermint/peer_exchange_disabled"
)

// PublicKeyToValidatorUpdate converts an Oasis node public key to a
// tendermint validator update.
func PublicKeyToValidatorUpdate(id signature.PublicKey, power int64) types.ValidatorUpdate {
	pk, _ := id.MarshalBinary()

	return types.ValidatorUpdate{
		PubKey: tmcrypto.PublicKey{
			Sum: &tmcrypto.PublicKey_Ed25519{
				Ed25519: pk,
			},
		},
		Power: power,
	}
}

// NodeToP2PAddr converts an Oasis node descriptor to a tendermint p2p
// address book entry.
func NodeToP2PAddr(n *node.Node) (*tmp2p.NetAddress, error) {
	// WARNING: p2p/transport.go:MultiplexTransport.upgrade() uses
	// a case sensitive string comparison to validate public keys,
	// because tendermint.

	if !n.HasRoles(node.RoleValidator) {
		return nil, fmt.Errorf("tendermint/api: node is not a validator")
	}

	if len(n.Consensus.Addresses) == 0 {
		// Should never happen, but check anyway.
		return nil, fmt.Errorf("tendermint/api: node has no consensus addresses")
	}

	// TODO: Should we extend the function to return more P2P addresses?
	consensusAddr := n.Consensus.Addresses[0]

	pubKey := crypto.PublicKeyToTendermint(&consensusAddr.ID)
	pubKeyAddrHex := strings.ToLower(pubKey.Address().String())

	coreAddress, _ := consensusAddr.Address.MarshalText()

	addr := pubKeyAddrHex + "@" + string(coreAddress)

	tmAddr, err := tmp2p.NewNetAddressString(addr)
	if err != nil {
		return nil, fmt.Errorf("tendermint/api: failed to reformat validator: %w", err)
	}

	return tmAddr, nil
}

// EventBuilder is a helper for constructing ABCI events.
type EventBuilder struct {
	app []byte
	ev  types.Event
}

// Attribute appends a key/value pair to the event.
func (bld *EventBuilder) Attribute(key, value []byte) *EventBuilder {
	bld.ev.Attributes = append(bld.ev.Attributes, types.EventAttribute{
		Key:   key,
		Value: value,
	})

	return bld
}

// Dirty returns true iff the EventBuilder has attributes.
func (bld *EventBuilder) Dirty() bool {
	return len(bld.ev.Attributes) > 0
}

// Event returns the event from the EventBuilder.
func (bld *EventBuilder) Event() types.Event {
	// Return a copy to support emitting incrementally.
	ev := types.Event{
		Type: bld.ev.Type,
	}
	ev.Attributes = append(ev.Attributes, bld.ev.Attributes...)

	return ev
}

// NewEventBuilder returns a new EventBuilder for the given ABCI app.
func NewEventBuilder(app string) *EventBuilder {
	return &EventBuilder{
		app: []byte(app),
		ev: types.Event{
			Type: EventTypeForApp(app),
		},
	}
}

// EventTypeForApp generates the ABCI event type for events belonging
// to the specified App.
func EventTypeForApp(eventApp string) string {
	return "oasis-event-" + eventApp
}

// QueryForApp generates a tmquery.Query for events belonging to the
// specified App.
func QueryForApp(eventApp string) tmpubsub.Query {
	return tmquery.MustParse(fmt.Sprintf("%s EXISTS", EventTypeForApp(eventApp)))
}

// BlockMeta is the Tendermint-specific per-block metadata that is
// exposed via the consensus API.
type BlockMeta struct {
	// Header is the Tendermint block header.
	Header *tmtypes.Header `json:"header"`
	// LastCommit is the Tendermint last commit info.
	LastCommit *tmtypes.Commit `json:"last_commit"`
}

// NewBlock creates a new consensus.Block from a Tendermint block.
func NewBlock(blk *tmtypes.Block) *consensus.Block {
	meta := BlockMeta{
		Header:     &blk.Header,
		LastCommit: blk.LastCommit,
	}
	rawMeta := cbor.Marshal(meta)

	var stateRoot hash.Hash
	switch blk.Header.AppHash {
	case nil:
		stateRoot.Empty()
	default:
		if err := stateRoot.UnmarshalBinary(blk.Header.AppHash); err != nil {
			// This should NEVER happen.
			panic(err)
		}
	}

	return &consensus.Block{
		Height: blk.Header.Height,
		Hash:   blk.Header.Hash(),
		Time:   blk.Header.Time,
		StateRoot: mkvsNode.Root{
			Version: uint64(blk.Header.Height) - 1,
			Type:    mkvsNode.RootTypeState,
			Hash:    stateRoot,
		},
		Meta: rawMeta,
	}
}

// Backend is a Tendermint consensus backend.
type Backend interface {
	consensus.Backend

	// RegisterApplication registers an ABCI multiplexer application
	// with this service instance and check that its dependencies are
	// registered.
	RegisterApplication(Application) error

	// SetTransactionAuthHandler configures the transaction fee handler for the
	// ABCI multiplexer.
	SetTransactionAuthHandler(TransactionAuthHandler) error

	// GetBlock returns the Tendermint block at the specified height.
	GetTendermintBlock(ctx context.Context, height int64) (*tmtypes.Block, error)

	// GetBlockResults returns the ABCI results from processing a block
	// at a specific height.
	GetBlockResults(ctx context.Context, height int64) (*tmrpctypes.ResultBlockResults, error)

	// WatchTendermintBlocks returns a stream of Tendermint blocks as they are
	// returned via the `EventDataNewBlock` query.
	WatchTendermintBlocks() (<-chan *tmtypes.Block, *pubsub.Subscription)

	// GetLastRetainedVersion returns the earliest retained version the ABCI
	// state.
	GetLastRetainedVersion(ctx context.Context) (int64, error)

	// GetLatestIndexedRuntimeBlock returns the last block indexed by the runtime history.
	GetLatestIndexedRuntimeBlock(id common.Namespace) (*api.AnnotatedBlock, error)
}

// TransactionAuthHandler is the interface for ABCI applications that handle
// authenticating transactions (checking nonces and fees).
type TransactionAuthHandler interface {
	consensus.TransactionAuthHandler

	// AuthenticateTx authenticates the given transaction by making sure
	// that the nonce is correct and deducts any fees as specified.
	//
	// It may reject the transaction in case of incorrect nonces, insufficient
	// balance to pay fees or (only during CheckTx) if the gas price is too
	// low.
	//
	// The context may be modified to configure a gas accountant.
	AuthenticateTx(ctx *Context, tx *transaction.Transaction) error

	// PostExecuteTx is called after the transaction has been executed. It is
	// only called in case the execution did not produce an error.
	PostExecuteTx(ctx *Context, tx *transaction.Transaction) error
}

// ServiceEvent is a Tendermint-specific consensus.ServiceEvent.
type ServiceEvent struct {
	Block *tmtypes.EventDataNewBlockHeader `json:"block,omitempty"`
	Tx    *tmtypes.EventDataTx             `json:"tx,omitempty"`
}

// ServiceDescriptor is a Tendermint consensus service descriptor.
type ServiceDescriptor interface {
	// Name returns the name of this service.
	Name() string

	// EventType returns the event type associated with the consensus service.
	EventType() string

	// Queries returns a channel that emits queries that need to be subscribed to.
	Queries() <-chan tmpubsub.Query

	// Commands returns a channel that emits commands for the service client.
	Commands() <-chan interface{}
}

type serviceDescriptor struct {
	name      string
	eventType string
	queryCh   <-chan tmpubsub.Query
	cmdCh     <-chan interface{}
}

func (sd *serviceDescriptor) Name() string {
	return sd.name
}

func (sd *serviceDescriptor) EventType() string {
	return sd.eventType
}

func (sd *serviceDescriptor) Queries() <-chan tmpubsub.Query {
	return sd.queryCh
}

func (sd *serviceDescriptor) Commands() <-chan interface{} {
	return sd.cmdCh
}

// NewServiceDescriptor creates a new consensus service descriptor.
func NewServiceDescriptor(name, eventType string, queryCh <-chan tmpubsub.Query, cmdCh <-chan interface{}) ServiceDescriptor {
	return &serviceDescriptor{
		name:      name,
		eventType: eventType,
		queryCh:   queryCh,
		cmdCh:     cmdCh,
	}
}

// NewStaticServiceDescriptor creates a new static consensus service descriptor.
func NewStaticServiceDescriptor(name, eventType string, queries []tmpubsub.Query) ServiceDescriptor {
	ch := make(chan tmpubsub.Query)
	go func() {
		defer close(ch)

		for _, q := range queries {
			ch <- q
		}
	}()
	return NewServiceDescriptor(name, eventType, ch, nil)
}

// ServiceClient is a consensus service client.
type ServiceClient interface {
	// ServiceDescriptor returns the consensus service descriptor.
	ServiceDescriptor() ServiceDescriptor

	// DeliverBlock delivers a new block.
	//
	// Execution of this method will block delivery of further events.
	DeliverBlock(ctx context.Context, height int64) error

	// DeliverEvent delivers an event emitted by the consensus service.
	DeliverEvent(ctx context.Context, height int64, tx tmtypes.Tx, ev *types.Event) error

	// DeliverCommand delivers a command emitted via the command channel.
	DeliverCommand(ctx context.Context, height int64, cmd interface{}) error
}

// BaseServiceClient is a default ServiceClient implementation that provides noop implementations of
// all the delivery methods. Implementations should override them as needed.
type BaseServiceClient struct {
}

// Implements ServiceClient.
func (bsc *BaseServiceClient) DeliverBlock(ctx context.Context, height int64) error {
	return nil
}

// Implements ServiceClient.
func (bsc *BaseServiceClient) DeliverEvent(ctx context.Context, height int64, tx tmtypes.Tx, ev *types.Event) error {
	return nil
}

// Implements ServiceClient.
func (bsc *BaseServiceClient) DeliverCommand(ctx context.Context, height int64, cmd interface{}) error {
	return nil
}

// BlockProposerKey is the block context key for storing the block proposer address.
type BlockProposerKey struct{}

// NewDefault returns a new default value for the given key.
func (bpk BlockProposerKey) NewDefault() interface{} {
	// This should never be called as a block proposer must always be created by the application
	// multiplexer.
	panic("no proposer address in block context")
}

type messageKind uint8

// MessageStateSyncCompleted is the message kind for when the node successfully performs a state
// sync. The message itself is nil.
var MessageStateSyncCompleted = messageKind(0)
