package api

import (
	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	"github.com/oasislabs/ekiden/go/common/entity"
	"github.com/oasislabs/ekiden/go/common/node"
	registry "github.com/oasislabs/ekiden/go/registry/api"
)

const (
	// RegistryTransactionTag is a unique byte used to identify
	// transactions for the entity registry application.
	RegistryTransactionTag byte = 0x01

	// RegistryAppName is the ABCI application name.
	RegistryAppName string = "registry"
)

var (
	// TagRegistryEntityRegistered is an ABCI transaction tag for new entity
	// registrations (value is entity id).
	TagRegistryEntityRegistered = []byte("registry.entity.registered")
	// TagRegistryEntityDeregistered is an ABCI transaction tag for new
	// entity registrations (value is entity id).
	TagRegistryEntityDeregistered = []byte("registry.entity.deregistered")

	// TagRegistryNodeRegistered is an ABCI transaction tag for new node
	// registrations (value is node id).
	TagRegistryNodeRegistered = []byte("registry.node.registered")

	// TagRegistryRuntimeRegistered is an ABCI transaction tag for new
	// runtime registrations (value is runtime id).
	TagRegistryRuntimeRegistered = []byte("registry.runtime.registered")
)

const (
	// QueryRegistryGetEntity is a path for GetEntity query.
	QueryRegistryGetEntity = "registry/entity"

	// QueryRegistryGetEntities is a path for GetEntities query.
	QueryRegistryGetEntities = "registry/entities"

	// QueryRegistryGetNode is a path for GetNode query.
	QueryRegistryGetNode = "registry/node"

	// QueryRegistryGetNodes is a path for GetNodes query.
	QueryRegistryGetNodes = "registry/nodes"

	// QueryRegistryGetRuntime is a path for GetRuntime query.
	QueryRegistryGetRuntime = "registry/runtime"

	// QueryRegistryGetRuntimes is a path for GetRuntimes query.
	QueryRegistryGetRuntimes = "registry/runtimes"
)

var (
	// QueryRegistryApp is a query for filtering transactions processed by
	// the registry application.
	QueryRegistryApp = QueryForEvent(TagApplication, []byte(RegistryAppName))
)

// TxRegistry is a transaction to be accepted by the registry app.
type TxRegistry struct {
	_struct struct{} `codec:",omitempty"` // nolint

	*TxRegisterEntity   `codec:"RegisterEntity"`
	*TxDeregisterEntity `codec:"DeregisterEntity"`
	*TxRegisterNode     `codec:"RegisterNode"`

	*TxRegisterRuntime `codec:"RegisterRuntime"`
}

// TxRegisterEntity is a transaction for registering a new entity.
type TxRegisterEntity struct {
	Entity entity.SignedEntity
}

// TxDeregisterEntity is a transaction for deregistering an entity.
type TxDeregisterEntity struct {
	ID signature.SignedPublicKey
}

// TxRegisterNode is a transaction for registering a new node.
type TxRegisterNode struct {
	Node node.SignedNode
}

// TxRegisterRuntime is a transaction for registering a new runtime.
type TxRegisterRuntime struct {
	Runtime registry.SignedRuntime
}

// OutputRegistry is an output of an registry app transaction.
type OutputRegistry struct {
	_struct struct{} `codec:",omitempty"` // nolint

	*OutputRegisterEntity   `codec:"RegisterEntity"`
	*OutputDeregisterEntity `codec:"DeregisterEntity"`
	*OutputRegisterNode     `codec:"RegisterNode"`

	*OutputRegisterRuntime `codec:"RegisterRuntime"`
}

// OutputRegisterEntity is an output of registering a new entity.
type OutputRegisterEntity struct {
	// Registered entity.
	Entity entity.Entity
}

// OutputDeregisterEntity is an output of deregistering an entity.
type OutputDeregisterEntity struct {
	// Deregistered entity.
	Entity entity.Entity

	// Deregistered nodes (if any).
	Nodes []node.Node
}

// OutputRegisterNode is an output of registering a new node.
type OutputRegisterNode struct {
	// Registered node.
	Node node.Node
}

// OutputRegisterRuntime is an output of registering a new node.
type OutputRegisterRuntime struct {
	// Registered runtime.
	Runtime registry.Runtime
}

// QueryGetByIDRequest is a request for fetching things by ids.
type QueryGetByIDRequest struct {
	ID signature.PublicKey
}
