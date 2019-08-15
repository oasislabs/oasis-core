// Package tests is a collection of registry implementation test cases.
package tests

import (
	"context"
	"crypto"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oasislabs/ekiden/go/common/crypto/drbg"
	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	memorySigner "github.com/oasislabs/ekiden/go/common/crypto/signature/signers/memory"
	"github.com/oasislabs/ekiden/go/common/entity"
	"github.com/oasislabs/ekiden/go/common/identity"
	"github.com/oasislabs/ekiden/go/common/node"
	epochtime "github.com/oasislabs/ekiden/go/epochtime/api"
	epochtimeTests "github.com/oasislabs/ekiden/go/epochtime/tests"
	"github.com/oasislabs/ekiden/go/registry/api"
)

const recvTimeout = 5 * time.Second

// RegistryImplementationTests exercises the basic functionality of a
// registry backend.
//
// WARNING: This assumes that the registry is empty, and will leave
// a Runtime registered.
func RegistryImplementationTests(t *testing.T, backend api.Backend, timeSource epochtime.SetableBackend) {
	EnsureRegistryEmpty(t, backend)

	testRegistryEntityNodes(t, backend, timeSource)

	// Runtime registry tests are after the entity/node tests to avoid
	// interacting with the scheduler as much as possible.
	t.Run("Runtime", func(t *testing.T) {
		testRegistryRuntime(t, backend)
	})
}

func testRegistryEntityNodes(t *testing.T, backend api.Backend, timeSource epochtime.SetableBackend) { // nolint: gocyclo
	// Generate the entities used for the test cases.
	entities, err := NewTestEntities([]byte("testRegistryEntityNodes"), 3)
	require.NoError(t, err, "NewTestEntities")

	epoch, err := timeSource.GetEpoch(context.Background(), 0)
	require.NoError(t, err, "GetEpoch")

	// All of these tests are combined because the Entity and Node structures
	// are linked togehter.

	entityCh, entitySub := backend.WatchEntities()
	defer entitySub.Close()

	t.Run("EntityRegistration", func(t *testing.T) {
		require := require.New(t)

		for _, v := range entities {
			err := backend.RegisterEntity(context.Background(), v.SignedRegistration)
			require.NoError(err, "RegisterEntity")

			select {
			case ev := <-entityCh:
				require.EqualValues(v.Entity, ev.Entity, "registered entity")
				require.True(ev.IsRegistration, "event is registration")
			case <-time.After(recvTimeout):
				t.Fatalf("failed to receive entity registration event")
			}
		}

		for _, v := range entities {
			ent, err := backend.GetEntity(context.Background(), v.Entity.ID)
			require.NoError(err, "GetEntity")
			require.EqualValues(v.Entity, ent, "retrieved entity")
		}

		registeredEntities, err := backend.GetEntities(context.Background())
		require.NoError(err, "GetEntities")
		require.Len(registeredEntities, len(entities), "entities after registration")

		seen := make(map[signature.MapKey]bool)
		for _, ent := range registeredEntities {
			var isValid bool
			for _, v := range entities {
				if v.Entity.ID.Equal(ent.ID) {
					require.EqualValues(v.Entity, ent, "bulk retrieved entity")
					seen[ent.ID.ToMapKey()] = true
					isValid = true
					break
				}
			}
			require.True(isValid, "bulk retrived entity was one registered")
		}
		require.Len(seen, len(entities), "unique bulk retrived entities")
	})

	// Node tests, because there needs to be entities.
	var numNodes int
	nodes := make([][]*TestNode, 0, len(entities))
	for i, v := range entities {
		// Stagger the expirations so that it's possible to test it.
		entityNodes, err := v.NewTestNodes(i+1, 1, nil, epoch+epochtime.EpochTime(i)+1)
		require.NoError(t, err, "NewTestNodes")

		nodes = append(nodes, entityNodes)
		numNodes += len(entityNodes)
	}

	nodeCh, nodeSub := backend.WatchNodes()
	defer nodeSub.Close()

	t.Run("NodeRegistration", func(t *testing.T) {
		require := require.New(t)

		for _, vec := range nodes {
			for _, v := range vec {
				err := backend.RegisterNode(context.Background(), v.SignedInvalidRegistration1)
				require.Error(err, "register committee node without P2P addresses")

				err = backend.RegisterNode(context.Background(), v.SignedInvalidRegistration2)
				require.Error(err, "register committee node without committee addresses")

				err = backend.RegisterNode(context.Background(), v.SignedInvalidRegistration3)
				require.Error(err, "register committee node without committee certificate")

				err = backend.RegisterNode(context.Background(), v.SignedInvalidRegistration4)
				require.Error(err, "register node without roles")

				err = backend.RegisterNode(context.Background(), v.SignedInvalidRegistration5)
				require.Error(err, "register node with reserved roles")

				err = backend.RegisterNode(context.Background(), v.SignedRegistration)
				require.NoError(err, "RegisterNode")

				select {
				case ev := <-nodeCh:
					require.EqualValues(v.Node, ev.Node, "registered node")
					require.True(ev.IsRegistration, "event is registration")
				case <-time.After(recvTimeout):
					t.Fatalf("failed to receive node registration event")
				}

				nod, err := backend.GetNode(context.Background(), v.Node.ID)
				require.NoError(err, "GetNode")
				require.EqualValues(v.Node, nod, "retrieved node")

				tp, err := backend.GetNodeTransport(context.Background(), v.Node.ID)
				require.NoError(err, "GetNodeTransport")
				require.EqualValues(v.Node.Committee.Addresses, tp.Addresses, "retrieved transport addresses")
				require.EqualValues(v.Node.Committee.Certificate, tp.Certificate, "retrieved transport certificate")

				err = backend.RegisterNode(context.Background(), v.SignedValidReRegistration)
				require.NoError(err, "Re-registering a node with differnet address should work")

				err = backend.RegisterNode(context.Background(), v.SignedInvalidReRegistration)
				require.Error(err, "Re-registering a node with different runtimes should fail")

				select {
				case ev := <-nodeCh:
					require.EqualValues(v.UpdatedNode, ev.Node, "updated node")
					require.True(ev.IsRegistration, "event is registration")
				case <-time.After(recvTimeout):
					t.Fatalf("failed to receive node registration event")
				}
			}
		}
	})

	getExpectedNodeList := func() []*node.Node {
		// Derive the expected node list.
		l := make([]*node.Node, 0, numNodes)
		for _, vec := range nodes {
			for _, v := range vec {
				l = append(l, v.UpdatedNode)
			}
		}
		api.SortNodeList(l)

		return l
	}

	t.Run("NodeList", func(t *testing.T) {
		require := require.New(t)

		expectedNodeList := getExpectedNodeList()
		epoch = epochtimeTests.MustAdvanceEpoch(t, timeSource, 1)

		registeredNodes, nerr := backend.GetNodes(context.Background())
		require.NoError(nerr, "GetNodes")
		require.EqualValues(expectedNodeList, registeredNodes, "node list")
	})

	t.Run("NodeExpiration", func(t *testing.T) {
		require := require.New(t)

		// Advancing the epoch should result in the 0th entity's nodes
		// being deregistered due to expiration.
		expectedDeregEvents := len(nodes[0])
		deregisteredNodes := make(map[signature.MapKey]*node.Node)

		epoch = epochtimeTests.MustAdvanceEpoch(t, timeSource, 1)

		for i := 0; i < expectedDeregEvents; i++ {
			select {
			case ev := <-nodeCh:
				require.False(ev.IsRegistration, "event is deregistration")
				deregisteredNodes[ev.Node.ID.ToMapKey()] = ev.Node
			case <-time.After(recvTimeout):
				t.Fatalf("failed to receive node deregistration event")
			}
		}
		require.Len(deregisteredNodes, expectedDeregEvents, "deregistration events")

		for _, v := range nodes[0] {
			n, ok := deregisteredNodes[v.Node.ID.ToMapKey()]
			require.True(ok, "got deregister event for node")
			require.EqualValues(v.UpdatedNode, n, "deregistered node")
		}

		// Remove the expired nodes from the test driver's view of
		// registered nodes.
		expiredNode := nodes[0][0]
		nodes = nodes[1:]
		numNodes -= expectedDeregEvents

		// Ensure the node list doesn't have the expired nodes.
		expectedNodeList := getExpectedNodeList()
		registeredNodes, nerr := backend.GetNodes(context.Background())
		require.NoError(nerr, "GetNodes")
		require.EqualValues(expectedNodeList, registeredNodes, "node list")

		// Ensure that registering an expired node will fail.
		err := backend.RegisterNode(context.Background(), expiredNode.SignedRegistration)
		require.Error(err, "RegisterNode with expired node")
	})

	t.Run("EntityDeregistration", func(t *testing.T) {
		require := require.New(t)

		for _, v := range entities {
			err := backend.DeregisterEntity(context.Background(), v.SignedDeregistration)
			require.NoError(err, "DeregisterEntity")

			select {
			case ev := <-entityCh:
				require.EqualValues(v.Entity, ev.Entity, "deregistered entity")
				require.False(ev.IsRegistration, "event is deregistration")
			case <-time.After(recvTimeout):
				t.Fatalf("failed to receive entity deregistration event")
			}
		}

		for _, v := range entities {
			_, err := backend.GetEntity(context.Background(), v.Entity.ID)
			// require.Equal(registry.ErrNoSuchEntity, err, "GetEntity")
			require.Error(err, "GetEntity") // XXX: tendermint backend doesn't use api errors.
		}
	})

	t.Run("NodeDeregistrationViaEntity", func(t *testing.T) {
		require := require.New(t)

		deregisteredNodes := make(map[signature.MapKey]*node.Node)

		for i := 0; i < numNodes; i++ {
			select {
			case ev := <-nodeCh:
				require.False(ev.IsRegistration, "event is deregistration")
				deregisteredNodes[ev.Node.ID.ToMapKey()] = ev.Node
			case <-time.After(recvTimeout):
				t.Fatalf("failed to receive node deregistration event")
			}
		}
		require.Len(deregisteredNodes, numNodes, "deregistration events")

		for _, vec := range nodes {
			for _, v := range vec {
				n, ok := deregisteredNodes[v.Node.ID.ToMapKey()]
				require.True(ok, "got deregister event for node")
				require.EqualValues(v.UpdatedNode, n, "deregistered node")
			}
		}
	})

	// TODO: Test the various failures. (ErrNoSuchEntity is already covered)

	EnsureRegistryEmpty(t, backend)
}

func testRegistryRuntime(t *testing.T, backend api.Backend) {
	seed := []byte("testRegistryRuntime")

	require := require.New(t)

	existingRuntimes, err := backend.GetRuntimes(context.Background(), 0)
	require.NoError(err, "GetRuntimes")

	entities, err := NewTestEntities(seed, 1)
	require.NoError(err, "NewTestEntities()")

	entity := entities[0]
	err = backend.RegisterEntity(context.Background(), entity.SignedRegistration)
	require.NoError(err, "RegisterEntity")

	rt, err := NewTestRuntime(seed, entity)
	require.NoError(err, "NewTestRuntime")

	rt.MustRegister(t, backend)

	registeredRuntimes, err := backend.GetRuntimes(context.Background(), 0)
	require.NoError(err, "GetRuntimes")
	// NOTE: There can be two runtimes registered here instead of one because the worker
	//       tests that run before this register their own runtime and this runtime
	//       cannot be deregistered.
	require.Len(registeredRuntimes, len(existingRuntimes)+1, "registry has one new runtime")
	rtFound := false
	for _, regRuntime := range registeredRuntimes {
		if regRuntime.ID.Equal(rt.Runtime.ID) {
			rtFound = true
			require.EqualValues(rt.Runtime, regRuntime, "expected runtime is registered")
			break
		}
	}
	require.Equal(true, rtFound, "newly registered runtime not found")

	// Subscribe to entity deregistration event.
	ch, sub := backend.WatchEntities()
	defer sub.Close()

	err = backend.DeregisterEntity(context.Background(), entity.SignedDeregistration)
	require.NoError(err, "DeregisterEntity")

	select {
	case ev := <-ch:
		require.False(ev.IsRegistration, "expected entity deregistration event")
	case <-time.After(recvTimeout):
		t.Fatalf("Failed to receive entity deregistration event")
	}

	// TODO: Test the various failures.

	// No way to de-register the runtime, so it will be left there.
}

// EnsureRegistryEmpty enforces that the registry has no entities or nodes
// registered.
//
// Note: Runtimes are allowed, as there is no way to deregister them.
func EnsureRegistryEmpty(t *testing.T, backend api.Backend) {
	registeredEntities, err := backend.GetEntities(context.Background())
	require.NoError(t, err, "GetEntities")
	require.Len(t, registeredEntities, 0, "registered entities")

	registeredNodes, err := backend.GetNodes(context.Background())
	require.NoError(t, err, "GetNodes")
	require.Len(t, registeredNodes, 0, "registered nodes")
}

// TestEntity is a testing Entity and some common pre-generated/signed
// blobs useful for testing.
type TestEntity struct {
	Entity *entity.Entity
	Signer signature.Signer

	SignedRegistration   *entity.SignedEntity
	SignedDeregistration *signature.Signed
}

// TestNode is a testing Node and some common pre-generated/signed blobs
// useful for testing.
type TestNode struct {
	Node        *node.Node
	UpdatedNode *node.Node
	Signer      signature.Signer

	SignedRegistration          *node.SignedNode
	SignedInvalidRegistration1  *node.SignedNode
	SignedInvalidRegistration2  *node.SignedNode
	SignedInvalidRegistration3  *node.SignedNode
	SignedInvalidRegistration4  *node.SignedNode
	SignedInvalidRegistration5  *node.SignedNode
	SignedValidReRegistration   *node.SignedNode
	SignedInvalidReRegistration *node.SignedNode
}

// NewTestNodes returns the specified number of TestNodes, generated
// deterministically using the entity's public key as the seed.
func (ent *TestEntity) NewTestNodes(nCompute int, nStorage int, runtimes []*TestRuntime, expiration epochtime.EpochTime) ([]*TestNode, error) {
	if nCompute <= 0 || nStorage <= 0 || nCompute > 254 || nStorage > 254 {
		return nil, errors.New("registry/tests: test node count out of bounds")
	}
	n := nCompute + nStorage

	rng, err := drbg.New(crypto.SHA512, hashForDrbg(ent.Entity.ID), nil, []byte("TestNodes"))
	if err != nil {
		return nil, err
	}

	var nodeRts []*node.Runtime
	for _, v := range runtimes {
		nodeRts = append(nodeRts, &node.Runtime{
			ID: v.Runtime.ID,
		})
	}

	nodes := make([]*TestNode, 0, n)
	for i := 0; i < n; i++ {
		var nod TestNode
		if nod.Signer, err = memorySigner.NewSigner(rng); err != nil {
			return nil, err
		}

		var role node.RolesMask
		if i < nCompute {
			role = node.RoleComputeWorker | node.RoleTransactionScheduler | node.RoleMergeWorker
		} else {
			role = node.RoleStorageWorker
		}

		nod.Node = &node.Node{
			ID:               nod.Signer.Public(),
			EntityID:         ent.Entity.ID,
			Expiration:       uint64(expiration),
			RegistrationTime: uint64(time.Now().Unix()),
			Runtimes:         nodeRts,
			Roles:            role,
		}
		addr := node.Address{
			TCPAddr: net.TCPAddr{
				IP:   []byte{192, 0, 2, byte(i + 1)},
				Port: 451,
			},
		}
		nod.Node.P2P.Addresses = append(nod.Node.P2P.Addresses, addr)
		nod.Node.Committee.Addresses = append(nod.Node.Committee.Addresses, addr)
		// Generate dummy TLS certificate.
		tlsCert, err := identity.GenerateTLSCert()
		if err != nil {
			return nil, err
		}
		nod.Node.Committee.Certificate = tlsCert.Certificate[0]

		nod.SignedRegistration, err = node.SignNode(ent.Signer, api.RegisterNodeSignatureContext, nod.Node)
		if err != nil {
			return nil, err
		}

		// Add a registration with no P2P addresses.
		invalid1 := *nod.Node
		invalid1.P2P.Addresses = nil

		nod.SignedInvalidRegistration1, err = node.SignNode(ent.Signer, api.RegisterNodeSignatureContext, &invalid1)
		if err != nil {
			return nil, err
		}

		// Add a registration with no committee addresses.
		invalid2 := *nod.Node
		invalid2.Committee.Addresses = nil

		nod.SignedInvalidRegistration2, err = node.SignNode(ent.Signer, api.RegisterNodeSignatureContext, &invalid2)
		if err != nil {
			return nil, err
		}

		// Add a registration with no committee certificate.
		invalid3 := *nod.Node
		invalid3.Committee.Certificate = nil

		nod.SignedInvalidRegistration3, err = node.SignNode(ent.Signer, api.RegisterNodeSignatureContext, &invalid3)
		if err != nil {
			return nil, err
		}

		// Add a registration without any roles.
		invalid4 := *nod.Node
		invalid4.Roles = 0

		nod.SignedInvalidRegistration4, err = node.SignNode(ent.Signer, api.RegisterNodeSignatureContext, &invalid4)
		if err != nil {
			return nil, err
		}

		// Add a registration with reserved roles.
		invalid5 := *nod.Node
		invalid5.Roles = 0xFFFFFFFF

		nod.SignedInvalidRegistration5, err = node.SignNode(ent.Signer, api.RegisterNodeSignatureContext, &invalid5)
		if err != nil {
			return nil, err
		}

		// Add another Re-Registration with different address field.
		nod.UpdatedNode = &node.Node{
			ID:               nod.Signer.Public(),
			EntityID:         ent.Entity.ID,
			Expiration:       uint64(expiration),
			RegistrationTime: uint64(time.Now().Unix()) + 10, // Ensure greater than initial registration.
			Runtimes:         nodeRts,
			Roles:            role,
		}
		addr = node.Address{
			TCPAddr: net.TCPAddr{
				IP:   []byte{192, 0, 2, byte(i + 1)},
				Port: 452,
			},
		}
		nod.UpdatedNode.P2P.Addresses = append(nod.UpdatedNode.P2P.Addresses, addr)
		nod.UpdatedNode.Committee.Addresses = append(nod.UpdatedNode.Committee.Addresses, addr)
		nod.UpdatedNode.Committee.Certificate = nod.Node.Committee.Certificate
		nod.SignedValidReRegistration, err = node.SignNode(ent.Signer, api.RegisterNodeSignatureContext, nod.UpdatedNode)
		if err != nil {
			return nil, err
		}

		// Add invalid Re-Registration with changed Roles field.
		testRuntimeSigner := memorySigner.NewTestSigner("invalod-registration-runtime-seed")
		newNode := &node.Node{
			ID:               nod.Signer.Public(),
			EntityID:         ent.Entity.ID,
			Expiration:       uint64(expiration),
			RegistrationTime: uint64(time.Now().Unix()),
			Runtimes:         append(nodeRts, &node.Runtime{ID: testRuntimeSigner.Public()}),
			Roles:            role,
			P2P:              nod.Node.P2P,
			Committee:        nod.Node.Committee,
		}
		nod.SignedInvalidReRegistration, err = node.SignNode(ent.Signer, api.RegisterNodeSignatureContext, newNode)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, &nod)
	}

	return nodes, nil
}

// NewTestEntities returns the specified number of TestEntities, generated
// deterministically from the seed.
func NewTestEntities(seed []byte, n int) ([]*TestEntity, error) {
	rng, err := drbg.New(crypto.SHA512, hashForDrbg(seed), nil, []byte("TestEntity"))
	if err != nil {
		return nil, err
	}

	entities := make([]*TestEntity, 0, n)
	for i := 0; i < n; i++ {
		var ent TestEntity
		if ent.Signer, err = memorySigner.NewSigner(rng); err != nil {
			return nil, err
		}
		ent.Entity = &entity.Entity{
			ID:                     ent.Signer.Public(),
			RegistrationTime:       uint64(time.Now().Unix()),
			AllowEntitySignedNodes: true,
		}

		signed, err := signature.SignSigned(ent.Signer, api.RegisterEntitySignatureContext, ent.Entity)
		if err != nil {
			return nil, err
		}
		ent.SignedRegistration = &entity.SignedEntity{Signed: *signed}

		ts := api.Timestamp(uint64(time.Now().Unix()))
		signed, err = signature.SignSigned(ent.Signer, api.DeregisterEntitySignatureContext, &ts)
		if err != nil {
			return nil, err
		}
		ent.SignedDeregistration = signed

		entities = append(entities, &ent)
	}

	return entities, nil
}

// TestRuntime is a testing Runtime and some common pre-generated/signed
// blobs useful for testing.
type TestRuntime struct {
	Runtime *api.Runtime
	Signer  signature.Signer

	entity *TestEntity
	nodes  []*TestNode

	didRegister bool
}

// MustRegister registers the TestRuntime with the provided registry.
func (rt *TestRuntime) MustRegister(t *testing.T, backend api.Backend) {
	require := require.New(t)

	ch, sub := backend.WatchRuntimes()
	defer sub.Close()

	rt.Runtime.RegistrationTime = uint64(time.Now().Unix())
	signed, err := signature.SignSigned(rt.Signer, api.RegisterRuntimeSignatureContext, rt.Runtime)
	require.NoError(err, "signed runtime descriptor")

	err = backend.RegisterRuntime(context.Background(), &api.SignedRuntime{Signed: *signed})
	require.NoError(err, "RegisterRuntime")

	var seen int
	for {
		select {
		case v := <-ch:
			if !rt.Runtime.ID.Equal(v.ID) {
				continue
			}

			// If the runtime is expected to already be in the registry
			// (this is a re-registration), skip the event emitted
			// corresponding to the pre-existing entry.
			if seen > 0 || !rt.didRegister {
				require.EqualValues(rt.Runtime, v, "registered runtime")
				rt.didRegister = true
				return
			}
			seen++
		case <-time.After(recvTimeout):
			t.Fatalf("failed to receive runtime registration event")
		}
	}
}

// Populate populates the registry for a given TestRuntime.
func (rt *TestRuntime) Populate(t *testing.T, backend api.Backend, runtime *TestRuntime, seed []byte) []*node.Node {
	require := require.New(t)

	require.Nil(rt.entity, "runtime has no associated entity")
	require.Nil(rt.nodes, "runtime has no associated nodes")

	return BulkPopulate(t, backend, []*TestRuntime{runtime}, seed)
}

// PopulateBulk bulk populates the registry for the given TestRuntimes.
func BulkPopulate(t *testing.T, backend api.Backend, runtimes []*TestRuntime, seed []byte) []*node.Node {
	require := require.New(t)

	require.True(len(runtimes) > 0, "at least one runtime")
	EnsureRegistryEmpty(t, backend)

	// Create the one entity that has ownership of every single node
	// that will be associated with every runtime.
	entityCh, entitySub := backend.WatchEntities()
	defer entitySub.Close()

	entities, err := NewTestEntities(seed, 1)
	require.NoError(err, "NewTestEntities")
	entity := entities[0]
	err = backend.RegisterEntity(context.Background(), entity.SignedRegistration)
	require.NoError(err, "RegisterEntity")
	select {
	case ev := <-entityCh:
		require.EqualValues(entity.Entity, ev.Entity, "registered entity")
		require.True(ev.IsRegistration, "event is registration")
	case <-time.After(recvTimeout):
		t.Fatalf("failed to receive entity registration event")
	}

	for _, v := range runtimes {
		v.Signer = entity.Signer
	}

	// For the sake of simplicity, require that all runtimes have the same
	// number of nodes for now.

	nodeCh, nodeSub := backend.WatchNodes()
	defer nodeSub.Close()

	numCompute := int(runtimes[0].Runtime.ReplicaGroupSize + runtimes[0].Runtime.ReplicaGroupBackupSize)
	numStorage := int(runtimes[0].Runtime.StorageGroupSize)
	nodes, err := entity.NewTestNodes(numCompute, numStorage, runtimes, epochtime.EpochInvalid)
	require.NoError(err, "NewTestNodes")

	ret := make([]*node.Node, 0, numCompute+numStorage)
	for _, node := range nodes {
		err = backend.RegisterNode(context.Background(), node.SignedRegistration)
		require.NoError(err, "RegisterNode")
		select {
		case ev := <-nodeCh:
			require.EqualValues(node.Node, ev.Node, "registered node")
			require.True(ev.IsRegistration, "event is registration")
		case <-time.After(recvTimeout):
			t.Fatalf("failed to receive node registration event")
		}
		ret = append(ret, node.Node)
	}

	for _, v := range runtimes {
		numNodes := v.Runtime.ReplicaGroupSize + v.Runtime.ReplicaGroupBackupSize + v.Runtime.StorageGroupSize
		require.EqualValues(len(nodes), numNodes, "runtime wants the expected number of nodes")
		v.entity = entity
		v.nodes = nodes
	}

	return ret
}

// TestNodes returns the test runtime's TestNodes.
func (rt *TestRuntime) TestNodes() []*TestNode {
	return rt.nodes
}

// Cleanup deregisteres the entity and nodes for a given TestRuntime.
func (rt *TestRuntime) Cleanup(t *testing.T, backend api.Backend) {
	require := require.New(t)

	require.NotNil(rt.entity, "runtime has an associated entity")
	require.NotNil(rt.nodes, "runtime has associated nodes")

	entityCh, entitySub := backend.WatchEntities()
	defer entitySub.Close()

	nodeCh, nodeSub := backend.WatchNodes()
	defer nodeSub.Close()

	err := backend.DeregisterEntity(context.Background(), rt.entity.SignedDeregistration)
	require.NoError(err, "DeregisterEntity")

	select {
	case ev := <-entityCh:
		require.EqualValues(rt.entity.Entity, ev.Entity, "deregistered entity")
		require.False(ev.IsRegistration, "event is deregistration")
	case <-time.After(recvTimeout):
		t.Fatalf("failed to receive entity deregistration event")
	}

	var numDereg int
	for numDereg < len(rt.nodes) {
		select {
		case ev := <-nodeCh:
			require.False(ev.IsRegistration, "event is deregistration")
			numDereg++
		case <-time.After(recvTimeout):
			t.Fatalf("failed to receive node deregistration event")
		}
	}

	EnsureRegistryEmpty(t, backend)
	rt.entity = nil
	rt.nodes = nil
}

// NewTestRuntime returns a pre-generated TestRuntime for use with various
// tests, generated deterministically from the seed.
func NewTestRuntime(seed []byte, entity *TestEntity) (*TestRuntime, error) {
	rng, err := drbg.New(crypto.SHA512, hashForDrbg(seed), nil, []byte("TestRuntime"))
	if err != nil {
		return nil, err
	}

	var rt TestRuntime
	if rt.Signer, err = memorySigner.NewSigner(rng); err != nil {
		return nil, err
	}

	rt.Runtime = &api.Runtime{
		ID:                            rt.Signer.Public(),
		ReplicaGroupSize:              3,
		ReplicaGroupBackupSize:        5,
		ReplicaAllowedStragglers:      1,
		StorageGroupSize:              3,
		TransactionSchedulerGroupSize: 3,
	}
	if entity != nil {
		rt.Signer = entity.Signer
	}

	// TODO: Test with non-empty state root when enabled.
	rt.Runtime.Genesis.StateRoot.Empty()

	return &rt, nil
}

func hashForDrbg(seed []byte) []byte {
	h := crypto.SHA512.New()
	_, _ = h.Write(seed)
	return h.Sum(nil)
}
