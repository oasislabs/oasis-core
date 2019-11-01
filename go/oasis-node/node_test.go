package main

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	beaconTests "github.com/oasislabs/oasis-core/go/beacon/tests"
	clientTests "github.com/oasislabs/oasis-core/go/client/tests"
	"github.com/oasislabs/oasis-core/go/common"
	"github.com/oasislabs/oasis-core/go/common/crypto/hash"
	"github.com/oasislabs/oasis-core/go/common/crypto/signature"
	fileSigner "github.com/oasislabs/oasis-core/go/common/crypto/signature/signers/file"
	"github.com/oasislabs/oasis-core/go/common/entity"
	epochtime "github.com/oasislabs/oasis-core/go/epochtime/api"
	epochtimeTests "github.com/oasislabs/oasis-core/go/epochtime/tests"
	cmdCommon "github.com/oasislabs/oasis-core/go/oasis-node/cmd/common"
	"github.com/oasislabs/oasis-core/go/oasis-node/cmd/node"
	registry "github.com/oasislabs/oasis-core/go/registry/api"
	registryTests "github.com/oasislabs/oasis-core/go/registry/tests"
	roothashTests "github.com/oasislabs/oasis-core/go/roothash/tests"
	schedulerTests "github.com/oasislabs/oasis-core/go/scheduler/tests"
	stakingTests "github.com/oasislabs/oasis-core/go/staking/tests"
	storageClient "github.com/oasislabs/oasis-core/go/storage/client"
	storageClientTests "github.com/oasislabs/oasis-core/go/storage/client/tests"
	storageTests "github.com/oasislabs/oasis-core/go/storage/tests"
	"github.com/oasislabs/oasis-core/go/tendermint/abci"
	computeCommittee "github.com/oasislabs/oasis-core/go/worker/compute/committee"
	computeWorkerTests "github.com/oasislabs/oasis-core/go/worker/compute/tests"
	storageWorkerTests "github.com/oasislabs/oasis-core/go/worker/storage/tests"
	txnschedulerCommittee "github.com/oasislabs/oasis-core/go/worker/txnscheduler/committee"
	txnschedulerWorkerTests "github.com/oasislabs/oasis-core/go/worker/txnscheduler/tests"
)

const (
	workerClientPort = "9010"
)

var (
	// NOTE: Configuration option that can't be set statically will be
	// configured directly in newTestNode().
	testNodeStaticConfig = []struct {
		key   string
		value interface{}
	}{
		{"log.level.default", "DEBUG"},
		{"consensus.validator", true},
		{"roothash.tendermint.index_blocks", true},
		{"storage.backend", "leveldb"},
		{"worker.compute.enabled", true},
		{"worker.runtime.backend", "mock"},
		{"worker.runtime.loader", "mock-runtime"},
		{"worker.runtime.binary", "mock-runtime"},
		{"worker.storage.enabled", true},
		{"worker.client.port", workerClientPort},
		{"worker.txnscheduler.enabled", true},
		{"worker.merge.enabled", true},
		{"debug.allow_test_keys", true},
	}

	testRuntime = &registry.Runtime{
		// ID: default value,
		ReplicaGroupSize:              1,
		StorageGroupSize:              1,
		TransactionSchedulerGroupSize: 1,
	}

	testNamespace common.Namespace
	testRuntimeID signature.PublicKey

	initConfigOnce sync.Once
)

type testNode struct {
	*node.Node

	runtimeID                 signature.PublicKey
	computeCommitteeNode      *computeCommittee.Node
	txnschedulerCommitteeNode *txnschedulerCommittee.Node

	entity       *entity.Entity
	entitySigner signature.Signer

	dataDir string
	start   time.Time
}

func (n *testNode) Stop() {
	const waitTime = 1 * time.Second

	// HACK: The gRPC server will cause a segfault if it is torn down
	// while it is still in the process of being initialized.  There is
	// currently no way to wait for it to launch either.
	if elapsed := time.Since(n.start); elapsed < waitTime {
		time.Sleep(waitTime - elapsed)
	}

	n.Node.Stop()
	n.Node.Wait()
	n.Node.Cleanup()
}

func newTestNode(t *testing.T) *testNode {
	initConfigOnce.Do(func() {
		cmdCommon.InitConfig()
	})

	require := require.New(t)

	dataDir, err := ioutil.TempDir("", "oasis-node-test_")
	require.NoError(err, "create data dir")

	signerFactory := fileSigner.NewFactory(dataDir, signature.SignerEntity)
	entity, entitySigner, err := entity.Generate(dataDir, signerFactory, nil)
	require.NoError(err, "create test entity")

	viper.Set("datadir", dataDir)
	viper.Set("log.file", filepath.Join(dataDir, "test-node.log"))
	viper.Set("worker.runtime.id", testRuntimeID.String())
	viper.Set("client.indexer.runtimes", []string{testRuntimeID.String()})
	viper.Set("worker.registration.entity", filepath.Join(dataDir, "entity.json"))
	for _, kv := range testNodeStaticConfig {
		viper.Set(kv.key, kv.value)
	}

	n := &testNode{
		runtimeID:    testRuntime.ID,
		dataDir:      dataDir,
		entity:       entity,
		entitySigner: entitySigner,
		start:        time.Now(),
	}
	t.Logf("starting node, data directory: %v", dataDir)
	n.Node, err = node.NewTestNode()
	require.NoError(err, "start node")

	// Add the testNode to the newly generated entity's list of nodes
	// that can self-certify.
	n.entity.Nodes = []signature.PublicKey{
		n.Node.Identity.NodeSigner.Public(),
	}

	return n
}

type testCase struct {
	name string
	fn   func(*testing.T, *testNode)
}

func (tc *testCase) Run(t *testing.T, node *testNode) {
	t.Run(tc.name, func(t *testing.T) {
		tc.fn(t, node)
	})
}

func TestNode(t *testing.T) {
	node := newTestNode(t)
	defer func() {
		node.Stop()
		switch t.Failed() {
		case true:
			t.Logf("one or more tests failed, preserving data directory: %v", node.dataDir)
		case false:
			os.RemoveAll(node.dataDir)
		}
	}()

	// Wait for consensus to become ready before proceeding.
	select {
	case <-node.Consensus.Synced():
	case <-time.After(5 * time.Second):
		t.Fatalf("failed to wait for consensus to become ready")
	}

	// NOTE: Order of test cases is important.
	testCases := []*testCase{
		{"AwaitFirstBlock", testAwaitFirstBlock},

		// Register the test entity and runtime used by every single test,
		// including the worker tests.
		{"RegisterTestEntityRuntime", testRegisterEntityRuntime},

		{"ComputeWorker", testComputeWorker},
		{"TransactionSchedulerWorker", testTransactionSchedulerWorker},

		// StorageWorker test case
		{"StorageWorker", testStorageWorker},

		// Client tests also need a functional runtime.
		{"Client", testClient},

		// Staking requires a registered node that is a validator.
		{"Staking", testStaking},

		// TestStorageClientWithNode runs storage tests against a storage client
		// connected to this node.
		{"TestStorageClientWithNode", testStorageClientWithNode},

		// Clean up and ensure the registry is empty for the following tests.
		{"DeregisterTestEntityRuntime", testDeregisterEntityRuntime},

		{"EpochTime", testEpochTime},
		{"Beacon", testBeacon},
		{"Storage", testStorage},
		{"Registry", testRegistry},
		{"Scheduler", testScheduler},
		{"RootHash", testRootHash},

		// TestStorageClientWithoutNode runs client tests that use a mock storage
		// node and mock committees.
		{"TestStorageClientWithoutNode", testStorageClientWithoutNode},
	}

	for _, tc := range testCases {
		tc.Run(t, node)
	}
}

func testRegisterEntityRuntime(t *testing.T, node *testNode) {
	require := require.New(t)

	// Register node entity.
	node.entity.RegistrationTime = uint64(time.Now().Unix())
	signedEnt, err := entity.SignEntity(node.entitySigner, registry.RegisterEntitySignatureContext, node.entity)
	require.NoError(err, "sign node entity")
	err = node.Node.Registry.RegisterEntity(context.Background(), signedEnt)
	require.NoError(err, "register test entity")

	// Register the test runtime.
	testRuntime.RegistrationTime = uint64(time.Now().Unix())
	signedRt, err := registry.SignRuntime(node.entitySigner, registry.RegisterRuntimeSignatureContext, testRuntime)
	require.NoError(err, "sign runtime descriptor")
	err = node.Node.Registry.RegisterRuntime(context.Background(), signedRt)
	require.NoError(err, "register test runtime")

	// Get the runtime and the corresponding compute committee node instance.
	computeRT := node.ComputeWorker.GetRuntime(testRuntime.ID)
	require.NotNil(t, computeRT)
	node.computeCommitteeNode = computeRT.GetNode()

	// Get the runtime and the corresponding transaction scheduler committee node instance.
	require.Equal(node.TransactionSchedulerWorker.GetConfig().Runtimes[0].ID, testRuntime.ID)
	txnschedulerRT := node.TransactionSchedulerWorker.GetRuntime(testRuntime.ID)
	require.NotNil(t, txnschedulerRT)
	node.txnschedulerCommitteeNode = txnschedulerRT.GetNode()
}

func testDeregisterEntityRuntime(t *testing.T, node *testNode) {
	// Stop the registration service and wait for it to fully stop. This is required
	// as otherwise it will re-register the node on each epoch transition.
	node.WorkerRegistration.Stop()
	<-node.WorkerRegistration.Quit()

	// Subscribe to node deregistration event.
	nodeCh, sub := node.Node.Registry.WatchNodes()
	defer sub.Close()

	// Perform an epoch transition to expire the node as otherwise there is no way
	// to deregister the entity.
	require.Implements(t, (*epochtime.SetableBackend)(nil), node.Epochtime, "epoch time backend is mock")
	timeSource := (node.Epochtime).(epochtime.SetableBackend)
	_ = epochtimeTests.MustAdvanceEpoch(t, timeSource, 2+1+1) // 2 epochs for expiry, 1 for debonding, 1 for removal.

WaitLoop:
	for {
		select {
		case ev := <-nodeCh:
			// NOTE: There can be in-flight registrations from before the registration worker
			//       was stopped. Make sure to skip them.
			if ev.IsRegistration {
				continue
			}

			require.Equal(t, ev.Node.ID, node.Identity.NodeSigner.Public(), "expected node deregistration event")
			break WaitLoop
		case <-time.After(1 * time.Second):
			t.Fatalf("Failed to receive node deregistration event")
		}
	}

	// Deregister the entity.
	ts := registry.Timestamp(uint64(time.Now().Unix()))
	signed, err := signature.SignSigned(node.entitySigner, registry.DeregisterEntitySignatureContext, &ts)
	require.NoError(t, err, "SignSigned")

	// Subscribe to entity deregistration event.
	entityCh, sub := node.Node.Registry.WatchEntities()
	defer sub.Close()

	err = node.Node.Registry.DeregisterEntity(context.Background(), signed)
	require.NoError(t, err, "DeregisterEntity")

	select {
	case ev := <-entityCh:
		require.False(t, ev.IsRegistration, "expected entity deregistration event")
	case <-time.After(1 * time.Second):
		t.Fatalf("Failed to receive entity deregistration event")
	}

	registryTests.EnsureRegistryEmpty(t, node.Node.Registry)
}

func testAwaitFirstBlock(t *testing.T, node *testNode) {
	timeSource := node.Epochtime

	for {
		epoch, err := timeSource.GetEpoch(context.Background(), 0)
		if err != nil {
			if err != abci.ErrNoCommittedBlocks {
				require.NoError(t, err, "GetEpoch")
				return
			}
			time.Sleep(1 * time.Second)
			continue
		}
		t.Log("ah, epoch", epoch)
		break
	}
}

func testEpochTime(t *testing.T, node *testNode) {
	epochtimeTests.EpochtimeSetableImplementationTest(t, node.Epochtime)
}

func testBeacon(t *testing.T, node *testNode) {
	timeSource := (node.Epochtime).(epochtime.SetableBackend)

	beaconTests.BeaconImplementationTests(t, node.Beacon, timeSource)
}

func testStorage(t *testing.T, node *testNode) {
	// Determine the current round. This is required so that we can commit into
	// storage at the next (non-finalized) round.
	blk, err := node.RootHash.GetLatestBlock(context.Background(), testRuntimeID, 0)
	require.NoError(t, err, "GetLatestBlock")

	storageTests.StorageImplementationTests(t, node.Storage, testNamespace, blk.Header.Round+1)
}

func testRegistry(t *testing.T, node *testNode) {
	timeSource := (node.Epochtime).(epochtime.SetableBackend)

	registryTests.RegistryImplementationTests(t, node.Registry, timeSource)
}

func testScheduler(t *testing.T, node *testNode) {
	timeSource := (node.Epochtime).(epochtime.SetableBackend)

	schedulerTests.SchedulerImplementationTests(t, node.Scheduler, timeSource, node.Registry)
}

func testStaking(t *testing.T, node *testNode) {
	timeSource := (node.Epochtime).(epochtime.SetableBackend)

	stakingTests.StakingImplementationTests(t, node.Staking, timeSource, node.Registry, node.RootHash, node.Identity, node.entity, node.entitySigner, testRuntimeID)
}

func testRootHash(t *testing.T, node *testNode) {
	timeSource := (node.Epochtime).(epochtime.SetableBackend)

	roothashTests.RootHashImplementationTests(t, node.RootHash, timeSource, node.Scheduler, node.Storage, node.Registry, node.Staking)
}

func testComputeWorker(t *testing.T, node *testNode) {
	timeSource := (node.Epochtime).(epochtime.SetableBackend)

	require.NotNil(t, node.computeCommitteeNode)
	computeWorkerTests.WorkerImplementationTests(t, node.ComputeWorker, node.runtimeID, node.computeCommitteeNode, timeSource)
}

func testStorageWorker(t *testing.T, node *testNode) {
	storageWorkerTests.WorkerImplementationTests(t, node.StorageWorker)
}

func testTransactionSchedulerWorker(t *testing.T, node *testNode) {
	timeSource := (node.Epochtime).(epochtime.SetableBackend)

	require.NotNil(t, node.txnschedulerCommitteeNode)
	txnschedulerWorkerTests.WorkerImplementationTests(t, node.TransactionSchedulerWorker, node.runtimeID, node.txnschedulerCommitteeNode, timeSource, node.RootHash, node.Storage)
}

func testClient(t *testing.T, node *testNode) {
	clientTests.ClientImplementationTests(t, node.Client, node.runtimeID)
}

func testStorageClientWithNode(t *testing.T, node *testNode) {
	ctx := context.Background()

	// Client storage implementation tests.
	config := []struct {
		key   string
		value interface{}
	}{
		{"storage.debug.client.address", "localhost:" + workerClientPort},
		{"storage.debug.client.tls", node.dataDir + "/tls_identity_cert.pem"},
	}
	for _, kv := range config {
		viper.Set(kv.key, kv.value)
	}
	debugClient, err := storageClient.New(ctx, node.Identity, nil, nil)
	require.NoError(t, err, "NewDebugStorageClient")

	// Determine the current round. This is required so that we can commit into
	// storage at the next (non-finalized) round.
	blk, err := node.RootHash.GetLatestBlock(ctx, testRuntimeID, 0)
	require.NoError(t, err, "GetLatestBlock")

	storageTests.StorageImplementationTests(t, debugClient, testNamespace, blk.Header.Round+1)

	// Reset configuration flags.
	for _, kv := range config {
		viper.Set(kv.key, "")
	}
}

func testStorageClientWithoutNode(t *testing.T, node *testNode) {
	timeSource := (node.Epochtime).(epochtime.SetableBackend)

	// Storage client tests without node.
	storageClientTests.ClientWorkerTests(t, node.Identity, node.Beacon, timeSource, node.Registry, node.Scheduler)
}

func init() {
	var ns hash.Hash
	ns.FromBytes([]byte("oasis node test namespace"))
	copy(testNamespace[:], ns[:])

	var err error
	testRuntimeID, err = testNamespace.ToRuntimeID()
	if err != nil {
		panic("Unable to convert namespace to runtime ID")
	}
	testRuntime.ID = testRuntimeID

	testRuntime.Genesis.StateRoot.Empty()
}
