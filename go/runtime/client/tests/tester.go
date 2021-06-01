// Package tests is a collection of client interface test cases.
package tests

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/runtime/client/api"
)

// Keep this above the test network's max batch timeout.
const timeout = 10 * time.Second

// ClientImplementationTests runs the client interface implementation tests.
func ClientImplementationTests(
	t *testing.T,
	client api.RuntimeClient,
	runtimeID common.Namespace,
) {
	// Include a timestamp so each test invocation uses an unique input.
	testInput := "octopus at:" + time.Now().String()
	t.Run("SubmitTx", func(t *testing.T) {
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		defer cancelFunc()
		testSubmitTransaction(ctx, t, runtimeID, client, testInput)
	})

	t.Run("Query", func(t *testing.T) {
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		defer cancelFunc()
		testQuery(ctx, t, runtimeID, client, testInput)
	})

	noWaitInput := "squid at: " + time.Now().String()
	t.Run("SubmitTxNoWait", func(t *testing.T) {
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		defer cancelFunc()
		testSubmitTransactionNoWait(ctx, t, runtimeID, client, noWaitInput)
	})
}

func testSubmitTransaction(
	ctx context.Context,
	t *testing.T,
	runtimeID common.Namespace,
	c api.RuntimeClient,
	input string,
) {
	testInput := []byte(input)
	// Submit a test transaction.
	testOutput, err := c.SubmitTx(ctx, &api.SubmitTxRequest{Data: testInput, RuntimeID: runtimeID})

	// Check if everything is in order.
	require.NoError(t, err, "SubmitTx")
	require.EqualValues(t, testInput, testOutput)
}

func testQuery(
	ctx context.Context,
	t *testing.T,
	runtimeID common.Namespace,
	c api.RuntimeClient,
	input string,
) {
	err := c.WaitBlockIndexed(ctx, &api.WaitBlockIndexedRequest{RuntimeID: runtimeID, Round: 3})
	require.NoError(t, err, "WaitBlockIndexed")

	// Fetch genesis block.
	genBlk, err := c.GetGenesisBlock(ctx, runtimeID)
	require.NoError(t, err, "GetGenesisBlock")
	require.EqualValues(t, 0, genBlk.Header.Round, "GetGenesisBlock should have Round: 0")

	// Based on SubmitTx and the mock worker.
	testInput := []byte(input)
	testOutput := testInput

	// Fetch blocks.
	blk, err := c.GetBlock(ctx, &api.GetBlockRequest{RuntimeID: runtimeID, Round: 1})
	// Epoch transition from TestNode/ExecutorWorker/InitialEpochTransition
	require.NoError(t, err, "GetBlock")
	require.EqualValues(t, 1, blk.Header.Round)

	// Normal block from TestNode/ExecutorWorker/QueueTx
	blk, err = c.GetBlock(ctx, &api.GetBlockRequest{RuntimeID: runtimeID, Round: 0xffffffffffffffff})

	// Normal block from TestNode/Client/SubmitTx
	// There can be multiple of these if ClientImplementationTests is run multiple times.
	require.NoError(t, err, "GetBlock")
	require.True(t, blk.Header.Round >= 3)
	expectedLatestRound := blk.Header.Round

	blkLatest, err := c.GetBlock(ctx, &api.GetBlockRequest{RuntimeID: runtimeID, Round: api.RoundLatest})
	require.NoError(t, err, "GetBlock(RoundLatest)")
	require.EqualValues(t, expectedLatestRound, blkLatest.Header.Round)

	// Out of bounds block round.
	_, err = c.GetBlock(ctx, &api.GetBlockRequest{RuntimeID: runtimeID, Round: expectedLatestRound + 1})
	require.Error(t, err, "GetBlock")

	err = c.WaitBlockIndexed(ctx, &api.WaitBlockIndexedRequest{RuntimeID: runtimeID, Round: expectedLatestRound})
	require.NoError(t, err, "WaitBlockIndexed")

	// Get transaction by latest round.
	tx, err := c.GetTx(ctx, &api.GetTxRequest{RuntimeID: runtimeID, Round: api.RoundLatest, Index: 0})
	require.NoError(t, err, "GetTx(RoundLatest)")
	require.EqualValues(t, expectedLatestRound, tx.Block.Header.Round)
	require.EqualValues(t, testInput, tx.Input)
	require.EqualValues(t, testOutput, tx.Output)

	// Out of bounds transaction index.
	_, err = c.GetTx(ctx, &api.GetTxRequest{RuntimeID: runtimeID, Round: api.RoundLatest, Index: 1})
	require.Error(t, err, "GetTx(1)")

	// Get transaction by block hash and index.
	tx, err = c.GetTxByBlockHash(ctx, &api.GetTxByBlockHashRequest{RuntimeID: runtimeID, BlockHash: blk.Header.EncodedHash(), Index: 0})
	require.NoError(t, err, "GetTxByBlockHash")
	require.EqualValues(t, expectedLatestRound, tx.Block.Header.Round)
	require.EqualValues(t, testInput, tx.Input)
	require.EqualValues(t, testOutput, tx.Output)

	// Invalid block hash.
	var invalidHash hash.Hash
	invalidHash.Empty()
	_, err = c.GetTxByBlockHash(ctx, &api.GetTxByBlockHashRequest{RuntimeID: runtimeID, BlockHash: invalidHash, Index: 0})
	require.Error(t, err, "GetTxByBlockHash(invalid)")

	// Check that indexer has indexed the block.
	blk, err = c.GetBlockByHash(ctx, &api.GetBlockByHashRequest{RuntimeID: runtimeID, BlockHash: blk.Header.EncodedHash()})
	require.NoError(t, err, "GetBlockByHash")
	require.EqualValues(t, expectedLatestRound, blk.Header.Round)

	// Check that indexer has indexed txn keys (check the mock worker for key/values).
	tx, err = c.QueryTx(ctx, &api.QueryTxRequest{RuntimeID: runtimeID, Key: []byte("txn_foo"), Value: []byte("txn_bar")})
	require.NoError(t, err, "QueryTx")
	require.EqualValues(t, 2, tx.Block.Header.Round)
	require.EqualValues(t, 0, tx.Index)
	// Check for values from TestNode/ExecutorWorker/QueueTx
	require.True(t, strings.HasPrefix(string(tx.Input), "hello world"))
	require.True(t, strings.HasPrefix(string(tx.Output), "hello world"))

	// Transactions (check the mock worker for content).
	txns, err := c.GetTxs(ctx, &api.GetTxsRequest{RuntimeID: runtimeID, Round: blk.Header.Round, IORoot: blk.Header.IORoot})
	require.NoError(t, err, "GetTxs")
	require.Len(t, txns, 1)
	// Check for values from TestNode/Client/SubmitTx
	require.EqualValues(t, testInput, txns[0])

	// Check events query (see mock worker for emitted events).
	events, err := c.GetEvents(ctx, &api.GetEventsRequest{RuntimeID: runtimeID, Round: 2})
	require.NoError(t, err, "GetEvents")
	require.Len(t, events, 1)
	require.EqualValues(t, []byte("txn_foo"), events[0].Key)
	require.EqualValues(t, []byte("txn_bar"), events[0].Value)

	// Test advanced transaction queries.
	query := api.Query{
		RoundMin: 0,
		RoundMax: 3,
		Conditions: []api.QueryCondition{
			{Key: []byte("txn_foo"), Values: [][]byte{[]byte("txn_bar")}},
		},
	}
	results, err := c.QueryTxs(ctx, &api.QueryTxsRequest{RuntimeID: runtimeID, Query: query})
	require.NoError(t, err, "QueryTxs")
	// One from TestNode/ExecutorWorker/QueueTx, one from TestNode/Client/SubmitTx
	require.Len(t, results, 2)
	sort.Slice(results, func(i, j int) bool {
		return bytes.Compare(results[i].Input, results[j].Input) < 0
	})
	require.EqualValues(t, 2, results[0].Block.Header.Round)
	require.EqualValues(t, 0, results[0].Index)
	// Check for values from TestNode/ExecutorWorker/QueueTx
	require.True(t, strings.HasPrefix(string(results[0].Input), "hello world"))
	require.True(t, strings.HasPrefix(string(results[0].Output), "hello world"))

	// Query genesis block again.
	genBlk2, err := c.GetGenesisBlock(ctx, runtimeID)
	require.NoError(t, err, "GetGenesisBlock2")
	require.EqualValues(t, genBlk, genBlk2, "GetGenesisBlock should match previous GetGenesisBlock")

	// Query runtime.
	// Since we are using the mock runtime host the response should be a CBOR-serialized method name
	// with the added " world" string.
	rsp, err := c.Query(ctx, &api.QueryRequest{
		RuntimeID: runtimeID,
		Round:     blk.Header.Round,
		Method:    "hello",
	})
	require.NoError(t, err, "Query")
	var decMethod string
	err = cbor.Unmarshal(rsp.Data, &decMethod)
	require.NoError(t, err, "cbor.Unmarshal(<QueryResponse.Data>)")
	require.EqualValues(t, "hello world", decMethod, "Query response should be correct")

	// Execute CheckTx using the mock runtime host.
	err = c.CheckTx(ctx, &api.CheckTxRequest{
		RuntimeID: runtimeID,
		Data:      []byte("test checktx request"),
	})
	require.NoError(t, err, "CheckTx")
}

func testSubmitTransactionNoWait(
	ctx context.Context,
	t *testing.T,
	runtimeID common.Namespace,
	c api.RuntimeClient,
	input string,
) {
	// Based on SubmitTx and the mock worker.
	testInput := []byte(input)
	testOutput := testInput

	// Query current block.
	blkLatest, err := c.GetBlock(ctx, &api.GetBlockRequest{RuntimeID: runtimeID, Round: api.RoundLatest})
	require.NoError(t, err, "GetBlock(RoundLatest)")

	// Submit a test transaction.
	err = c.SubmitTxNoWait(ctx, &api.SubmitTxRequest{Data: testInput, RuntimeID: runtimeID})

	// Check if everything is in order.
	require.NoError(t, err, "SubmitTxNoWait")

	// Ensure transaction was executed.
	err = c.WaitBlockIndexed(ctx, &api.WaitBlockIndexedRequest{RuntimeID: runtimeID, Round: blkLatest.Header.Round + 1})
	require.NoError(t, err, "WaitBlockIndexed")

	// Get transaction by latest round.
	tx, err := c.GetTx(ctx, &api.GetTxRequest{RuntimeID: runtimeID, Round: api.RoundLatest, Index: 0})
	require.NoError(t, err, "GetTx(RoundLatest)")
	require.EqualValues(t, testInput, tx.Input)
	require.EqualValues(t, testOutput, tx.Output)
}
