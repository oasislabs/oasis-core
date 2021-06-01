// Package client contains the runtime client.
package client

import (
	"context"
	"fmt"
	"sync"

	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/errors"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/common/pubsub"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	keymanagerAPI "github.com/oasisprotocol/oasis-core/go/keymanager/api"
	keymanager "github.com/oasisprotocol/oasis-core/go/keymanager/client"
	cmdFlags "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common/flags"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/block"
	"github.com/oasisprotocol/oasis-core/go/runtime/client/api"
	enclaverpc "github.com/oasisprotocol/oasis-core/go/runtime/enclaverpc/api"
	"github.com/oasisprotocol/oasis-core/go/runtime/host"
	runtimeRegistry "github.com/oasisprotocol/oasis-core/go/runtime/registry"
	"github.com/oasisprotocol/oasis-core/go/runtime/tagindexer"
	"github.com/oasisprotocol/oasis-core/go/runtime/transaction"
	storage "github.com/oasisprotocol/oasis-core/go/storage/api"
	"github.com/oasisprotocol/oasis-core/go/worker/common/p2p"
)

const (
	// CfgMaxTransactionAge is the number of consensus blocks after which
	// submitted transactions will be considered expired.
	CfgMaxTransactionAge = "runtime.client.max_transaction_age"

	minMaxTransactionAge = 30
)

var (
	_ api.RuntimeClient    = (*runtimeClient)(nil)
	_ enclaverpc.Transport = (*runtimeClient)(nil)

	// Flags has the flags used by the runtime client.
	Flags = flag.NewFlagSet("", flag.ContinueOnError)
)

type clientCommon struct {
	storage         storage.Backend
	consensus       consensus.Backend
	runtimeRegistry runtimeRegistry.Registry
	// p2p may be nil.
	p2p *p2p.P2P

	ctx context.Context
}

type runtimeClient struct {
	sync.Mutex

	common *clientCommon
	quitCh chan struct{}

	hosts        map[common.Namespace]*clientHost
	txSubmitters map[common.Namespace]*txSubmitter
	kmClients    map[common.Namespace]*keymanager.Client

	maxTransactionAge int64

	logger *logging.Logger
}

func (c *runtimeClient) tagIndexer(runtimeID common.Namespace) (tagindexer.QueryableBackend, error) {
	rt, err := c.common.runtimeRegistry.GetRuntime(runtimeID)
	if err != nil {
		return nil, err
	}

	return rt.TagIndexer(), nil
}

func (c *runtimeClient) submitTx(ctx context.Context, request *api.SubmitTxRequest) (<-chan *txResult, error) {
	if c.common.p2p == nil {
		return nil, fmt.Errorf("client: cannot submit transaction, p2p disabled")
	}

	// Make sure consensus is synced.
	select {
	case <-c.common.consensus.Synced():
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, api.ErrNotSynced
	}

	// Perform a local transaction check when a hosted runtime is available.
	if hrt, ok := c.hosts[request.RuntimeID]; ok && hrt.GetHostedRuntime() != nil {
		err := c.CheckTx(ctx, &api.CheckTxRequest{
			RuntimeID: request.RuntimeID,
			Data:      request.Data,
		})
		if err != nil {
			return nil, err
		}
	}

	var submitter *txSubmitter
	var ok bool
	c.Lock()
	if submitter, ok = c.txSubmitters[request.RuntimeID]; !ok {
		submitter = newTxSubmitter(c.common, request.RuntimeID, c.common.p2p, c.maxTransactionAge)
		submitter.Start()
		c.txSubmitters[request.RuntimeID] = submitter
	}
	c.Unlock()

	// Send a request for watching a new runtime transaction.
	respCh := make(chan *txResult)
	req := &txRequest{
		ctx:    ctx,
		respCh: respCh,
		req:    request,
	}
	req.id.FromBytes(request.Data)
	select {
	case <-ctx.Done():
		// The context we're working in was canceled, abort.
		return nil, ctx.Err()
	case <-c.common.ctx.Done():
		// Client is shutting down.
		return nil, fmt.Errorf("client: shutting down")
	case submitter.newCh <- req:
	}

	return respCh, nil
}

// Implements api.RuntimeClient.
func (c *runtimeClient) SubmitTx(ctx context.Context, request *api.SubmitTxRequest) ([]byte, error) {
	respCh, err := c.submitTx(ctx, request)
	if err != nil {
		return nil, err
	}

	// Wait for result.
	for {
		var resp *txResult
		var ok bool

		select {
		case <-ctx.Done():
			// The context we're working in was canceled, abort.
			return nil, ctx.Err()
		case <-c.common.ctx.Done():
			// Client is shutting down.
			return nil, fmt.Errorf("client: shutting down")
		case resp, ok = <-respCh:
			if !ok {
				return nil, fmt.Errorf("client: block watch channel closed unexpectedly (unknown error)")
			}
			return resp.result, resp.err
		}
	}
}

// Implements api.RuntimeClient.
func (c *runtimeClient) SubmitTxNoWait(ctx context.Context, request *api.SubmitTxRequest) error {
	_, err := c.submitTx(ctx, request)
	return err
}

// Implements api.RuntimeClient.
func (c *runtimeClient) CheckTx(ctx context.Context, request *api.CheckTxRequest) error {
	hrt, ok := c.hosts[request.RuntimeID]
	if !ok {
		return api.ErrNoHostedRuntime
	}
	rt := hrt.GetHostedRuntime()
	if rt == nil {
		return api.ErrNoHostedRuntime
	}

	// Get current blocks.
	rs, err := c.common.consensus.RootHash().GetRuntimeState(ctx, request.RuntimeID, consensus.HeightLatest)
	if err != nil {
		return fmt.Errorf("client: failed to get runtime %s state: %w", request.RuntimeID, err)
	}
	lb, err := c.common.consensus.GetLightBlock(ctx, rs.CurrentBlockHeight)
	if err != nil {
		return fmt.Errorf("client: failed to get light block at height %d: %w", rs.CurrentBlockHeight, err)
	}
	epoch, err := c.common.consensus.Beacon().GetEpoch(ctx, consensus.HeightLatest)
	if err != nil {
		return fmt.Errorf("client: failed to get current epoch: %w", err)
	}

	_, err = rt.CheckTx(ctx, rs.CurrentBlock, lb, epoch, request.Data)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, host.ErrCheckTxFailed):
		return errors.WithContext(api.ErrCheckTxFailed, errors.Context(err))
	default:
		return fmt.Errorf("client: local transaction check failed: %w", err)
	}
}

// Implements api.RuntimeClient.
func (c *runtimeClient) WatchBlocks(ctx context.Context, runtimeID common.Namespace) (<-chan *roothash.AnnotatedBlock, pubsub.ClosableSubscription, error) {
	rt, err := c.common.runtimeRegistry.GetRuntime(runtimeID)
	if err != nil {
		return nil, nil, err
	}
	return rt.History().WatchBlocks(ctx)
}

// Implements api.RuntimeClient.
func (c *runtimeClient) GetGenesisBlock(ctx context.Context, runtimeID common.Namespace) (*block.Block, error) {
	return c.common.consensus.RootHash().GetGenesisBlock(ctx, runtimeID, consensus.HeightLatest)
}

// Implements api.RuntimeClient.
func (c *runtimeClient) GetBlock(ctx context.Context, request *api.GetBlockRequest) (*block.Block, error) {
	rt, err := c.common.runtimeRegistry.GetRuntime(request.RuntimeID)
	if err != nil {
		return nil, err
	}

	var annBlk *roothash.AnnotatedBlock
	switch request.Round {
	case api.RoundLatest:
		annBlk, err = rt.History().GetLatestBlock(ctx)
	default:
		annBlk, err = rt.History().GetBlock(ctx, request.Round)
	}
	if err != nil {
		return nil, err
	}
	return annBlk.Block, nil
}

func (c *runtimeClient) getTxnTree(blk *block.Block) *transaction.Tree {
	ioRoot := storage.Root{
		Namespace: blk.Header.Namespace,
		Version:   blk.Header.Round,
		Type:      storage.RootTypeIO,
		Hash:      blk.Header.IORoot,
	}

	return transaction.NewTree(c.common.storage, ioRoot)
}

func (c *runtimeClient) getTxnByHash(ctx context.Context, blk *block.Block, txHash hash.Hash) (*transaction.Transaction, error) {
	tree := c.getTxnTree(blk)
	defer tree.Close()

	return tree.GetTransaction(ctx, txHash)
}

// Implements api.RuntimeClient.
func (c *runtimeClient) GetTx(ctx context.Context, request *api.GetTxRequest) (*api.TxResult, error) {
	tagIndexer, err := c.tagIndexer(request.RuntimeID)
	if err != nil {
		return nil, err
	}

	blk, err := c.GetBlock(ctx, &api.GetBlockRequest{RuntimeID: request.RuntimeID, Round: request.Round})
	if err != nil {
		return nil, err
	}

	txHash, err := tagIndexer.QueryTxnByIndex(ctx, blk.Header.Round, request.Index)
	if err != nil {
		return nil, err
	}

	tx, err := c.getTxnByHash(ctx, blk, txHash)
	if err != nil {
		return nil, err
	}

	return &api.TxResult{
		Block:  blk,
		Index:  request.Index,
		Input:  tx.Input,
		Output: tx.Output,
	}, nil
}

// Implements api.RuntimeClient.
func (c *runtimeClient) GetTxByBlockHash(ctx context.Context, request *api.GetTxByBlockHashRequest) (*api.TxResult, error) {
	tagIndexer, err := c.tagIndexer(request.RuntimeID)
	if err != nil {
		return nil, err
	}

	blk, err := c.GetBlockByHash(ctx, &api.GetBlockByHashRequest{RuntimeID: request.RuntimeID, BlockHash: request.BlockHash})
	if err != nil {
		return nil, err
	}

	txHash, err := tagIndexer.QueryTxnByIndex(ctx, blk.Header.Round, request.Index)
	if err != nil {
		return nil, err
	}

	tx, err := c.getTxnByHash(ctx, blk, txHash)
	if err != nil {
		return nil, err
	}

	return &api.TxResult{
		Block:  blk,
		Index:  request.Index,
		Input:  tx.Input,
		Output: tx.Output,
	}, nil
}

// Implements api.RuntimeClient.
func (c *runtimeClient) GetTxs(ctx context.Context, request *api.GetTxsRequest) ([][]byte, error) {
	if request.IORoot.IsEmpty() {
		return [][]byte{}, nil
	}

	ioRoot := storage.Root{
		Version: request.Round,
		Type:    storage.RootTypeIO,
		Hash:    request.IORoot,
	}
	copy(ioRoot.Namespace[:], request.RuntimeID[:])

	tree := transaction.NewTree(c.common.storage, ioRoot)
	defer tree.Close()

	txs, err := tree.GetTransactions(ctx)
	if err != nil {
		return nil, err
	}

	inputs := [][]byte{}
	for _, tx := range txs {
		inputs = append(inputs, tx.Input)
	}

	return inputs, nil
}

// Implements api.RuntimeClient.
func (c *runtimeClient) GetEvents(ctx context.Context, request *api.GetEventsRequest) ([]*api.Event, error) {
	blk, err := c.GetBlock(ctx, &api.GetBlockRequest{RuntimeID: request.RuntimeID, Round: request.Round})
	if err != nil {
		return nil, err
	}

	tree := c.getTxnTree(blk)
	defer tree.Close()

	tags, err := tree.GetTags(ctx)
	if err != nil {
		return nil, err
	}

	var events []*api.Event
	for _, tag := range tags {
		events = append(events, &api.Event{
			Key:    tag.Key,
			Value:  tag.Value,
			TxHash: tag.TxHash,
		})
	}
	return events, nil
}

// Implements api.RuntimeClient.
func (c *runtimeClient) GetBlockByHash(ctx context.Context, request *api.GetBlockByHashRequest) (*block.Block, error) {
	tagIndexer, err := c.tagIndexer(request.RuntimeID)
	if err != nil {
		return nil, err
	}

	round, err := tagIndexer.QueryBlock(ctx, request.BlockHash)
	if err != nil {
		return nil, err
	}

	return c.GetBlock(ctx, &api.GetBlockRequest{RuntimeID: request.RuntimeID, Round: round})
}

// Implements api.RuntimeClient.
func (c *runtimeClient) Query(ctx context.Context, request *api.QueryRequest) (*api.QueryResponse, error) {
	hrt, ok := c.hosts[request.RuntimeID]
	if !ok {
		return nil, api.ErrNoHostedRuntime
	}
	rt := hrt.GetHostedRuntime()
	if rt == nil {
		return nil, api.ErrNoHostedRuntime
	}

	// Get current blocks.
	rs, err := c.common.consensus.RootHash().GetRuntimeState(ctx, request.RuntimeID, consensus.HeightLatest)
	if err != nil {
		return nil, fmt.Errorf("client: failed to get runtime %s state: %w", request.RuntimeID, err)
	}
	lb, err := c.common.consensus.GetLightBlock(ctx, rs.CurrentBlockHeight)
	if err != nil {
		return nil, fmt.Errorf("client: failed to get light block at height %d: %w", rs.CurrentBlockHeight, err)
	}
	epoch, err := c.common.consensus.Beacon().GetEpoch(ctx, rs.CurrentBlockHeight)
	if err != nil {
		return nil, fmt.Errorf("client: failed to get epoch at height %d: %w", rs.CurrentBlockHeight, err)
	}

	data, err := rt.Query(ctx, rs.CurrentBlock, lb, epoch, request.Method, request.Args)
	if err != nil {
		return nil, err
	}
	return &api.QueryResponse{Data: data}, nil
}

// Implements api.RuntimeClient.
func (c *runtimeClient) QueryTx(ctx context.Context, request *api.QueryTxRequest) (*api.TxResult, error) {
	tagIndexer, err := c.tagIndexer(request.RuntimeID)
	if err != nil {
		return nil, err
	}

	round, txHash, txIndex, err := tagIndexer.QueryTxn(ctx, request.Key, request.Value)
	if err != nil {
		return nil, err
	}

	blk, err := c.GetBlock(ctx, &api.GetBlockRequest{RuntimeID: request.RuntimeID, Round: round})
	if err != nil {
		return nil, err
	}

	tx, err := c.getTxnByHash(ctx, blk, txHash)
	if err != nil {
		return nil, err
	}

	return &api.TxResult{
		Block:  blk,
		Index:  txIndex,
		Input:  tx.Input,
		Output: tx.Output,
	}, nil
}

// Implements api.RuntimeClient.
func (c *runtimeClient) QueryTxs(ctx context.Context, request *api.QueryTxsRequest) ([]*api.TxResult, error) {
	tagIndexer, err := c.tagIndexer(request.RuntimeID)
	if err != nil {
		return nil, err
	}

	results, err := tagIndexer.QueryTxns(ctx, request.Query)
	if err != nil {
		return nil, err
	}

	output := []*api.TxResult{}
	for round, txResults := range results {
		// Fetch block for the given round.
		var blk *block.Block
		blk, err = c.GetBlock(ctx, &api.GetBlockRequest{RuntimeID: request.RuntimeID, Round: round})
		if err != nil {
			return nil, fmt.Errorf("failed to fetch block: %w", err)
		}

		tree := c.getTxnTree(blk)
		defer tree.Close()

		// Extract transaction data for the specified indices.
		var txHashes []hash.Hash
		for _, txResult := range txResults {
			txHashes = append(txHashes, txResult.TxHash)
		}

		txes, err := tree.GetTransactionMultiple(ctx, txHashes)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch transaction data: %w", err)
		}
		for _, txResult := range txResults {
			tx, ok := txes[txResult.TxHash]
			if !ok {
				return nil, fmt.Errorf("transaction %s not found", txResult.TxHash)
			}

			output = append(output, &api.TxResult{
				Block:  blk,
				Index:  txResult.TxIndex,
				Input:  tx.Input,
				Output: tx.Output,
			})
		}
	}

	return output, nil
}

// Implements api.RuntimeClient.
func (c *runtimeClient) WaitBlockIndexed(ctx context.Context, request *api.WaitBlockIndexedRequest) error {
	tagIndexer, err := c.tagIndexer(request.RuntimeID)
	if err != nil {
		return err
	}

	return tagIndexer.WaitBlockIndexed(ctx, request.Round)
}

// Implements enclaverpc.Transport.
func (c *runtimeClient) CallEnclave(ctx context.Context, request *enclaverpc.CallEnclaveRequest) ([]byte, error) {
	switch request.Endpoint {
	case keymanagerAPI.EnclaveRPCEndpoint:
		// Key manager.
		rt, err := c.common.runtimeRegistry.GetRuntime(request.RuntimeID)
		if err != nil {
			return nil, err
		}

		var km *keymanager.Client
		c.Lock()
		if km = c.kmClients[rt.ID()]; km == nil {
			c.logger.Debug("creating new key manager client instance")

			km, err = keymanager.New(c.common.ctx, rt, c.common.consensus.KeyManager(), c.common.consensus.Registry(), nil)
			if err != nil {
				c.Unlock()
				c.logger.Error("failed to create key manager client instance",
					"err", err,
				)
				return nil, api.ErrInternal
			}
			c.kmClients[rt.ID()] = km
		}
		c.Unlock()

		return km.CallRemote(ctx, request.Payload)
	default:
		c.logger.Warn("failed to route EnclaveRPC call",
			"endpoint", request.Endpoint,
		)
		return nil, fmt.Errorf("unknown EnclaveRPC endpoint: %s", request.Endpoint)
	}
}

// Implements service.BackgroundService.
func (c *runtimeClient) Name() string {
	return "runtime client"
}

// Implements service.BackgroundService.
func (c *runtimeClient) Start() error {
	for _, host := range c.hosts {
		if err := host.Start(); err != nil {
			return err
		}
	}
	go func() {
		defer close(c.quitCh)
		for _, host := range c.hosts {
			<-host.Quit()
		}
	}()
	return nil
}

// Implements service.BackgroundService.
func (c *runtimeClient) Stop() {
	// Watchers.
	c.Lock()
	for _, submitter := range c.txSubmitters {
		submitter.Stop()
	}
	c.Unlock()
	// Hosts.
	for _, host := range c.hosts {
		host.Stop()
	}
}

// Implements service.BackgroundService.
func (c *runtimeClient) Quit() <-chan struct{} {
	return c.quitCh
}

// Cleanup waits for all block watchers to finish.
func (c *runtimeClient) Cleanup() {
	// Watchers.
	c.Lock()
	for _, submitter := range c.txSubmitters {
		<-submitter.Quit()
	}
	c.Unlock()
}

// New returns a new runtime client instance.
func New(
	ctx context.Context,
	dataDir string,
	consensus consensus.Backend,
	runtimeRegistry runtimeRegistry.Registry,
	p2p *p2p.P2P,
) (api.RuntimeClientService, error) {
	maxTransactionAge := viper.GetInt64(CfgMaxTransactionAge)
	if maxTransactionAge < minMaxTransactionAge && !cmdFlags.DebugDontBlameOasis() {
		return nil, fmt.Errorf("max transaction age too low: %d, minimum: %d", maxTransactionAge, minMaxTransactionAge)
	}

	c := &runtimeClient{
		common: &clientCommon{
			storage:         runtimeRegistry.StorageRouter(),
			consensus:       consensus,
			runtimeRegistry: runtimeRegistry,
			ctx:             ctx,
			p2p:             p2p,
		},
		quitCh:            make(chan struct{}),
		hosts:             make(map[common.Namespace]*clientHost),
		txSubmitters:      make(map[common.Namespace]*txSubmitter),
		kmClients:         make(map[common.Namespace]*keymanager.Client),
		maxTransactionAge: maxTransactionAge,
		logger:            logging.GetLogger("runtime/client"),
	}

	// Create all configured runtime hosts.
	for _, rt := range runtimeRegistry.Runtimes() {
		if !rt.HasHost() {
			continue
		}

		host, err := newClientHost(rt, consensus)
		if err != nil {
			return nil, fmt.Errorf("failed to create new client host for %s: %w", rt.ID(), err)
		}
		c.hosts[rt.ID()] = host
	}

	return c, nil
}

func init() {
	Flags.Int64(CfgMaxTransactionAge, 1500, "number of consensus blocks after which submitted transactions will be considered expired")

	_ = viper.BindPFlags(Flags)
}
