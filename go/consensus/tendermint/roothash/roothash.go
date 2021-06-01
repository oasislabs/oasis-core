// Package roothash implements the tendermint backed roothash backend.
package roothash

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/eapache/channels"
	"github.com/hashicorp/go-multierror"
	tmabcitypes "github.com/tendermint/tendermint/abci/types"
	tmpubsub "github.com/tendermint/tendermint/libs/pubsub"
	tmrpctypes "github.com/tendermint/tendermint/rpc/core/types"
	tmtypes "github.com/tendermint/tendermint/types"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/common/pubsub"
	tmapi "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/api"
	app "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/roothash"
	registry "github.com/oasisprotocol/oasis-core/go/registry/api"
	"github.com/oasisprotocol/oasis-core/go/roothash/api"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/block"
)

// ServiceClient is the roothash service client interface.
type ServiceClient interface {
	api.Backend
	tmapi.ServiceClient
}

type runtimeBrokers struct {
	sync.Mutex

	blockNotifier *pubsub.Broker
	eventNotifier *pubsub.Broker

	lastBlockHeight int64
	lastBlock       *block.Block
}

type serviceClient struct {
	tmapi.BaseServiceClient
	sync.RWMutex

	ctx    context.Context
	logger *logging.Logger

	backend tmapi.Backend
	querier *app.QueryFactory

	allBlockNotifier *pubsub.Broker
	runtimeNotifiers map[common.Namespace]*runtimeBrokers
	genesisBlocks    map[common.Namespace]*block.Block
}

func (sc *serviceClient) GetGenesisBlock(ctx context.Context, runtimeID common.Namespace, height int64) (*block.Block, error) {
	// First check if we have the genesis blocks cached. They are immutable so easy
	// to cache to avoid repeated requests to the Tendermint app.
	sc.RLock()
	if blk := sc.genesisBlocks[runtimeID]; blk != nil {
		sc.RUnlock()
		return blk, nil
	}
	sc.RUnlock()

	q, err := sc.querier.QueryAt(ctx, height)
	if err != nil {
		return nil, err
	}

	blk, err := q.GenesisBlock(ctx, runtimeID)
	if err != nil {
		return nil, err
	}

	// Update the genesis block cache.
	sc.Lock()
	sc.genesisBlocks[runtimeID] = blk
	sc.Unlock()

	return blk, nil
}

func (sc *serviceClient) GetLatestBlock(ctx context.Context, runtimeID common.Namespace, height int64) (*block.Block, error) {
	return sc.getLatestBlockAt(ctx, runtimeID, height)
}

func (sc *serviceClient) getLatestBlockAt(ctx context.Context, runtimeID common.Namespace, height int64) (*block.Block, error) {
	q, err := sc.querier.QueryAt(ctx, height)
	if err != nil {
		return nil, err
	}

	return q.LatestBlock(ctx, runtimeID)
}

func (sc *serviceClient) GetRuntimeState(ctx context.Context, runtimeID common.Namespace, height int64) (*api.RuntimeState, error) {
	q, err := sc.querier.QueryAt(ctx, height)
	if err != nil {
		return nil, err
	}

	return q.RuntimeState(ctx, runtimeID)
}

func (sc *serviceClient) WatchBlocks(id common.Namespace) (<-chan *api.AnnotatedBlock, *pubsub.Subscription, error) {
	notifiers := sc.getRuntimeNotifiers(id)

	sub := notifiers.blockNotifier.SubscribeEx(-1, func(ch channels.Channel) {
		// Replay the latest block if it exists.
		notifiers.Lock()
		defer notifiers.Unlock()
		if notifiers.lastBlock != nil {
			ch.In() <- &api.AnnotatedBlock{
				Height: notifiers.lastBlockHeight,
				Block:  notifiers.lastBlock,
			}
		}
	})
	ch := make(chan *api.AnnotatedBlock)
	sub.Unwrap(ch)

	// Make sure that we only ever emit monotonically increasing blocks. Without
	// special handling this can happen for the first received block due to
	// replaying the latest block (see above).
	invalidRound := uint64(math.MaxUint64)
	lastRound := invalidRound
	monotonicCh := make(chan *api.AnnotatedBlock)
	go func() {
		defer close(monotonicCh)

		for {
			blk, ok := <-ch
			if !ok {
				return
			}
			if lastRound != invalidRound && blk.Block.Header.Round <= lastRound {
				continue
			}
			lastRound = blk.Block.Header.Round
			monotonicCh <- blk
		}
	}()

	return monotonicCh, sub, nil
}

func (sc *serviceClient) WatchAllBlocks() (<-chan *block.Block, *pubsub.Subscription) {
	sub := sc.allBlockNotifier.Subscribe()
	ch := make(chan *block.Block)
	sub.Unwrap(ch)

	return ch, sub
}

func (sc *serviceClient) WatchEvents(id common.Namespace) (<-chan *api.Event, *pubsub.Subscription, error) {
	notifiers := sc.getRuntimeNotifiers(id)
	sub := notifiers.eventNotifier.Subscribe()
	ch := make(chan *api.Event)
	sub.Unwrap(ch)

	return ch, sub, nil
}

func (sc *serviceClient) StateToGenesis(ctx context.Context, height int64) (*api.Genesis, error) {
	q, err := sc.querier.QueryAt(ctx, height)
	if err != nil {
		return nil, err
	}

	g, err := q.Genesis(ctx)
	if err != nil {
		return nil, err
	}

	// Add any last emitted events for the given runtime.
	//
	// NOTE: This requires historic lookups.
	for runtimeID, rt := range g.RuntimeStates {
		state, err := sc.GetRuntimeState(ctx, runtimeID, height)
		if err != nil {
			return nil, err
		}

		rr, err := sc.GetRoundResults(ctx, runtimeID, state.LastNormalHeight)
		if err != nil {
			return nil, fmt.Errorf("roothash: failed to get last message results for runtime %s: %w", runtimeID, err)
		}
		rt.MessageResults = rr.Messages
	}

	return g, nil
}

func (sc *serviceClient) getNodeEntities(ctx context.Context, height int64, nodes []signature.PublicKey) ([]signature.PublicKey, error) {
	var entities []signature.PublicKey
	seen := make(map[signature.PublicKey]bool)
	for _, id := range nodes {
		node, err := sc.backend.Registry().GetNode(ctx, &registry.IDQuery{
			Height: height,
			ID:     id,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to fetch node %s: %w", id, err)
		}

		if seen[node.EntityID] {
			continue
		}
		seen[node.EntityID] = true
		entities = append(entities, node.EntityID)
	}
	return entities, nil
}

func (sc *serviceClient) GetRoundResults(ctx context.Context, runtimeID common.Namespace, height int64) (*api.RoundResults, error) {
	evs, err := sc.getEvents(ctx, height, nil)
	if err != nil {
		return nil, err
	}

	results := new(api.RoundResults)
	for _, ev := range evs {
		switch {
		case !ev.RuntimeID.Equal(&runtimeID):
			continue
		case ev.Message != nil:
			// Runtime message processed event.
			results.Messages = append(results.Messages, ev.Message)
		case ev.Finalized != nil:
			// Round finalized event.
			results.GoodComputeEntities, err = sc.getNodeEntities(ctx, height, ev.Finalized.GoodComputeNodes)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve good compute entities: %w", err)
			}

			results.BadComputeEntities, err = sc.getNodeEntities(ctx, height, ev.Finalized.BadComputeNodes)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve bad compute entities: %w", err)
			}
		default:
		}
	}
	return results, nil
}

func (sc *serviceClient) getEvents(ctx context.Context, height int64, txns [][]byte) ([]*api.Event, error) {
	// Get block results at given height.
	var results *tmrpctypes.ResultBlockResults
	results, err := sc.backend.GetBlockResults(ctx, height)
	if err != nil {
		sc.logger.Error("failed to get tendermint block results",
			"err", err,
			"height", height,
		)
		return nil, err
	}

	var events []*api.Event
	// Decode events from block results.
	blockEvs, err := EventsFromTendermint(nil, results.Height, results.BeginBlockEvents)
	if err != nil {
		return nil, err
	}
	events = append(events, blockEvs...)

	blockEvs, err = EventsFromTendermint(nil, results.Height, results.EndBlockEvents)
	if err != nil {
		return nil, err
	}
	events = append(events, blockEvs...)

	// Decode events from transaction results.
	for txIdx, txResult := range results.TxsResults {
		// The order of transactions in txns and results.TxsResults is
		// supposed to match, so the same index in both slices refers to the
		// same transaction.
		var tx tmtypes.Tx
		if txns != nil {
			tx = txns[txIdx]
		}
		evs, txErr := EventsFromTendermint(tx, results.Height, txResult.Events)
		if txErr != nil {
			return nil, txErr
		}
		events = append(events, evs...)
	}

	return events, nil
}

func (sc *serviceClient) GetEvents(ctx context.Context, height int64) ([]*api.Event, error) {
	// Get transactions at given height.
	txns, err := sc.backend.GetTransactions(ctx, height)
	if err != nil {
		sc.logger.Error("failed to get tendermint transactions",
			"err", err,
			"height", height,
		)
		return nil, err
	}

	return sc.getEvents(ctx, height, txns)
}

func (sc *serviceClient) Cleanup() {
}

func (sc *serviceClient) getRuntimeNotifiers(id common.Namespace) *runtimeBrokers {
	sc.Lock()
	defer sc.Unlock()

	notifiers := sc.runtimeNotifiers[id]
	if notifiers == nil {
		notifiers = &runtimeBrokers{
			blockNotifier: pubsub.NewBroker(false),
			eventNotifier: pubsub.NewBroker(false),
		}
		sc.runtimeNotifiers[id] = notifiers

		// Return latest indexed runtime block upon subscription.
		annBlk, err := sc.backend.GetLatestIndexedRuntimeBlock(id)
		if err != nil {
			sc.logger.Error("error getting runtime block from history", "runtime_id", id, "err", err)
		} else {
			notifiers.lastBlock = annBlk.Block
			notifiers.lastBlockHeight = annBlk.Height
		}
	}

	return notifiers
}

// Implements api.ServiceClient.
func (sc *serviceClient) ServiceDescriptor() tmapi.ServiceDescriptor {
	return tmapi.NewStaticServiceDescriptor(api.ModuleName, app.EventType, []tmpubsub.Query{app.QueryApp})
}

// Implements api.ServiceClient.
func (sc *serviceClient) DeliverCommand(ctx context.Context, height int64, cmd interface{}) error {
	return nil
}

// Implements api.ServiceClient.
func (sc *serviceClient) DeliverEvent(ctx context.Context, height int64, tx tmtypes.Tx, ev *tmabcitypes.Event) error {
	events, err := EventsFromTendermint(tx, height, []tmabcitypes.Event{*ev})
	if err != nil {
		return fmt.Errorf("roothash: failed to process tendermint events: %w", err)
	}

	for _, ev := range events {
		// Notify non-finalized events.
		if ev.Finalized == nil {
			notifiers := sc.getRuntimeNotifiers(ev.RuntimeID)
			notifiers.eventNotifier.Broadcast(ev)
			continue
		}

		if _, _, err = sc.processFinalizedEvent(ctx, height, ev.RuntimeID, &ev.Finalized.Round, true); err != nil {
			return fmt.Errorf("roothash: failed to process finalized event: %w", err)
		}
	}

	return nil
}

func (sc *serviceClient) processFinalizedEvent(
	ctx context.Context,
	height int64,
	runtimeID common.Namespace,
	round *uint64,
	notify bool,
) (*api.AnnotatedBlock, *api.RoundResults, error) {
	// Process finalized event.
	blk, err := sc.getLatestBlockAt(ctx, runtimeID, height)
	if err != nil {
		sc.logger.Error("failed to fetch latest block",
			"err", err,
			"height", height,
			"runtime_id", runtimeID,
		)
		return nil, nil, fmt.Errorf("roothash: failed to fetch latest block: %w", err)
	}
	if round != nil && blk.Header.Round != *round {
		sc.logger.Error("finalized event/query round mismatch",
			"block_round", blk.Header.Round,
			"event_round", *round,
		)
		return nil, nil, fmt.Errorf("roothash: finalized event/query round mismatch")
	}

	roundResults, err := sc.GetRoundResults(ctx, runtimeID, height)
	if err != nil {
		sc.logger.Error("failed to fetch round results",
			"err", err,
			"height", height,
			"runtime_id", runtimeID,
		)
		return nil, nil, fmt.Errorf("roothash: failed to fetch round results: %w", err)
	}

	annBlk := &api.AnnotatedBlock{
		Height: height,
		Block:  blk,
	}

	// Skip notify if not set.
	if !notify {
		return annBlk, roundResults, nil
	}

	notifiers := sc.getRuntimeNotifiers(runtimeID)
	// Ensure latest block is set.
	notifiers.Lock()
	notifiers.lastBlock = blk
	notifiers.lastBlockHeight = height
	notifiers.Unlock()

	sc.allBlockNotifier.Broadcast(blk)
	notifiers.blockNotifier.Broadcast(annBlk)

	return annBlk, roundResults, nil
}

// EventsFromTendermint extracts staking events from tendermint events.
func EventsFromTendermint(
	tx tmtypes.Tx,
	height int64,
	tmEvents []tmabcitypes.Event,
) ([]*api.Event, error) {
	var txHash hash.Hash
	switch tx {
	case nil:
		txHash.Empty()
	default:
		txHash = hash.NewFromBytes(tx)
	}

	var events []*api.Event
	var errs error
	for _, tmEv := range tmEvents {
		// Ignore events that don't relate to the roothash app.
		if tmEv.GetType() != app.EventType {
			continue
		}

		for _, pair := range tmEv.GetAttributes() {
			key := pair.GetKey()
			val := pair.GetValue()

			switch {
			case bytes.Equal(key, app.KeyFinalized):
				// Finalized event.
				var value app.ValueFinalized
				if err := cbor.Unmarshal(val, &value); err != nil {
					errs = multierror.Append(errs, fmt.Errorf("roothash: corrupt Finalized event: %w", err))
					continue
				}

				ev := &api.Event{RuntimeID: value.ID, Height: height, TxHash: txHash, Finalized: &value.Event}
				events = append(events, ev)
			case bytes.Equal(key, app.KeyExecutionDiscrepancyDetected):
				// An execution discrepancy has been detected.
				var value app.ValueExecutionDiscrepancyDetected
				if err := cbor.Unmarshal(val, &value); err != nil {
					errs = multierror.Append(errs, fmt.Errorf("roothash: corrupt ValueExectutionDiscrepancy event: %w", err))
					continue
				}

				ev := &api.Event{RuntimeID: value.ID, Height: height, TxHash: txHash, ExecutionDiscrepancyDetected: &value.Event}
				events = append(events, ev)
			case bytes.Equal(key, app.KeyExecutorCommitted):
				// An executor commit has been processed.
				var value app.ValueExecutorCommitted
				if err := cbor.Unmarshal(val, &value); err != nil {
					errs = multierror.Append(errs, fmt.Errorf("roothash: corrupt ValueExecutorCommitted event: %w", err))
					continue
				}

				ev := &api.Event{RuntimeID: value.ID, Height: height, TxHash: txHash, ExecutorCommitted: &value.Event}
				events = append(events, ev)
			case bytes.Equal(key, app.KeyMessage):
				// Runtime message has been processed.
				var value app.ValueMessage
				if err := cbor.Unmarshal(val, &value); err != nil {
					errs = multierror.Append(errs, fmt.Errorf("roothash: corrupt message event: %w", err))
					continue
				}

				ev := &api.Event{RuntimeID: value.ID, Height: height, TxHash: txHash, Message: &value.Event}
				events = append(events, ev)
			case bytes.Equal(key, app.KeyRuntimeID):
				// Runtime ID attribute (Base64-encoded to allow queries).
			default:
				errs = multierror.Append(errs, fmt.Errorf("roothash: unknown event type: key: %s, val: %s", key, val))
			}
		}
	}
	return events, errs
}

// New constructs a new tendermint-based root hash backend.
func New(
	ctx context.Context,
	dataDir string,
	backend tmapi.Backend,
) (ServiceClient, error) {
	// Initialize and register the tendermint service component.
	a := app.New()
	if err := backend.RegisterApplication(a); err != nil {
		return nil, err
	}

	return &serviceClient{
		ctx:              ctx,
		logger:           logging.GetLogger("roothash/tendermint"),
		backend:          backend,
		querier:          a.QueryFactory().(*app.QueryFactory),
		allBlockNotifier: pubsub.NewBroker(false),
		runtimeNotifiers: make(map[common.Namespace]*runtimeBrokers),
		genesisBlocks:    make(map[common.Namespace]*block.Block),
	}, nil
}
