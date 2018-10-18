// Package tendermint implements the tendermint backed registry backend.
package tendermint

import (
	"encoding/hex"
	"sync"

	"github.com/eapache/channels"
	"github.com/pkg/errors"
	tmtypes "github.com/tendermint/tendermint/types"
	"golang.org/x/net/context"

	"github.com/oasislabs/ekiden/go/common/cbor"
	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	"github.com/oasislabs/ekiden/go/common/entity"
	"github.com/oasislabs/ekiden/go/common/logging"
	"github.com/oasislabs/ekiden/go/common/node"
	"github.com/oasislabs/ekiden/go/common/pubsub"
	epochtime "github.com/oasislabs/ekiden/go/epochtime/api"
	"github.com/oasislabs/ekiden/go/registry/api"
	tmapi "github.com/oasislabs/ekiden/go/tendermint/api"
	tmregistry "github.com/oasislabs/ekiden/go/tendermint/apps/registry"
	"github.com/oasislabs/ekiden/go/tendermint/service"
)

// BackendName is the name of this implementation.
const BackendName = "tendermint"

var (
	_ api.Backend      = (*tendermintBackend)(nil)
	_ api.BlockBackend = (*tendermintBackend)(nil)
)

type tendermintBackend struct {
	logger *logging.Logger

	timeSource epochtime.BlockBackend
	service    service.TendermintService

	entityNotifier   *pubsub.Broker
	nodeNotifier     *pubsub.Broker
	nodeListNotifier *pubsub.Broker
	runtimeNotifier  *pubsub.Broker

	cached struct {
		sync.Mutex
		nodeLists map[epochtime.EpochTime]*api.NodeList
	}
	lastEpoch epochtime.EpochTime

	closeCh   chan struct{}
	closeOnce sync.Once
	closedWg  sync.WaitGroup
}

func (r *tendermintBackend) RegisterEntity(ctx context.Context, sigEnt *entity.SignedEntity) error {
	tx := tmapi.TxRegistry{
		TxRegisterEntity: &tmapi.TxRegisterEntity{
			Entity: *sigEnt,
		},
	}

	if err := r.service.BroadcastTx(tmapi.RegistryTransactionTag, tx); err != nil {
		return errors.Wrap(err, "registry: register entity failed")
	}

	return nil
}

func (r *tendermintBackend) DeregisterEntity(ctx context.Context, sigID *signature.SignedPublicKey) error {
	tx := tmapi.TxRegistry{
		TxDeregisterEntity: &tmapi.TxDeregisterEntity{
			ID: *sigID,
		},
	}

	if err := r.service.BroadcastTx(tmapi.RegistryTransactionTag, tx); err != nil {
		return errors.Wrap(err, "registry: deregister entity failed")
	}

	return nil
}

func (r *tendermintBackend) GetEntity(ctx context.Context, id signature.PublicKey) (*entity.Entity, error) {
	query := tmapi.QueryGetByIDRequest{
		ID: id,
	}

	response, err := r.service.Query(tmapi.QueryRegistryGetEntity, query, 0)
	if err != nil {
		return nil, errors.Wrap(err, "registry: get entity query failed")
	}

	var ent entity.Entity
	if err := cbor.Unmarshal(response, &ent); err != nil {
		return nil, errors.Wrap(err, "registry: get entity malformed response")
	}

	return &ent, nil
}

func (r *tendermintBackend) GetEntities(ctx context.Context) ([]*entity.Entity, error) {
	response, err := r.service.Query(tmapi.QueryRegistryGetEntities, nil, 0)
	if err != nil {
		return nil, errors.Wrap(err, "registry: get entities query failed")
	}

	var ents []*entity.Entity
	if err := cbor.Unmarshal(response, &ents); err != nil {
		return nil, errors.Wrap(err, "registry: get entities malformed response")
	}

	return ents, nil
}

func (r *tendermintBackend) WatchEntities() (<-chan *api.EntityEvent, *pubsub.Subscription) {
	typedCh := make(chan *api.EntityEvent)
	sub := r.entityNotifier.Subscribe()
	sub.Unwrap(typedCh)

	return typedCh, sub
}

func (r *tendermintBackend) RegisterNode(ctx context.Context, sigNode *node.SignedNode) error {
	tx := tmapi.TxRegistry{
		TxRegisterNode: &tmapi.TxRegisterNode{
			Node: *sigNode,
		},
	}

	if err := r.service.BroadcastTx(tmapi.RegistryTransactionTag, tx); err != nil {
		return errors.Wrap(err, "registry: register node failed")
	}

	return nil
}

func (r *tendermintBackend) GetNode(ctx context.Context, id signature.PublicKey) (*node.Node, error) {
	query := tmapi.QueryGetByIDRequest{
		ID: id,
	}

	response, err := r.service.Query(tmapi.QueryRegistryGetNode, query, 0)
	if err != nil {
		return nil, errors.Wrap(err, "registry: get node query failed")
	}

	var node node.Node
	if err := cbor.Unmarshal(response, &node); err != nil {
		return nil, errors.Wrap(err, "registry: get node malformed response")
	}

	return &node, nil
}

func (r *tendermintBackend) GetNodes(ctx context.Context) ([]*node.Node, error) {
	response, err := r.service.Query(tmapi.QueryRegistryGetNodes, nil, 0)
	if err != nil {
		return nil, errors.Wrap(err, "registry: get nodes query failed")
	}

	var nodes []*node.Node
	if err := cbor.Unmarshal(response, &nodes); err != nil {
		return nil, errors.Wrap(err, "registry: get nodes malformed response")
	}

	return nodes, nil
}

func (r *tendermintBackend) GetNodesForEntity(ctx context.Context, id signature.PublicKey) []*node.Node {
	// TODO: Need support for range queries on previous versions of the tree.
	return nil
}

func (r *tendermintBackend) WatchNodes() (<-chan *api.NodeEvent, *pubsub.Subscription) {
	typedCh := make(chan *api.NodeEvent)
	sub := r.nodeNotifier.Subscribe()
	sub.Unwrap(typedCh)

	return typedCh, sub
}

func (r *tendermintBackend) WatchNodeList() (<-chan *api.NodeList, *pubsub.Subscription) {
	typedCh := make(chan *api.NodeList)
	sub := r.nodeListNotifier.Subscribe()
	sub.Unwrap(typedCh)

	return typedCh, sub
}

func (r *tendermintBackend) RegisterRuntime(ctx context.Context, sigCon *api.SignedRuntime) error {
	tx := tmapi.TxRegistry{
		TxRegisterRuntime: &tmapi.TxRegisterRuntime{
			Runtime: *sigCon,
		},
	}

	if err := r.service.BroadcastTx(tmapi.RegistryTransactionTag, tx); err != nil {
		return errors.Wrap(err, "registry: register runtime failed")
	}

	return nil
}

func (r *tendermintBackend) GetRuntime(ctx context.Context, id signature.PublicKey) (*api.Runtime, error) {
	query := tmapi.QueryGetByIDRequest{
		ID: id,
	}

	response, err := r.service.Query(tmapi.QueryRegistryGetRuntime, query, 0)
	if err != nil {
		return nil, errors.Wrap(err, "registry: get runtime query failed")
	}

	var con api.Runtime
	if err := cbor.Unmarshal(response, &con); err != nil {
		return nil, errors.Wrap(err, "registry: get runtime malformed response")
	}

	return &con, nil
}

func (r *tendermintBackend) WatchRuntimes() (<-chan *api.Runtime, *pubsub.Subscription) {
	typedCh := make(chan *api.Runtime)
	sub := r.runtimeNotifier.Subscribe()
	sub.Unwrap(typedCh)

	return typedCh, sub
}

func (r *tendermintBackend) GetBlockNodeList(ctx context.Context, height int64) (*api.NodeList, error) {
	epoch, _, err := r.timeSource.GetBlockEpoch(ctx, height)
	if err != nil {
		return nil, err
	}

	return r.getNodeList(ctx, epoch)
}

func (r *tendermintBackend) Cleanup() {
	r.closeOnce.Do(func() {
		close(r.closeCh)
		r.closedWg.Wait()
	})
}

func (r *tendermintBackend) getRuntimes(ctx context.Context) ([]*api.Runtime, error) {
	response, err := r.service.Query(tmapi.QueryRegistryGetRuntimes, nil, 0)
	if err != nil {
		return nil, errors.Wrap(err, "registry: get runtimes query failed")
	}

	var runtimes []*api.Runtime
	if err := cbor.Unmarshal(response, &runtimes); err != nil {
		return nil, errors.Wrap(err, "registry: get runtimes malformed response")
	}

	return runtimes, nil
}

func (r *tendermintBackend) workerEvents() {
	defer r.closedWg.Done()

	// Subscribe to transactions which modify state.
	ctx := context.Background()
	txChannel := make(chan interface{})

	if err := r.service.Subscribe(ctx, "registry-worker", tmapi.QueryRegistryApp, txChannel); err != nil {
		panic("worker: failed to subscribe")
	}
	defer r.service.Unsubscribe(ctx, "registry-worker", tmapi.QueryRegistryApp) // nolint: errcheck

	// Process transactions and emit notifications for our subscribers.
	for {
		var rawTx interface{}
		var ok bool

		select {
		case rawTx, ok = <-txChannel:
			if !ok {
				r.logger.Debug("worker: terminating")
				return
			}
		case <-r.closeCh:
			return
		}

		// Extract output information from transaction.
		tx := rawTx.(tmtypes.EventDataTx)

		output := &tmapi.OutputRegistry{}
		if err := cbor.Unmarshal(tx.Result.GetData(), output); err != nil {
			r.logger.Error("worker: malformed transaction output",
				"tx", hex.EncodeToString(tx.Result.GetData()),
			)
			continue
		}

		if re := output.OutputRegisterEntity; re != nil {
			// Entity registration.
			r.entityNotifier.Broadcast(&api.EntityEvent{
				Entity:         &re.Entity,
				IsRegistration: true,
			})
		} else if de := output.OutputDeregisterEntity; de != nil {
			// Entity deregistration.
			r.entityNotifier.Broadcast(&api.EntityEvent{
				Entity:         &de.Entity,
				IsRegistration: false,
			})

			// Node deregistrations.
			for _, node := range output.Nodes {
				r.nodeNotifier.Broadcast(&api.NodeEvent{
					Node:           &node,
					IsRegistration: false,
				})
			}
		} else if rn := output.OutputRegisterNode; rn != nil {
			// Node registration.
			r.nodeNotifier.Broadcast(&api.NodeEvent{
				Node:           &rn.Node,
				IsRegistration: true,
			})
		} else if rc := output.OutputRegisterRuntime; rc != nil {
			// Runtime registration.
			r.runtimeNotifier.Broadcast(&rc.Runtime)
		}
	}
}

func (r *tendermintBackend) workerPerEpochList() {
	defer r.closedWg.Done()

	epochEvents, sub := r.timeSource.WatchEpochs()
	defer sub.Close()
	for {
		var newEpoch epochtime.EpochTime
		var ok bool

		select {
		case newEpoch, ok = <-epochEvents:
			if !ok {
				r.logger.Debug("worker: terminating")
				return
			}
		case <-r.closeCh:
			return
		}

		r.logger.Debug("worker: epoch transition",
			"prev_epoch", r.lastEpoch,
			"epoch", newEpoch,
		)

		if newEpoch == r.lastEpoch {
			continue
		}

		nl, err := r.getNodeList(context.Background(), newEpoch)
		if err != nil {
			r.logger.Error("worker: failed to generate node list for epoch",
				"err", err,
				"epoch", newEpoch,
			)
			continue
		}

		r.logger.Debug("worker: built node list",
			"newEpoch", newEpoch,
			"nodes", nl.Nodes,
		)

		r.nodeListNotifier.Broadcast(nl)
		r.sweepNodeLists(newEpoch)
		r.lastEpoch = newEpoch
	}
}

func (r *tendermintBackend) getNodeList(ctx context.Context, epoch epochtime.EpochTime) (*api.NodeList, error) {
	r.cached.Lock()
	defer r.cached.Unlock()

	// Service the request from the cache if possible.
	nl, ok := r.cached.nodeLists[epoch]
	if ok {
		return nl, nil
	}

	// Generate the nodelist.
	height, err := r.timeSource.GetEpochBlock(ctx, epoch)
	if err != nil {
		return nil, errors.Wrap(err, "registry: failed to query block height")
	}

	response, err := r.service.Query(tmapi.QueryRegistryGetNodes, nil, height)
	if err != nil {
		return nil, errors.Wrap(err, "registry: failed to query nodes")
	}

	var nodes []*node.Node
	if err := cbor.Unmarshal(response, &nodes); err != nil {
		return nil, errors.Wrap(err, "registry: failed node deserialization")
	}

	api.SortNodeList(nodes)

	nl = &api.NodeList{
		Epoch: epoch,
		Nodes: nodes,
	}

	r.cached.nodeLists[epoch] = nl

	return nl, nil
}

func (r *tendermintBackend) sweepNodeLists(epoch epochtime.EpochTime) {
	const nrKept = 3

	if epoch < nrKept {
		return
	}

	r.cached.Lock()
	defer r.cached.Unlock()

	for k := range r.cached.nodeLists {
		if k < epoch-nrKept {
			delete(r.cached.nodeLists, k)
		}
	}
}

// New constructs a new tendermint backed registry Backend instance.
func New(timeSource epochtime.Backend, service service.TendermintService) (api.Backend, error) {
	// We can only work with a block-based epochtime.
	blockTimeSource, ok := timeSource.(epochtime.BlockBackend)
	if !ok {
		return nil, errors.New("registry/tendermint: need a block-based epochtime backend")
	}

	// Initialze and register the tendermint service component.
	app := tmregistry.New()
	if err := service.RegisterApplication(app); err != nil {
		return nil, err
	}

	r := &tendermintBackend{
		logger:           logging.GetLogger("registry/tendermint"),
		timeSource:       blockTimeSource,
		service:          service,
		entityNotifier:   pubsub.NewBroker(false),
		nodeNotifier:     pubsub.NewBroker(false),
		nodeListNotifier: pubsub.NewBroker(true),
		lastEpoch:        epochtime.EpochInvalid,
		closeCh:          make(chan struct{}),
	}
	r.cached.nodeLists = make(map[epochtime.EpochTime]*api.NodeList)
	r.runtimeNotifier = pubsub.NewBrokerEx(func(ch *channels.InfiniteChannel) {
		wr := ch.In()
		runtimes, err := r.getRuntimes(context.Background())
		if err != nil {
			r.logger.Error("runtime notifier: unable to get a list of runtimes",
				"err", err,
			)
			return
		}

		for _, v := range runtimes {
			wr <- v
		}
	})

	r.closedWg.Add(2)
	go r.workerEvents()
	go r.workerPerEpochList()

	return r, nil
}
