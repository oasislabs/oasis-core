// Package memory implements the memory backed registry backend.
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/eapache/channels"

	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	"github.com/oasislabs/ekiden/go/common/entity"
	"github.com/oasislabs/ekiden/go/common/logging"
	"github.com/oasislabs/ekiden/go/common/node"
	"github.com/oasislabs/ekiden/go/common/pubsub"
	epochtime "github.com/oasislabs/ekiden/go/epochtime/api"
	"github.com/oasislabs/ekiden/go/registry/api"
)

// BackendName is the name of this implementation.
const BackendName = "memory"

var _ api.Backend = (*memoryBackend)(nil)

type memoryBackend struct {
	sync.Once

	logger *logging.Logger

	state memoryBackendState

	entityNotifier   *pubsub.Broker
	nodeNotifier     *pubsub.Broker
	nodeListNotifier *pubsub.Broker
	runtimeNotifier  *pubsub.Broker

	closeCh  chan struct{}
	closedCh chan struct{}
}

type memoryBackendState struct {
	sync.RWMutex

	entities map[signature.MapKey]*entity.Entity
	nodes    map[signature.MapKey]*node.Node
	runtimes map[signature.MapKey]*api.Runtime

	lastEpoch epochtime.EpochTime
}

func (r *memoryBackend) RegisterEntity(ctx context.Context, sigEnt *entity.SignedEntity) error {
	ent, err := api.VerifyRegisterEntityArgs(r.logger, sigEnt, false)
	if err != nil {
		return err
	}

	r.state.Lock()
	r.state.entities[ent.ID.ToMapKey()] = ent
	r.state.Unlock()

	r.logger.Debug("RegisterEntity: registered",
		"entity", ent,
	)

	r.entityNotifier.Broadcast(&api.EntityEvent{
		Entity:         ent,
		IsRegistration: true,
	})

	return nil
}

func (r *memoryBackend) DeregisterEntity(ctx context.Context, sigTimestamp *signature.Signed) error {
	id, _, err := api.VerifyDeregisterEntityArgs(r.logger, sigTimestamp)
	if err != nil {
		return err
	}

	var removedEntity *entity.Entity
	var removedNodes []*node.Node
	mk := id.ToMapKey()
	r.state.Lock()
	if removedEntity = r.state.entities[mk]; removedEntity != nil {
		delete(r.state.entities, mk)
		removedNodes = r.getNodesForEntryLocked(id)
		for _, v := range removedNodes {
			delete(r.state.nodes, v.ID.ToMapKey())
		}
	}
	r.state.Unlock()

	r.logger.Debug("DeregisterEntity: complete",
		"entity_id", id,
		"removed_entity", removedEntity,
		"nodes_pruned", len(removedNodes),
	)

	if removedEntity != nil {
		r.entityNotifier.Broadcast(&api.EntityEvent{
			Entity:         removedEntity,
			IsRegistration: false,
		})
		for _, v := range removedNodes {
			r.nodeNotifier.Broadcast(&api.NodeEvent{
				Node:           v,
				IsRegistration: false,
			})
		}
	}

	return nil
}

func (r *memoryBackend) GetEntity(ctx context.Context, id signature.PublicKey) (*entity.Entity, error) {
	r.state.RLock()
	defer r.state.RUnlock()

	ent := r.state.entities[id.ToMapKey()]
	if ent == nil {
		return nil, api.ErrNoSuchEntity
	}

	return ent, nil
}

func (r *memoryBackend) GetEntities(ctx context.Context) ([]*entity.Entity, error) {
	r.state.RLock()
	defer r.state.RUnlock()

	ret := make([]*entity.Entity, 0, len(r.state.entities))
	for _, v := range r.state.entities {
		ret = append(ret, v)
	}

	return ret, nil
}

func (r *memoryBackend) WatchEntities() (<-chan *api.EntityEvent, *pubsub.Subscription) {
	typedCh := make(chan *api.EntityEvent)
	sub := r.entityNotifier.Subscribe()
	sub.Unwrap(typedCh)

	return typedCh, sub
}

func (r *memoryBackend) RegisterNode(ctx context.Context, sigNode *node.SignedNode) error {
	node, err := api.VerifyRegisterNodeArgs(r.logger, sigNode, time.Now())
	if err != nil {
		return err
	}

	r.state.Lock()
	if r.state.entities[node.EntityID.ToMapKey()] == nil {
		r.state.Unlock()
		r.logger.Error("RegisterNode: unknown entity in node registration",
			"node", node,
		)
		return api.ErrBadEntityForNode
	}

	// Ensure node is not expired.
	if epochtime.EpochTime(node.Expiration) < r.state.lastEpoch {
		r.state.Unlock()
		return api.ErrNodeExpired
	}

	r.state.nodes[node.ID.ToMapKey()] = node
	r.state.Unlock()

	r.logger.Debug("RegisterNode: registered",
		"node", node,
	)

	r.nodeNotifier.Broadcast(&api.NodeEvent{
		Node:           node,
		IsRegistration: true,
	})

	return nil
}

func (r *memoryBackend) GetNode(ctx context.Context, id signature.PublicKey) (*node.Node, error) {
	r.state.RLock()
	defer r.state.RUnlock()

	node := r.state.nodes[id.ToMapKey()]
	if node == nil {
		return nil, api.ErrNoSuchNode
	}

	return node, nil
}

func (r *memoryBackend) GetNodes(ctx context.Context) ([]*node.Node, error) {
	r.state.RLock()
	defer r.state.RUnlock()

	ret := make([]*node.Node, 0, len(r.state.nodes))
	for _, v := range r.state.nodes {
		ret = append(ret, v)
	}

	return ret, nil
}

func (r *memoryBackend) GetNodesForEntity(ctx context.Context, id signature.PublicKey) []*node.Node {
	r.state.RLock()
	defer r.state.RUnlock()

	return r.getNodesForEntryLocked(id)
}

func (r *memoryBackend) GetNodeTransport(ctx context.Context, id signature.PublicKey) (*api.NodeTransport, error) {
	node, err := r.GetNode(ctx, id)
	if err != nil {
		return nil, err
	}

	return &api.NodeTransport{
		Addresses:   node.Addresses,
		Certificate: node.Certificate,
	}, nil
}

func (r *memoryBackend) WatchNodes() (<-chan *api.NodeEvent, *pubsub.Subscription) {
	typedCh := make(chan *api.NodeEvent)
	sub := r.nodeNotifier.Subscribe()
	sub.Unwrap(typedCh)

	return typedCh, sub
}

func (r *memoryBackend) WatchNodeList() (<-chan *api.NodeList, *pubsub.Subscription) {
	typedCh := make(chan *api.NodeList)
	sub := r.nodeListNotifier.Subscribe()
	sub.Unwrap(typedCh)

	return typedCh, sub
}

func (r *memoryBackend) getNodesForEntryLocked(id signature.PublicKey) []*node.Node {
	var ret []*node.Node

	// TODO/perf: This could be cached if it's a common operation.
	for _, v := range r.state.nodes {
		if id.Equal(v.EntityID) {
			ret = append(ret, v)
		}
	}

	return ret
}

func (r *memoryBackend) worker(ctx context.Context, timeSource epochtime.Backend) {
	defer close(r.closedCh)

	epochEvents, sub := timeSource.WatchEpochs()
	defer sub.Close()

	lastEpoch := epochtime.EpochInvalid
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
			"prev_epoch", lastEpoch,
			"epoch", newEpoch,
		)

		if newEpoch == lastEpoch {
			continue
		}

		r.sweepNodeList(newEpoch)
		r.buildNodeList(ctx, newEpoch)
		lastEpoch = newEpoch

		r.state.Lock()
		r.state.lastEpoch = lastEpoch
		r.state.Unlock()
	}
}

func (r *memoryBackend) sweepNodeList(newEpoch epochtime.EpochTime) {
	r.state.Lock()
	defer r.state.Unlock()

	for _, v := range r.state.nodes {
		if epochtime.EpochTime(v.Expiration) >= newEpoch {
			continue
		}

		r.nodeNotifier.Broadcast(&api.NodeEvent{
			Node:           v,
			IsRegistration: false,
		})
		delete(r.state.nodes, v.ID.ToMapKey())
	}
}

func (r *memoryBackend) buildNodeList(ctx context.Context, newEpoch epochtime.EpochTime) {
	nodes, err := r.GetNodes(ctx)
	if err != nil {
		panic(err)
	}

	api.SortNodeList(nodes)

	r.logger.Debug("worker: built node list",
		"epoch", newEpoch,
		"nodes_len", len(nodes),
	)

	r.nodeListNotifier.Broadcast(&api.NodeList{
		Epoch: newEpoch,
		Nodes: nodes,
	})
}

func (r *memoryBackend) RegisterRuntime(ctx context.Context, sigRt *api.SignedRuntime) error {
	rt, err := api.VerifyRegisterRuntimeArgs(r.logger, sigRt, false)
	if err != nil {
		return err
	}

	ent := sigRt.Signature.PublicKey

	r.state.Lock()
	if r.state.entities[ent.ToMapKey()] == nil {
		r.state.Unlock()
		r.logger.Error("RegisterRuntime: unknown entity in runtime registration",
			"runtime", rt,
		)
		return api.ErrBadEntityForRuntime
	}
	// XXX: Should this reject attempts to alter an existing registration?
	r.state.runtimes[rt.ID.ToMapKey()] = rt
	r.state.Unlock()

	r.logger.Debug("RegisterRuntime: registered",
		"runtime", rt,
	)

	r.runtimeNotifier.Broadcast(rt)

	return nil
}

func (r *memoryBackend) GetRuntime(ctx context.Context, id signature.PublicKey) (*api.Runtime, error) {
	r.state.RLock()
	defer r.state.RUnlock()

	con := r.state.runtimes[id.ToMapKey()]
	if con == nil {
		return nil, api.ErrNoSuchRuntime
	}

	return con, nil
}

func (r *memoryBackend) GetRuntimes(ctx context.Context) ([]*api.Runtime, error) {
	r.state.Lock()
	defer r.state.Unlock()

	ret := make([]*api.Runtime, 0, len(r.state.runtimes))
	for _, v := range r.state.runtimes {
		ret = append(ret, v)
	}

	return ret, nil
}

func (r *memoryBackend) WatchRuntimes() (<-chan *api.Runtime, *pubsub.Subscription) {
	typedCh := make(chan *api.Runtime)
	sub := r.runtimeNotifier.Subscribe()
	sub.Unwrap(typedCh)

	return typedCh, sub
}

func (r *memoryBackend) Cleanup() {
	r.Once.Do(func() {
		close(r.closeCh)
		<-r.closedCh
	})
}

// New constructs a new memory backed registry Backend instance.
func New(ctx context.Context, timeSource epochtime.Backend) api.Backend {
	r := &memoryBackend{
		logger: logging.GetLogger("registry/memory"),
		state: memoryBackendState{
			entities:  make(map[signature.MapKey]*entity.Entity),
			nodes:     make(map[signature.MapKey]*node.Node),
			runtimes:  make(map[signature.MapKey]*api.Runtime),
			lastEpoch: epochtime.EpochInvalid,
		},
		entityNotifier:   pubsub.NewBroker(false),
		nodeNotifier:     pubsub.NewBroker(false),
		nodeListNotifier: pubsub.NewBroker(true),
		closeCh:          make(chan struct{}),
		closedCh:         make(chan struct{}),
	}
	r.runtimeNotifier = pubsub.NewBrokerEx(func(ch *channels.InfiniteChannel) {
		wr := ch.In()

		r.state.RLock()
		defer r.state.RUnlock()
		for _, v := range r.state.runtimes {
			wr <- v
		}
	})

	go r.worker(ctx, timeSource)

	return r
}
