package registry

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"

	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	"github.com/oasislabs/ekiden/go/common/entity"
	"github.com/oasislabs/ekiden/go/common/node"
	"github.com/oasislabs/ekiden/go/registry/api"
)

const metricsUpdateInterval = 10 * time.Second

var (
	registryFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ekiden_registry_failures",
			Help: "Number of registry failures.",
		},
		[]string{"call"},
	)
	registryNodes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "ekiden_registry_nodes",
			Help: "Number of registry nodes.",
		},
	)
	registryEntities = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "ekiden_registry_entities",
			Help: "Number of registry entities.",
		},
	)
	registryRuntimes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "ekiden_registry_runtimes",
			Help: "Number of registry runtimes.",
		},
	)
	registeryCollectors = []prometheus.Collector{
		registryFailures,
		registryNodes,
		registryEntities,
		registryRuntimes,
	}

	_ api.Backend = (*metricsWrapper)(nil)

	metricsOnce sync.Once
)

type metricsWrapper struct {
	api.Backend

	closeOnce sync.Once
	closeCh   chan struct{}
	closedCh  chan struct{}
}

func (w *metricsWrapper) RegisterEntity(ctx context.Context, sigEnt *entity.SignedEntity) error {
	if err := w.Backend.RegisterEntity(ctx, sigEnt); err != nil {
		registryFailures.With(prometheus.Labels{"call": "registerEntity"}).Inc()
		return err
	}

	return nil
}

func (w *metricsWrapper) DeregisterEntity(ctx context.Context, sigID *signature.SignedPublicKey) error {
	if err := w.Backend.DeregisterEntity(ctx, sigID); err != nil {
		registryFailures.With(prometheus.Labels{"call": "deregisterEntity"}).Inc()
		return err
	}

	return nil
}

func (w *metricsWrapper) RegisterNode(ctx context.Context, sigNode *node.SignedNode) error {
	if err := w.Backend.RegisterNode(ctx, sigNode); err != nil {
		registryFailures.With(prometheus.Labels{"call": "registerNode"}).Inc()
		return err
	}

	return nil
}

func (w *metricsWrapper) RegisterRuntime(ctx context.Context, sigCon *api.SignedRuntime) error {
	if err := w.Backend.RegisterRuntime(ctx, sigCon); err != nil {
		registryFailures.With(prometheus.Labels{"call": "registerRuntime"}).Inc()
		return err
	}

	return nil
}

func (w *metricsWrapper) Cleanup() {
	w.closeOnce.Do(func() {
		close(w.closeCh)
		<-w.closedCh
	})

	w.Backend.Cleanup()
}

func (w *metricsWrapper) worker() {
	defer close(w.closedCh)

	t := time.NewTicker(metricsUpdateInterval)
	defer t.Stop()

	runtimeCh, sub := w.Backend.WatchRuntimes()
	defer sub.Close()

	for {
		select {
		case <-w.closeCh:
			return
		case <-runtimeCh:
			registryRuntimes.Inc()
			continue
		case <-t.C:
		}

		w.updatePeriodicMetrics()
	}
}

func (w *metricsWrapper) updatePeriodicMetrics() {
	nodes, err := w.Backend.GetNodes(context.Background())
	if err == nil {
		registryNodes.Set(float64(len(nodes)))
	}

	entities, err := w.Backend.GetEntities(context.Background())
	if err == nil {
		registryEntities.Set(float64(len(entities)))
	}
}

type blockMetricsWrapper struct {
	*metricsWrapper
	blockBackend api.BlockBackend
}

func (w *blockMetricsWrapper) GetBlockNodeList(ctx context.Context, height int64) (*api.NodeList, error) {
	return w.blockBackend.GetBlockNodeList(ctx, height)
}

func newMetricsWrapper(base api.Backend) api.Backend {
	metricsOnce.Do(func() {
		prometheus.MustRegister(registeryCollectors...)
	})

	// XXX: When the registry backends support node deregistration,
	// handle this on the metrics side.

	wrapper := &metricsWrapper{
		Backend:  base,
		closeCh:  make(chan struct{}),
		closedCh: make(chan struct{}),
	}

	wrapper.updatePeriodicMetrics()
	go wrapper.worker()

	blockBackend, ok := base.(api.BlockBackend)
	if ok {
		return &blockMetricsWrapper{
			metricsWrapper: wrapper,
			blockBackend:   blockBackend,
		}
	}

	return wrapper
}
