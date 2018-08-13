package registry

import (
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/oasislabs/ekiden/go/common/contract"
	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	"github.com/oasislabs/ekiden/go/common/entity"
	"github.com/oasislabs/ekiden/go/common/node"
	"github.com/oasislabs/ekiden/go/registry/api"

	commonPB "github.com/oasislabs/ekiden/go/grpc/common"
	pb "github.com/oasislabs/ekiden/go/grpc/registry"
)

var (
	_ pb.EntityRegistryServer   = (*grpcServer)(nil)
	_ pb.ContractRegistryServer = (*grpcServer)(nil)
)

var registryFailures = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "ekiden_registry_failures",
		Help: "Number of registry failures.",
	},
	[]string{"call"},
)
var registryNodes = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "ekiden_registry_nodes",
		Help: "Number of registry nodes.",
	},
)
var registryEntities = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "ekiden_registry_entities",
		Help: "Number of registry entities.",
	},
)
var registryContracts = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "ekiden_registry_contracts",
		Help: "Number of registry contracts.",
	},
)
var registeryCollectors = []prometheus.Collector{
	registryFailures,
	registryNodes,
	registryEntities,
	registryContracts,
}

type grpcServer struct {
	backend api.Backend
}

func (s *grpcServer) RegisterEntity(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	var ent entity.Entity
	if err := ent.FromProto(req.GetEntity()); err != nil {
		return nil, err
	}
	var sig signature.Signature
	if err := sig.FromProto(req.GetSignature()); err != nil {
		return nil, err
	}

	if err := s.backend.RegisterEntity(ctx, &ent, &sig); err != nil {
		registryFailures.With(prometheus.Labels{"call": "registerEntity"}).Inc()
		return nil, err
	}

	registryEntities.Inc()
	return &pb.RegisterResponse{}, nil
}

func (s *grpcServer) DeregisterEntity(ctx context.Context, req *pb.DeregisterRequest) (*pb.DeregisterResponse, error) {
	var id signature.PublicKey
	if err := id.UnmarshalBinary(req.GetId()); err != nil {
		return nil, err
	}
	var sig signature.Signature
	if err := sig.FromProto(req.GetSignature()); err != nil {
		return nil, err
	}

	if err := s.backend.DeregisterEntity(ctx, id, &sig); err != nil {
		registryFailures.With(prometheus.Labels{"call": "deregisterEntity"}).Inc()
		return nil, err
	}

	registryEntities.Dec()
	return &pb.DeregisterResponse{}, nil
}

func (s *grpcServer) GetEntity(ctx context.Context, req *pb.EntityRequest) (*pb.EntityResponse, error) {
	var id signature.PublicKey
	if err := id.UnmarshalBinary(req.GetId()); err != nil {
		return nil, err
	}

	ent, err := s.backend.GetEntity(ctx, id)
	if err == nil {
		return nil, err
	}

	var resp pb.EntityResponse
	if ent != nil {
		resp.Entity = ent.ToProto()
	}

	return &resp, nil
}

func (s *grpcServer) GetEntities(ctx context.Context, req *pb.EntitiesRequest) (*pb.EntitiesResponse, error) {
	ents := s.backend.GetEntities(ctx)

	pbEnts := make([]*commonPB.Entity, 0, len(ents))
	for _, v := range ents {
		pbEnts = append(pbEnts, v.ToProto())
	}

	return &pb.EntitiesResponse{Entity: pbEnts}, nil
}

func (s *grpcServer) WatchEntities(req *pb.WatchEntityRequest, stream pb.EntityRegistry_WatchEntitiesServer) error {
	ch, sub := s.backend.WatchEntities()
	defer sub.Close()

	for {
		var ev *api.EntityEvent
		var ok bool

		select {
		case ev, ok = <-ch:
		case <-stream.Context().Done():
		}
		if !ok {
			break
		}

		resp := &pb.WatchEntityResponse{
			EventType: pb.WatchEntityResponse_REGISTERED,
			Entity:    ev.Entity.ToProto(),
		}
		if !ev.IsRegistration {
			resp.EventType = pb.WatchEntityResponse_DEREGISTERED
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func (s *grpcServer) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	var node node.Node
	if err := node.FromProto(req.GetNode()); err != nil {
		return nil, err
	}
	var sig signature.Signature
	if err := sig.FromProto(req.GetSignature()); err != nil {
		return nil, err
	}

	if err := s.backend.RegisterNode(ctx, &node, &sig); err != nil {
		registryFailures.With(prometheus.Labels{"call": "registerNode"}).Inc()
		return nil, err
	}

	registryNodes.Inc()
	return &pb.RegisterNodeResponse{}, nil
}

func (s *grpcServer) GetNode(ctx context.Context, req *pb.NodeRequest) (*pb.NodeResponse, error) {
	var id signature.PublicKey
	if err := id.UnmarshalBinary(req.GetId()); err != nil {
		return nil, err
	}

	node, err := s.backend.GetNode(ctx, id)
	if err != nil {
		return nil, err
	}

	var resp pb.NodeResponse
	if node != nil {
		resp.Node = node.ToProto()
	}

	return &resp, nil
}

func (s *grpcServer) GetNodes(ctx context.Context, req *pb.NodesRequest) (*pb.NodesResponse, error) {
	nodes := s.backend.GetNodes(ctx)

	// XXX: Epoch????  The underlying implementation doesn't take this
	// argument.

	pbNodes := make([]*commonPB.Node, 0, len(nodes))
	for _, v := range nodes {
		pbNodes = append(pbNodes, v.ToProto())
	}

	return &pb.NodesResponse{Node: pbNodes}, nil
}

func (s *grpcServer) GetNodesForEntity(ctx context.Context, req *pb.EntityNodesRequest) (*pb.EntityNodesResponse, error) {
	var id signature.PublicKey
	if err := id.UnmarshalBinary(req.GetId()); err != nil {
		return nil, err
	}

	nodes := s.backend.GetNodesForEntity(ctx, id)
	pbNodes := make([]*commonPB.Node, 0, len(nodes))
	for _, v := range nodes {
		pbNodes = append(pbNodes, v.ToProto())
	}

	return &pb.EntityNodesResponse{Node: pbNodes}, nil
}

func (s *grpcServer) WatchNodes(req *pb.WatchNodeRequest, stream pb.EntityRegistry_WatchNodesServer) error {
	ch, sub := s.backend.WatchNodes()
	defer sub.Close()

	for {
		var ev *api.NodeEvent
		var ok bool

		select {
		case ev, ok = <-ch:
		case <-stream.Context().Done():
		}
		if !ok {
			break
		}

		resp := &pb.WatchNodeResponse{
			EventType: pb.WatchNodeResponse_REGISTERED,
			Node:      ev.Node.ToProto(),
		}
		if !ev.IsRegistration {
			resp.EventType = pb.WatchNodeResponse_DEREGISTERED
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func (s *grpcServer) WatchNodeList(req *pb.WatchNodeListRequest, stream pb.EntityRegistry_WatchNodeListServer) error {
	ch, sub := s.backend.WatchNodeList()
	defer sub.Close()

	for {
		var nl *api.NodeList
		var ok bool

		select {
		case nl, ok = <-ch:
		case <-stream.Context().Done():
		}
		if !ok {
			break
		}

		nodes := make([]*commonPB.Node, 0, len(nl.Nodes))
		for _, n := range nl.Nodes {
			nodes = append(nodes, n.ToProto())
		}
		resp := &pb.WatchNodeListResponse{
			Epoch: uint64(nl.Epoch),
			Node:  nodes,
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func (s *grpcServer) RegisterContract(ctx context.Context, req *pb.RegisterContractRequest) (*pb.RegisterContractResponse, error) {
	var con contract.Contract
	if err := con.FromProto(req.GetContract()); err != nil {
		return nil, err
	}
	var sig signature.Signature
	if err := sig.FromProto(req.GetSignature()); err != nil {
		return nil, err
	}

	if err := s.backend.RegisterContract(ctx, &con, &sig); err != nil {
		registryFailures.With(prometheus.Labels{"call": "registerContract"}).Inc()
		return nil, err
	}

	registryContracts.Inc()
	return &pb.RegisterContractResponse{}, nil
}

func (s *grpcServer) GetContract(ctx context.Context, req *pb.ContractRequest) (*pb.ContractResponse, error) {
	var id signature.PublicKey
	if err := id.UnmarshalBinary(req.GetId()); err != nil {
		return nil, err
	}

	con, err := s.backend.GetContract(ctx, id)
	if err != nil {
		return nil, err
	}

	var resp pb.ContractResponse
	if con != nil {
		resp.Contract = con.ToProto()
	}

	return &resp, err
}

func (s *grpcServer) GetContracts(req *pb.ContractsRequest, stream pb.ContractRegistry_GetContractsServer) error {
	ch, sub := s.backend.WatchContracts()
	defer sub.Close()

	for {
		var con *contract.Contract
		var ok bool

		select {
		case con, ok = <-ch:
		case <-stream.Context().Done():
		}
		if !ok {
			break
		}

		resp := &pb.ContractsResponse{
			Contract: con.ToProto(),
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

// NewGRPCServer initializes and registers a new gRPC registry server
// backed by the provided Backend.
func NewGRPCServer(srv *grpc.Server, backend api.Backend) {
	prometheus.MustRegister(registeryCollectors...)

	s := &grpcServer{
		backend: backend,
	}
	pb.RegisterEntityRegistryServer(srv, s)
	pb.RegisterContractRegistryServer(srv, s)
}
