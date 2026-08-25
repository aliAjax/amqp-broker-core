package grpcx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	"github.com/enterprise/amqp-broker-core/internal/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"net"
	"strings"
	"time"
)

type Server struct {
	Address        string
	Runtime        *broker.Runtime
	Token          string
	AllowAnonymous bool
	server         *grpc.Server
	listener       net.Listener
}
type ManagementServer interface {
	Health(context.Context, *structpb.Struct) (*structpb.Struct, error)
	CreateAddress(context.Context, *structpb.Struct) (*structpb.Struct, error)
	GetDepth(context.Context, *structpb.Struct) (*structpb.Struct, error)
	PauseAddress(context.Context, *structpb.Struct) (*structpb.Struct, error)
	ListConnections(context.Context, *structpb.Struct) (*structpb.Struct, error)
	GetTransaction(context.Context, *structpb.Struct) (*structpb.Struct, error)
}

func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.Address)
	if err != nil {
		return fmt.Errorf("listen gRPC: %v", err)
	}
	opts := []grpc.ServerOption{grpc.UnaryInterceptor(s.interceptor)}
	s.server = grpc.NewServer(opts...)
	s.listener = ln
	s.server.RegisterService(&managementServiceDesc, s)
	reflection.Register(s.server)
	go func() { <-ctx.Done(); s.server.GracefulStop() }()
	go func() { _ = s.server.Serve(ln) }()
	return nil
}
func (s *Server) Close(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	done := make(chan struct{})
	go func() { s.server.GracefulStop(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.server.Stop()
		return ctx.Err()
	}
}
func (s *Server) interceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if s.AllowAnonymous || strings.HasSuffix(info.FullMethod, "/Health") {
		return handler(ctx, req)
	}
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || strings.TrimPrefix(values[0], "Bearer ") != s.Token {
		return nil, status.Error(codes.Unauthenticated, "valid bearer token required")
	}
	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return handler(callCtx, req)
}
func (s *Server) Health(ctx context.Context, _ *structpb.Struct) (*structpb.Struct, error) {
	return toStruct(map[string]any{"status": "ready", "node_id": s.Runtime.Config.GetNodeID()})
}
func (s *Server) CreateAddress(ctx context.Context, in *structpb.Struct) (*structpb.Struct, error) {
	var req address.Create
	if err := fromStruct(in, &req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	a, err := s.Runtime.Addresses.Create(ctx, req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return toStruct(a)
}
func (s *Server) GetDepth(ctx context.Context, in *structpb.Struct) (*structpb.Struct, error) {
	name := stringField(in, "name")
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	depth, err := s.Runtime.Delivery.Depth(ctx, name)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return toStruct(map[string]any{"name": name, "depth": depth})
}
func (s *Server) PauseAddress(ctx context.Context, in *structpb.Struct) (*structpb.Struct, error) {
	name := stringField(in, "name")
	paused := true
	if field := in.GetFields()["paused"]; field != nil {
		paused = field.GetBoolValue()
	}
	a, err := s.Runtime.Addresses.Pause(ctx, name, paused)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return toStruct(a)
}
func (s *Server) ListConnections(context.Context, *structpb.Struct) (*structpb.Struct, error) {
	return toStruct(map[string]any{"items": s.Runtime.Connections.List(), "count": s.Runtime.Connections.Count()})
}
func (s *Server) GetTransaction(ctx context.Context, in *structpb.Struct) (*structpb.Struct, error) {
	id := stringField(in, "id")
	tx, err := s.Runtime.Transactions.Get(ctx, id)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return toStruct(tx)
}
func stringField(in *structpb.Struct, name string) string {
	if in == nil {
		return ""
	}
	return in.GetFields()[name].GetStringValue()
}
func fromStruct(in *structpb.Struct, out any) error {
	if in == nil {
		return errors.New("request required")
	}
	b, err := in.MarshalJSON()
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}
func toStruct(v any) (*structpb.Struct, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var raw map[string]any
	if err = json.Unmarshal(b, &raw); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out, err := structpb.NewStruct(raw)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return out, nil
}

var managementServiceDesc = grpc.ServiceDesc{ServiceName: "broker.v1.Management", HandlerType: (*ManagementServer)(nil), Methods: []grpc.MethodDesc{{MethodName: "Health", Handler: unaryHealth}, {MethodName: "CreateAddress", Handler: unaryCreateAddress}, {MethodName: "GetDepth", Handler: unaryGetDepth}, {MethodName: "PauseAddress", Handler: unaryPauseAddress}, {MethodName: "ListConnections", Handler: unaryListConnections}, {MethodName: "GetTransaction", Handler: unaryGetTransaction}}, Streams: []grpc.StreamDesc{}, Metadata: "api/proto/management.proto"}

func unaryHealth(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	return dispatch(srv, ctx, dec, interceptor, "/broker.v1.Management/Health", func(ctx context.Context, in *structpb.Struct) (*structpb.Struct, error) {
		return srv.(ManagementServer).Health(ctx, in)
	})
}
func unaryCreateAddress(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	return dispatch(srv, ctx, dec, interceptor, "/broker.v1.Management/CreateAddress", func(ctx context.Context, in *structpb.Struct) (*structpb.Struct, error) {
		return srv.(ManagementServer).CreateAddress(ctx, in)
	})
}
func unaryGetDepth(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	return dispatch(srv, ctx, dec, interceptor, "/broker.v1.Management/GetDepth", func(ctx context.Context, in *structpb.Struct) (*structpb.Struct, error) {
		return srv.(ManagementServer).GetDepth(ctx, in)
	})
}
func unaryPauseAddress(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	return dispatch(srv, ctx, dec, interceptor, "/broker.v1.Management/PauseAddress", func(ctx context.Context, in *structpb.Struct) (*structpb.Struct, error) {
		return srv.(ManagementServer).PauseAddress(ctx, in)
	})
}
func unaryListConnections(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	return dispatch(srv, ctx, dec, interceptor, "/broker.v1.Management/ListConnections", func(ctx context.Context, in *structpb.Struct) (*structpb.Struct, error) {
		return srv.(ManagementServer).ListConnections(ctx, in)
	})
}
func unaryGetTransaction(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	return dispatch(srv, ctx, dec, interceptor, "/broker.v1.Management/GetTransaction", func(ctx context.Context, in *structpb.Struct) (*structpb.Struct, error) {
		return srv.(ManagementServer).GetTransaction(ctx, in)
	})
}
func dispatch(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor, method string, call func(context.Context, *structpb.Struct) (*structpb.Struct, error)) (any, error) {
	in := new(structpb.Struct)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return call(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: method}
	handler := func(ctx context.Context, req any) (any, error) { return call(ctx, req.(*structpb.Struct)) }
	return interceptor(ctx, in, info, handler)
}
