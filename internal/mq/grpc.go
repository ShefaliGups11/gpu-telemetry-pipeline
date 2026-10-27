package mq

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const serviceName = "mq.Broker"

// GRPCServer exposes Broker over gRPC (JSON payloads inside protobuf BytesValue).
type GRPCServer struct {
	inner *Broker
}

// NewGRPCServer wraps a Broker.
func NewGRPCServer(inner *Broker) *GRPCServer {
	return &GRPCServer{inner: inner}
}

// Register attaches the Broker service to a gRPC server.
func (s *GRPCServer) Register(gs *grpc.Server) {
	gs.RegisterService(&grpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*rpcServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "Produce", Handler: unary(s.produce)},
			{MethodName: "Join", Handler: unary(s.join)},
			{MethodName: "Heartbeat", Handler: unary(s.heartbeat)},
			{MethodName: "Fetch", Handler: unary(s.fetch)},
			{MethodName: "Ack", Handler: unary(s.ack)},
			{MethodName: "Health", Handler: unary(s.health)},
			{MethodName: "Replicate", Handler: unary(s.replicate)},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "mq.proto",
	}, s)
}

type rpcServer interface {
	produce(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error)
}

func unary(fn func(context.Context, *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error)) grpc.MethodHandler {
	return func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
		in := new(wrapperspb.BytesValue)
		if err := dec(in); err != nil {
			return nil, err
		}
		if interceptor == nil {
			return fn(ctx, in)
		}
		info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/mq.Broker/RPC"}
		return interceptor(ctx, in, info, func(ctx context.Context, req interface{}) (interface{}, error) {
			return fn(ctx, req.(*wrapperspb.BytesValue))
		})
	}
}

func decode[T any](in *wrapperspb.BytesValue) (*T, error) {
	var v T
	if err := json.Unmarshal(in.GetValue(), &v); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "json: %v", err)
	}
	return &v, nil
}

func encode(v any) (*wrapperspb.BytesValue, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "json: %v", err)
	}
	return wrapperspb.Bytes(b), nil
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrFenced), errors.Is(err, ErrNotAssigned):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, ErrNotLeader):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrUnknownMember), errors.Is(err, ErrUnknownTopic), errors.Is(err, ErrUnknownPartition):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrBackpressure):
		return status.Error(codes.ResourceExhausted, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (s *GRPCServer) produce(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
	req, err := decode[ProduceRequest](in)
	if err != nil {
		return nil, err
	}
	resp, err := s.inner.Produce(ctx, req)
	if err != nil {
		return nil, mapErr(err)
	}
	return encode(resp)
}

func (s *GRPCServer) join(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
	req, err := decode[JoinRequest](in)
	if err != nil {
		return nil, err
	}
	resp, err := s.inner.Join(ctx, req)
	if err != nil {
		return nil, mapErr(err)
	}
	return encode(resp)
}

func (s *GRPCServer) heartbeat(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
	req, err := decode[HeartbeatRequest](in)
	if err != nil {
		return nil, err
	}
	resp, err := s.inner.Heartbeat(ctx, req)
	if err != nil {
		return nil, mapErr(err)
	}
	return encode(resp)
}

func (s *GRPCServer) fetch(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
	req, err := decode[FetchRequest](in)
	if err != nil {
		return nil, err
	}
	resp, err := s.inner.Fetch(ctx, req)
	if err != nil {
		return nil, mapErr(err)
	}
	return encode(resp)
}

func (s *GRPCServer) ack(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
	req, err := decode[AckRequest](in)
	if err != nil {
		return nil, err
	}
	resp, err := s.inner.Ack(ctx, req)
	if err != nil {
		return nil, mapErr(err)
	}
	return encode(resp)
}

func (s *GRPCServer) replicate(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
	_ = ctx
	req, err := decode[walRecord](in)
	if err != nil {
		return nil, err
	}
	if err := s.inner.ApplyReplica(*req); err != nil {
		return nil, mapErr(err)
	}
	return encode(map[string]string{"ok": "1"})
}

func (s *GRPCServer) health(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
	_ = in
	resp, err := s.inner.Health(ctx, &HealthRequest{})
	if err != nil {
		return nil, mapErr(err)
	}
	return encode(resp)
}

// ServeGRPC starts the gRPC listener.
func ServeGRPC(ctx context.Context, addr string, inner *Broker) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	NewGRPCServer(inner).Register(gs)
	go func() {
		<-ctx.Done()
		gs.GracefulStop()
	}()
	return gs.Serve(ln)
}

// HealthHandler is an HTTP /healthz endpoint for Kubernetes probes.
func HealthHandler(inner *Broker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp, err := inner.Health(r.Context(), &HealthRequest{})
		if err != nil || resp == nil || !resp.OK {
			http.Error(w, "unhealthy", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}
