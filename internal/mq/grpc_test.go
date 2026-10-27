package mq

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestGRPCRoundTrip(t *testing.T) {
	b := testBroker(t, 50)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- ServeGRPC(ctx, addr, b) }()
	time.Sleep(50 * time.Millisecond)

	c, err := Dial(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Health(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Produce(ctx, &ProduceRequest{Topic: DefaultTopic, Key: "gpu", ID: "id1", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	j, err := c.Join(ctx, &JoinRequest{Group: "collectors", MemberID: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Heartbeat(ctx, &HeartbeatRequest{Group: "collectors", MemberID: "c1"}); err != nil {
		t.Fatal(err)
	}
	part := PartitionFor("gpu", b.PartitionCount())
	fctx, fc := context.WithTimeout(ctx, time.Second)
	fr, err := c.Fetch(fctx, &FetchRequest{
		Group: "collectors", MemberID: "c1", Generation: j.Generation, Partition: part, MaxRecords: 8,
	})
	fc()
	if err != nil || len(fr.Records) == 0 {
		t.Fatalf("fetch %v %+v", err, fr)
	}
	if _, err := c.Ack(ctx, &AckRequest{
		Group: "collectors", MemberID: "c1", Generation: j.Generation,
		Partition: fr.Records[0].Partition, Offset: fr.Records[0].Offset,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Produce(ctx, &ProduceRequest{Topic: "nope", Key: "x", ID: "z"}); err == nil {
		t.Fatal("expected topic error")
	}
	cancel()
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
	}
}

func TestUnaryInterceptor(t *testing.T) {
	b := testBroker(t, 8)
	gs := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(ctx, req)
	}))
	NewGRPCServer(b).Register(gs)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(gs.Stop)
	ctx := context.Background()
	c, err := Dial(ctx, ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Health(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Join(ctx, &JoinRequest{Group: "g", MemberID: "m"}); err != nil {
		t.Fatal(err)
	}
}

func TestServeGRPCBadAddr(t *testing.T) {
	b := testBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := ServeGRPC(ctx, "256.256.256.256:9", b); err == nil {
		t.Fatal("expected listen error")
	}
}

func TestDialFail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := Dial(ctx, "127.0.0.1:1"); err == nil {
		t.Fatal("expected dial error")
	}
}

func TestDecodeBadJSON(t *testing.T) {
	_, err := decode[JoinRequest](wrapperspb.Bytes([]byte("{")))
	if err == nil {
		t.Fatal("expected")
	}
}
