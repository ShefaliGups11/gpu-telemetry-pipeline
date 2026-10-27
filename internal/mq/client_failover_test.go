package mq

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func serveBroker(t *testing.T, addr string, b *Broker) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- ServeGRPC(ctx, addr, b) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
		}
	})
	time.Sleep(50 * time.Millisecond)
	return cancel
}

func TestClientFailsOverToStandby(t *testing.T) {
	leader, err := Open(Options{DataDir: t.TempDir(), Partitions: 4, Heartbeat: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = leader.Close() })
	standby, err := Open(Options{DataDir: t.TempDir(), Partitions: 4, Heartbeat: time.Second, ReadOnly: true, AutoPromote: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = standby.Close() })

	const a, b = "127.0.0.1:19571", "127.0.0.1:19572"
	stopLeader := serveBroker(t, a, leader)
	serveBroker(t, b, standby)
	leader.SetReplicator(PeerReplicator("", []string{b}))

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	c, err := Dial(ctx, a+","+b)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	h, err := c.Health(ctx)
	if err != nil || !h.Leader {
		t.Fatalf("want leader, got %+v %v", h, err)
	}
	if _, err := c.Produce(ctx, &ProduceRequest{Key: "gpu", ID: "fa-1", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	stopLeader()
	_ = leader.Close()
	time.Sleep(80 * time.Millisecond)
	pctx, pc := context.WithTimeout(ctx, 6*time.Second)
	defer pc()
	if _, err := c.Produce(pctx, &ProduceRequest{Key: "gpu", ID: "fa-2", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("failover produce: %v", err)
	}
}

func TestClientSkipsStandbyWhenLeaderListed(t *testing.T) {
	leader, err := Open(Options{DataDir: t.TempDir(), Partitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = leader.Close() })
	standby, err := Open(Options{DataDir: t.TempDir(), Partitions: 2, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = standby.Close() })
	const a, b = "127.0.0.1:19573", "127.0.0.1:19574"
	serveBroker(t, b, standby) // listed first
	serveBroker(t, a, leader)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, b+","+a)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	h, err := c.Health(ctx)
	if err != nil || !h.Leader {
		t.Fatalf("dial should prefer leader %+v %v", h, err)
	}
}

func TestFailoverRPC(t *testing.T) {
	if failoverRPC(nil) {
		t.Fatal("nil")
	}
	if !failoverRPC(status.Error(codes.Unavailable, "down")) {
		t.Fatal("unavailable")
	}
	if !failoverRPC(status.Error(codes.FailedPrecondition, "not leader")) {
		t.Fatal("not leader")
	}
	if failoverRPC(status.Error(codes.DeadlineExceeded, "wait")) {
		t.Fatal("fetch wait is not failover")
	}
}
