package mq

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func testBroker(t *testing.T, maxPending int) *Broker {
	t.Helper()
	dir := t.TempDir()
	b, err := Open(Options{
		DataDir:    dir,
		Partitions: 4,
		MaxPending: maxPending,
		Heartbeat:  200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestProduceFetchAck(t *testing.T) {
	b := testBroker(t, 100)
	ctx := context.Background()
	pr, err := b.Produce(ctx, &ProduceRequest{Topic: DefaultTopic, Key: "gpu-a", ID: "m1", Payload: []byte(`{"v":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	join, err := b.Join(ctx, &JoinRequest{Group: "collectors", MemberID: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	var found *Record
	assigned := false
	for _, p := range join.Partitions {
		if p == pr.Partition {
			assigned = true
			break
		}
	}
	if !assigned {
		t.Fatalf("produced partition %d not assigned to c1: %v", pr.Partition, join.Partitions)
	}
	fctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	fr, err := b.Fetch(fctx, &FetchRequest{
		Group: "collectors", MemberID: "c1", Generation: join.Generation,
		Partition: pr.Partition, MaxRecords: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := range fr.Records {
		if fr.Records[i].ID == "m1" {
			found = &fr.Records[i]
		}
	}
	if found == nil {
		t.Fatal("did not fetch produced record")
	}
	if found.Offset != pr.Offset || found.Partition != pr.Partition {
		t.Fatalf("mismatch produce %+v fetch %+v", pr, found)
	}
	if _, err := b.Ack(ctx, &AckRequest{
		Group: "collectors", MemberID: "c1", Generation: join.Generation,
		Partition: found.Partition, Offset: found.Offset,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSameKeySamePartition(t *testing.T) {
	b := testBroker(t, 100)
	ctx := context.Background()
	a, err := b.Produce(ctx, &ProduceRequest{Key: "same", ID: "1", Payload: []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	c, err := b.Produce(ctx, &ProduceRequest{Key: "same", ID: "2", Payload: []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	if a.Partition != c.Partition {
		t.Fatalf("expected same partition, got %d %d", a.Partition, c.Partition)
	}
	if c.Offset != a.Offset+1 {
		t.Fatalf("expected monotonic offset, got %d then %d", a.Offset, c.Offset)
	}
}

func TestWALReplay(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	b, err := Open(Options{DataDir: dir, Partitions: 2, MaxPending: 100})
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Produce(ctx, &ProduceRequest{Key: "k", ID: "id-1", Payload: []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	join, err := b.Join(ctx, &JoinRequest{Group: "collectors", MemberID: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	var rec Record
	for _, p := range join.Partitions {
		fctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fr, err := b.Fetch(fctx, &FetchRequest{
			Group: "collectors", MemberID: "c1", Generation: join.Generation, Partition: p, MaxRecords: 8,
		})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if len(fr.Records) > 0 {
			rec = fr.Records[0]
			break
		}
	}
	if rec.ID == "" {
		t.Fatal("did not fetch produced record before ack")
	}
	_, err = b.Ack(ctx, &AckRequest{
		Group: "collectors", MemberID: "c1", Generation: join.Generation,
		Partition: rec.Partition, Offset: rec.Offset,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := Open(Options{DataDir: dir, Partitions: 2, MaxPending: 100})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b2.Close() })
	join, err = b2.Join(ctx, &JoinRequest{Group: "collectors", MemberID: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range join.Partitions {
		fctx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		fr, err := b2.Fetch(fctx, &FetchRequest{
			Group: "collectors", MemberID: "c1", Generation: join.Generation, Partition: p, MaxRecords: 8,
		})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range fr.Records {
			if r.ID == "id-1" {
				t.Fatal("acked record redelivered after replay")
			}
		}
	}
}

func TestRebalanceFence(t *testing.T) {
	b := testBroker(t, 100)
	ctx := context.Background()
	j1, err := b.Join(ctx, &JoinRequest{Group: "collectors", MemberID: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	j2, err := b.Join(ctx, &JoinRequest{Group: "collectors", MemberID: "c2"})
	if err != nil {
		t.Fatal(err)
	}
	if j2.Generation <= j1.Generation {
		t.Fatalf("expected generation bump, %d then %d", j1.Generation, j2.Generation)
	}
	_, err = b.Fetch(ctx, &FetchRequest{
		Group: "collectors", MemberID: "c1", Generation: j1.Generation,
		Partition: j1.Partitions[0], MaxRecords: 1,
	})
	if err == nil {
		t.Fatal("expected fenced fetch with stale generation")
	}
}

func TestBackpressure(t *testing.T) {
	b := testBroker(t, 2)
	ctx := context.Background()
	key := "only"
	if _, err := b.Produce(ctx, &ProduceRequest{Key: key, ID: "1", Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Produce(ctx, &ProduceRequest{Key: key, ID: "2", Payload: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	tctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err := b.Produce(tctx, &ProduceRequest{Key: key, ID: "3", Payload: []byte("c")})
	if err == nil {
		t.Fatal("expected backpressure")
	}
}

func TestOpenCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "data")
	b, err := Open(Options{DataDir: dir, Partitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
}
