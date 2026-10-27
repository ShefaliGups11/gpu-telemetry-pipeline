package mq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSplitAddrs(t *testing.T) {
	got := SplitAddrs(" a:1, b:2 ")
	if len(got) != 2 || got[0] != "a:1" {
		t.Fatal(got)
	}
}

func TestApplyReplicaAndNotLeader(t *testing.T) {
	leader := testBroker(t, 10)
	standby, err := Open(Options{DataDir: t.TempDir(), Partitions: 4, MaxPending: 10, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = standby.Close() })
	ctx := context.Background()
	if _, err := standby.Produce(ctx, &ProduceRequest{Key: "k", ID: "no"}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("readonly %v", err)
	}
	pr, err := leader.Produce(ctx, &ProduceRequest{Key: "k", ID: "m1", Payload: []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	if err := standby.ApplyReplica(walRecord{
		Type: walMsg, Partition: pr.Partition, Offset: pr.Offset, ID: "m1", Key: "k", Payload: []byte("a"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := standby.ApplyReplica(walRecord{
		Type: walMsg, Partition: pr.Partition, Offset: pr.Offset, ID: "m1", Key: "k", Payload: []byte("a"),
	}); err != nil {
		t.Fatal(err)
	}
	j, err := standby.Join(ctx, &JoinRequest{Group: "g", MemberID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	fr, err := standby.Fetch(ctx, &FetchRequest{
		Group: "g", MemberID: "c", Generation: j.Generation, Partition: pr.Partition, MaxRecords: 8,
	})
	if err != nil || len(fr.Records) == 0 {
		t.Fatalf("replica fetch %v %+v", err, fr)
	}
}

func TestReplicateRPC(t *testing.T) {
	leader := testBroker(t, 10)
	standby := testBroker(t, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ServeGRPC(ctx, "127.0.0.1:19201", standby) }()
	time.Sleep(40 * time.Millisecond)
	leader.SetReplicator(PeerReplicator("", []string{"127.0.0.1:19201"}))
	if _, err := leader.Produce(ctx, &ProduceRequest{Key: "gpu", ID: "rep1", Payload: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	j, err := standby.Join(ctx, &JoinRequest{Group: "g", MemberID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	part := PartitionFor("gpu", standby.PartitionCount())
	fctx, fc := context.WithTimeout(ctx, time.Second)
	defer fc()
	fr, err := standby.Fetch(fctx, &FetchRequest{
		Group: "g", MemberID: "c", Generation: j.Generation, Partition: part, MaxRecords: 8,
	})
	if err != nil || len(fr.Records) == 0 {
		t.Fatalf("got %v %+v", err, fr)
	}
}

func TestMapErrNotLeader(t *testing.T) {
	if mapErr(ErrNotLeader) == nil {
		t.Fatal("map")
	}
}

func TestApplyReplicaOffsetAndSkipPart(t *testing.T) {
	b := testBroker(t, 10)
	if err := b.ApplyReplica(walRecord{Type: walMsg, Partition: 99, Offset: 0}); err != nil {
		t.Fatal(err)
	}
	if err := b.ApplyReplica(walRecord{Type: walOff, Partition: 0, Offset: 2, Group: "g"}); err != nil {
		t.Fatal(err)
	}
	if err := b.ApplyReplica(walRecord{Type: "x", Partition: 0}); err != nil {
		t.Fatal(err)
	}
	b.SetReadOnly(true)
	b.SetReplicator(func(context.Context, walRecord) error { return nil })
	if err := b.fanout(context.Background(), walRecord{}); err != nil {
		t.Fatal(err)
	}
}

func TestPeerReplicatorSkipsSelf(t *testing.T) {
	fn := PeerReplicator("127.0.0.1:1", []string{"", "127.0.0.1:1"})
	if err := fn(context.Background(), walRecord{Type: walMsg}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	fn2 := PeerReplicator("", []string{"127.0.0.1:1"})
	if err := fn2(ctx, walRecord{Type: walMsg}); err == nil {
		t.Fatal("expected peer dial error")
	}
}

func TestDialEmptySpec(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := Dial(ctx, ""); err == nil {
		t.Fatal("expected dial error")
	}
}

func TestAutoPromote(t *testing.T) {
	b, err := Open(Options{DataDir: t.TempDir(), Partitions: 2, MaxPending: 10, ReadOnly: true, AutoPromote: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if _, err := b.Produce(context.Background(), &ProduceRequest{Key: "k", ID: "p"}); err != nil {
		t.Fatal(err)
	}
}
