package mq

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPartitionForZero(t *testing.T) {
	if PartitionFor("k", 0) != 0 {
		t.Fatal("n<=0")
	}
}

func TestUnknownTopicAndJoin(t *testing.T) {
	b := testBroker(t, 10)
	ctx := context.Background()
	if _, err := b.Produce(ctx, &ProduceRequest{Topic: "other", Key: "k", ID: "1"}); err == nil {
		t.Fatal("expected unknown topic")
	}
	if _, err := b.Join(ctx, &JoinRequest{}); err == nil {
		t.Fatal("expected join error")
	}
	if _, err := b.Fetch(ctx, &FetchRequest{Partition: 99}); err == nil {
		t.Fatal("expected unknown partition")
	}
}

func TestHeartbeatRejoin(t *testing.T) {
	b := testBroker(t, 10)
	ctx := context.Background()
	hb, err := b.Heartbeat(ctx, &HeartbeatRequest{Group: "g", MemberID: "m"})
	if err != nil || !hb.Rejoin {
		t.Fatalf("missing group %+v %v", hb, err)
	}
	if _, err := b.Join(ctx, &JoinRequest{Group: "g", MemberID: "m"}); err != nil {
		t.Fatal(err)
	}
	hb, err = b.Heartbeat(ctx, &HeartbeatRequest{Group: "g", MemberID: "nope"})
	if err != nil || !hb.Rejoin {
		t.Fatal("missing member")
	}
	hb, err = b.Heartbeat(ctx, &HeartbeatRequest{Group: "g", MemberID: "m"})
	if err != nil || hb.Rejoin {
		t.Fatal("ok member")
	}
}

func TestHealthAndHandler(t *testing.T) {
	b := testBroker(t, 10)
	ctx := context.Background()
	h, err := b.Health(ctx, &HealthRequest{})
	if err != nil || !h.OK || !h.Leader {
		t.Fatal(h, err)
	}
	if b.TopicName() == "" || b.PartitionCount() != 4 {
		t.Fatal("meta")
	}
	rr := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	HealthHandler(b).ServeHTTP(w, rr)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	_ = b.Close()
	_ = b.Close()
	h, _ = b.Health(ctx, &HealthRequest{})
	if h.OK {
		t.Fatal("closed")
	}
	w = httptest.NewRecorder()
	HealthHandler(b).ServeHTTP(w, rr)
	if w.Code != 503 {
		t.Fatalf("unhealthy %d", w.Code)
	}
}

func TestAckFenced(t *testing.T) {
	b := testBroker(t, 10)
	ctx := context.Background()
	j1, err := b.Join(ctx, &JoinRequest{Group: "g", MemberID: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Join(ctx, &JoinRequest{Group: "g", MemberID: "n"}); err != nil {
		t.Fatal(err)
	}
	_, err = b.Ack(ctx, &AckRequest{Group: "g", MemberID: "m", Generation: j1.Generation, Partition: j1.Partitions[0], Offset: 0})
	if err == nil {
		t.Fatal("expected fenced")
	}
}

func TestReapMember(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(Options{DataDir: dir, Partitions: 2, MaxPending: 10, Heartbeat: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	ctx := context.Background()
	if _, err := b.Join(ctx, &JoinRequest{Group: "g", MemberID: "m"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	hb, err := b.Heartbeat(ctx, &HeartbeatRequest{Group: "g", MemberID: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if !hb.Rejoin {
		t.Fatal("expected reap")
	}
}

func TestWALCorruptSize(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], 0)
	if _, err := w.f.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	_ = w.f.Sync()
	if _, err := w.replay(); err == nil {
		t.Fatal("expected invalid size")
	}
	_ = w.Close()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	if err := os.WriteFile(path, []byte{1, 0, 0}, 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := openWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	recs, err := w.replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatal(recs)
	}
}

func TestEncodeFail(t *testing.T) {
	if _, err := encode(map[string]any{"bad": func() {}}); err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestRestoreSkipsUnknownPartition(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.append(walRecord{Type: walMsg, Partition: 99, Offset: 0, ID: "x", Key: "k", Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := w.append(walRecord{Type: walOff, Partition: 0, Offset: 3, Group: "g"}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	b, err := Open(Options{DataDir: dir, Partitions: 2, MaxPending: 10})
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
}

func TestMapErrAndFetchMax(t *testing.T) {
	b := testBroker(t, 10)
	ctx := context.Background()
	_, err := b.Produce(ctx, &ProduceRequest{Key: "k", ID: "1", Payload: []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	j, err := b.Join(ctx, &JoinRequest{Group: "g", MemberID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	part := PartitionFor("k", b.PartitionCount())
	fr, err := b.Fetch(ctx, &FetchRequest{
		Group: "g", MemberID: "c", Generation: j.Generation, Partition: part, MaxRecords: 0,
	})
	if err != nil || len(fr.Records) == 0 {
		t.Fatalf("%v %+v", err, fr)
	}
	if mapErr(nil) != nil {
		t.Fatal("nil")
	}
	if mapErr(ErrFenced) == nil || mapErr(ErrUnknownTopic) == nil || mapErr(ErrBackpressure) == nil {
		t.Fatal("map")
	}
	if mapErr(context.Canceled) == nil {
		t.Fatal("default")
	}
}
