package mq

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestOpenDefaultsAndOpenFail(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if b.PartitionCount() != DefaultPartitions || b.TopicName() != DefaultTopic {
		t.Fatal("defaults")
	}
	_ = b.Close()

	file := filepath.Join(t.TempDir(), "notdir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{DataDir: file, Partitions: 1}); err == nil {
		t.Fatal("expected open fail")
	}
}

func TestWALInvalidJSONAndHugeSize(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("{")
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.f.Write(append(hdr[:], body...)); err != nil {
		t.Fatal(err)
	}
	_ = w.f.Sync()
	if _, err := w.replay(); err == nil {
		t.Fatal("json")
	}
	_ = w.Close()

	dir2 := t.TempDir()
	w2, err := openWAL(dir2)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(hdr[:], 17<<20)
	if _, err := w2.f.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	_ = w2.f.Sync()
	if _, err := w2.replay(); err == nil {
		t.Fatal("huge")
	}
	_ = w2.Close()
}

func TestWALAppendAfterFileClose(t *testing.T) {
	w, err := openWAL(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_ = w.f.Close()
	if err := w.append(walRecord{Type: walMsg}); err == nil {
		t.Fatal("append")
	}
	if _, err := w.replay(); err == nil {
		t.Fatal("replay")
	}
}

func TestProduceAckWALError(t *testing.T) {
	b := testBroker(t, 8)
	_ = b.wal.f.Close()
	if _, err := b.Produce(context.Background(), &ProduceRequest{Key: "k", ID: "1"}); err == nil {
		t.Fatal("produce wal")
	}
	b2 := testBroker(t, 8)
	j, err := b2.Join(context.Background(), &JoinRequest{Group: "g", MemberID: "m"})
	if err != nil {
		t.Fatal(err)
	}
	_ = b2.wal.f.Close()
	if _, err := b2.Ack(context.Background(), &AckRequest{
		Group: "g", MemberID: "m", Generation: j.Generation, Partition: j.Partitions[0], Offset: 0,
	}); err == nil {
		t.Fatal("ack wal")
	}
}

func TestRestoreCorruptWAL(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("not-json")
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.f.Write(append(hdr[:], body...)); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	if _, err := Open(Options{DataDir: dir, Partitions: 1}); err == nil {
		t.Fatal("restore")
	}
}

func TestFetchNotAssignedAndUnknownMember(t *testing.T) {
	b := testBroker(t, 8)
	ctx := context.Background()
	if _, err := b.Join(ctx, &JoinRequest{Group: "g", MemberID: "m"}); err != nil {
		t.Fatal(err)
	}
	j, err := b.Join(ctx, &JoinRequest{Group: "g", MemberID: "n"})
	if err != nil {
		t.Fatal(err)
	}
	other := -1
	owned := map[int]bool{}
	for _, p := range j.Partitions {
		owned[p] = true
	}
	for i := 0; i < b.PartitionCount(); i++ {
		if !owned[i] {
			other = i
			break
		}
	}
	if other < 0 {
		t.Fatal("expected unassigned partition")
	}
	if _, err := b.Fetch(ctx, &FetchRequest{
		Group: "g", MemberID: "n", Generation: j.Generation, Partition: other,
	}); !errors.Is(err, ErrNotAssigned) {
		t.Fatalf("not assigned %v", err)
	}
	if _, err := b.Fetch(ctx, &FetchRequest{
		Group: "nope", MemberID: "m", Generation: 1, Partition: 0,
	}); !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("unknown %v", err)
	}
}

func TestFetchCheckAssignAfterWait(t *testing.T) {
	b := testBroker(t, 8)
	ctx := context.Background()
	j, err := b.Join(ctx, &JoinRequest{Group: "g", MemberID: "m"})
	if err != nil {
		t.Fatal(err)
	}
	part := j.Partitions[0]
	done := make(chan error, 1)
	fctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	go func() {
		_, err := b.Fetch(fctx, &FetchRequest{
			Group: "g", MemberID: "m", Generation: j.Generation, Partition: part,
		})
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_, _ = b.Join(ctx, &JoinRequest{Group: "g", MemberID: "n"})
	p := b.parts[part]
	p.mu.Lock()
	p.notEmpty.Broadcast()
	p.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestFetchTimeoutEmpty(t *testing.T) {
	b := testBroker(t, 8)
	ctx := context.Background()
	j, err := b.Join(ctx, &JoinRequest{Group: "g", MemberID: "m"})
	if err != nil {
		t.Fatal(err)
	}
	fctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	fr, err := b.Fetch(fctx, &FetchRequest{
		Group: "g", MemberID: "m", Generation: j.Generation, Partition: j.Partitions[0],
	})
	if err != nil || fr == nil {
		t.Fatal(err, fr)
	}
}

func TestUnaryDecoderError(t *testing.T) {
	h := unary(func(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
		return in, nil
	})
	_, err := h(nil, context.Background(), func(interface{}) error { return errors.New("dec") }, nil)
	if err == nil {
		t.Fatal("dec")
	}
	okh := unary(func(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
		return wrapperspb.Bytes([]byte("ok")), nil
	})
	out, err := okh(nil, context.Background(), func(v interface{}) error {
		*(v.(*wrapperspb.BytesValue)) = wrapperspb.BytesValue{Value: []byte("x")}
		return nil
	}, nil)
	if err != nil || out == nil {
		t.Fatal(err)
	}
}

func TestGRPCMethodDecodeErrors(t *testing.T) {
	s := NewGRPCServer(testBroker(t, 4))
	ctx := context.Background()
	bad := wrapperspb.Bytes([]byte("{"))
	if _, err := s.produce(ctx, bad); err == nil {
		t.Fatal("p")
	}
	if _, err := s.join(ctx, bad); err == nil {
		t.Fatal("j")
	}
	if _, err := s.heartbeat(ctx, bad); err == nil {
		t.Fatal("h")
	}
	if _, err := s.fetch(ctx, bad); err == nil {
		t.Fatal("f")
	}
	if _, err := s.ack(ctx, bad); err == nil {
		t.Fatal("a")
	}
}

func TestGRPCMethodInnerErrors(t *testing.T) {
	s := NewGRPCServer(testBroker(t, 4))
	ctx := context.Background()
	raw, _ := json.Marshal(ProduceRequest{Topic: "nope", Key: "k"})
	if _, err := s.produce(ctx, wrapperspb.Bytes(raw)); err == nil {
		t.Fatal("topic")
	}
	raw, _ = json.Marshal(JoinRequest{})
	if _, err := s.join(ctx, wrapperspb.Bytes(raw)); err == nil {
		t.Fatal("join")
	}
	raw, _ = json.Marshal(FetchRequest{Partition: 99})
	if _, err := s.fetch(ctx, wrapperspb.Bytes(raw)); err == nil {
		t.Fatal("fetch")
	}
	raw, _ = json.Marshal(AckRequest{Group: "g", MemberID: "m", Generation: 1, Partition: 0})
	if _, err := s.ack(ctx, wrapperspb.Bytes(raw)); err == nil {
		t.Fatal("ack")
	}
	raw, _ = json.Marshal(HeartbeatRequest{Group: "missing", MemberID: "m"})
	if _, err := s.heartbeat(ctx, wrapperspb.Bytes(raw)); err != nil {
		t.Fatal(err)
	}
}

func TestClientNilClose(t *testing.T) {
	c := &Client{}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAssignEmpty(t *testing.T) {
	if Assign(0, []string{"a"})["a"] != nil {
		t.Fatal("npart 0")
	}
	if len(Assign(4, nil)) != 0 {
		t.Fatal("no members")
	}
}

func TestMapErrNotAssigned(t *testing.T) {
	if mapErr(ErrNotAssigned) == nil || mapErr(ErrUnknownPartition) == nil || mapErr(ErrUnknownMember) == nil {
		t.Fatal("map")
	}
}

func TestUnaryWithInterceptorDirect(t *testing.T) {
	h := unary(func(ctx context.Context, in *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
		return in, nil
	})
	interceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(ctx, req)
	}
	out, err := h(nil, context.Background(), func(v interface{}) error {
		*(v.(*wrapperspb.BytesValue)) = wrapperspb.BytesValue{Value: []byte("z")}
		return nil
	}, interceptor)
	if err != nil || out == nil {
		t.Fatal(err)
	}
}
