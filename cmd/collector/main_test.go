package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"gpu-telemetry-pipeline/internal/model"
	"gpu-telemetry-pipeline/internal/mq"
	"gpu-telemetry-pipeline/internal/store"
)

func TestMainCanceled(t *testing.T) {
	var code int
	fatalExit = func(c int) { code = c }
	defer func() { fatalExit = os.Exit }()
	t.Setenv("DATABASE_URL", "postgres://gpu:gpu@127.0.0.1:1/gpu?sslmode=disable")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	newRootCtx = func() (context.Context, context.CancelFunc) { return ctx, func() {} }
	defer func() { newRootCtx = func() (context.Context, context.CancelFunc) { return context.Background(), func() {} } }()
	main()
	if code != 1 {
		t.Fatal(code)
	}
}

func TestWaitPostgresCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := waitPostgres(ctx, "postgres://gpu:gpu@127.0.0.1:1/gpu?sslmode=disable"); err == nil {
		t.Fatal("expected cancel")
	}
}

func TestRunCanceled(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://gpu:gpu@127.0.0.1:1/gpu?sslmode=disable")
	t.Setenv("BROKER_ADDR", "127.0.0.1:1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx); err == nil {
		t.Fatal("expected")
	}
}

func startBroker(t *testing.T, addr string) (*mq.Broker, context.CancelFunc, *mq.Client) {
	t.Helper()
	b, err := mq.Open(mq.Options{DataDir: t.TempDir(), Partitions: 2, Heartbeat: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = b.Close()
	})
	go func() { _ = mq.ServeGRPC(ctx, addr, b) }()
	time.Sleep(40 * time.Millisecond)
	c, err := mq.Dial(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return b, cancel, c
}

func TestRunSession(t *testing.T) {
	_, _, c := startBroker(t, "127.0.0.1:19101")
	st := store.NewMemory()
	pt := model.Point{MessageID: "mid", ProcessedAt: time.Now().UTC(), UUID: "gpu-1", Hostname: "h", Device: "d", GPUID: "0", ModelName: "m", MetricName: "util", Value: 9}
	raw, _ := json.Marshal(pt)
	if _, err := c.Produce(context.Background(), &mq.ProduceRequest{Key: pt.UUID, ID: pt.MessageID, Payload: raw}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = runSession(ctx, c, st, "collectors", "c1")
	gpus, _ := st.ListGPUs(context.Background())
	if len(gpus) == 0 {
		t.Fatal("expected persist")
	}
}

func TestRunSessionBadJSONAndDuplicate(t *testing.T) {
	_, _, c := startBroker(t, "127.0.0.1:19102")
	st := store.NewMemory()
	if _, err := c.Produce(context.Background(), &mq.ProduceRequest{Key: "k", ID: "bad", Payload: []byte("not-json")}); err != nil {
		t.Fatal(err)
	}
	pt := model.Point{MessageID: "dup", ProcessedAt: time.Now().UTC(), UUID: "gpu-2", MetricName: "u", Value: 1}
	raw, _ := json.Marshal(pt)
	if _, err := c.Produce(context.Background(), &mq.ProduceRequest{Key: "gpu-2", ID: "dup", Payload: raw}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Produce(context.Background(), &mq.ProduceRequest{Key: "gpu-2", ID: "dup2", Payload: raw}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = runSession(ctx, c, st, "collectors", "c2")
}

func TestConsumeLoop(t *testing.T) {
	_, _, c := startBroker(t, "127.0.0.1:19103")
	st := store.NewMemory()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if err := consumeLoop(ctx, c, st, "collectors", "c3"); err != nil {
		t.Fatal(err)
	}
}

func TestMainOK(t *testing.T) {
	openStore = func(ctx context.Context, url string) (store.Store, error) {
		return store.NewMemory(), nil
	}
	defer func() { openStore = func(ctx context.Context, url string) (store.Store, error) { return store.OpenPostgres(ctx, url) } }()
	_, _, _ = startBroker(t, "127.0.0.1:19109")
	t.Setenv("BROKER_ADDR", "127.0.0.1:19109")
	os.Unsetenv("MEMBER_ID")
	t.Setenv("HOSTNAME", "main-host")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	fatalExit = func(int) { t.Fatal("exit") }
	defer func() { fatalExit = os.Exit }()
	newRootCtx = func() (context.Context, context.CancelFunc) { return ctx, cancel }
	defer func() {
		newRootCtx = func() (context.Context, context.CancelFunc) { return context.Background(), func() {} }
	}()
	main()
}

func TestRunWithMemoryAndBroker(t *testing.T) {
	openStore = func(ctx context.Context, url string) (store.Store, error) {
		return store.NewMemory(), nil
	}
	defer func() { openStore = func(ctx context.Context, url string) (store.Store, error) { return store.OpenPostgres(ctx, url) } }()
	_, _, _ = startBroker(t, "127.0.0.1:19107")
	t.Setenv("BROKER_ADDR", "127.0.0.1:19107")
	os.Unsetenv("MEMBER_ID")
	t.Setenv("HOSTNAME", "collector-host")
	sessionRetry = 10 * time.Millisecond
	heartbeatEvery = 30 * time.Millisecond
	defer func() {
		sessionRetry = time.Second
		heartbeatEvery = 2 * time.Second
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunInitError(t *testing.T) {
	openStore = func(ctx context.Context, url string) (store.Store, error) {
		return initFailStore{Store: store.NewMemory()}, nil
	}
	defer func() { openStore = func(ctx context.Context, url string) (store.Store, error) { return store.OpenPostgres(ctx, url) } }()
	if err := run(context.Background()); err == nil {
		t.Fatal("init")
	}
}

func TestRunDialFailAfterStore(t *testing.T) {
	openStore = func(ctx context.Context, url string) (store.Store, error) {
		return store.NewMemory(), nil
	}
	defer func() { openStore = func(ctx context.Context, url string) (store.Store, error) { return store.OpenPostgres(ctx, url) } }()
	t.Setenv("BROKER_ADDR", "127.0.0.1:1")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := run(ctx); err == nil {
		t.Fatal("dial")
	}
}

func TestWaitPostgresRetrySuccess(t *testing.T) {
	n := 0
	openStore = func(ctx context.Context, url string) (store.Store, error) {
		n++
		if n == 1 {
			return nil, context.DeadlineExceeded
		}
		return store.NewMemory(), nil
	}
	defer func() { openStore = func(ctx context.Context, url string) (store.Store, error) { return store.OpenPostgres(ctx, url) } }()
	st, err := waitPostgres(context.Background(), "ignored")
	if err != nil || st == nil {
		t.Fatal(err)
	}
}

type initFailStore struct{ store.Store }

func (initFailStore) Init(ctx context.Context) error { return errors.New("init") }

type persistErrStore struct{ store.Store }

func (p persistErrStore) UpsertAndInsert(ctx context.Context, pt model.Point) error {
	return errors.New("persist")
}

func TestRunSessionPersistErrorAndEmptyID(t *testing.T) {
	heartbeatEvery = 30 * time.Millisecond
	sessionRetry = 10 * time.Millisecond
	persistRetry = 5 * time.Millisecond
	defer func() {
		heartbeatEvery = 2 * time.Second
		sessionRetry = time.Second
		persistRetry = 200 * time.Millisecond
	}()
	_, _, c := startBroker(t, "127.0.0.1:19105")
	pt := model.Point{ProcessedAt: time.Now().UTC(), UUID: "gpu-3", Hostname: "h", Device: "d", GPUID: "0", ModelName: "m", MetricName: "util", Value: 2}
	raw, _ := json.Marshal(pt)
	if _, err := c.Produce(context.Background(), &mq.ProduceRequest{Key: pt.UUID, ID: "noid", Payload: raw}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_ = runSession(ctx, c, persistErrStore{Store: store.NewMemory()}, "collectors", "c5")
}

func TestRunSessionHeartbeatRejoin(t *testing.T) {
	heartbeatEvery = 20 * time.Millisecond
	defer func() { heartbeatEvery = 2 * time.Second }()
	b, _, c := startBroker(t, "127.0.0.1:19106")
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	go func() {
		time.Sleep(80 * time.Millisecond)
		_, _ = b.Join(context.Background(), &mq.JoinRequest{Group: "collectors", MemberID: "other"})
	}()
	_ = runSession(ctx, c, store.NewMemory(), "collectors", "c6")
}

func TestWaitPostgresRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if _, err := waitPostgres(ctx, "postgres://gpu:gpu@127.0.0.1:1/gpu?sslmode=disable"); err == nil {
		t.Fatal("expected timeout")
	}
}

func TestRunSessionFetchCancel(t *testing.T) {
	_, cancelBroker, c := startBroker(t, "127.0.0.1:19108")
	go func() {
		time.Sleep(80 * time.Millisecond)
		cancelBroker()
		_ = c.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = runSession(ctx, c, store.NewMemory(), "collectors", "c7")
}

type fenceOnInsert struct {
	store.Store
	b *mq.Broker
}

func (f fenceOnInsert) UpsertAndInsert(ctx context.Context, pt model.Point) error {
	_, _ = f.b.Join(ctx, &mq.JoinRequest{Group: "collectors", MemberID: "fencer"})
	return f.Store.UpsertAndInsert(ctx, pt)
}

func TestRunSessionAckFenced(t *testing.T) {
	b, _, c := startBroker(t, "127.0.0.1:19110")
	st := store.NewMemory()
	pt := model.Point{MessageID: "ack-fence", ProcessedAt: time.Now().UTC(), UUID: "gpu-9", Hostname: "h", Device: "d", GPUID: "0", ModelName: "m", MetricName: "util", Value: 3}
	raw, _ := json.Marshal(pt)
	if _, err := c.Produce(context.Background(), &mq.ProduceRequest{Key: pt.UUID, ID: pt.MessageID, Payload: raw}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = runSession(ctx, c, fenceOnInsert{Store: st, b: b}, "collectors", "c8")
}

func TestConsumeLoopSessionError(t *testing.T) {
	_, cancelBroker, c := startBroker(t, "127.0.0.1:19111")
	cancelBroker()
	_ = c.Close()
	sessionRetry = 5 * time.Millisecond
	defer func() { sessionRetry = time.Second }()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := consumeLoop(ctx, c, store.NewMemory(), "collectors", "c9"); err != nil {
		t.Fatal(err)
	}
}

func TestRunMemberID(t *testing.T) {
	openStore = func(ctx context.Context, url string) (store.Store, error) {
		return store.NewMemory(), nil
	}
	defer func() { openStore = func(ctx context.Context, url string) (store.Store, error) { return store.OpenPostgres(ctx, url) } }()
	t.Setenv("BROKER_ADDR", "127.0.0.1:1")
	t.Setenv("MEMBER_ID", "explicit-member")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := run(ctx); err == nil {
		t.Fatal("dial")
	}
}

func TestRunSessionJoinFail(t *testing.T) {
	_, cancelBroker, c := startBroker(t, "127.0.0.1:19104")
	cancelBroker()
	time.Sleep(50 * time.Millisecond)
	err := runSession(context.Background(), c, store.NewMemory(), "g", "m")
	if err == nil {
		t.Fatal("expected join error")
	}
}
