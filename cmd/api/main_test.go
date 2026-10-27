package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"gpu-telemetry-pipeline/internal/store"
)

type initFailStore struct{ store.Store }

func (initFailStore) Init(ctx context.Context) error { return context.Canceled }

func TestBootHandlerHealthBeforeReady(t *testing.T) {
	b := &bootHandler{}
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "http://x/healthz", nil)
	b.ServeHTTP(w, req)
	if w.Code != 200 || w.Body.String() != "ok" {
		t.Fatalf("%d %q", w.Code, w.Body.String())
	}
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodGet, "http://x/api/v1/gpus", nil)
	b.ServeHTTP(w2, req2)
	if w2.Code != 503 {
		t.Fatalf("gpus before ready %d", w2.Code)
	}
}

func TestRootCtx(t *testing.T) {
	ctx, stop := rootCtx()
	defer stop()
	if ctx == nil {
		t.Fatal("nil ctx")
	}
}

func TestMainErrorExit(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://gpu:gpu@127.0.0.1:1/gpu?sslmode=disable")
	var code int
	fatalExit = func(c int) { code = c }
	defer func() { fatalExit = os.Exit }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	newRootCtx = func() (context.Context, context.CancelFunc) { return ctx, func() {} }
	defer func() {
		newRootCtx = func() (context.Context, context.CancelFunc) { return context.Background(), func() {} }
	}()
	main()
	if code != 1 {
		t.Fatalf("code %d", code)
	}
}

func TestServeListenError(t *testing.T) {
	err := serve(context.Background(), "256.256.256.256:9", store.NewMemory())
	if err == nil {
		t.Fatal("expected listen error")
	}
}

func TestWaitStoreCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := waitStore(ctx, "postgres://gpu:gpu@127.0.0.1:1/gpu?sslmode=disable"); err == nil {
		t.Fatal("expected cancel")
	}
}

func TestWaitStoreRetryThenCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	if _, err := waitStore(ctx, "postgres://gpu:gpu@127.0.0.1:1/gpu?sslmode=disable"); err == nil {
		t.Fatal("expected timeout")
	}
}

func TestServe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	st := store.NewMemory()
	errCh := make(chan error, 1)
	go func() { errCh <- serve(ctx, "127.0.0.1:0", st) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestServeHealth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := store.NewMemory()
	lnAddr := "127.0.0.1:18081"
	errCh := make(chan error, 1)
	go func() { errCh <- serve(ctx, lnAddr, st) }()
	time.Sleep(80 * time.Millisecond)
	resp, err := http.Get("http://" + lnAddr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(b) != "ok" {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	cancel()
	<-errCh
}

func TestRunCanceled(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://gpu:gpu@127.0.0.1:1/gpu?sslmode=disable")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx); err == nil {
		t.Fatal("expected error")
	}
}

func TestMainOK(t *testing.T) {
	openStore = func(ctx context.Context, url string) (store.Store, error) {
		return store.NewMemory(), nil
	}
	defer func() {
		openStore = func(ctx context.Context, url string) (store.Store, error) { return store.OpenPostgres(ctx, url) }
	}()
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
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

func TestRunWithMemoryStore(t *testing.T) {
	openStore = func(ctx context.Context, url string) (store.Store, error) {
		return store.NewMemory(), nil
	}
	defer func() {
		openStore = func(ctx context.Context, url string) (store.Store, error) { return store.OpenPostgres(ctx, url) }
	}()
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunInitError(t *testing.T) {
	openStore = func(ctx context.Context, url string) (store.Store, error) {
		return initFailStore{Store: store.NewMemory()}, nil
	}
	defer func() {
		openStore = func(ctx context.Context, url string) (store.Store, error) { return store.OpenPostgres(ctx, url) }
	}()
	if err := run(context.Background()); err == nil {
		t.Fatal("expected init error")
	}
}

func TestWaitStoreSuccessAfterRetry(t *testing.T) {
	n := 0
	openStore = func(ctx context.Context, url string) (store.Store, error) {
		n++
		if n == 1 {
			return nil, context.DeadlineExceeded
		}
		return store.NewMemory(), nil
	}
	defer func() {
		openStore = func(ctx context.Context, url string) (store.Store, error) { return store.OpenPostgres(ctx, url) }
	}()
	st, err := waitStore(context.Background(), "ignored")
	if err != nil || st == nil {
		t.Fatal(err)
	}
}
