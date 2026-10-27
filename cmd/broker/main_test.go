package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFatalExit(t *testing.T) {
	var code int
	fatalExit = func(c int) { code = c }
	defer func() { fatalExit = os.Exit }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t.Setenv("DATA_DIR", t.TempDir()+"/missing-parent/x")
	t.Setenv("GRPC_ADDR", "256.256.256.256:9")
	t.Setenv("HTTP_ADDR", "256.256.256.256:9")
	newRootCtx = func() (context.Context, context.CancelFunc) { return ctx, func() {} }
	defer func() {
		fatalExit = os.Exit
		newRootCtx = func() (context.Context, context.CancelFunc) { return context.Background(), func() {} }
	}()
	main()
	if code != 1 {
		t.Fatal(code)
	}
}

func TestRunHTTPBadAddr(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("GRPC_ADDR", "127.0.0.1:0")
	t.Setenv("HTTP_ADDR", "256.256.256.256:9")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = run(ctx)
}

func TestMainOK(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DATA_DIR", dir)
	t.Setenv("GRPC_ADDR", "127.0.0.1:0")
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

func TestRunWithPeers(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("GRPC_ADDR", "127.0.0.1:0")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("HOSTNAME", "gpu-telemetry-broker-0")
	t.Setenv("BROKER_PEERS", "gpu-telemetry-broker-0.gpu-telemetry-broker:9090,127.0.0.1:1")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = run(ctx)
}

func TestRun(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DATA_DIR", dir)
	t.Setenv("GRPC_ADDR", "127.0.0.1:0")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("PARTITIONS", "2")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_ = run(ctx)
}

func TestRunListenError(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("GRPC_ADDR", "256.256.256.256:9")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	if err := run(context.Background()); err == nil {
		t.Fatal("expected listen error")
	}
}

func TestRunOpenError(t *testing.T) {
	p := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATA_DIR", p)
	t.Setenv("GRPC_ADDR", "127.0.0.1:0")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	if err := run(context.Background()); err == nil {
		t.Fatal("expected open error")
	}
}
