package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gpu-telemetry-pipeline/internal/ingest"
	"gpu-telemetry-pipeline/internal/mq"
)

func TestMainOK(t *testing.T) {
	dir := t.TempDir()
	b, err := mq.Open(mq.Options{DataDir: dir, Partitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	go func() { _ = mq.ServeGRPC(ctx, "127.0.0.1:19095", b) }()
	time.Sleep(40 * time.Millisecond)
	t.Setenv("CSV_PATH", filepath.Join("..", "..", "testdata", "dcgm_metrics.csv"))
	t.Setenv("BROKER_ADDR", "127.0.0.1:19095")
	t.Setenv("STREAM_INTERVAL", "10ms")
	fatalExit = func(int) { t.Fatal("exit") }
	defer func() { fatalExit = os.Exit }()
	newRootCtx = func() (context.Context, context.CancelFunc) { return ctx, cancel }
	defer func() {
		newRootCtx = func() (context.Context, context.CancelFunc) { return context.Background(), func() {} }
	}()
	main()
}

func TestMainMissingCSV(t *testing.T) {
	var code int
	fatalExit = func(c int) { code = c }
	defer func() { fatalExit = os.Exit }()
	t.Setenv("CSV_PATH", filepath.Join(t.TempDir(), "no.csv"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	newRootCtx = func() (context.Context, context.CancelFunc) { return ctx, func() {} }
	defer func() { newRootCtx = func() (context.Context, context.CancelFunc) { return context.Background(), func() {} } }()
	main()
	if code != 1 {
		t.Fatal(code)
	}
}

func TestRunMissingCSV(t *testing.T) {
	t.Setenv("CSV_PATH", filepath.Join(t.TempDir(), "no.csv"))
	if err := run(context.Background()); err == nil {
		t.Fatal("expected csv error")
	}
}

func TestRunDialFail(t *testing.T) {
	p := filepath.Join("..", "..", "testdata", "dcgm_metrics.csv")
	t.Setenv("CSV_PATH", p)
	t.Setenv("BROKER_ADDR", "127.0.0.1:1")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := run(ctx); err == nil {
		t.Fatal("expected dial error")
	}
}

func TestStreamProduceError(t *testing.T) {
	dir := t.TempDir()
	b, err := mq.Open(mq.Options{DataDir: dir, Partitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = mq.ServeGRPC(ctx, "127.0.0.1:19094", b) }()
	time.Sleep(40 * time.Millisecond)
	c, err := mq.Dial(ctx, "127.0.0.1:19094")
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	cancel()
	_ = b.Close()
	rows := []ingest.Row{{UUID: "gpu-a", MetricName: "u", Value: 1}}
	sctx, scancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer scancel()
	_ = stream(sctx, c, rows, 15*time.Millisecond, 0, 1)
}

func TestStreamLoop(t *testing.T) {
	dir := t.TempDir()
	b, err := mq.Open(mq.Options{DataDir: dir, Partitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = mq.ServeGRPC(ctx, "127.0.0.1:19091", b) }()
	time.Sleep(40 * time.Millisecond)
	c, err := mq.Dial(ctx, "127.0.0.1:19091")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	rows := []ingest.Row{{UUID: "gpu-a", Hostname: "h", Device: "d", GPUID: "0", ModelName: "m", MetricName: "util", Value: 1}}
	sctx, scancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer scancel()
	if err := stream(sctx, c, rows, 20*time.Millisecond, 0, 1); err != nil {
		t.Fatal(err)
	}
}

func TestRunEndToEnd(t *testing.T) {
	dir := t.TempDir()
	b, err := mq.Open(mq.Options{DataDir: dir, Partitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = mq.ServeGRPC(ctx, "127.0.0.1:19093", b) }()
	time.Sleep(40 * time.Millisecond)
	t.Setenv("CSV_PATH", filepath.Join("..", "..", "testdata", "dcgm_metrics.csv"))
	t.Setenv("BROKER_ADDR", "127.0.0.1:19093")
	t.Setenv("STREAM_INTERVAL", "10ms")
	t.Setenv("STREAMER_COUNT", "1")
	sctx, scancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer scancel()
	if err := run(sctx); err != nil {
		t.Fatal(err)
	}
}

func TestStreamMarshalError(t *testing.T) {
	orig := marshalJSON
	marshalJSON = func(any) ([]byte, error) { return nil, errors.New("marshal") }
	defer func() { marshalJSON = orig }()
	rows := []ingest.Row{
		{UUID: "gpu-a", MetricName: "u", Value: 1},
		{UUID: "gpu-b", MetricName: "u", Value: 2},
	}
	sctx, scancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer scancel()
	_ = stream(sctx, &mq.Client{}, rows, 10*time.Millisecond, 0, 1)
}

func TestStreamSkipShard(t *testing.T) {
	dir := t.TempDir()
	b, err := mq.Open(mq.Options{DataDir: dir, Partitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = mq.ServeGRPC(ctx, "127.0.0.1:19092", b) }()
	time.Sleep(40 * time.Millisecond)
	c, err := mq.Dial(ctx, "127.0.0.1:19092")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	rows := []ingest.Row{
		{UUID: "gpu-a", MetricName: "u", Value: 1},
		{UUID: "gpu-b", MetricName: "u", Value: 1},
	}
	sctx, scancel := context.WithTimeout(ctx, 120*time.Millisecond)
	defer scancel()
	_ = stream(sctx, c, rows, 15*time.Millisecond, 0, 2)
}
