package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"gpu-telemetry-pipeline/internal/model"
)

func TestOpenPostgresErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := OpenPostgres(ctx, "://not-a-url"); err == nil {
		t.Fatal("parse")
	}
	if _, err := OpenPostgres(ctx, "postgres://gpu:gpu@127.0.0.1:1/gpu?sslmode=disable"); err == nil {
		t.Fatal("ping")
	}
}

func TestPoolAdapter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	cfg, err := pgxpool.ParseConfig("postgres://gpu:gpu@127.0.0.1:1/gpu?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	a := poolAdapter{pool}
	_, _ = a.Exec(ctx, "select 1")
	if rows, qerr := a.Query(ctx, "select 1"); qerr == nil && rows != nil {
		rows.Close()
	}
	if tx, berr := a.Begin(ctx); berr == nil && tx != nil {
		_ = tx.Rollback(ctx)
	}
	_ = a.Ping(ctx)
	a.Close()
}

func TestPostgresLive(t *testing.T) {
	url := "postgres://gpu:gpu@127.0.0.1:15432/gpu?sslmode=disable"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := OpenPostgres(ctx, url)
	if err != nil {
		t.Skip("postgres not running on 15432:", err)
	}
	defer p.Close()
	if err := p.Init(ctx); err != nil {
		t.Fatal(err)
	}
	pt := model.Point{
		MessageID:   "cov-1-" + time.Now().Format("150405.000"),
		ProcessedAt: time.Now().UTC(), UUID: "gpu-cov",
		Hostname: "h", Device: "d", GPUID: "0", ModelName: "m", MetricName: "util", Value: 1,
		CSVTimestamp: "2025-07-18T20:42:34Z", LabelsRaw: "x",
	}
	if err := p.UpsertAndInsert(ctx, pt); err != nil {
		t.Fatal(err)
	}
	if err := p.UpsertAndInsert(ctx, pt); err != ErrDuplicate {
		t.Fatalf("dup %v", err)
	}
	pt2 := pt
	pt2.MessageID = pt.MessageID + "-b"
	pt2.Hostname = "h2"
	if err := p.UpsertAndInsert(ctx, pt2); err != nil {
		t.Fatal(err)
	}
	gpus, err := p.ListGPUs(ctx)
	if err != nil || len(gpus) == 0 {
		t.Fatal(gpus, err)
	}
	start := pt.ProcessedAt.Add(-time.Second)
	end := pt.ProcessedAt.Add(time.Second)
	samples, err := p.QueryTelemetry(ctx, pt.UUID, &start, &end)
	if err != nil || len(samples) == 0 {
		t.Fatal(samples, err)
	}
	found := false
	for _, s := range samples {
		if s.MessageID == pt.MessageID {
			found = true
			if s.Timestamp != pt.CSVTimestamp || s.LabelsRaw != pt.LabelsRaw ||
				s.Hostname != pt.Hostname || s.Device != pt.Device || s.GPUID != pt.GPUID {
				t.Fatalf("csv fields %+v", s)
			}
		}
	}
	if !found {
		t.Fatalf("missing inserted row in %+v", samples)
	}
	if _, err := p.QueryTelemetry(ctx, pt.UUID, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := p.QueryTelemetry(ctx, "missing", &start, nil); err != nil {
		t.Fatal(err)
	}
}
