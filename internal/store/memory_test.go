package store

import (
	"context"
	"testing"
	"time"

	"gpu-telemetry-pipeline/internal/model"
)

func TestMemoryInsertAndQuery(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	p := model.Point{
		MessageID: "m1", ProcessedAt: ts, UUID: "gpu-1", Hostname: "h",
		Device: "nvidia0", GPUID: "0", ModelName: "H100",
		MetricName: "DCGM_FI_DEV_GPU_UTIL", Value: 90,
		CSVTimestamp: "2025-07-18T20:42:34Z", Container: "ctr", Pod: "pod-a",
		Namespace: "ns", LabelsRaw: "labels",
	}
	if err := m.UpsertAndInsert(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := m.UpsertAndInsert(ctx, p); err != ErrDuplicate {
		t.Fatalf("expected duplicate, got %v", err)
	}
	gpus, err := m.ListGPUs(ctx)
	if err != nil || len(gpus) != 1 || gpus[0].UUID != "gpu-1" {
		t.Fatalf("gpus %+v err %v", gpus, err)
	}
	start := ts.Add(-time.Second)
	end := ts.Add(time.Second)
	samples, err := m.QueryTelemetry(ctx, "gpu-1", &start, &end)
	if err != nil || len(samples) != 1 {
		t.Fatalf("samples %+v err %v", samples, err)
	}
	if samples[0] != p.Sample() {
		t.Fatalf("csv fields got %+v want %+v", samples[0], p.Sample())
	}
	missStart := ts.Add(time.Hour)
	samples, err = m.QueryTelemetry(ctx, "gpu-1", &missStart, nil)
	if err != nil || len(samples) != 0 {
		t.Fatalf("expected empty window, got %+v", samples)
	}
	missEnd := ts.Add(-time.Hour)
	samples, err = m.QueryTelemetry(ctx, "gpu-1", nil, &missEnd)
	if err != nil || len(samples) != 0 {
		t.Fatalf("expected empty end window, got %+v", samples)
	}
	samples, err = m.QueryTelemetry(ctx, "other", nil, nil)
	if err != nil || len(samples) != 0 {
		t.Fatalf("other gpu %+v", samples)
	}
	p2 := p
	p2.MessageID = "m2"
	p2.Hostname = "h2"
	p2.ProcessedAt = ts.Add(time.Minute)
	if err := m.UpsertAndInsert(ctx, p2); err != nil {
		t.Fatal(err)
	}
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}
