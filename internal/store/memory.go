package store

import (
	"context"
	"sort"
	"sync"
	"time"

	"gpu-telemetry-pipeline/internal/model"
)

// Memory is an in-process Store used in unit tests.
type Memory struct {
	mu      sync.RWMutex
	gpus    map[string]model.GPU
	samples []model.Sample
	ids     map[string]struct{}
}

// NewMemory constructs an empty memory store.
func NewMemory() *Memory {
	return &Memory{
		gpus: make(map[string]model.GPU),
		ids:  make(map[string]struct{}),
	}
}

func (m *Memory) Init(ctx context.Context) error { return nil }

func (m *Memory) Close() error { return nil }

func (m *Memory) UpsertAndInsert(ctx context.Context, p model.Point) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.ids[p.MessageID]; ok {
		return ErrDuplicate
	}
	g, ok := m.gpus[p.UUID]
	if !ok {
		g = model.GPU{
			UUID: p.UUID, Hostname: p.Hostname, Device: p.Device,
			GPUID: p.GPUID, ModelName: p.ModelName,
			FirstSeen: p.ProcessedAt, LastSeen: p.ProcessedAt,
		}
	} else {
		g.Hostname = p.Hostname
		g.Device = p.Device
		g.GPUID = p.GPUID
		g.ModelName = p.ModelName
		g.LastSeen = p.ProcessedAt
	}
	m.gpus[p.UUID] = g
	m.ids[p.MessageID] = struct{}{}
	m.samples = append(m.samples, p.Sample())
	return nil
}

func (m *Memory) ListGPUs(ctx context.Context) ([]model.GPU, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]model.GPU, 0, len(m.gpus))
	for _, g := range m.gpus {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UUID < out[j].UUID })
	return out, nil
}

func (m *Memory) QueryTelemetry(ctx context.Context, gpuUUID string, start, end *time.Time) ([]model.Sample, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []model.Sample
	for _, s := range m.samples {
		if s.UUID != gpuUUID {
			continue
		}
		if start != nil && s.ProcessedAt.Before(*start) {
			continue
		}
		if end != nil && s.ProcessedAt.After(*end) {
			continue
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ProcessedAt.Before(out[j].ProcessedAt) })
	return out, nil
}
