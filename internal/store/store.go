package store

import (
	"context"
	"errors"
	"time"

	"gpu-telemetry-pipeline/internal/model"
)

// ErrDuplicate is returned when message_id already exists.
var ErrDuplicate = errors.New("duplicate message_id")

// Store persists GPUs and telemetry samples.
type Store interface {
	Init(ctx context.Context) error
	UpsertAndInsert(ctx context.Context, p model.Point) error
	ListGPUs(ctx context.Context) ([]model.GPU, error)
	QueryTelemetry(ctx context.Context, gpuUUID string, start, end *time.Time) ([]model.Sample, error)
	Close() error
}
