package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"gpu-telemetry-pipeline/internal/model"
)

const schema = `
CREATE TABLE IF NOT EXISTS gpus (
  uuid TEXT PRIMARY KEY,
  hostname TEXT NOT NULL,
  device TEXT NOT NULL,
  gpu_id TEXT NOT NULL,
  model_name TEXT NOT NULL,
  first_seen TIMESTAMPTZ NOT NULL,
  last_seen TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS telemetry (
  id BIGSERIAL PRIMARY KEY,
  gpu_uuid TEXT NOT NULL REFERENCES gpus(uuid),
  metric_name TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  message_id TEXT NOT NULL UNIQUE,
  csv_timestamp TEXT NOT NULL DEFAULT '',
  hostname TEXT NOT NULL DEFAULT '',
  device TEXT NOT NULL DEFAULT '',
  gpu_id TEXT NOT NULL DEFAULT '',
  model_name TEXT NOT NULL DEFAULT '',
  container TEXT NOT NULL DEFAULT '',
  pod TEXT NOT NULL DEFAULT '',
  namespace TEXT NOT NULL DEFAULT '',
  labels_raw TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS telemetry_gpu_ts ON telemetry (gpu_uuid, ts);
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS csv_timestamp TEXT NOT NULL DEFAULT '';
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS hostname TEXT NOT NULL DEFAULT '';
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS device TEXT NOT NULL DEFAULT '';
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS gpu_id TEXT NOT NULL DEFAULT '';
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS model_name TEXT NOT NULL DEFAULT '';
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS container TEXT NOT NULL DEFAULT '';
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS pod TEXT NOT NULL DEFAULT '';
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS namespace TEXT NOT NULL DEFAULT '';
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS labels_raw TEXT NOT NULL DEFAULT '';
`

type pgTx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type pgPool interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Begin(ctx context.Context) (pgTx, error)
	Ping(ctx context.Context) error
	Close()
}

// Postgres implements Store with pgx.
type Postgres struct {
	pool pgPool
}

// NewPostgres wraps an existing pool (tests).
func NewPostgres(pool pgPool) *Postgres {
	return &Postgres{pool: pool}
}

// OpenPostgres connects and pings.
func OpenPostgres(ctx context.Context, url string) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	a := poolAdapter{pool}
	if err := a.Ping(ctx); err != nil {
		a.Close()
		return nil, err
	}
	return NewPostgres(a), nil
}

type poolAdapter struct{ p *pgxpool.Pool }

func (a poolAdapter) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return a.p.Exec(ctx, sql, arguments...)
}
func (a poolAdapter) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return a.p.Query(ctx, sql, args...)
}
func (a poolAdapter) Begin(ctx context.Context) (pgTx, error) {
	return a.p.Begin(ctx)
}
func (a poolAdapter) Ping(ctx context.Context) error { return a.p.Ping(ctx) }
func (a poolAdapter) Close()                         { a.p.Close() }

func (p *Postgres) Init(ctx context.Context) error {
	_, err := p.pool.Exec(ctx, schema)
	return err
}

func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}

func (p *Postgres) UpsertAndInsert(ctx context.Context, pt model.Point) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `
INSERT INTO gpus (uuid, hostname, device, gpu_id, model_name, first_seen, last_seen)
VALUES ($1,$2,$3,$4,$5,$6,$6)
ON CONFLICT (uuid) DO UPDATE SET
  hostname = EXCLUDED.hostname,
  device = EXCLUDED.device,
  gpu_id = EXCLUDED.gpu_id,
  model_name = EXCLUDED.model_name,
  last_seen = EXCLUDED.last_seen
`, pt.UUID, pt.Hostname, pt.Device, pt.GPUID, pt.ModelName, pt.ProcessedAt)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
INSERT INTO telemetry (gpu_uuid, metric_name, value, ts, message_id, csv_timestamp, hostname, device, gpu_id, model_name, container, pod, namespace, labels_raw)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
`, pt.UUID, pt.MetricName, pt.Value, pt.ProcessedAt, pt.MessageID, pt.CSVTimestamp, pt.Hostname, pt.Device, pt.GPUID, pt.ModelName, pt.Container, pt.Pod, pt.Namespace, pt.LabelsRaw)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrDuplicate
		}
		return err
	}
	return tx.Commit(ctx)
}

func (p *Postgres) ListGPUs(ctx context.Context) ([]model.GPU, error) {
	rows, err := p.pool.Query(ctx, `
SELECT uuid, hostname, device, gpu_id, model_name, first_seen, last_seen
FROM gpus ORDER BY uuid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.GPU
	for rows.Next() {
		var g model.GPU
		if err := rows.Scan(&g.UUID, &g.Hostname, &g.Device, &g.GPUID, &g.ModelName, &g.FirstSeen, &g.LastSeen); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func (p *Postgres) QueryTelemetry(ctx context.Context, gpuUUID string, start, end *time.Time) ([]model.Sample, error) {
	q := `SELECT message_id, gpu_uuid, metric_name, value, ts, csv_timestamp, hostname, device, gpu_id, model_name, container, pod, namespace, labels_raw FROM telemetry WHERE gpu_uuid = $1`
	args := []any{gpuUUID}
	n := 2
	if start != nil {
		q += fmt.Sprintf(" AND ts >= $%d", n)
		args = append(args, *start)
		n++
	}
	if end != nil {
		q += fmt.Sprintf(" AND ts <= $%d", n)
		args = append(args, *end)
	}
	q += " ORDER BY ts ASC"
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Sample
	for rows.Next() {
		var s model.Sample
		if err := rows.Scan(&s.MessageID, &s.UUID, &s.MetricName, &s.Value, &s.ProcessedAt, &s.Timestamp, &s.Hostname, &s.Device, &s.GPUID, &s.ModelName, &s.Container, &s.Pod, &s.Namespace, &s.LabelsRaw); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if errors.Is(rows.Err(), pgx.ErrNoRows) {
		return out, nil
	}
	return out, rows.Err()
}
