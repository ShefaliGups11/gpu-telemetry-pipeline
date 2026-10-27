package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"gpu-telemetry-pipeline/internal/model"
)

type stubTx struct {
	n         int
	gpuErr    error
	telErr    error
	commitErr error
}

func (t *stubTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	t.n++
	if t.n == 1 && t.gpuErr != nil {
		return pgconn.CommandTag{}, t.gpuErr
	}
	if t.n >= 2 && t.telErr != nil {
		return pgconn.CommandTag{}, t.telErr
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}
func (t *stubTx) Commit(ctx context.Context) error   { return t.commitErr }
func (t *stubTx) Rollback(ctx context.Context) error { return nil }

type stubRows struct {
	data    [][]any
	i       int
	err     error
	scanErr error
}

func (r *stubRows) Close()                                       {}
func (r *stubRows) Err() error                                   { return r.err }
func (r *stubRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *stubRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *stubRows) Next() bool {
	if r.i >= len(r.data) {
		return false
	}
	r.i++
	return true
}
func (r *stubRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.data[r.i-1]
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = row[i].(string)
		case *float64:
			*p = row[i].(float64)
		case *time.Time:
			*p = row[i].(time.Time)
		}
	}
	return nil
}
func (r *stubRows) Values() ([]any, error) { return nil, nil }
func (r *stubRows) RawValues() [][]byte    { return nil }
func (r *stubRows) Conn() *pgx.Conn        { return nil }

type stubPool struct {
	execErr  error
	beginErr error
	queryErr error
	tx       *stubTx
	gpuRows  pgx.Rows
	telRows  pgx.Rows
	closed   bool
}

func (p *stubPool) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, p.execErr
}
func (p *stubPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	if strings.Contains(sql, "FROM gpus") {
		return p.gpuRows, nil
	}
	return p.telRows, nil
}
func (p *stubPool) Begin(ctx context.Context) (pgTx, error) {
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	if p.tx == nil {
		p.tx = &stubTx{}
	}
	return p.tx, nil
}
func (p *stubPool) Ping(ctx context.Context) error { return nil }
func (p *stubPool) Close()                         { p.closed = true }

func testPoint() model.Point {
	return model.Point{
		MessageID: "m1", ProcessedAt: time.Unix(1, 0).UTC(), UUID: "gpu-1",
		Hostname: "h", Device: "d", GPUID: "0", ModelName: "m", MetricName: "util", Value: 1,
		CSVTimestamp: "2025-07-18T20:42:34Z", Container: "", Pod: "", Namespace: "", LabelsRaw: "x",
	}
}

func TestPostgresMockHappy(t *testing.T) {
	ts := time.Unix(1, 0).UTC()
	pool := &stubPool{
		gpuRows: &stubRows{data: [][]any{{"gpu-1", "h", "d", "0", "m", ts, ts}}},
		telRows: &stubRows{data: [][]any{{"m1", "gpu-1", "util", 1.0, ts, "2025-07-18T20:42:34Z", "h", "d", "0", "m", "", "", "", "x"}}},
	}
	p := NewPostgres(pool)
	ctx := context.Background()
	if err := p.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.UpsertAndInsert(ctx, testPoint()); err != nil {
		t.Fatal(err)
	}
	gpus, err := p.ListGPUs(ctx)
	if err != nil || len(gpus) != 1 {
		t.Fatal(gpus, err)
	}
	start, end := ts.Add(-time.Second), ts.Add(time.Second)
	samples, err := p.QueryTelemetry(ctx, "gpu-1", &start, &end)
	if err != nil || len(samples) != 1 {
		t.Fatal(samples, err)
	}
	if samples[0] != testPoint().Sample() {
		t.Fatalf("csv fields %+v", samples[0])
	}
	if _, err := p.QueryTelemetry(ctx, "gpu-1", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := p.QueryTelemetry(ctx, "gpu-1", &start, nil); err != nil {
		t.Fatal(err)
	}
	p2 := NewPostgres(&stubPool{telRows: &stubRows{data: [][]any{{"m1", "gpu-1", "util", 1.0, ts, "", "h", "d", "0", "m", "", "", "", ""}}}})
	if _, err := p2.QueryTelemetry(ctx, "gpu-1", nil, &end); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil || !pool.closed {
		t.Fatal(err, pool.closed)
	}
}

func TestPostgresMockErrors(t *testing.T) {
	ctx := context.Background()
	p := NewPostgres(&stubPool{execErr: errors.New("schema")})
	if err := p.Init(ctx); err == nil {
		t.Fatal("init")
	}
	p = NewPostgres(&stubPool{beginErr: errors.New("begin")})
	if err := p.UpsertAndInsert(ctx, testPoint()); err == nil {
		t.Fatal("begin")
	}
	p = NewPostgres(&stubPool{tx: &stubTx{gpuErr: errors.New("gpu")}})
	if err := p.UpsertAndInsert(ctx, testPoint()); err == nil {
		t.Fatal("gpu")
	}
	p = NewPostgres(&stubPool{tx: &stubTx{telErr: errors.New("tel")}})
	if err := p.UpsertAndInsert(ctx, testPoint()); err == nil {
		t.Fatal("tel")
	}
	p = NewPostgres(&stubPool{tx: &stubTx{telErr: &pgconn.PgError{Code: "23505"}}})
	if err := p.UpsertAndInsert(ctx, testPoint()); err != ErrDuplicate {
		t.Fatalf("dup %v", err)
	}
	p = NewPostgres(&stubPool{tx: &stubTx{commitErr: errors.New("commit")}})
	if err := p.UpsertAndInsert(ctx, testPoint()); err == nil {
		t.Fatal("commit")
	}
	p = NewPostgres(&stubPool{queryErr: errors.New("q")})
	if _, err := p.ListGPUs(ctx); err == nil {
		t.Fatal("list q")
	}
	if _, err := p.QueryTelemetry(ctx, "x", nil, nil); err == nil {
		t.Fatal("tel q")
	}
	p = NewPostgres(&stubPool{gpuRows: &stubRows{data: [][]any{{"a"}}, scanErr: errors.New("scan")}})
	if _, err := p.ListGPUs(ctx); err == nil {
		t.Fatal("list scan")
	}
	p = NewPostgres(&stubPool{telRows: &stubRows{data: [][]any{{"a"}}, scanErr: errors.New("scan")}})
	if _, err := p.QueryTelemetry(ctx, "x", nil, nil); err == nil {
		t.Fatal("tel scan")
	}
	p = NewPostgres(&stubPool{telRows: &stubRows{err: pgx.ErrNoRows}})
	_, err := p.QueryTelemetry(ctx, "x", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	p = NewPostgres(&stubPool{telRows: &stubRows{err: errors.New("rowerr")}})
	if _, err := p.QueryTelemetry(ctx, "x", nil, nil); err == nil {
		t.Fatal("rowerr")
	}
	p = NewPostgres(&stubPool{gpuRows: &stubRows{err: errors.New("gerr")}})
	if _, err := p.ListGPUs(ctx); err == nil {
		t.Fatal("gerr")
	}
}
