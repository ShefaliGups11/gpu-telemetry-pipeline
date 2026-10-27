package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gpu-telemetry-pipeline/internal/model"
	"gpu-telemetry-pipeline/internal/store"
)

type errStore struct {
	inner     store.Store
	failList  bool
	failQuery bool
}

func (e *errStore) Init(ctx context.Context) error { return e.inner.Init(ctx) }
func (e *errStore) Close() error                   { return e.inner.Close() }
func (e *errStore) UpsertAndInsert(ctx context.Context, p model.Point) error {
	return e.inner.UpsertAndInsert(ctx, p)
}
func (e *errStore) ListGPUs(ctx context.Context) ([]model.GPU, error) {
	if e.failList {
		return nil, errors.New("list boom")
	}
	return e.inner.ListGPUs(ctx)
}
func (e *errStore) QueryTelemetry(ctx context.Context, id string, start, end *time.Time) ([]model.Sample, error) {
	if e.failQuery {
		return nil, errors.New("query boom")
	}
	return e.inner.QueryTelemetry(ctx, id, start, end)
}

func TestListAndQuery(t *testing.T) {
	st := store.NewMemory()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	p1 := model.Point{
		MessageID: "m1", ProcessedAt: ts, UUID: "gpu-1", Hostname: "h",
		Device: "nvidia0", GPUID: "0", ModelName: "H100",
		MetricName: "DCGM_FI_DEV_GPU_UTIL", Value: 10, CSVTimestamp: "2025-07-18T20:42:34Z",
		Container: "c", Pod: "p", Namespace: "ns", LabelsRaw: "x",
	}
	p2 := p1
	p2.MessageID = "m2"
	p2.MetricName = "DCGM_FI_DEV_GPU_TEMP"
	p2.Value = 71
	p2.ProcessedAt = ts.Add(time.Second)
	_ = st.UpsertAndInsert(nil, p1)
	_ = st.UpsertAndInsert(nil, p2)
	h := New(st)

	rr := httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code != 200 {
		t.Fatalf("list status %d %s", w.Code, w.Body.String())
	}
	var gpus []model.GPU
	if err := json.Unmarshal(w.Body.Bytes(), &gpus); err != nil || len(gpus) != 1 {
		t.Fatalf("gpus %s err %v", w.Body.String(), err)
	}

	rr = httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus/gpu-1/telemetry?start_time=2026-05-01T00:00:00Z&end_time=2026-05-01T23:00:00Z", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code != 200 {
		t.Fatalf("query status %d %s", w.Code, w.Body.String())
	}
	var samples []model.Sample
	if err := json.Unmarshal(w.Body.Bytes(), &samples); err != nil || len(samples) != 2 {
		t.Fatalf("samples %s", w.Body.String())
	}
	assertCSVSample(t, samples[0], p1)
	assertCSVSample(t, samples[1], p2)

	rr = httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus/missing/telemetry", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code != 404 {
		t.Fatalf("expected 404, got %d", w.Code)
	}

	rr = httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus/gpu-1/telemetry?start_time=nope", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code != 400 {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	rr = httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus/gpu-1/telemetry?end_time=nope", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code != 400 {
		t.Fatalf("expected 400 end, got %d", w.Code)
	}

	rr = httptest.NewRequest(http.MethodGet, "http://x/healthz", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code != 200 {
		t.Fatalf("health %d", w.Code)
	}
}

func TestOpenAPI(t *testing.T) {
	h := New(store.NewMemory())
	for _, path := range []string{"/openapi.json", "/swagger.json"} {
		rr := httptest.NewRequest(http.MethodGet, "http://x"+path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, rr)
		if w.Code != 200 {
			t.Fatal(w.Body.String())
		}
		var spec map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &spec); err != nil {
			t.Fatal(err)
		}
		if spec["openapi"] != "3.0.3" {
			t.Fatalf("missing openapi: %+v", spec["openapi"])
		}
	}
}

func TestEmptyList(t *testing.T) {
	h := New(store.NewMemory())
	rr := httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code != 200 || w.Body.String() != "[]\n" {
		t.Fatalf("empty list %d %q", w.Code, w.Body.String())
	}
}

func TestStoreErrors(t *testing.T) {
	e := &errStore{inner: store.NewMemory(), failList: true}
	h := New(e)
	rr := httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code != 500 {
		t.Fatalf("list err %d", w.Code)
	}
	rr = httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus/x/telemetry", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code != 500 {
		t.Fatalf("query list err %d", w.Code)
	}

	st := store.NewMemory()
	_ = st.UpsertAndInsert(nil, model.Point{MessageID: "m", ProcessedAt: time.Now(), UUID: "g"})
	e2 := &errStore{inner: st, failQuery: true}
	h = New(e2)
	rr = httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus/g/telemetry", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code != 500 {
		t.Fatalf("query err %d", w.Code)
	}
}

func TestMissingGPUID(t *testing.T) {
	h := New(store.NewMemory())
	rr := httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus//telemetry", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code == 200 {
		t.Fatal("expected error")
	}
	s := &Server{store: store.NewMemory()}
	w = httptest.NewRecorder()
	s.gpuTelemetry(w, httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus/telemetry", nil))
	if w.Code != 400 {
		t.Fatalf("direct missing id %d", w.Code)
	}
}

type nilStore struct{ store.Store }

func (n nilStore) ListGPUs(ctx context.Context) ([]model.GPU, error) { return nil, nil }
func (n nilStore) QueryTelemetry(ctx context.Context, id string, start, end *time.Time) ([]model.Sample, error) {
	return nil, nil
}

func TestNilSlices(t *testing.T) {
	st := store.NewMemory()
	_ = st.UpsertAndInsert(nil, model.Point{MessageID: "m", ProcessedAt: time.Now(), UUID: "g"})
	h := New(nilStore{Store: st})
	rr := httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
}

type nilQueryStore struct{ store.Store }

func (n nilQueryStore) QueryTelemetry(ctx context.Context, id string, start, end *time.Time) ([]model.Sample, error) {
	return nil, nil
}

func TestNilQuerySamples(t *testing.T) {
	st := store.NewMemory()
	_ = st.UpsertAndInsert(nil, model.Point{MessageID: "m", ProcessedAt: time.Now(), UUID: "g"})
	h := New(nilQueryStore{Store: st})
	rr := httptest.NewRequest(http.MethodGet, "http://x/api/v1/gpus/g/telemetry", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, rr)
	if w.Code != 200 || w.Body.String() != "[]\n" {
		t.Fatalf("%d %q", w.Code, w.Body.String())
	}
}

func assertCSVSample(t *testing.T, got model.Sample, p model.Point) {
	t.Helper()
	want := p.Sample()
	if got.MessageID != want.MessageID || got.Timestamp != want.Timestamp ||
		got.MetricName != want.MetricName || got.GPUID != want.GPUID ||
		got.Device != want.Device || got.UUID != want.UUID ||
		got.ModelName != want.ModelName || got.Hostname != want.Hostname ||
		got.Container != want.Container || got.Pod != want.Pod ||
		got.Namespace != want.Namespace || got.Value != want.Value ||
		got.LabelsRaw != want.LabelsRaw || !got.ProcessedAt.Equal(want.ProcessedAt) {
		t.Fatalf("got %+v want %+v", got, want)
	}
}
