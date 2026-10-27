package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"gpu-telemetry-pipeline/internal/api"
	"gpu-telemetry-pipeline/internal/ingest"
	"gpu-telemetry-pipeline/internal/model"
	"gpu-telemetry-pipeline/internal/mq"
	"gpu-telemetry-pipeline/internal/obs"
	"gpu-telemetry-pipeline/internal/store"
)

func startBroker(t *testing.T, addr string) *mq.Broker {
	t.Helper()
	b, err := mq.Open(mq.Options{DataDir: t.TempDir(), Partitions: 4, Heartbeat: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = b.Close()
	})
	go func() { _ = mq.ServeGRPC(ctx, addr, b) }()
	time.Sleep(50 * time.Millisecond)
	return b
}

func dial(t *testing.T, addr string) *mq.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := mq.Dial(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func collect(ctx context.Context, client *mq.Client, st store.Store, group, member string) {
	for ctx.Err() == nil {
		if err := collectSession(ctx, client, st, group, member); err != nil && ctx.Err() == nil {
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func collectSession(ctx context.Context, client *mq.Client, st store.Store, group, member string) error {
	join, err := client.Join(ctx, &mq.JoinRequest{Group: group, MemberID: member})
	if err != nil {
		return err
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for _, p := range join.Partitions {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			from := uint64(0)
			for sctx.Err() == nil {
				fctx, fc := context.WithTimeout(sctx, 2*time.Second)
				fr, err := client.Fetch(fctx, &mq.FetchRequest{
					Group: group, MemberID: member, Generation: join.Generation,
					Partition: p, FromOffset: from, MaxRecords: 32,
				})
				fc()
				if err != nil {
					cancel()
					return
				}
				for _, rec := range fr.Records {
					var pt model.Point
					if err := json.Unmarshal(rec.Payload, &pt); err != nil {
						from = rec.Offset + 1
						continue
					}
					if pt.MessageID == "" {
						pt.MessageID = rec.ID
					}
					if err := st.UpsertAndInsert(sctx, pt); err != nil && !errors.Is(err, store.ErrDuplicate) {
						continue
					}
					if _, err := client.Ack(sctx, &mq.AckRequest{
						Group: group, MemberID: member, Generation: join.Generation,
						Partition: rec.Partition, Offset: rec.Offset,
					}); err != nil {
						cancel()
						return
					}
					from = rec.Offset + 1
				}
			}
		}()
	}
	wg.Wait()
	return errors.New("rejoin")
}

func publishCSV(t *testing.T, client *mq.Client, limit int) []model.Point {
	t.Helper()
	rows, err := ingest.LoadCSV(filepath.Join("..", "testdata", "dcgm_metrics.csv"))
	if err != nil {
		t.Fatal(err)
	}
	var published []model.Point
	for i, row := range rows {
		if i >= limit {
			break
		}
		if !ingest.Owns(row.UUID, 0, 1) {
			continue
		}
		pt := ingest.ToPoint(row)
		pt.MessageID = ulid.Make().String()
		pt.ProcessedAt = time.Now().UTC()
		raw, err := json.Marshal(pt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Produce(context.Background(), &mq.ProduceRequest{
			Topic: mq.DefaultTopic, Key: pt.UUID, ID: pt.MessageID, Payload: raw,
		}); err != nil {
			t.Fatal(err)
		}
		published = append(published, pt)
	}
	return published
}

func waitGPUs(t *testing.T, st store.Store, n int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		gpus, err := st.ListGPUs(context.Background())
		if err == nil && len(gpus) >= n {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	gpus, _ := st.ListGPUs(context.Background())
	t.Fatalf("timeout waiting for %d gpus, have %d", n, len(gpus))
}

func TestPipelineCSVToAPI(t *testing.T) {
	const brokerAddr = "127.0.0.1:19451"
	startBroker(t, brokerAddr)
	prod := dial(t, brokerAddr)
	cons := dial(t, brokerAddr)
	st := store.NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go collect(ctx, cons, st, "collectors", "e2e-c1")

	published := publishCSV(t, prod, 30)
	if len(published) == 0 {
		t.Fatal("no rows published")
	}
	waitGPUs(t, st, 1)

	hs := httptest.NewServer(obs.Mux(api.New(st)))
	defer hs.Close()

	resp, err := http.Get(hs.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "ok" {
		t.Fatalf("health %d %s", resp.StatusCode, body)
	}

	resp, err = http.Get(hs.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	mb, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || len(mb) == 0 {
		t.Fatal("metrics")
	}

	resp, err = http.Get(hs.URL + "/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if spec["openapi"] != "3.0.3" {
		t.Fatalf("openapi %+v", spec["openapi"])
	}

	resp, err = http.Get(hs.URL + "/api/v1/gpus")
	if err != nil {
		t.Fatal(err)
	}
	var gpus []model.GPU
	if err := json.NewDecoder(resp.Body).Decode(&gpus); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || len(gpus) == 0 {
		t.Fatalf("gpus %d %d", resp.StatusCode, len(gpus))
	}

	id := gpus[0].UUID
	start := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	end := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	resp, err = http.Get(hs.URL + "/api/v1/gpus/" + id + "/telemetry?start_time=" + start + "&end_time=" + end)
	if err != nil {
		t.Fatal(err)
	}
	var samples []model.Sample
	if err := json.NewDecoder(resp.Body).Decode(&samples); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || len(samples) == 0 {
		t.Fatalf("telemetry %d %d", resp.StatusCode, len(samples))
	}
	var want []model.Point
	for _, p := range published {
		if p.UUID == id {
			want = append(want, p)
		}
	}
	if len(samples) != len(want) {
		t.Fatalf("telemetry count %d want %d for %s", len(samples), len(want), id)
	}
	byID := map[string]model.Point{}
	for _, p := range want {
		byID[p.MessageID] = p
	}
	for _, s := range samples {
		p, ok := byID[s.MessageID]
		if !ok {
			t.Fatalf("unexpected sample %+v", s)
		}
		if s.Timestamp != p.CSVTimestamp || s.MetricName != p.MetricName || s.GPUID != p.GPUID ||
			s.Device != p.Device || s.UUID != p.UUID || s.ModelName != p.ModelName ||
			s.Hostname != p.Hostname || s.Container != p.Container || s.Pod != p.Pod ||
			s.Namespace != p.Namespace || s.Value != p.Value || s.LabelsRaw != p.LabelsRaw {
			t.Fatalf("csv mismatch got %+v want %+v", s, p.Sample())
		}
	}

	resp, err = http.Get(hs.URL + "/api/v1/gpus/missing-gpu/telemetry")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}

	dup := published[0]
	raw, _ := json.Marshal(dup)
	if _, err := prod.Produce(context.Background(), &mq.ProduceRequest{
		Key: dup.UUID, ID: dup.MessageID, Payload: raw,
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
}

func TestPipelineTwoCollectors(t *testing.T) {
	const brokerAddr = "127.0.0.1:19452"
	startBroker(t, brokerAddr)
	prod := dial(t, brokerAddr)
	st := store.NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go collect(ctx, dial(t, brokerAddr), st, "collectors", "e2e-a")
	go collect(ctx, dial(t, brokerAddr), st, "collectors", "e2e-b")
	time.Sleep(80 * time.Millisecond)
	_ = publishCSV(t, prod, 16)
	waitGPUs(t, st, 1)
}

func TestPipelineReplicaHA(t *testing.T) {
	leader := startBroker(t, "127.0.0.1:19453")
	startBroker(t, "127.0.0.1:19454")
	leader.SetReplicator(mq.PeerReplicator("", []string{"127.0.0.1:19454"}))
	prod := dial(t, "127.0.0.1:19453")
	pt := ingest.ToPoint(ingest.Row{UUID: "GPU-e2e", Hostname: "h", Device: "d", GPUID: "0", ModelName: "H100", MetricName: "util", Value: 9})
	pt.MessageID = "e2e-ha-1"
	pt.ProcessedAt = time.Now().UTC()
	raw, _ := json.Marshal(pt)
	if _, err := prod.Produce(context.Background(), &mq.ProduceRequest{
		Key: pt.UUID, ID: pt.MessageID, Payload: raw,
	}); err != nil {
		t.Fatal(err)
	}
	standbyClient := dial(t, "127.0.0.1:19454")
	st := store.NewMemory()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go collect(ctx, standbyClient, st, "collectors", "e2e-ha")
	waitGPUs(t, st, 1)
}
