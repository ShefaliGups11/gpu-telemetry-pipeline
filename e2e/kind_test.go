package e2e_test

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"gpu-telemetry-pipeline/internal/model"
)

func kindAPIBase() string {
	if u := os.Getenv("E2E_API_URL"); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "http://127.0.0.1:8080"
}

func kindRequired() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("E2E_KIND")))
	return v == "1" || v == "true" || v == "yes"
}

func kindClient() *http.Client {
	return &http.Client{Timeout: 20 * time.Second}
}

func kindGET(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := kindClient().Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

func waitKindAPI(t *testing.T) string {
	t.Helper()
	base := kindAPIBase()
	wait := 2 * time.Second
	if kindRequired() {
		wait = 2 * time.Minute
	}
	deadline := time.Now().Add(wait)
	probe := &http.Client{Timeout: 800 * time.Millisecond}
	var last error
	for time.Now().Before(deadline) {
		resp, err := probe.Get(base + "/healthz")
		if err != nil {
			last = err
			time.Sleep(300 * time.Millisecond)
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 && string(b) == "ok" {
			return base
		}
		last = err
		time.Sleep(300 * time.Millisecond)
	}
	if kindRequired() {
		t.Fatalf("kind API not ready at %s (make kind-up kind-load kind-install): %v", base, last)
	}
	t.Skipf("kind API not at %s; skip (set E2E_KIND=1 after make kind-install)", base)
	return base
}

func waitKindGPUs(t *testing.T, base string) []model.GPU {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var last []byte
	for time.Now().Before(deadline) {
		resp, err := kindClient().Get(base + "/api/v1/gpus")
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		last = body
		if resp.StatusCode != 200 {
			time.Sleep(time.Second)
			continue
		}
		var gpus []model.GPU
		if err := json.Unmarshal(body, &gpus); err != nil {
			time.Sleep(time.Second)
			continue
		}
		if len(gpus) > 0 {
			return gpus
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("no GPUs from kind API after 90s: %s", last)
	return nil
}

func TestKindAPI(t *testing.T) {
	if !kindRequired() {
		t.Skip("kind e2e is opt-in; run make e2e-kind (E2E_KIND=1) after kind-install")
	}
	base := waitKindAPI(t)

	t.Run("healthz", func(t *testing.T) {
		resp := kindGET(t, base+"/healthz")
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 || string(b) != "ok" {
			t.Fatalf("%d %s", resp.StatusCode, b)
		}
	})

	t.Run("metrics", func(t *testing.T) {
		resp := kindGET(t, base+"/metrics")
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 || !strings.Contains(string(b), "gpu_telemetry_") {
			t.Fatalf("metrics %d", resp.StatusCode)
		}
	})

	t.Run("openapi", func(t *testing.T) {
		resp := kindGET(t, base+"/openapi.json")
		defer resp.Body.Close()
		var spec map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&spec); err != nil {
			t.Fatal(err)
		}
		if spec["openapi"] != "3.0.3" {
			t.Fatalf("%v", spec["openapi"])
		}
		paths, _ := spec["paths"].(map[string]any)
		if paths["/api/v1/gpus"] == nil || paths["/api/v1/gpus/{id}/telemetry"] == nil {
			t.Fatalf("paths %#v", paths)
		}
	})

	gpus := waitKindGPUs(t, base)
	id := gpus[0].UUID
	if id == "" || gpus[0].Hostname == "" || gpus[0].ModelName == "" {
		t.Fatalf("gpu identity %+v", gpus[0])
	}

	t.Run("telemetry_csv_fields", func(t *testing.T) {
		start := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
		end := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
		resp := kindGET(t, base+"/api/v1/gpus/"+id+"/telemetry?start_time="+start+"&end_time="+end)
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("%d %s", resp.StatusCode, b)
		}
		var samples []model.Sample
		if err := json.NewDecoder(resp.Body).Decode(&samples); err != nil {
			t.Fatal(err)
		}
		if len(samples) == 0 {
			t.Fatal("expected samples for listed GPU")
		}
		for _, s := range samples {
			if s.UUID != id {
				t.Fatalf("sample uuid %s want %s", s.UUID, id)
			}
			if s.MessageID == "" || s.MetricName == "" || s.Timestamp == "" ||
				s.Device == "" || s.GPUID == "" || s.Hostname == "" || s.ModelName == "" ||
				s.ProcessedAt.IsZero() {
				t.Fatalf("missing csv/ingest fields %+v", s)
			}
		}
	})

	t.Run("unknown_gpu_404", func(t *testing.T) {
		resp := kindGET(t, base+"/api/v1/gpus/missing-gpu/telemetry")
		defer resp.Body.Close()
		if resp.StatusCode != 404 {
			t.Fatalf("got %d", resp.StatusCode)
		}
	})

	t.Run("bad_time_400", func(t *testing.T) {
		resp := kindGET(t, base+"/api/v1/gpus/"+id+"/telemetry?start_time=not-rfc3339")
		defer resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatalf("got %d", resp.StatusCode)
		}
	})
}
