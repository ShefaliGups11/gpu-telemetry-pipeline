package obs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServeMetricsOff(t *testing.T) {
	ServeMetrics("")
	ServeMetrics("off")
	ServeMetrics("127.0.0.1:0")
}

func TestMuxMetrics(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	h := Mux(inner)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rr.Code != 204 {
		t.Fatal(rr.Code)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != 200 || !strings.Contains(rr.Body.String(), "gpu_telemetry_http_requests_total") {
		t.Fatalf("%d %s", rr.Code, rr.Body.String())
	}
}
