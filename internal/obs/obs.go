package obs

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gpu_telemetry_http_requests_total",
		Help: "HTTP requests by method and status",
	}, []string{"method", "code"})
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gpu_telemetry_http_request_duration_seconds",
		Help:    "HTTP request latency",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})
	MQProduce = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gpu_telemetry_mq_produce_total",
		Help: "Records produced to the broker",
	})
	MQFetch = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gpu_telemetry_mq_fetch_records_total",
		Help: "Records returned by fetch",
	})
	MQAck = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gpu_telemetry_mq_ack_total",
		Help: "Acked offsets",
	})
	MQReplicate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gpu_telemetry_mq_replicate_total",
		Help: "WAL records replicated to peers",
	})
	StreamerOut = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gpu_telemetry_streamer_produce_total",
		Help: "Streamer produce attempts",
	})
	CollectorIn = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gpu_telemetry_collector_persist_total",
		Help: "Collector persist attempts",
	})
)

// Handler serves Prometheus text metrics.
func Handler() http.Handler { return promhttp.Handler() }

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) {
	w.code = c
	w.ResponseWriter.WriteHeader(c)
}

// Middleware records latency, status, and a slog trace line.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: 200}
		next.ServeHTTP(sw, r)
		d := time.Since(start).Seconds()
		HTTPRequests.WithLabelValues(r.Method, strconv.Itoa(sw.code)).Inc()
		HTTPDuration.WithLabelValues(r.Method).Observe(d)
		slog.Info("http",
			"trace", r.Header.Get("X-Request-Id"),
			"method", r.Method,
			"path", r.URL.Path,
			"code", sw.code,
			"seconds", d,
		)
	})
}

// Mux wraps h with metrics and /metrics.
func Mux(h http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())
	mux.Handle("/", Middleware(h))
	return mux
}

// ServeMetrics starts /metrics (and optional extra handler) until the process exits.
func ServeMetrics(addr string) {
	if addr == "" || addr == "off" {
		return
	}
	go func() {
		_ = http.ListenAndServe(addr, Mux(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})))
	}()
}
