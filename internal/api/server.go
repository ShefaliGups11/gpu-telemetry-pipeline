package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"gpu-telemetry-pipeline/internal/model"
	"gpu-telemetry-pipeline/internal/store"
)

// Server is the REST API gateway.
type Server struct {
	store store.Store
}

// New constructs an HTTP handler.
func New(st store.Store) http.Handler {
	s := &Server{store: st}
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))
	r.Get("/healthz", s.health)
	r.Get("/openapi.json", s.openapi)
	r.Get("/swagger.json", s.openapi)
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/gpus", s.listGPUs)
		r.Get("/gpus/{id}/telemetry", s.gpuTelemetry)
	})
	return r
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) openapi(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(OpenAPIJSON())
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) listGPUs(w http.ResponseWriter, r *http.Request) {
	gpus, err := s.store.ListGPUs(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if gpus == nil {
		gpus = []model.GPU{}
	}
	writeJSON(w, http.StatusOK, gpus)
}

func (s *Server) gpuTelemetry(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "missing gpu id", http.StatusBadRequest)
		return
	}
	gpus, err := s.store.ListGPUs(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	found := false
	for _, g := range gpus {
		if g.UUID == id {
			found = true
			break
		}
	}
	if !found {
		http.Error(w, "gpu not found", http.StatusNotFound)
		return
	}
	var start, end *time.Time
	if v := r.URL.Query().Get("start_time"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			http.Error(w, "invalid start_time, use RFC3339", http.StatusBadRequest)
			return
		}
		start = &t
	}
	if v := r.URL.Query().Get("end_time"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			http.Error(w, "invalid end_time, use RFC3339", http.StatusBadRequest)
			return
		}
		end = &t
	}
	samples, err := s.store.QueryTelemetry(r.Context(), id, start, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if samples == nil {
		samples = []model.Sample{}
	}
	writeJSON(w, http.StatusOK, samples)
}
