package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"gpu-telemetry-pipeline/internal/api"
	"gpu-telemetry-pipeline/internal/config"
	"gpu-telemetry-pipeline/internal/obs"
	"gpu-telemetry-pipeline/internal/store"
)

// bootHandler serves /healthz before Postgres is ready so kube probes pass.
type bootHandler struct {
	ready atomic.Pointer[http.Handler]
}

func (b *bootHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h := b.ready.Load(); h != nil {
		(*h).ServeHTTP(w, r)
		return
	}
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}
	http.Error(w, "starting", http.StatusServiceUnavailable)
}

var fatalExit = os.Exit

func rootCtx() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

var newRootCtx = rootCtx

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := newRootCtx()
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("api", "err", err)
		fatalExit(1)
	}
}

var openStore = func(ctx context.Context, url string) (store.Store, error) {
	return store.OpenPostgres(ctx, url)
}

func run(ctx context.Context) error {
	dbURL := config.Get("DATABASE_URL", "postgres://gpu:gpu@127.0.0.1:5432/gpu?sslmode=disable")
	addr := config.Get("HTTP_ADDR", ":8080")
	boot := &bootHandler{}
	hs := &http.Server{Addr: addr, Handler: boot}
	errCh := make(chan error, 1)
	go func() {
		slog.Info("api listen", "addr", addr)
		err := hs.ListenAndServe()
		if err == http.ErrServerClosed {
			err = nil
		}
		errCh <- err
	}()
	st, err := waitStore(ctx, dbURL)
	if err != nil {
		_ = hs.Close()
		<-errCh
		return err
	}
	defer st.Close()
	if err := st.Init(ctx); err != nil {
		_ = hs.Close()
		<-errCh
		return err
	}
	h := obs.Mux(api.New(st))
	boot.ready.Store(&h)
	select {
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Shutdown(sctx)
		return <-errCh
	case err := <-errCh:
		return err
	}
}

func waitStore(ctx context.Context, dbURL string) (store.Store, error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		p, err := openStore(cctx, dbURL)
		cancel()
		if err == nil {
			return p, nil
		}
		slog.Warn("waiting for postgres", "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func serve(ctx context.Context, addr string, st store.Store) error {
	hs := &http.Server{Addr: addr, Handler: obs.Mux(api.New(st))}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Shutdown(sctx)
	}()
	slog.Info("api listen", "addr", addr)
	err := hs.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
