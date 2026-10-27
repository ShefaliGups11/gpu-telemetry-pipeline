package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"gpu-telemetry-pipeline/internal/config"
	"gpu-telemetry-pipeline/internal/mq"
	"gpu-telemetry-pipeline/internal/obs"
)

var fatalExit = os.Exit

var newRootCtx = func() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := newRootCtx()
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("broker", "err", err)
		fatalExit(1)
	}
}

func run(ctx context.Context) error {
	dataDir := config.Get("DATA_DIR", "/data")
	grpcAddr := config.Get("GRPC_ADDR", ":9090")
	httpAddr := config.Get("HTTP_ADDR", ":8081")
	npart := config.Int("PARTITIONS", mq.DefaultPartitions)
	idx := config.ReplicaIndex()
	peers := config.List("BROKER_PEERS", "")
	self := ""
	if idx >= 0 && idx < len(peers) {
		self = peers[idx]
	}

	b, err := mq.Open(mq.Options{
		DataDir: dataDir, Partitions: npart,
		ReadOnly: idx > 0, AutoPromote: idx > 0,
	})
	if err != nil {
		return err
	}
	defer b.Close()
	if len(peers) > 0 {
		b.SetReplicator(mq.PeerReplicator(self, peers))
	}

	mux := http.NewServeMux()
	mux.Handle("/healthz", mq.HealthHandler(b))
	mux.Handle("/metrics", obs.Handler())
	h := obs.Middleware(mux)
	hs := &http.Server{Addr: httpAddr, Handler: h}
	go func() {
		slog.Info("broker http", "addr", httpAddr)
		if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		_ = hs.Shutdown(context.Background())
	}()

	slog.Info("broker grpc", "addr", grpcAddr, "partitions", npart, "data", dataDir, "index", idx, "peers", peers)
	return mq.ServeGRPC(ctx, grpcAddr, b)
}
