package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oklog/ulid/v2"

	"gpu-telemetry-pipeline/internal/config"
	"gpu-telemetry-pipeline/internal/ingest"
	"gpu-telemetry-pipeline/internal/mq"
	"gpu-telemetry-pipeline/internal/obs"
)

var fatalExit = os.Exit
var marshalJSON = json.Marshal

var newRootCtx = func() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := newRootCtx()
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("streamer", "err", err)
		fatalExit(1)
	}
}

func run(ctx context.Context) error {
	csvPath := config.Get("CSV_PATH", "/data/dcgm_metrics.csv")
	brokerAddr := config.Get("BROKER_ADDR", "127.0.0.1:9090")
	interval := config.Duration("STREAM_INTERVAL", 200*time.Millisecond)
	index := config.ReplicaIndex()
	count := config.Int("STREAMER_COUNT", 1)

	rows, err := ingest.LoadCSV(csvPath)
	if err != nil {
		return err
	}
	obs.ServeMetrics(config.Get("METRICS_ADDR", ":8082"))
	slog.Info("streamer start", "rows", len(rows), "index", index, "count", count, "interval", interval.String())

	client, err := mq.Dial(ctx, brokerAddr)
	if err != nil {
		return err
	}
	defer client.Close()
	return stream(ctx, client, rows, interval, index, count)
}

func stream(ctx context.Context, client *mq.Client, rows []ingest.Row, interval time.Duration, index, count int) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	i := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			row := rows[i]
			if !ingest.Owns(row.UUID, index, count) {
				i = (i + 1) % len(rows)
				continue
			}
			pt := ingest.ToPoint(row)
			pt.MessageID = ulid.Make().String()
			pt.ProcessedAt = time.Now().UTC()
			payload, err := marshalJSON(pt)
			if err != nil {
				slog.Error("marshal", "err", err)
				i = (i + 1) % len(rows)
				continue
			}
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			obs.StreamerOut.Inc()
			_, err = client.Produce(pctx, &mq.ProduceRequest{
				Topic:   mq.DefaultTopic,
				Key:     pt.UUID,
				ID:      pt.MessageID,
				Payload: payload,
			})
			cancel()
			if err != nil {
				slog.Error("produce", "err", err)
				continue
			}
			i = (i + 1) % len(rows)
		}
	}
}
