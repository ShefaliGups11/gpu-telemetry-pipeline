package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gpu-telemetry-pipeline/internal/config"
	"gpu-telemetry-pipeline/internal/model"
	"gpu-telemetry-pipeline/internal/mq"
	"gpu-telemetry-pipeline/internal/obs"
	"gpu-telemetry-pipeline/internal/store"
)

var fatalExit = os.Exit

var newRootCtx = func() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

var heartbeatEvery = 2 * time.Second
var sessionRetry = time.Second
var persistRetry = 200 * time.Millisecond

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := newRootCtx()
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("collector", "err", err)
		fatalExit(1)
	}
}

func run(ctx context.Context) error {
	brokerAddr := config.Get("BROKER_ADDR", "127.0.0.1:9090")
	dbURL := config.Get("DATABASE_URL", "postgres://gpu:gpu@127.0.0.1:5432/gpu?sslmode=disable")
	group := config.Get("CONSUMER_GROUP", "collectors")
	member := config.Get("HOSTNAME", "collector-0")
	if v := os.Getenv("MEMBER_ID"); v != "" {
		member = v
	}
	obs.ServeMetrics(config.Get("METRICS_ADDR", ":8082"))
	st, err := waitPostgres(ctx, dbURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Init(ctx); err != nil {
		return err
	}
	client, err := mq.Dial(ctx, brokerAddr)
	if err != nil {
		return err
	}
	defer client.Close()
	return consumeLoop(ctx, client, st, group, member)
}

var openStore = func(ctx context.Context, url string) (store.Store, error) {
	return store.OpenPostgres(ctx, url)
}

func waitPostgres(ctx context.Context, dbURL string) (store.Store, error) {
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

func consumeLoop(ctx context.Context, client *mq.Client, st store.Store, group, member string) error {
	slog.Info("collector start", "member", member, "group", group)
	for ctx.Err() == nil {
		if err := runSession(ctx, client, st, group, member); err != nil && ctx.Err() == nil {
			slog.Warn("session ended", "err", err)
			time.Sleep(sessionRetry)
		}
	}
	return nil
}

func runSession(ctx context.Context, client *mq.Client, st store.Store, group, member string) error {
	join, err := client.Join(ctx, &mq.JoinRequest{Group: group, MemberID: member})
	if err != nil {
		return err
	}
	slog.Info("joined", "generation", join.Generation, "partitions", join.Partitions)
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		t := time.NewTicker(heartbeatEvery)
		defer t.Stop()
		for {
			select {
			case <-sctx.Done():
				return
			case <-t.C:
				hb, err := client.Heartbeat(sctx, &mq.HeartbeatRequest{Group: group, MemberID: member})
				if err != nil || hb.Rejoin || hb.Generation != join.Generation {
					cancel()
					return
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for _, p := range join.Partitions {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			from := uint64(0)
			for sctx.Err() == nil {
				fctx, fc := context.WithTimeout(sctx, 15*time.Second)
				fr, err := client.Fetch(fctx, &mq.FetchRequest{
					Group: group, MemberID: member, Generation: join.Generation,
					Partition: p, FromOffset: from, MaxRecords: 64,
				})
				fc()
				if err != nil {
					cancel()
					return
				}
				for _, rec := range fr.Records {
					var pt model.Point
					if err := json.Unmarshal(rec.Payload, &pt); err != nil {
						slog.Error("payload", "err", err)
						from = rec.Offset + 1
						continue
					}
					if pt.MessageID == "" {
						pt.MessageID = rec.ID
					}
					obs.CollectorIn.Inc()
					if err := st.UpsertAndInsert(sctx, pt); err != nil && !errors.Is(err, store.ErrDuplicate) {
						slog.Error("persist", "err", err)
						time.Sleep(persistRetry)
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
