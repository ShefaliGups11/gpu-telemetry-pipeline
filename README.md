# Elastic GPU Telemetry Pipeline

CSV streamers, a **custom gRPC message queue** (not Kafka, RabbitMQ, or ZeroMQ), collectors, PostgreSQL, and a REST API. Deploy with Helm on **kind**. Streamers and collectors scale to **10**. The broker is two StatefulSet replicas (leader + standby).

## Architecture

```
CSV baked into the streamer image
              │
              ▼
     Streamers (1–10)
     hash(uuid) % N
              │ produce key=GPU UUID
              ▼
     Broker leader  ──Replicate WAL──►  Broker standby
     8 partitions, PVC wal.log          read-only, auto-promote
              │
              │  consumer group "collectors"
              ▼
     Collectors (1–10)  ──upsert + ack──►  PostgreSQL
                                              │
                                              │  read only
                                              ▼
                                         REST API
                                      NodePort 30080
```

1. Each streamer loops `dcgm_metrics.csv` forever. **`processed_at`** is `time.Now().UTC()` when the row is emitted. The original CSV **`timestamp`** is kept on the sample (not used for the API time window).
2. Produce JSON to topic `telemetry` with **key = GPU UUID** and a ULID `message_id`.
3. The **leader** appends `hash(key) % 8`, fsyncs the WAL, and sync-copies the record to the standby. `broker-1` starts read-only and auto-promotes if the leader is gone.
4. Collectors in group `collectors` fetch assigned partitions, upsert GPU identity + insert the sample (every CSV column), then ack. `UNIQUE(message_id)` drops at-least-once duplicates.
5. The API **reads Postgres**. `GET /gpus` is one row per UUID. `GET /gpus/{id}/telemetry` is **many rows** for that UUID (metrics × CSV loops).

**GPU `{id}`** is the DCGM UUID, not host-local `gpu_id`. Streamers shard with `hash(uuid) % replicaCount`. Collectors share partitions via the group (no collector-side map). Protocol: JSON inside gRPC `BytesValue` (`Produce`, `Join`, `Heartbeat`, `Fetch`, `Ack`, `Health`, `Replicate`). Produce/Ack fsync; Fetch long-polls; Produce blocks if a partition is over `MaxPending` (10 000). Clients dial `broker-0,broker-1` **at process start**; a live TCP session does not switch peers until the pod restarts. Membership is in-memory per broker process; after failover, collectors Join again. Brief redelivery is possible; SQL dedup holds the API invariant.

CSV is in the streamer image (ConfigMaps cap at 1 MiB). Postgres uses `subPath: pgdata` on its PVC. Each process exposes Prometheus `/metrics` (API `:8080`, broker `:8081`, streamer/collector `:8082`).

### REST

| Method | Path | Body |
|---|---|---|
| GET | `/api/v1/gpus` | JSON array of GPU identity (`uuid`, `hostname`, `device`, `gpu_id`, `model_name`, `first_seen`, `last_seen`), sorted by uuid. Empty cluster: `[]`. |
| GET | `/api/v1/gpus/{id}/telemetry` | JSON array of samples for that UUID, oldest `processed_at` first. One CSV row → one sample. |
| GET | `/api/v1/gpus/{id}/telemetry?start_time=&end_time=` | Inclusive RFC3339 window on **`processed_at`**. Unknown UUID → 404. Bad time → 400. Known GPU with no rows in window → `[]`. |
| GET | `/healthz` | `ok` (liveness; does not check Postgres) |
| GET | `/openapi.json` | OpenAPI 3.0.3 (`/swagger.json` is the same) |
| GET | `/metrics` | Prometheus text |

Telemetry sample fields: CSV columns `timestamp`, `metric_name`, `gpu_id`, `device`, `uuid`, `model_name`, `hostname`, `container`, `pod`, `namespace`, `value`, `labels_raw`, plus pipeline `message_id` and `processed_at`.

`make openapi` writes `docs/openapi.json` from the chi router and `encoding/json` tags on `model.GPU` / `model.Sample`.

## Build, test, package

Needs Go 1.22+, Docker, kind, kubectl, helm. GNU Make is optional.

```bash
make tidy
make test
make e2e
make e2e-kind
make cover
make openapi
make build
make docker-build
```

Without Make:

```bash
go test ./cmd/... ./internal/...
go test ./e2e -count=1 -timeout 120s -v -run Pipeline
go run ./cmd/openapi > docs/openapi.json
```

**In-process e2e** (`make e2e`): CSV → broker → collector → REST (two collectors, leader→standby replicate). No Kubernetes.

**Kind e2e** is **opt-in** (`make e2e-kind` sets `E2E_KIND=1`). It hits NodePort `http://127.0.0.1:8080` (override `E2E_API_URL`). After `git pull`, rebuild and reload images (`make kind-load kind-install`) or telemetry will fail with `missing csv/ingest fields` against an old API. `make test` does not talk to kind.

```powershell
$env:E2E_KIND = '1'
go test ./e2e -count=1 -timeout 180s -v -run '^TestKind'
```

`make cover` reports owned pipeline packages, not `internal/testutil`.

## Install on kind

```bash
make kind-up
make kind-load      # docker build + kind load docker-image
make kind-install
make e2e-kind
```

API NodePort **30080** maps to host **8080**:

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/api/v1/gpus
curl "http://127.0.0.1:8080/api/v1/gpus/<GPU-UUID>/telemetry"
curl "http://127.0.0.1:8080/api/v1/gpus/<GPU-UUID>/telemetry?start_time=2026-01-01T00:00:00Z&end_time=2026-12-31T23:59:59Z"
curl http://127.0.0.1:8080/openapi.json
curl http://127.0.0.1:8080/metrics
```

Scale (keep each ≤ 10):

```bash
helm upgrade gpu-telemetry deploy/helm/gpu-telemetry -n gpu-telemetry \
  -f deploy/helm/gpu-telemetry/values-kind.yaml \
  --set streamer.replicaCount=3 --set collector.replicaCount=3
```

```bash
make kind-uninstall
# or
kind delete cluster --name gpu-telemetry
```

## Sample workflow

1. `make kind-up kind-load kind-install`.
2. Wait a few seconds (or `make e2e-kind`) until `GET /api/v1/gpus` is non-empty. There are **247** H100 UUIDs in the sample CSV once each has been ingested.
3. Pick a UUID. Telemetry is **many records** (util, temp, … and later loops). `start_time` / `end_time` filter ingest time, so a window around **now** returns recent samples. CSV `timestamp` is the original exporter time on each row.
4. Scale streamers/collectors and confirm data still moves.
5. Optional: delete `gpu-telemetry-broker-0`. Streamers/collectors redial `broker-1` on the next Produce/Fetch (`Unavailable` or not-leader). Collectors Join again; WAL offsets were already replicated. Membership is not copied — that re-Join is the recovery.
