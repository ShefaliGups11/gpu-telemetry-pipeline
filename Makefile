GO ?= go
CLUSTER ?= gpu-telemetry
NS ?= gpu-telemetry
CHART := deploy/helm/gpu-telemetry
APPS := broker streamer collector api
TAG ?= dev

.PHONY: build test e2e e2e-kind cover openapi docker-build kind-up kind-load kind-install kind-uninstall helm-lint tidy

build:
	$(GO) build -o bin/broker$(EXE) ./cmd/broker
	$(GO) build -o bin/streamer$(EXE) ./cmd/streamer
	$(GO) build -o bin/collector$(EXE) ./cmd/collector
	$(GO) build -o bin/api$(EXE) ./cmd/api

tidy:
	$(GO) mod tidy

# Unit tests only. Kind/NodePort coverage is `make e2e-kind` (needs E2E_KIND=1).
test:
	$(GO) test ./cmd/... ./internal/...

e2e:
	$(GO) test ./e2e -count=1 -timeout 120s -v -run Pipeline

e2e-kind: export E2E_KIND := 1
e2e-kind:
	$(GO) test ./e2e -count=1 -timeout 180s -v -run '^TestKind'

# Pipeline packages we own (excludes internal/testutil helpers).
PKGS := ./cmd/... ./internal/api ./internal/config ./internal/ingest ./internal/model ./internal/mq ./internal/obs ./internal/store

cover:
	$(GO) test $(PKGS) -coverprofile=coverage.out
	$(GO) tool cover -func=coverage.out
	$(GO) tool cover -html=coverage.out -o coverage.html

openapi:
	mkdir -p docs
	$(GO) run ./cmd/openapi > docs/openapi.json

helm-lint:
	helm lint $(CHART)

docker-build:
	docker build -t gpu-telemetry/broker:$(TAG) --build-arg TARGET=broker .
	docker build -t gpu-telemetry/streamer:$(TAG) --build-arg TARGET=streamer .
	docker build -t gpu-telemetry/collector:$(TAG) --build-arg TARGET=collector .
	docker build -t gpu-telemetry/api:$(TAG) --build-arg TARGET=api .

kind-up:
	kind get clusters | grep -qx $(CLUSTER) || kind create cluster --name $(CLUSTER) --config deploy/kind/cluster.yaml

kind-load: docker-build
	kind load docker-image gpu-telemetry/broker:$(TAG) --name $(CLUSTER)
	kind load docker-image gpu-telemetry/streamer:$(TAG) --name $(CLUSTER)
	kind load docker-image gpu-telemetry/collector:$(TAG) --name $(CLUSTER)
	kind load docker-image gpu-telemetry/api:$(TAG) --name $(CLUSTER)

kind-install:
	helm upgrade --install gpu-telemetry $(CHART) --namespace $(NS) --create-namespace -f $(CHART)/values-kind.yaml
	kubectl -n $(NS) rollout status statefulset/gpu-telemetry-postgres --timeout=180s
	kubectl -n $(NS) rollout status statefulset/gpu-telemetry-broker --timeout=180s
	kubectl -n $(NS) rollout status statefulset/gpu-telemetry-streamer --timeout=180s
	kubectl -n $(NS) rollout status deployment/gpu-telemetry-collector --timeout=180s
	kubectl -n $(NS) rollout status deployment/gpu-telemetry-api --timeout=180s

kind-uninstall:
	-helm uninstall gpu-telemetry --namespace $(NS)
	-kubectl delete ns $(NS)

kind-delete:
	kind delete cluster --name $(CLUSTER)
