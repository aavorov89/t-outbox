GO           ?= go
COMPOSE      := docker compose -f deploy/docker-compose.yml --env-file deploy/.env
LINT_IMAGE   ?= golangci/golangci-lint:v2.15.0
DATABASE_URL ?= postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable
GOMODCACHE   := $(shell $(GO) env GOMODCACHE)
LINT_CACHE   ?= $(HOME)/.cache/golangci-lint-docker
UIDGID       := $(shell id -u):$(shell id -g)

.PHONY: up down app migrate run-api run-relay run-consumer demo chaos load test test-integration lint check-prometheus fmt tidy

## Local stack: postgres + redpanda (+ topics) + jaeger (UI :16686) + prometheus (UI :9090),
## then migrations.
up:
	$(COMPOSE) up -d --wait postgres redpanda jaeger prometheus
	$(COMPOSE) up topics
	DATABASE_URL='$(DATABASE_URL)' $(GO) run ./cmd/migrate

down:
	$(COMPOSE) --profile app down -v

## The binaries as containers (deploy/Dockerfile): api on :8080, relay-1, relay-2,
## ledger-consumer. Needs `make up`.
app:
	$(COMPOSE) --profile app up -d --build api relay-1 relay-2 ledger-consumer

migrate:
	DATABASE_URL='$(DATABASE_URL)' $(GO) run ./cmd/migrate

## The three binaries against the `make up` stack (one terminal each), tracing to Jaeger.
KAFKA_BROKERS ?= localhost:19092
OTEL_EXPORTER_OTLP_ENDPOINT ?= http://localhost:4318
RUN_ENV := DATABASE_URL='$(DATABASE_URL)' OTEL_EXPORTER_OTLP_ENDPOINT='$(OTEL_EXPORTER_OTLP_ENDPOINT)'
run-api:
	$(RUN_ENV) $(GO) run ./cmd/payments-api

## Metrics on :9464, scraped by the compose Prometheus. A second relay on the host:
## make run-relay METRICS_ADDR=:9465
METRICS_ADDR ?= :9464
run-relay:
	$(RUN_ENV) KAFKA_BROKERS='$(KAFKA_BROKERS)' METRICS_ADDR='$(METRICS_ADDR)' $(GO) run ./cmd/outbox-relay

run-consumer:
	$(RUN_ENV) KAFKA_BROKERS='$(KAFKA_BROKERS)' $(GO) run ./cmd/ledger-consumer

## Create and capture 100 payments via the API (make app, or the run-* targets), wait for
## the consumer, print the ledger balance. DEMO_PAYMENTS, DEMO_CONCURRENCY, API_URL.
demo:
	COMPOSE='$(COMPOSE)' scripts/demo.sh

## SIGKILL relay containers in a loop under load; fails unless the ledger is exact.
## Starts the app profile itself. CHAOS_PAYMENTS, CHAOS_CONCURRENCY, CHAOS_TARGETS.
chaos:
	COMPOSE='$(COMPOSE)' scripts/chaos.sh

## Load numbers (cmd/loadgen): steady state, then backlog drain with 1 and 2 relays.
## Starts the app profile itself. LOAD_DURATION, LOAD_CONCURRENCY, LOAD_BACKLOG.
load:
	COMPOSE='$(COMPOSE)' scripts/load.sh

## Unit tests only: no Docker needed.
test:
	$(GO) test -race ./...

## Integration tests: testcontainers starts its own Postgres/Redpanda per package.
test-integration:
	$(GO) test -tags=integration -race -count=1 -timeout 10m ./...

## Validate the Prometheus config and unit-test the alert rules with promtool (in Docker).
PROMETHEUS_IMAGE ?= $(shell sed -n 's/^PROMETHEUS_IMAGE=//p' deploy/.env)
check-prometheus:
	docker run --rm -v $(CURDIR)/deploy/prometheus:/etc/prometheus:ro -w /etc/prometheus \
		--entrypoint promtool $(PROMETHEUS_IMAGE) check config prometheus.yml
	docker run --rm -v $(CURDIR)/deploy/prometheus:/etc/prometheus:ro -w /etc/prometheus \
		--entrypoint promtool $(PROMETHEUS_IMAGE) test rules alerts_test.yml

## golangci-lint in Docker, so nothing has to be installed locally. Runs as your uid so
## the caches it writes are not root-owned.
lint:
	mkdir -p $(LINT_CACHE)/lint $(LINT_CACHE)/go-build
	docker run --rm --user $(UIDGID) -w /app -e HOME=/tmp \
		-e GOFLAGS=-mod=mod -e GOMODCACHE=/gomod -e GOCACHE=/cache/go-build \
		-e GOLANGCI_LINT_CACHE=/cache/lint \
		-v $(CURDIR):/app -v $(GOMODCACHE):/gomod -v $(LINT_CACHE):/cache \
		$(LINT_IMAGE) golangci-lint run ./...

fmt:
	gofmt -w .

tidy:
	$(GO) mod tidy
