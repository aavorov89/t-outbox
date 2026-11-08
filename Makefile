GO           ?= go
COMPOSE      := docker compose -f deploy/docker-compose.yml --env-file deploy/.env
LINT_IMAGE   ?= golangci/golangci-lint:v2.15.0
DATABASE_URL ?= postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable
GOMODCACHE   := $(shell $(GO) env GOMODCACHE)
LINT_CACHE   ?= $(HOME)/.cache/golangci-lint-docker
UIDGID       := $(shell id -u):$(shell id -g)

.PHONY: up down migrate test test-integration lint fmt tidy

## Local stack: postgres + redpanda (+ topics), then migrations.
up:
	$(COMPOSE) up -d --wait postgres redpanda
	$(COMPOSE) up topics
	DATABASE_URL='$(DATABASE_URL)' $(GO) run ./cmd/migrate

down:
	$(COMPOSE) down -v

migrate:
	DATABASE_URL='$(DATABASE_URL)' $(GO) run ./cmd/migrate

## Unit tests only: no Docker needed.
test:
	$(GO) test -race ./...

## Integration tests: testcontainers starts its own Postgres/Redpanda per package.
test-integration:
	$(GO) test -tags=integration -race -count=1 -timeout 10m ./...

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
