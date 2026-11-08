# t-outbox

Transactional outbox in Go: a small payments system that publishes domain events from
PostgreSQL to Kafka reliably, without distributed transactions.

At-least-once delivery from the outbox plus an idempotent consumer gives
**effectively-once processing**.

> Work in progress. Done: M1 (foundations: event contract, interfaces, test harness),
> M2 (walking skeleton: API → outbox → relay → Kafka → logging consumer) and M3 (capture
> with idempotency keys; relay with `SKIP LOCKED`, per-aggregate ordering, backoff and
> graceful shutdown).
> See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design, the failure-mode matrix
> and the milestone plan.

## Quickstart

Requires Go 1.27+ and Docker.

```sh
make up                 # postgres + redpanda + topics, then migrations
make test               # unit tests, no Docker
make test-integration   # testcontainers: starts its own Postgres and Redpanda
make lint               # golangci-lint in Docker
make down
```

Run the chain locally (after `make up`, one terminal each):

```sh
make run-api            # :8080
make run-relay          # as many as you like: they split the work with SKIP LOCKED
make run-consumer       # logs every event

curl -i localhost:8080/payments \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" \
  -d '{"merchant_id":"m_42","amount":{"amount_minor":1999,"currency":"EUR"}}'

curl -i -X POST localhost:8080/payments/<id>/capture -H "Idempotency-Key: $(uuidgen)"
```

Relay knobs: `RELAY_BATCH_SIZE` (100), `RELAY_POLL_INTERVAL` (200ms). `Ctrl-C` lets the
in-flight batch finish before it exits.

Amounts are `int64` minor units (`1999` = €19.99). JavaScript clients lose precision above
2^53, which is why some APIs send amounts as strings. Errors are RFC 9457 problem details;
see [docs/api-errors.md](docs/api-errors.md).
