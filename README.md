# t-outbox

Transactional outbox in Go: a small payments system that publishes domain events from
PostgreSQL to Kafka reliably, without distributed transactions.

At-least-once delivery from the outbox plus an idempotent consumer gives
**effectively-once processing**.

> Work in progress. M1 (foundations: event contract, interfaces, test harness) is done.
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
