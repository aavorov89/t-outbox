# t-outbox

A transactional outbox in Go: a small payments system that publishes its domain events
from PostgreSQL to Kafka reliably, without distributed transactions. At-least-once
delivery plus an idempotent consumer gives **effectively-once processing**, and every
failure mode that claim depends on is pinned by an integration test against real
Postgres and Kafka.

```mermaid
flowchart LR
    C[Client] -->|POST /payments<br/>Idempotency-Key| API[payments-api]
    API -->|1 tx: payment + outbox row| PG[(PostgreSQL)]
    R1[outbox-relay #1] -->|claim: FOR UPDATE SKIP LOCKED<br/>+ per-aggregate guard| PG
    R2[outbox-relay #2] -->|claim| PG
    R1 -->|produce, key = aggregate_id<br/>idempotent, acks=all| K[(Kafka<br/>payments.events)]
    R2 --> K
    K --> L[ledger-consumer]
    L -->|1 tx: processed_events + ledger entries| PG
    L -->|poison / retries exhausted| DLQ[(payments.events.dlq)]
```

1. **payments-api** writes the payment and its event (`payment.created`,
   `payment.captured`) into the `outbox` table **in the same transaction**: both or
   neither.
2. **outbox-relay** (any number of instances) claims pending rows with `FOR UPDATE SKIP
   LOCKED`, publishes them, and marks them published, all in one transaction. A guard in
   the claim keeps each payment's events in order across relays.
3. **ledger-consumer** applies each `payment.captured` as a balanced double-entry ledger
   transaction. It records the `event_id` in `processed_events` in the same transaction, so
   a duplicate is skipped, and commits Kafka offsets only after the database commit.

## Guarantees

| | Guarantee | How |
|---|---|---|
| **No lost events** | An event exists iff its business change committed | Payment row and outbox row in one transaction (`pgx.Batch`) |
| **At-least-once delivery** | Every committed event reaches Kafka, eventually | Rows stay pending until a relay commits them as published; a dead relay's locks are released by Postgres |
| **Effectively-once processing** | Each event changes the ledger exactly once, whatever the duplicates | `processed_events` marker and ledger rows in one transaction; offsets committed after it |
| **Per-aggregate order** | A payment's events are published and consumed in version order | v is claimable only once v-1 is published; key = `aggregate_id` keeps them in one partition |
| **Exact money** | No rounding anywhere; every ledger transaction sums to zero | `int64` minor units, overflow-checked; balance invariant enforced in code and checked by tests |
| **Bounded failure handling** | A broker outage delays, a poison message is dead-lettered, a hung publish times out | Backoff per row; error classification + DLQ; timeout chain checked at start-up |

What it does **not** claim: exactly-once *delivery*. Kafka will hold duplicates after a
relay crash; they are expected, counted (`make chaos`) and skipped by the consumer.

## Failure modes, each with a test

| # | What fails | What happens | Why it's safe | Test |
|---|---|---|---|---|
| F1 | API dies before COMMIT | Nothing persisted | Single tx | [`TestWrite_Atomicity`](internal/payment/postgres/repo_integration_test.go) |
| F2 | Outbox insert fails | Payment write rolled back too | Single tx | [`TestWrite_OutboxFailureRollsBackPayment`](internal/payment/postgres/repo_integration_test.go) |
| F3 | Client retries create or capture | Same payment, no second event | Idempotency keys + row lock | [`TestAPI_IdempotencyKey`, `TestAPI_CaptureIdempotencyKey`](internal/httpapi/api_integration_test.go) |
| F4 | Two relays poll at once | Disjoint row sets | Row locks + `SKIP LOCKED` | [`TestRelay_ConcurrentWorkersNoDoubleClaim`](internal/outbox/relay_integration_test.go) |
| F5 | **Relay dies after publish, before mark** | Re-published: a duplicate in Kafka | Consumer dedups by `event_id` | [`TestRelay_CrashAfterPublishBeforeMark`](test/e2e/e2e_integration_test.go) |
| F6 | Kafka unavailable | Rows stay pending, `attempts` grows, published after recovery | Backoff, durable outbox | [`TestRelay_BrokerOutage`](internal/outbox/relay_integration_test.go) |
| F7 | Consumer dies after DB commit, before offset commit | Redelivery | `processed_events` | [`TestConsumer_RedeliveryIsIdempotent`](internal/consumer/consumer_integration_test.go) |
| F8 | Poison message | DLQ, partition keeps moving | Error classification | [`TestConsumer_PoisonGoesToDLQ`](internal/consumer/consumer_integration_test.go) |
| F9 | One aggregate's events across N relays, 30% publish failures | Published in version order | Ordering guard + key partitioning | [`TestRelay_PerAggregateOrdering`](internal/outbox/relay_integration_test.go) |
| F10 | Trace across the async gaps | One trace from the HTTP request to the ledger | `traceparent` in the outbox row and the Kafka headers | [`TestTracing_EndToEnd`](test/e2e/e2e_integration_test.go) |
| F11 | Sudden backlog while the planner thinks the queue is empty | Claims stay O(batch) | Guard is a unique-key probe; `statement_timeout` | [`TestRelay_ClaimCostWithStaleStats`](internal/outbox/relay_integration_test.go) |

F5 kills the relay's Postgres backend from a hook between Kafka's ack and the mark, the
way a `kill -9` would. `make chaos` does it for real: it SIGKILLs relay containers at
random under 2,000 payments and fails unless the ledger is exact afterwards.
F11 was found by `make load`, see [below](#load-numbers).

## Quickstart

Requires Go 1.27+ and Docker.

```sh
make up      # postgres, redpanda + topics, jaeger, prometheus; migrations
make app     # api on :8080, relay-1, relay-2, ledger-consumer (deploy/Dockerfile)
make demo    # create + capture 100 payments, wait for the consumer, check the ledger
```

```sh
make chaos              # SIGKILL relays at random under load; fails unless the ledger is exact
make load               # the load numbers below
make test               # unit tests, no Docker
make test-integration   # testcontainers: starts its own Postgres and Redpanda
make lint               # golangci-lint in Docker
make check-prometheus   # promtool: scrape config + alert rule tests
make down
```

By hand, with the binaries on the host (one terminal each, after `make up`):

```sh
make run-api            # :8080
make run-relay          # as many as you like (a second one: METRICS_ADDR=:9465)
make run-consumer

curl -i localhost:8080/payments \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" \
  -d '{"merchant_id":"m_42","amount":{"amount_minor":1999,"currency":"EUR"}}'
curl -i -X POST localhost:8080/payments/<id>/capture -H "Idempotency-Key: $(uuidgen)"

psql postgres://outbox:outbox@localhost:5432/outbox -c \
  'SELECT account, currency, sum(amount_minor) FROM ledger_entries GROUP BY 1, 2'
```

Knobs: `RELAY_BATCH_SIZE` (100), `RELAY_POLL_INTERVAL` (200ms), `METRICS_ADDR` (`:9464`);
`KAFKA_DLQ_TOPIC` (`payments.events.dlq`), `CONSUMER_MAX_ATTEMPTS` (5),
`CONSUMER_STOP_ON_EXHAUSTED` (false). `Ctrl-C`/SIGTERM lets the in-flight batch finish.

Amounts are `int64` minor units (`1999` = €19.99). JavaScript clients lose precision above
2^53, which is why some APIs send amounts as strings. Errors are RFC 9457 problem details;
see [docs/api-errors.md](docs/api-errors.md).

## Tracing

Every request is one trace across the API, the relay and the consumer. The trace context
(W3C `traceparent` + `baggage`) is stored in the outbox row, copied into the Kafka
headers by the relay's producer span, and picked up by the consumer. Jaeger:
http://localhost:16686.

![One capture in Jaeger: API → relay → consumer](docs/img/jaeger-trace.png)

Log lines carry `trace_id`/`span_id`. A client that sends its own `traceparent` gets the
whole chain in its trace. Details:
[ARCHITECTURE §9](docs/ARCHITECTURE.md#9-distributed-tracing-opentelemetry).

## Metrics

The relays serve Prometheus metrics on `/metrics` (`:9464`); Prometheus runs at
http://localhost:9090.

| Metric | |
|---|---|
| `outbox_oldest_pending_age_seconds` | age of the oldest unpublished event: **the SLO signal** (relays down, Kafka down, an aggregate stuck) |
| `outbox_pending` | unpublished rows |
| `outbox_published_total`, `outbox_publish_failures_total{error_type}` | published / rescheduled rows (`publish` or `build`) |
| `outbox_publish_duration_seconds` | one `Produce` call (a batch) |
| `outbox_cycle_failures_total` | relay cycles that rolled back |

The two gauges describe the table, so every relay reports the same value: use `max()`.
Alert rules, with `promtool` unit tests, are in
[deploy/prometheus/alerts.yml](deploy/prometheus/alerts.yml)
([ARCHITECTURE §7](docs/ARCHITECTURE.md#metrics)).

## Load numbers

`make load` (`cmd/loadgen`) against the compose stack on a dev laptop (WSL2, 16 cores; one
Postgres, one Redpanda broker, 6 partitions, everything on one host). Pipeline numbers
come from the database's own timestamps.

| Scenario | Result |
|---|---|
| **API capacity**: 16 clients, 30s, create + capture as fast as it answers | **1,267 payments/s** (2,533 requests/s, 2,533 events/s); p50 5.7ms, p99 10.7ms (create) / 11.3ms (capture) |
| … the relays meanwhile (2 relays) | kept up: 2,537 events/s, write → published p50 118ms, p99 261ms |
| **Steady state**: 100 payments/s (200 events/s), 30s | write → applied in the ledger: **p50 258ms, p99 481ms** (write → published p50 196ms, published → applied p50 60ms) |
| **Backlog drain**, 20,000 events: 1 relay | **9,387 events/s** |
| … 2 relays | **14,754 events/s** |
| **Consumer** (one tx per event, sequential per member): 1 member | **315–333 events/s** |
| … 3 members (6 partitions) | **972 events/s** |
| One relay claim of 100 with 20,000 pending rows, planner stats of an empty queue | **1.1ms**; 435ms with the original guard (F11) |

Reading them:

- **The relays are not the bottleneck.** One relay drains a backlog at about 9,400 events/s
  (full batches skip the poll sleep), several times what the API can write.
- **Idle latency is the poll interval.** At a low rate most of the end-to-end lag is
  write → published, about one `RELAY_POLL_INTERVAL` (200ms). `LISTEN/NOTIFY` would cut
  it; a shorter interval would too, at the price of more idle claims.
- **The consumer is the slowest stage**: a transaction per event (`processed_events` +
  ledger rows), with the partitions of a poll handled one after another. It scales
  linearly with members up to the partition count (≈ 3× with 3 members). At the API's
  full rate, one member falls behind (the e2e lag in the capacity run grew to 198s while
  it drained 76,000 events). Batching several events per transaction is the next step if
  that matters.

The first run of `make load` found a real bug. With the original `NOT EXISTS` ordering
guard, a backlog that appeared while the planner's statistics described an empty queue
made every claim O(backlog²): 2M rows filtered per claim at 20,000 pending, and one claim
ran for over 6 minutes holding locks on 39 rows that no other relay could take. The guard
is now a unique-key probe of the previous version, relay sessions have a 5s
`statement_timeout`, and F11 pins the plan. Full story:
[ADR 0004, M7 amendment](docs/adr/0004-per-aggregate-ordering-guard.md#amendment-m7-the-form-of-the-guard-matters-to-the-planner).

## Design

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): data model, the write path, the relay
  (claim query, timeout chain, graceful shutdown), the consumer (dedup, DLQ, offsets,
  rebalancing), tracing, metrics, and the test harness.
- [docs/api-errors.md](docs/api-errors.md): problem types and idempotency semantics.
- Architecture decision records ([docs/adr](docs/adr/README.md)):
  1. [Poll the outbox instead of CDC (Debezium)](docs/adr/0001-outbox-polling-vs-cdc.md)
  2. [Hold the row locks while publishing instead of a lease column](docs/adr/0002-row-locks-during-publish.md)
  3. [`processed_events` in the side effect's transaction instead of Redis `SETNX`](docs/adr/0003-processed-events-vs-redis.md)
  4. [Per-aggregate ordering with a guard in the claim](docs/adr/0004-per-aggregate-ordering-guard.md), and why its form matters to the planner
  5. [Money as `int64` minor units](docs/adr/0005-money-int64-minor-units.md)
  6. [franz-go as the Kafka client](docs/adr/0006-franz-go.md)
