# 0001. Poll the outbox table instead of CDC

**Status:** Accepted

## Context

The outbox rows must get from PostgreSQL to Kafka. There are two ways to read them:

- **Polling:** relays query the table (`SELECT ... FOR UPDATE SKIP LOCKED`), publish, and
  mark the rows published.
- **Change data capture:** Debezium reads the write-ahead log through a logical
  replication slot and publishes each inserted row, usually with its outbox event router.

## Decision

Poll. The relay is a small Go binary in this repository (`internal/outbox`), scaled by
running more instances.

## Consequences

- **Everything is in the application and testable in-process.** Crash safety (F5),
  ordering (F9), broker outages (F6) and the timeout chain are covered by integration
  tests against real Postgres and Redpanda. With CDC, the equivalent behaviour lives in a
  Kafka Connect cluster and its configuration.
- **Nothing extra to run.** No Kafka Connect, no replication slot. An abandoned slot keeps
  WAL on the primary until the disk fills, which is a failure mode of its own.
- **The table churns.** Every event is an insert plus an update, and the pending set is a
  queue that is usually empty. That needs vacuum to keep up, a cleanup of published rows
  (batched deletes; partitioning by `created_at` at high volume, so old partitions can be
  dropped), and care with the planner: its statistics describe an empty queue most of the time
  (see [0004](0004-per-aggregate-ordering-guard.md), M7 amendment).
- **Latency and load.** Idle latency is up to the poll interval (200ms), and each idle
  relay runs about five cheap claims per second. `LISTEN/NOTIFY` would cut the latency
  without more polling.
- **Throughput** is bounded by claim → publish → mark round trips per relay. See the
  load numbers in the README; that is far above what this system writes, and relays
  scale out.

## Alternatives

- **Debezium + outbox event router.** Better for very high volume, for commit-order
  publication without a guard, or when the application cannot be changed. Revisit if the
  sustained rate outgrows a handful of relays, or if the latency budget drops below what
  `LISTEN/NOTIFY` gives.
- **Publish directly from the API after the commit** ("dual write"). Loses events when
  the process dies between the commit and the publish. Not an option.
