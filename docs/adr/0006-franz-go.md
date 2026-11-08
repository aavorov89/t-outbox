# 0006. franz-go as the Kafka client

**Status:** Accepted

## Context

The relay needs an idempotent producer with `acks=all` and a per-record result, so that
each outbox row maps to one ack or one error. The consumer needs manual offset commits
and control over rebalances, so that offsets are committed only for applied records of
partitions it still owns.

## Decision

`github.com/twmb/franz-go` (`kgo`, `kadm` in tests).

## Consequences

- **Pure Go, no cgo.** Static binaries in a distroless image, `-race` works, and builds
  need no C toolchain.
- **Idempotent producer on by default**, with per-record promises, which map directly to
  `Produce(ctx, []Record) []error`.
- **Consumer-group control** that the Runner relies on: `DisableAutoCommit`,
  `CommitRecords`, `BlockRebalanceOnPoll` with `AllowRebalance`, and
  `OnPartitionsCallbackBlocked` to interrupt a batch when a rebalance is waiting
  (ARCHITECTURE §8).
- **One caveat, handled explicitly.** By default an in-flight idempotent record is never
  failed: with a hung broker, `ProduceSync` ignored both the delivery timeout and the
  context. `kafkax.ProducerOpts` sets `AllowIdempotentProduceCancellation` and a request
  timeout, so the publish bound of [0002](0002-row-locks-during-publish.md) really holds,
  at the cost of a possible duplicate.
- `kadm` gives the tests explicit topic creation and committed-offset checks.

## Alternatives

- **confluent-kafka-go** (librdkafka): mature and fast, but cgo, which complicates
  builds, static images and the race detector.
- **segmentio/kafka-go**: pure Go, but its idempotent-producer and group-rebalance
  controls are thinner than what the Runner needs.
- **IBM/sarama**: supports the idempotent producer, but its consumer-group API makes
  commit-after-processing and rebalance interruption harder to express.
