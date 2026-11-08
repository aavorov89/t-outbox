# Architecture decision records

| # | Decision | Status |
|---|---|---|
| [0001](0001-outbox-polling-vs-cdc.md) | Poll the outbox table instead of CDC (Debezium) | Accepted |
| [0002](0002-row-locks-during-publish.md) | Hold the row locks while publishing instead of a lease column | Accepted |
| [0003](0003-processed-events-vs-redis.md) | Dedup in a `processed_events` table, in the side effect's transaction, instead of Redis `SETNX` | Accepted |
| [0004](0004-per-aggregate-ordering-guard.md) | Per-aggregate ordering with a guard in the claim query | Accepted (amended in M7) |
| [0005](0005-money-int64-minor-units.md) | Money as `int64` minor units | Accepted |
| [0006](0006-franz-go.md) | franz-go as the Kafka client | Accepted |

Format: context, decision, consequences, alternatives considered. An ADR is not edited
once accepted, except to amend it with a dated section; a reversal gets a new ADR.
