# 0003. Dedup in a `processed_events` table instead of Redis `SETNX`

**Status:** Accepted

## Context

Delivery is at-least-once: a relay that dies between Kafka's ack and its commit
re-publishes (F5), and a consumer that dies between its database commit and the offset
commit gets the records again (F7). The ledger consumer must apply every event exactly
once.

## Decision

The consumer inserts `(consumer, event_id)` into `processed_events` **in the same
transaction** as the side effect (the ledger postings), with `ON CONFLICT DO NOTHING`. No
row inserted means the event was already applied: commit and skip.

## Consequences

The marker and the side effect commit atomically, so neither half can exist without the
other:

| Approach | Crash between the marker and the side effect |
|---|---|
| `processed_events` in the same PG transaction | Impossible: one commit |
| Redis `SETNX`, then the DB write | Marker set, effect lost: the **event is lost** |
| DB write, then Redis `SETNX` | Effect applied, no marker: **applied twice** |

- A failed attempt rolls back the marker too, so a dead-lettered event leaves no marker
  and a replay from the DLQ can still apply it.
- Two members processing the same event at once (a rebalance overlap) are serialized by
  the primary key: the second insert waits for the first transaction, then sees the row.
- The table grows with every event. Its rows are only needed while a duplicate can
  still arrive: the topic's retention plus the DLQ replay window. A periodic delete of
  older rows bounds it (not implemented here).
- It only works because the side effect is in the same database. A consumer whose effect
  is elsewhere (an email, an HTTP call) needs an idempotency key on that side instead.

## Alternatives

- **Redis `SETNX` with a TTL**: fine when the side effect is idempotent or not
  transactional anyway (sending a notification with a dedup window). Wrong for a ledger,
  per the table above.
- **Kafka transactions / exactly-once semantics**: cover Kafka-to-Kafka processing, not a
  write to Postgres. The database commit and the offset commit would still be two
  commits.
- **A unique constraint on the ledger rows** (`transaction_id = UUIDv5(event_id)`): this
  is kept as a second line of defence (a double application shows up as the same id
  twice), but it covers one handler's table, not "this event was handled", and it turns
  every duplicate into an error to classify.
