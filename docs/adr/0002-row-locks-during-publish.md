# 0002. Hold the row locks while publishing instead of a lease column

**Status:** Accepted

## Context

A relay claims rows, publishes them and marks them published. While it publishes, no
other relay may claim the same rows, and if it dies, the rows must become claimable again.

## Decision

Claim, publish and mark in **one transaction**. The claim's `FOR UPDATE SKIP LOCKED` row
locks are held while the records are produced, and released by `COMMIT` or, when the
relay dies, by Postgres rolling back its transaction.

## Consequences

- **Crash safety for free.** A relay that dies mid-batch loses nothing and leaves no state
  to clean up. Whether it was SIGKILLed, frozen, or partitioned away, its session ends and
  its rows are pending again (F5, `make chaos`).
- **A transaction is open during the publish**, so the publish needs a hard bound. Hence
  the timeout chain (ARCHITECTURE §7): franz-go delivery timeout 10s < `PublishTimeout`
  15s < `idle_in_transaction_session_timeout` 30s. `Run` refuses a pool that breaks it,
  and `statement_timeout` (5s) bounds the claim itself.
- **The bound has a price.** franz-go only fails an in-flight idempotent record with
  `AllowIdempotentProduceCancellation`, so a record failed on timeout may still have
  landed, and the retry duplicates it. That is the same at-least-once duplicate as F5;
  the consumer dedups by `event_id`.
- **One connection per relay is held** for the duration of a batch, and the open
  transaction holds back the vacuum horizon for up to the publish time. Both are small
  at the batch sizes used here.

## Alternatives

- **Lease column** (`locked_by`, `locked_until`): claim with a short `UPDATE ... RETURNING`,
  publish outside any transaction, then mark. No long transactions, but the lease must
  outlast the slowest publish. If it expires too early, two relays publish the same rows
  at the same time, and the ordering guard has to treat leased rows as in flight. Every
  row also gets one more update (claim and mark), and a crashed relay's rows wait for the
  lease to expire.
- **Advisory locks** keyed by row id: these work outside transactions, but the lock
  lives as long as the session, and the claim still has to filter locked rows itself.
