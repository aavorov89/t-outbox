# 0004. Per-aggregate ordering with a guard in the claim query

**Status:** Accepted; amended in M7 (the guard's form, see below)

## Context

Consumers need the events of one payment in order: `payment.created` before
`payment.captured`. Kafka keeps order within a partition, and the key (`aggregate_id`)
puts all events of an aggregate in one partition. But N relays with `SKIP LOCKED` can
claim v1 and v2 of the same aggregate in different batches, and v2's batch may commit
first. A failed v1 that backs off would also let v2 overtake it.

## Decision

A row is claimable only when the previous event of its aggregate is published. The claim
query enforces it, so any number of relays can run without coordinating.

## Consequences

- **Ordering holds across relays, retries and crashes** (F9: 4 relays, 30% of publishes
  failing, versions 1..10 in offset order for every key).
- **Head-of-line blocking per aggregate, by design.** An event that keeps failing blocks
  the later events of its aggregate and nothing else. Its age shows in
  `outbox_oldest_pending_age_seconds`, and an unbuildable row has an alert of its own
  (`OutboxUnbuildableRow`).
- **A batch holds at most one event per aggregate.** An aggregate with k pending events
  needs k cycles, which is fine for payments (2 events each).
- **It relies on the writer invariant.** Events of one aggregate are written under a
  lock on it (`Repo.Update` does `FOR UPDATE`), so v-1 is committed before v exists.
- **One index probe per candidate row.** That is cheap only if it really is a probe; see
  the amendment.

## Alternatives

- **One relay.** Ordered for free, but no scale-out and no failover without leader
  election.
- **Shard the outbox by `hash(aggregate_id) mod N`**, one relay per shard: static, and
  changing N needs a migration of in-flight rows.
- **`pg_try_advisory_xact_lock(hashtext(aggregate_id))` in the claim**: the lock can be
  taken for rows the query then discards, and hash collisions block unrelated aggregates.

## Amendment (M7): the form of the guard matters to the planner

The first version of the guard was the obvious one:

```sql
AND NOT EXISTS (SELECT 1 FROM outbox p
                WHERE p.aggregate_id = o.aggregate_id
                  AND p.published_at IS NULL AND p.id < o.id)
```

`make load` found that this is quadratic in the backlog. On a healthy queue table, the
partial index `outbox_pending` (`WHERE published_at IS NULL`) is empty whenever
autovacuum measures it, so its `reltuples` is 0. The planner then believes a full scan of
it is free, and plans the `NOT EXISTS` as exactly that, once per candidate row. With a
sudden backlog (load faster than the relays, or a broker outage) each claim scanned the
whole backlog per candidate. At 20,000 pending rows that was 435ms and 2 million rows
filtered per claim. In the first load run, one claim with a snapshot from before the
drain ran for over 6 minutes, holding locks on 39 rows that no other relay could take.

The guard now probes the previous version through the unique key:

```sql
AND (SELECT p.published_at IS NOT NULL FROM outbox p
     WHERE p.aggregate_type    = o.aggregate_type
       AND p.aggregate_id      = o.aggregate_id
       AND p.aggregate_version = o.aggregate_version - 1) IS NOT FALSE
```

The probe says nothing about `published_at` in its `WHERE`, so the partial index does not
match it, and its only sensible plan is one unique-index lookup. The same claim takes
about 1ms and touches about 1,000 buffers instead of 117,000. It is equivalent to the old
guard: versions are contiguous, so "v-1 is published" means every earlier version is
(they were published in order). A missing predecessor (v1, or a row deleted by the
cleanup after it was published) does not block. The index `outbox_pending_aggr` lost its
only reader and was dropped (migration 00003).

Two defences were added with it:

- `TestRelay_ClaimCostWithStaleStats` builds the stale-statistics state on purpose
  (published rows, `VACUUM ANALYZE`, then a sudden backlog of 10,000) and fails if one
  claim of 100 touches more than 3,000 buffers. The old guard touches 22,600.
- `statement_timeout` = 5s on relay sessions (`outbox.ConfigurePool`). The idle timeout
  does not cover a statement that is running, so a pathological claim used to hold its
  row locks for as long as it ran.

Lesson for queue tables in Postgres: test the claim with the statistics of an empty
queue and the data of a full one.
