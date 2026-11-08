package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Timeouts of the relay, from inner to outer. Each must be larger than the previous one:
//
//	producer delivery timeout (kafkax.DefaultDeliveryTimeout, 10s)
//	  < Config.PublishTimeout (15s): ctx deadline on Produce, a bound for any Producer
//	  < IdleInTransactionTimeout (30s): the server kills a relay that froze mid-batch
//
// Publishing happens inside the claiming transaction, and Postgres counts that time as
// "idle in transaction". If the server timeout fired first on a healthy relay, every slow
// batch would be published and then rolled back: a duplicate per record, forever.
const (
	DefaultPublishTimeout    = 15 * time.Second
	IdleInTransactionTimeout = 30 * time.Second
)

// maxLastErrorLen bounds outbox.last_error.
const maxLastErrorLen = 1024

// ConfigurePool sets the session parameters a relay's pool needs. Pass it to
// pgxpool.ParseConfig's result (or testenv.NewPool).
func ConfigurePool(c *pgxpool.Config) {
	rp := c.ConnConfig.RuntimeParams
	rp["idle_in_transaction_session_timeout"] = strconv.FormatInt(IdleInTransactionTimeout.Milliseconds(), 10)
	if rp["application_name"] == "" {
		rp["application_name"] = "outbox-relay"
	}
}

// Hooks are test-only seams. All fields are nil in production.
type Hooks struct {
	// AfterClaim runs after rows are locked, before publishing. F4 uses it to record
	// in-flight IDs per worker.
	AfterClaim func(ctx context.Context, ids []int64)
	// AfterPublish runs after Kafka acked, before the rows are marked published: the window
	// of F5. ids are the acked rows. It gets the claiming tx, so a test can kill the
	// backend that holds the row locks: SELECT pg_backend_pid() on tx gives its PID (the
	// server's answer, right even behind a pooler), and pg_terminate_backend on a side
	// connection ends the session the way a kill -9 of the relay would. A non-nil
	// error aborts the cycle and rolls back the tx.
	AfterPublish func(ctx context.Context, tx pgx.Tx, ids []int64) error
}

// Config configures one relay worker. Zero values get the DefaultConfig values.
type Config struct {
	Topic string // destination topic; tests use a unique one each

	// BatchSize is the max rows per claim. It bounds the locks held and the records in one
	// Produce call; a batch takes as long as its slowest record. Because of the ordering
	// guard, a batch holds at most one event per aggregate.
	BatchSize int
	// PollInterval is the sleep after a cycle that claimed less than a full batch. It is
	// the publish latency when idle; a full batch is followed by the next cycle at once.
	PollInterval time.Duration
	// PublishTimeout bounds one Produce call. It must be below the pool's
	// idle_in_transaction_session_timeout; Run refuses to start otherwise.
	PublishTimeout time.Duration
	// DrainTimeout is how long Run lets the in-flight batch finish after ctx is cancelled.
	// Past it, the batch is abandoned: its tx rolls back and the rows are re-published by
	// the next relay (duplicates of what already landed). Default PublishTimeout + 5s.
	DrainTimeout time.Duration
	// Backoff is the delay before re-trying a failed row, by attempt number. Run also uses
	// it between consecutive failed cycles (e.g. database down).
	Backoff Backoff
	Hooks   Hooks
	Logger  *slog.Logger
}

// DefaultConfig returns production defaults for topic.
func DefaultConfig(topic string) Config {
	return Config{
		Topic:          topic,
		BatchSize:      100,
		PollInterval:   200 * time.Millisecond,
		PublishTimeout: DefaultPublishTimeout,
		DrainTimeout:   DefaultPublishTimeout + 5*time.Second,
		Backoff:        ExpBackoff(100*time.Millisecond, 30*time.Second, nil),
		Logger:         slog.Default(),
	}
}

// Relay claims pending outbox rows, publishes them, and marks them published.
//
// Any number of relays can run against one table. Each cycle is one transaction:
//
//	BEGIN
//	  claim up to BatchSize rows        FOR UPDATE SKIP LOCKED + per-aggregate guard
//	  Produce                           bounded by PublishTimeout
//	  mark acked rows published         one round trip for both updates
//	  reschedule failed rows            attempts+1, next_attempt_at, last_error
//	COMMIT
//
// The row locks are held while publishing, so a relay that dies mid-batch releases its
// rows with the rollback and another relay re-publishes them: delivery is at-least-once,
// and the consumer dedups by event_id.
//
// Ordering: a row is claimable only if no earlier row of the same aggregate is still
// unpublished, so the next event of an aggregate is published only after the previous
// one is committed as published. A failing event blocks its aggregate (and only it) until
// it goes through.
type Relay struct {
	pool     *pgxpool.Pool
	producer Producer
	cfg      Config
}

func NewRelay(pool *pgxpool.Pool, p Producer, cfg Config) *Relay {
	def := DefaultConfig(cfg.Topic)
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = def.BatchSize
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = def.PollInterval
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = def.PublishTimeout
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = cfg.PublishTimeout + 5*time.Second
	}
	if cfg.Backoff == nil {
		cfg.Backoff = def.Backoff
	}
	if cfg.Logger == nil {
		cfg.Logger = def.Logger
	}
	return &Relay{pool: pool, producer: p, cfg: cfg}
}

// PublishError reports the records of a cycle that were not published. They were
// rescheduled with backoff and the cycle itself succeeded, so Run does not slow down.
type PublishError struct {
	Failed, Total int
	Err           error // the per-record errors, joined
}

func (e *PublishError) Error() string {
	return fmt.Sprintf("outbox: %d of %d records not published: %v", e.Failed, e.Total, e.Err)
}

func (e *PublishError) Unwrap() error { return e.Err }

var errDrainTimeout = errors.New("outbox: relay: drain timeout")

// Run loops cycles until ctx is cancelled and returns nil then.
//
// Graceful shutdown: cancelling ctx stops claiming, but the in-flight batch runs to its
// COMMIT on a context that is not cancelled, for up to DrainTimeout. So a SIGTERM
// normally loses nothing and causes no duplicates.
//
// Errors of a cycle are logged. A failed cycle (database down, tx aborted) is retried
// after Backoff(consecutive failures); records that failed to publish were already
// rescheduled in the database, so the loop just continues after PollInterval.
func (r *Relay) Run(ctx context.Context) error {
	if r.cfg.Topic == "" {
		return errors.New("outbox: relay: empty topic")
	}
	err := r.checkIdleTimeout(ctx)
	switch {
	case ctx.Err() != nil:
		return nil //nolint:nilerr // cancelled during start-up: a normal stop, err is its echo
	case err != nil:
		return err
	}
	failures := 0
	for ctx.Err() == nil {
		n, err := r.cycle(ctx)
		var pubErr *PublishError
		delay := r.cfg.PollInterval
		switch {
		case err == nil:
			failures = 0
			if n == r.cfg.BatchSize {
				continue // backlog: no sleep
			}
		case errors.As(err, &pubErr):
			failures = 0
			r.cfg.Logger.WarnContext(ctx, "outbox relay: records rescheduled",
				"failed", pubErr.Failed, "total", pubErr.Total, "err", pubErr.Err)
		default:
			failures++
			delay = max(delay, r.cfg.Backoff(failures))
			r.cfg.Logger.WarnContext(ctx, "outbox relay cycle failed",
				"consecutive_failures", failures, "retry_in", delay, "err", err)
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
	return nil
}

// cycle runs RunOnce detached from ctx's cancellation, so a shutdown lets the batch
// finish. Once ctx is done, the batch gets DrainTimeout more, then it is cancelled.
func (r *Relay) cycle(ctx context.Context) (int, error) {
	work, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancel(nil)
	stop := context.AfterFunc(ctx, func() {
		t := time.NewTimer(r.cfg.DrainTimeout)
		defer t.Stop()
		select {
		case <-t.C:
			cancel(errDrainTimeout)
		case <-work.Done():
		}
	})
	defer stop()

	n, err := r.RunOnce(work)
	if errors.Is(context.Cause(work), errDrainTimeout) {
		r.cfg.Logger.WarnContext(ctx, "outbox relay: in-flight batch abandoned at shutdown; its rows stay pending",
			"drain_timeout", r.cfg.DrainTimeout, "rows", n, "err", err)
	}
	return n, err
}

// checkIdleTimeout refuses a pool whose idle_in_transaction_session_timeout would kill
// the claiming transaction of a healthy, merely slow publish.
func (r *Relay) checkIdleTimeout(ctx context.Context) error {
	var ms int64
	err := r.pool.QueryRow(ctx,
		`SELECT setting::bigint FROM pg_settings WHERE name = 'idle_in_transaction_session_timeout'`).Scan(&ms)
	if err != nil {
		return fmt.Errorf("outbox: relay: read idle_in_transaction_session_timeout: %w", err)
	}
	idle := time.Duration(ms) * time.Millisecond
	switch {
	case idle == 0:
		r.cfg.Logger.WarnContext(ctx, "outbox relay: idle_in_transaction_session_timeout is disabled; "+
			"a frozen relay keeps its rows locked until its connection dies (use outbox.ConfigurePool)")
	case idle <= r.cfg.PublishTimeout:
		return fmt.Errorf("outbox: relay: idle_in_transaction_session_timeout (%v) must be greater than "+
			"PublishTimeout (%v): publishing happens inside the claiming transaction", idle, r.cfg.PublishTimeout)
	}
	return nil
}

// claimSQL takes the oldest claimable rows. A row is claimable if it is pending, due, and
// no earlier row of its aggregate is still unpublished (pending, backing off, or locked by
// another relay). SKIP LOCKED makes concurrent relays take disjoint rows without waiting.
const claimSQL = `
SELECT o.id, o.event_id, o.aggregate_type, o.aggregate_id, o.aggregate_version,
       o.event_type, o.event_version, o.occurred_at, o.payload, o.headers, o.attempts
FROM outbox o
WHERE o.published_at IS NULL
  AND o.next_attempt_at <= now()
  AND NOT EXISTS (
        SELECT 1 FROM outbox p
        WHERE p.aggregate_id = o.aggregate_id
          AND p.published_at IS NULL
          AND p.id < o.id)
ORDER BY o.id
LIMIT $1
FOR UPDATE SKIP LOCKED`

const markPublishedSQL = `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`

const rescheduleSQL = `
UPDATE outbox AS o
SET attempts        = o.attempts + 1,
    next_attempt_at = now() + f.delay_ms * interval '1 millisecond',
    last_error      = f.err
FROM unnest($1::bigint[], $2::bigint[], $3::text[]) AS f(id, delay_ms, err)
WHERE o.id = f.id`

// RunOnce performs one claim → publish → mark cycle in one transaction and returns the
// number of rows claimed. Tests drive the relay with it for deterministic steps.
//
// Records that failed to publish (or to build) are rescheduled with backoff and reported
// as a *PublishError after the commit. Any other error means the tx rolled back and
// nothing changed in the database, even if some records reached Kafka.
//
// Unlike Run, RunOnce stops as soon as ctx is cancelled: the tx rolls back.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("outbox: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after Commit

	rows, err := claim(ctx, tx, r.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, tx.Commit(ctx)
	}
	if h := r.cfg.Hooks.AfterClaim; h != nil {
		ids := make([]int64, len(rows))
		for i, row := range rows {
			ids[i] = row.ID
		}
		h(ctx, ids)
	}

	// Build every record; a row that cannot be built (corrupted by hand) fails alone.
	errs := make([]error, len(rows))
	recs := make([]Record, 0, len(rows))
	idx := make([]int, 0, len(rows)) // recs[j] belongs to rows[idx[j]]
	for i, row := range rows {
		rec, err := BuildRecord(r.cfg.Topic, row)
		if err != nil {
			errs[i] = err
			continue
		}
		recs = append(recs, rec)
		idx = append(idx, i)
	}
	if len(recs) > 0 {
		spans := startSendSpans(rows, recs, idx)
		pubCtx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
		perRec := r.producer.Produce(pubCtx, recs)
		cancel()
		for j, err := range perRec {
			errs[idx[j]] = err
			endSpan(spans[j], err)
		}
	}

	var (
		ok                    = make([]int64, 0, len(rows))
		failIDs, failDelaysMs []int64
		failMsgs              []string
		failed                []error
	)
	for i, row := range rows {
		if errs[i] == nil {
			ok = append(ok, row.ID)
			continue
		}
		failIDs = append(failIDs, row.ID)
		failDelaysMs = append(failDelaysMs, r.cfg.Backoff(row.Attempts+1).Milliseconds())
		failMsgs = append(failMsgs, truncate(errs[i].Error(), maxLastErrorLen))
		failed = append(failed, fmt.Errorf("row %d (%s v%d): %w", row.ID, row.AggregateID, row.AggregateVersion, errs[i]))
	}

	if h := r.cfg.Hooks.AfterPublish; h != nil {
		if err := h(ctx, tx, ok); err != nil {
			return len(rows), fmt.Errorf("outbox: after publish hook: %w", err)
		}
	}

	b := &pgx.Batch{}
	if len(ok) > 0 {
		b.Queue(markPublishedSQL, ok)
	}
	if len(failIDs) > 0 {
		b.Queue(rescheduleSQL, failIDs, failDelaysMs, failMsgs)
	}
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		// Acked records are in Kafka but not marked: the next claim re-publishes them
		// (an at-least-once duplicate the consumer dedups).
		return len(rows), fmt.Errorf("outbox: mark %d published, %d failed: %w", len(ok), len(failIDs), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return len(rows), fmt.Errorf("outbox: commit: %w", err)
	}
	if len(failed) > 0 {
		return len(rows), &PublishError{Failed: len(failed), Total: len(rows), Err: errors.Join(failed...)}
	}
	return len(rows), nil
}

func claim(ctx context.Context, tx pgx.Tx, limit int) ([]Row, error) {
	rows, err := tx.Query(ctx, claimSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Row, error) {
		var o Row
		err := row.Scan(&o.ID, &o.EventID, &o.AggregateType, &o.AggregateID, &o.AggregateVersion,
			&o.EventType, &o.EventVersion, &o.OccurredAt, &o.Payload, &o.Headers, &o.Attempts)
		return o, err
	})
	if err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	return out, nil
}

// truncate cuts s to at most n bytes of valid UTF-8 without NUL bytes, which TEXT rejects.
func truncate(s string, n int) string {
	if len(s) > n {
		s = s[:n]
	}
	return strings.ReplaceAll(strings.ToValidUTF8(s, ""), "\x00", "")
}
