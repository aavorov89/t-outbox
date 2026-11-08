package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Hooks are test-only seams. All fields are nil in production.
type Hooks struct {
	// AfterClaim runs after rows are locked, before publishing. F4 uses it to record
	// in-flight IDs per worker. (M3)
	AfterClaim func(ctx context.Context, ids []int64)
	// AfterPublish runs after Kafka acked, before the rows are marked published. It gets the
	// claiming tx so F5 can read pg_backend_pid() and kill that backend. A non-nil error
	// aborts the cycle and rolls back the tx. (M5)
	AfterPublish func(ctx context.Context, tx pgx.Tx, ids []int64) error
}

// Config configures one relay worker. Zero BatchSize, PollInterval and Logger get the
// DefaultConfig values.
type Config struct {
	Topic        string        // destination topic; tests use a unique one each
	BatchSize    int           // max rows per claim
	PollInterval time.Duration // sleep when nothing was claimed
	Backoff      Backoff       // delay before re-trying a failed row (M3)
	Hooks        Hooks
	Logger       *slog.Logger
}

// DefaultConfig returns production defaults for topic.
func DefaultConfig(topic string) Config {
	return Config{
		Topic:        topic,
		BatchSize:    100,
		PollInterval: 200 * time.Millisecond,
		Backoff:      ExpBackoff(100*time.Millisecond, 30*time.Second, nil),
		Logger:       slog.Default(),
	}
}

// Relay reads pending outbox rows, publishes them, and marks them published.
//
// M2 version, deliberately naive: no row locks and no transaction, so run exactly ONE
// relay. A failed record is not marked and is simply picked up again by the next cycle,
// with no attempts/backoff bookkeeping. M3 replaces the claim with
// FOR UPDATE SKIP LOCKED + the per-aggregate NOT EXISTS guard, holds the tx during
// publish, and records attempts/next_attempt_at/last_error.
//
// Delivery is already at-least-once: a crash between publish and mark re-publishes the
// rows on restart.
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
	if cfg.Backoff == nil {
		cfg.Backoff = def.Backoff
	}
	if cfg.Logger == nil {
		cfg.Logger = def.Logger
	}
	return &Relay{pool: pool, producer: p, cfg: cfg}
}

// Run loops RunOnce until ctx is cancelled. It returns nil on cancellation. Errors of a
// cycle are logged and retried after PollInterval. A full batch is followed immediately by
// the next cycle, so a backlog drains without sleeping.
func (r *Relay) Run(ctx context.Context) error {
	if r.cfg.Topic == "" {
		return errors.New("outbox: relay: empty topic")
	}
	for {
		n, err := r.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return nil // cancellation is the normal way to stop; err is just its echo
		default:
		}
		if err != nil {
			r.cfg.Logger.WarnContext(ctx, "outbox relay cycle failed", "err", err)
		}
		if err == nil && n == r.cfg.BatchSize {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.cfg.PollInterval):
		}
	}
}

const selectPendingSQL = `
SELECT id, event_id, aggregate_type, aggregate_id, aggregate_version,
       event_type, event_version, occurred_at, payload, headers, attempts
FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT $1`

const markPublishedSQL = `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`

// RunOnce performs one read → publish → mark cycle and returns the number of rows read.
// Tests drive the relay with it for deterministic steps. If some records fail, the others
// are still marked and the error reports the failures.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	rows, err := r.pending(ctx)
	if err != nil || len(rows) == 0 {
		return 0, err
	}

	recs := make([]Record, len(rows))
	for i, row := range rows {
		if recs[i], err = BuildRecord(r.cfg.Topic, row); err != nil {
			// Queue validated the row, so this is a manually inserted or corrupted row.
			return len(rows), err
		}
	}

	errs := r.producer.Produce(ctx, recs)
	var (
		ok     = make([]int64, 0, len(rows))
		failed []error
	)
	for i, row := range rows {
		if errs[i] != nil {
			failed = append(failed, fmt.Errorf("row %d: %w", row.ID, errs[i]))
			continue
		}
		ok = append(ok, row.ID)
	}

	if len(ok) > 0 {
		if _, err := r.pool.Exec(ctx, markPublishedSQL, ok); err != nil {
			// The records are in Kafka but not marked: the next cycle re-publishes them
			// (an at-least-once duplicate the consumer dedups).
			return len(rows), fmt.Errorf("outbox: mark %d rows published: %w", len(ok), err)
		}
	}
	if len(failed) > 0 {
		return len(rows), fmt.Errorf("outbox: %d of %d records failed: %w", len(failed), len(rows), errors.Join(failed...))
	}
	return len(rows), nil
}

func (r *Relay) pending(ctx context.Context) ([]Row, error) {
	rows, err := r.pool.Query(ctx, selectPendingSQL, r.cfg.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("outbox: select pending: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Row, error) {
		var o Row
		err := row.Scan(&o.ID, &o.EventID, &o.AggregateType, &o.AggregateID, &o.AggregateVersion,
			&o.EventType, &o.EventVersion, &o.OccurredAt, &o.Payload, &o.Headers, &o.Attempts)
		return o, err
	})
	if err != nil {
		return nil, fmt.Errorf("outbox: select pending: %w", err)
	}
	return out, nil
}
