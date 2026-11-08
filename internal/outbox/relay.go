package outbox

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotImplemented is returned by the relay until M3.
var ErrNotImplemented = errors.New("outbox: relay not implemented yet (M3)")

// Hooks are test-only seams. All fields are nil in production.
type Hooks struct {
	// AfterClaim runs after rows are locked, before publishing. F4 uses it to record
	// in-flight IDs per worker.
	AfterClaim func(ctx context.Context, ids []int64)
	// AfterPublish runs after Kafka acked, before the rows are marked published. It gets the
	// claiming tx so F5 can read pg_backend_pid() and kill that backend. A non-nil error
	// aborts the cycle and rolls back the tx.
	AfterPublish func(ctx context.Context, tx pgx.Tx, ids []int64) error
}

// Config configures one relay worker.
type Config struct {
	Topic        string        // destination topic; tests use a unique one each
	BatchSize    int           // max rows per claim
	PollInterval time.Duration // sleep when nothing was claimed
	Backoff      Backoff       // delay before re-trying a failed row
	Hooks        Hooks
}

// DefaultConfig returns production defaults for topic.
func DefaultConfig(topic string) Config {
	return Config{
		Topic:        topic,
		BatchSize:    100,
		PollInterval: 200 * time.Millisecond,
		Backoff:      ExpBackoff(100*time.Millisecond, 30*time.Second, nil),
	}
}

// Relay polls the outbox with FOR UPDATE SKIP LOCKED, publishes, and marks rows.
// Any number of Relays may run against the same table.
//
// The pool's connections must set idle_in_transaction_session_timeout greater than the
// producer's delivery timeout, because the claim tx stays open while publishing.
type Relay struct {
	pool     *pgxpool.Pool
	producer Producer
	cfg      Config
}

func NewRelay(pool *pgxpool.Pool, p Producer, cfg Config) *Relay {
	return &Relay{pool: pool, producer: p, cfg: cfg}
}

// Run loops RunOnce until ctx is cancelled. It returns nil on cancellation.
func (r *Relay) Run(ctx context.Context) error {
	return ErrNotImplemented
}

// RunOnce performs one claim → publish → mark cycle and returns the number of rows
// claimed. Tests drive the relay with it for deterministic steps.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	return 0, ErrNotImplemented
}
