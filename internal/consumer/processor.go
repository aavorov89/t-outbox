package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/backoff"
	"github.com/aavorov89/t-outbox/internal/event"
)

// ErrRetriesExhausted wraps the last transient error once MaxAttempts were made.
var ErrRetriesExhausted = errors.New("consumer: retries exhausted")

// ProcessorConfig configures a Processor. Zero values get the DefaultProcessorConfig values,
// except Consumer, which is required.
type ProcessorConfig struct {
	// Consumer is this consumer's key in processed_events, e.g. "ledger". Two consumers
	// with different keys each process every event once.
	Consumer string
	// MaxAttempts bounds the attempts for transient errors, the first one included.
	MaxAttempts int
	// AttemptTimeout bounds one attempt: its transaction, the Handler and the COMMIT.
	AttemptTimeout time.Duration
	// Backoff is the delay before attempt n+1 after attempt n failed.
	Backoff backoff.Func
	Logger  *slog.Logger
}

// DefaultProcessorConfig returns production defaults for consumer. The worst case for one
// record is MaxAttempts × AttemptTimeout plus the backoff, about 52s; a Runner cuts it short
// when a rebalance is waiting.
func DefaultProcessorConfig(consumer string) ProcessorConfig {
	return ProcessorConfig{
		Consumer:       consumer,
		MaxAttempts:    5,
		AttemptTimeout: 10 * time.Second,
		Backoff:        backoff.Exp(100*time.Millisecond, 2*time.Second, nil),
		Logger:         slog.Default(),
	}
}

// Result describes a processed record.
type Result struct {
	// Duplicate is true if the event had already been processed: nothing was applied.
	Duplicate bool
	// Attempts is the number of attempts made. A record that does not decode counts as 1.
	Attempts int
}

// Processor applies one record exactly once per Consumer:
//
//	decode the envelope                      malformed → permanent, no attempt at the DB
//	BEGIN
//	  INSERT INTO processed_events ... ON CONFLICT DO NOTHING
//	  0 rows → COMMIT, duplicate: done
//	  Handler.Handle(tx)                     the side effect, same tx
//	COMMIT                                   marker and effect, atomically
//
// A transient error rolls the attempt back and is retried with backoff, up to
// MaxAttempts. A permanent error (IsPermanent) is returned at once.
//
// Two members processing the same event at once (during a rebalance) are safe: the second
// INSERT waits for the first transaction, then either finds the marker (duplicate) or, if
// the first rolled back, applies the event itself.
//
// Processor is safe for concurrent use.
type Processor struct {
	pool    *pgxpool.Pool
	handler Handler
	cfg     ProcessorConfig
}

func NewProcessor(pool *pgxpool.Pool, h Handler, cfg ProcessorConfig) *Processor {
	def := DefaultProcessorConfig(cfg.Consumer)
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = def.MaxAttempts
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = def.AttemptTimeout
	}
	if cfg.Backoff == nil {
		cfg.Backoff = def.Backoff
	}
	if cfg.Logger == nil {
		cfg.Logger = def.Logger
	}
	return &Processor{pool: pool, handler: h, cfg: cfg}
}

const insertProcessedSQL = `
INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2)
ON CONFLICT (consumer, event_id) DO NOTHING`

// Process decodes rec and applies it. A nil error means the event is applied, now or
// before (Result.Duplicate), and rec's offset may be committed.
//
// A non-nil error is one of:
//   - permanent (IsPermanent): the record can never be applied; send it to the DLQ;
//   - ErrRetriesExhausted, wrapping the last transient error;
//   - ctx's error, when ctx ends first. Nothing was applied by the attempt that was cut
//     short, unless its COMMIT had already reached the server: then the redelivery is a
//     duplicate. Either way, do not DLQ and do not commit the offset.
func (p *Processor) Process(ctx context.Context, rec *kgo.Record) (Result, error) {
	if p.cfg.Consumer == "" {
		return Result{}, errors.New("consumer: processor: empty Consumer")
	}
	env, err := event.Decode(rec.Value)
	if err != nil {
		return Result{Attempts: 1}, fmt.Errorf("consumer: %s/%d@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
	}
	log := p.cfg.Logger.With("event_id", env.EventID, "event_type", env.EventType,
		"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset)

	for attempt := 1; ; attempt++ {
		dup, err := p.attempt(ctx, env)
		res := Result{Duplicate: dup, Attempts: attempt}
		switch {
		case err == nil:
			if dup {
				log.InfoContext(ctx, "consumer: duplicate event skipped", "consumer", p.cfg.Consumer)
			}
			return res, nil
		case ctx.Err() != nil:
			return res, fmt.Errorf("consumer: event %s: stopped: %w (attempt %d: %w)", env.EventID, context.Cause(ctx), attempt, err)
		case IsPermanent(err):
			return res, fmt.Errorf("consumer: event %s: %w", env.EventID, err)
		case attempt >= p.cfg.MaxAttempts:
			return res, fmt.Errorf("consumer: event %s: %w after %d attempts: %w", env.EventID, ErrRetriesExhausted, attempt, err)
		}
		delay := p.cfg.Backoff(attempt)
		log.WarnContext(ctx, "consumer: transient error, retrying", "attempt", attempt, "retry_in", delay, "err", err)
		if !sleep(ctx, delay) {
			return res, fmt.Errorf("consumer: event %s: stopped: %w (attempt %d: %w)", env.EventID, context.Cause(ctx), attempt, err)
		}
	}
}

// attempt runs one transaction: the dedup marker and the Handler's side effect. A panic in
// the Handler rolls back and is returned as a (transient) error.
func (p *Processor) attempt(ctx context.Context, env event.Envelope) (dup bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.AttemptTimeout)
	defer cancel()
	err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) (err error) {
		tag, err := tx.Exec(ctx, insertProcessedSQL, p.cfg.Consumer, env.EventID)
		if err != nil {
			return fmt.Errorf("processed_events: %w", err)
		}
		if tag.RowsAffected() == 0 {
			dup = true
			return nil // COMMIT of an empty tx; the marker is already there
		}
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("handler panicked: %v", r)
			}
		}()
		return p.handler.Handle(ctx, tx, env)
	})
	if err != nil {
		return false, err
	}
	return dup, nil
}

// sleep waits d or until ctx is done; it reports whether the full delay passed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
