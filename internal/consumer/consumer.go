// Package consumer is the generic idempotent consumer: dedup by event_id in
// processed_events, the side effect in the same transaction, retries, and DLQ.
//
// It is split in two:
//   - Processor (processor.go) handles one record: decode, dedup, the Handler's side
//     effect in one transaction, in-process retries, error classification. It needs only
//     Postgres.
//   - Runner (runner.go) is the franz-go poll loop: Processor per record, DLQ for records
//     that failed, offsets committed only after the DB commit (and the DLQ send).
package consumer

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/event"
)

// Handler applies one event's side effect. tx is shared with the processed_events
// insert; the caller begins and commits it. Return Permanent(err) for errors a retry
// cannot fix; any other error is treated as transient.
type Handler interface {
	Handle(ctx context.Context, tx pgx.Tx, env event.Envelope) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, tx pgx.Tx, env event.Envelope) error

func (f HandlerFunc) Handle(ctx context.Context, tx pgx.Tx, env event.Envelope) error {
	return f(ctx, tx, env)
}

type permanentError struct{ err error }

func (e permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks err as not retryable: the record goes to the DLQ right away.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent reports whether err should skip retries and go to the DLQ right away:
//   - errors marked with Permanent;
//   - event.ErrMalformed (the envelope or its data does not decode);
//   - Postgres errors caused by the data itself: SQLSTATE class 22 (data exception, e.g.
//     numeric out of range) and class 23 (integrity constraint violation, e.g. a CHECK),
//     except 23505 unique_violation, which may be a race that a retry resolves.
//
// Everything else is transient: connection errors, timeouts, serialization failures
// (40001), deadlocks (40P01), and also code bugs such as a syntax error (42xxx), so a bad
// deploy is retried and stops at the DLQ instead of skipping straight past the data.
func IsPermanent(err error) bool {
	var p permanentError
	if errors.As(err, &p) || errors.Is(err, event.ErrMalformed) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) == 5 {
		switch pgErr.Code[:2] {
		case "22":
			return true
		case "23":
			return pgErr.Code != "23505"
		}
	}
	return false
}

// DLQ header keys, added on top of the original record's headers.
const (
	HeaderDLQOriginalTopic = "dlq.original_topic"
	HeaderDLQPartition     = "dlq.partition"
	HeaderDLQOffset        = "dlq.offset"
	HeaderDLQError         = "dlq.error"
	HeaderDLQAttempts      = "dlq.attempts"
	HeaderDLQFailedAt      = "dlq.failed_at"
)

// DLQ receives records that failed permanently or ran out of retries.
type DLQ interface {
	Send(ctx context.Context, rec *kgo.Record, cause error, attempts int) error
}
