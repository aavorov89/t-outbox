// Package consumer is the generic idempotent consumer: dedup by event_id in
// processed_events, the side effect in the same transaction, retries, and DLQ.
// Processor and Runner arrive in M4; the contracts below are fixed now.
package consumer

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
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

// IsPermanent reports whether err should skip retries. Malformed envelopes are permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p) || errors.Is(err, event.ErrMalformed)
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
