package consumer

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/event"
)

// EnvelopeFunc handles one decoded record.
type EnvelopeFunc func(ctx context.Context, rec *kgo.Record, env event.Envelope) error

// Poll is the M2 consumer loop: poll, decode the envelope, call fn, mark the record for
// commit. It returns nil when ctx is cancelled or the client is closed.
//
// Deliberately naive. No processed_events dedup, no retries, no DLQ: a malformed record
// or an fn error is logged and skipped. M4 replaces this with Processor/Runner.
//
// Offsets are committed only for records already handled: cl must be built with
// kgo.AutoCommitMarks (kafkax.ConsumerOpts does that), and a record is marked after fn
// returns. A crash therefore redelivers, never skips.
func Poll(ctx context.Context, cl *kgo.Client, fn EnvelopeFunc, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil // cancellation is the normal way to stop
		default:
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.WarnContext(ctx, "kafka fetch failed", "topic", topic, "partition", partition, "err", err)
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			env, err := event.Decode(rec.Value)
			if err == nil {
				err = fn(ctx, rec, env)
			}
			if err != nil {
				log.ErrorContext(ctx, "skipping record (no retries/DLQ until M4)",
					"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", err)
			}
			cl.MarkCommitRecords(rec)
		})
	}
}

// Log returns an EnvelopeFunc that only logs the event.
func Log(log *slog.Logger) EnvelopeFunc {
	return func(ctx context.Context, rec *kgo.Record, env event.Envelope) error {
		log.InfoContext(ctx, "event received",
			"event_id", env.EventID,
			"event_type", env.EventType,
			"event_version", env.EventVersion,
			"aggregate_id", env.AggregateID,
			"aggregate_version", env.AggregateVersion,
			"occurred_at", env.OccurredAt,
			"topic", rec.Topic,
			"partition", rec.Partition,
			"offset", rec.Offset,
			"data", string(env.Data),
		)
		return nil
	}
}
