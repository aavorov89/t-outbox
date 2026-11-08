// Package outbox writes events into the outbox table inside the caller's transaction
// (Queue) and publishes them to Kafka (Relay).
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/aavorov89/t-outbox/internal/event"
)

// Message is one event to enqueue. Payload becomes the envelope's "data".
//
// Invariant for callers: messages of one aggregate must be queued while holding a lock
// on that aggregate (e.g. SELECT ... FOR UPDATE on the payment row). Otherwise outbox.id
// order may differ from commit order and the relay's per-aggregate ordering guard breaks.
// UNIQUE (aggregate_type, aggregate_id, aggregate_version) catches violations.
type Message struct {
	EventID          uuid.UUID
	AggregateType    string
	AggregateID      string
	AggregateVersion int
	EventType        string
	EventVersion     int
	OccurredAt       time.Time
	Payload          json.RawMessage
}

const insertSQL = `
INSERT INTO outbox (event_id, aggregate_type, aggregate_id, aggregate_version,
                    event_type, event_version, occurred_at, payload, headers)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

// Queue validates m and appends its INSERT to b, so the business write and the outbox
// write go out in one round trip and commit together.
//
// Trace context from ctx is injected into the row's headers via the global OTel
// propagator. Until telemetry is configured (M6) that propagator is a no-op and
// headers stay empty.
func Queue(ctx context.Context, b *pgx.Batch, m Message) error {
	if err := m.toEnvelope().Validate(); err != nil {
		return fmt.Errorf("outbox: invalid message: %w", err)
	}
	headers := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, headers)
	for k := range headers {
		if event.IsReserved(k) {
			delete(headers, k)
		}
	}
	b.Queue(insertSQL,
		m.EventID, m.AggregateType, m.AggregateID, m.AggregateVersion,
		m.EventType, m.EventVersion, m.OccurredAt, m.Payload, map[string]string(headers))
	return nil
}

func (m Message) toEnvelope() event.Envelope {
	return event.Envelope{
		EventID:          m.EventID,
		EventType:        m.EventType,
		EventVersion:     m.EventVersion,
		AggregateType:    m.AggregateType,
		AggregateID:      m.AggregateID,
		AggregateVersion: m.AggregateVersion,
		OccurredAt:       m.OccurredAt,
		Data:             m.Payload,
	}
}

// Row is a claimed outbox row as the relay sees it.
type Row struct {
	ID int64
	Message
	Headers  map[string]string
	Attempts int
}

// BuildRecord turns a claimed row into the Kafka record: key = aggregate_id, value = the
// envelope, headers = row headers plus the relay-owned event_id/event_type/content-type.
func BuildRecord(topic string, r Row) (Record, error) {
	if topic == "" {
		return Record{}, errors.New("outbox: empty topic")
	}
	value, err := event.Encode(r.toEnvelope())
	if err != nil {
		return Record{}, fmt.Errorf("outbox: row %d: %w", r.ID, err)
	}
	headers := make(map[string]string, len(r.Headers)+3)
	for k, v := range r.Headers {
		if !event.IsReserved(k) {
			headers[k] = v
		}
	}
	headers[event.HeaderEventID] = r.EventID.String()
	headers[event.HeaderEventType] = r.EventType
	headers[event.HeaderContentType] = event.ContentType
	return Record{
		Topic:   topic,
		Key:     []byte(r.AggregateID),
		Value:   value,
		Headers: headers,
	}, nil
}
