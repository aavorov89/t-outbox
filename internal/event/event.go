// Package event is the wire contract between the outbox relay and consumers:
// the JSON envelope carried as the Kafka record value, and the reserved Kafka headers.
//
// It has no database or Kafka dependencies, so both producer and consumer sides import it
// without depending on each other. Changes here are contract changes: update the golden
// files in testdata (go test ./internal/event -update) and review the diff.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ContentType is the value of the content-type Kafka header.
const ContentType = "application/json"

// Kafka header keys. The relay sets the first three itself from outbox columns;
// trace headers come from outbox.headers.
const (
	HeaderEventID     = "event_id"
	HeaderEventType   = "event_type"
	HeaderContentType = "content-type"
	HeaderTraceparent = "traceparent"
	HeaderTracestate  = "tracestate"
)

// IsReserved reports whether a header key is owned by the relay and must not be
// taken from outbox.headers.
func IsReserved(key string) bool {
	switch key {
	case HeaderEventID, HeaderEventType, HeaderContentType:
		return true
	}
	return false
}

// ErrMalformed wraps every Decode/Validate failure. Consumers treat it as a permanent error.
var ErrMalformed = errors.New("event: malformed envelope")

// Envelope is the Kafka record value.
type Envelope struct {
	EventID          uuid.UUID       `json:"event_id"`
	EventType        string          `json:"event_type"`
	EventVersion     int             `json:"event_version"`
	AggregateType    string          `json:"aggregate_type"`
	AggregateID      string          `json:"aggregate_id"`
	AggregateVersion int             `json:"aggregate_version"`
	OccurredAt       time.Time       `json:"occurred_at"`
	Data             json.RawMessage `json:"data"`
}

// Validate checks the fields every envelope must carry.
func (e Envelope) Validate() error {
	var errs []error
	if e.EventID == uuid.Nil {
		errs = append(errs, errors.New("event_id is empty"))
	}
	if e.EventType == "" {
		errs = append(errs, errors.New("event_type is empty"))
	}
	if e.EventVersion < 1 {
		errs = append(errs, fmt.Errorf("event_version %d < 1", e.EventVersion))
	}
	if e.AggregateType == "" {
		errs = append(errs, errors.New("aggregate_type is empty"))
	}
	if e.AggregateID == "" {
		errs = append(errs, errors.New("aggregate_id is empty"))
	}
	if e.AggregateVersion < 1 {
		errs = append(errs, fmt.Errorf("aggregate_version %d < 1", e.AggregateVersion))
	}
	if e.OccurredAt.IsZero() {
		errs = append(errs, errors.New("occurred_at is zero"))
	}
	if !isJSONObject(e.Data) {
		errs = append(errs, errors.New("data is not a JSON object"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrMalformed, errors.Join(errs...))
	}
	return nil
}

// Encode validates and serializes the envelope. OccurredAt is normalized to UTC.
func Encode(e Envelope) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	e.OccurredAt = e.OccurredAt.UTC()
	return json.Marshal(e)
}

// Decode parses and validates an envelope. Unknown top-level fields are ignored so that
// adding a field to the envelope is a compatible change.
func Decode(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return Envelope{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if err := e.Validate(); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

// DecodeData unmarshals the envelope's data into T.
func DecodeData[T any](e Envelope) (T, error) {
	var v T
	if err := json.Unmarshal(e.Data, &v); err != nil {
		return v, fmt.Errorf("%w: data of %s v%d: %w", ErrMalformed, e.EventType, e.EventVersion, err)
	}
	return v, nil
}

func isJSONObject(b []byte) bool {
	b = bytes.TrimSpace(b)
	return len(b) >= 2 && b[0] == '{' && json.Valid(b)
}
