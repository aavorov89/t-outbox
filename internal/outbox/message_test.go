package outbox

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/aavorov89/t-outbox/internal/event"
)

func validMessage() Message {
	return Message{
		EventID:          uuid.Must(uuid.NewV7()),
		AggregateType:    event.AggregatePayment,
		AggregateID:      "agg-1",
		AggregateVersion: 1,
		EventType:        event.TypePaymentCreated,
		EventVersion:     1,
		OccurredAt:       time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC),
		Payload:          json.RawMessage(`{"payment_id":"x"}`),
	}
}

func TestQueueRejectsInvalid(t *testing.T) {
	tests := map[string]func(*Message){
		"nil event id":       func(m *Message) { m.EventID = uuid.Nil },
		"no aggregate id":    func(m *Message) { m.AggregateID = "" },
		"version 0":          func(m *Message) { m.AggregateVersion = 0 },
		"event version 0":    func(m *Message) { m.EventVersion = 0 },
		"zero occurred_at":   func(m *Message) { m.OccurredAt = time.Time{} },
		"payload not object": func(m *Message) { m.Payload = json.RawMessage(`[1]`) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			m := validMessage()
			mutate(&m)
			b := &pgx.Batch{}
			require.Error(t, Queue(context.Background(), b, m))
			require.Zero(t, b.Len(), "nothing must be queued on error")
		})
	}
}

func TestQueueAppendsOneStatement(t *testing.T) {
	b := &pgx.Batch{}
	require.NoError(t, Queue(context.Background(), b, validMessage()))
	require.Equal(t, 1, b.Len())
}

func TestBuildRecord(t *testing.T) {
	row := Row{
		ID:      7,
		Message: validMessage(),
		Headers: map[string]string{
			event.HeaderTraceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			event.HeaderEventID:     "forged", // reserved: must be overwritten
		},
	}
	rec, err := BuildRecord("payments.events", row)
	require.NoError(t, err)

	require.Equal(t, "payments.events", rec.Topic)
	require.Equal(t, "agg-1", string(rec.Key))
	require.Equal(t, map[string]string{
		event.HeaderTraceparent: row.Headers[event.HeaderTraceparent],
		event.HeaderEventID:     row.EventID.String(),
		event.HeaderEventType:   event.TypePaymentCreated,
		event.HeaderContentType: event.ContentType,
	}, rec.Headers)

	env, err := event.Decode(rec.Value)
	require.NoError(t, err)
	require.Equal(t, row.EventID, env.EventID)
	require.Equal(t, row.AggregateVersion, env.AggregateVersion)
	require.JSONEq(t, string(row.Payload), string(env.Data))
}

func TestBuildRecordRejectsEmptyTopic(t *testing.T) {
	_, err := BuildRecord("", Row{Message: validMessage()})
	require.Error(t, err)
}
