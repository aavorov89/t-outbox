//go:build integration

package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/aavorov89/t-outbox/internal/event"
	"github.com/aavorov89/t-outbox/internal/outbox"
	"github.com/aavorov89/t-outbox/internal/testenv"
)

var env *testenv.Env

func TestMain(m *testing.M) {
	testenv.Main(m, &env, testenv.WithPostgres())
}

func message(aggID string, version int, payload string) outbox.Message {
	return outbox.Message{
		EventID:          uuid.Must(uuid.NewV7()),
		AggregateType:    event.AggregatePayment,
		AggregateID:      aggID,
		AggregateVersion: version,
		EventType:        event.TypePaymentCaptured,
		EventVersion:     1,
		OccurredAt:       time.Date(2026, 11, 8, 12, 0, 0, 123456000, time.UTC),
		Payload:          json.RawMessage(payload),
	}
}

func queue(t *testing.T, tx pgx.Tx, msgs ...outbox.Message) error {
	t.Helper()
	b := &pgx.Batch{}
	for _, m := range msgs {
		require.NoError(t, outbox.Queue(context.Background(), b, m))
	}
	return tx.SendBatch(context.Background(), b).Close()
}

// What Queue writes, read back the way the relay will, must build a valid record —
// including amounts above 2^53 that pass through JSONB.
func TestQueue_RoundTripThroughJSONB(t *testing.T) {
	pool := env.PG.NewPool(t, nil)
	ctx := context.Background()
	const big = int64(1<<53 + 1)
	m := message("agg-1", 1, `{"amount":{"amount_minor":9007199254740993,"currency":"EUR"},"payment_id":"0192f0c1-1111-7222-8333-444455556666","merchant_id":"m_42"}`)

	require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return queue(t, tx, m) }))

	var row outbox.Row
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT id, event_id, aggregate_type, aggregate_id, aggregate_version, event_type,
		       event_version, occurred_at, payload, headers, attempts
		FROM outbox`).Scan(
		&row.ID, &row.EventID, &row.AggregateType, &row.AggregateID, &row.AggregateVersion, &row.EventType,
		&row.EventVersion, &row.OccurredAt, &row.Payload, &row.Headers, &row.Attempts))

	require.Equal(t, m.EventID, row.EventID)
	require.True(t, m.OccurredAt.Equal(row.OccurredAt))
	require.Empty(t, row.Headers, "no propagator configured yet → no trace headers")

	rec, err := outbox.BuildRecord("payments.events", row)
	require.NoError(t, err)
	env, err := event.Decode(rec.Value)
	require.NoError(t, err)
	data, err := event.DecodeData[event.PaymentCapturedV1](env)
	require.NoError(t, err)
	require.Equal(t, big, data.Amount.AmountMinor)
}

// Two events with the same aggregate version are a contract violation the DB rejects,
// and the whole batch rolls back.
func TestQueue_DuplicateAggregateVersionRejected(t *testing.T) {
	pool := env.PG.NewPool(t, nil)
	ctx := context.Background()

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return queue(t, tx, message("agg-1", 1, `{}`), message("agg-1", 1, `{}`))
	})
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "err = %v", err)
	require.Equal(t, "23505", pgErr.Code) // unique_violation

	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&n))
	require.Zero(t, n)
}
