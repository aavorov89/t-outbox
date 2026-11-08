//go:build integration

package outbox_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/aavorov89/t-outbox/internal/event"
	"github.com/aavorov89/t-outbox/internal/outbox"
	"github.com/aavorov89/t-outbox/internal/outbox/outboxtest"
)

func pendingIDs(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT aggregate_id FROM outbox WHERE published_at IS NULL ORDER BY id`)
	require.NoError(t, err)
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	return ids
}

// M2 relay: one cycle publishes pending rows in outbox.id order, in batches, and marks only
// the acked ones. A failed record stays pending and is published by a later cycle.
func TestRelay_PublishesAndMarks(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	ctx := context.Background()
	require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return queue(t, tx,
			message("a", 1, `{"n":1}`), message("b", 1, `{"n":2}`), message("c", 1, `{"n":3}`))
	}))

	brokerDown := errors.New("broker down")
	p := &outboxtest.FakeProducer{Fail: func(rec outbox.Record) error {
		if string(rec.Key) == "b" {
			return brokerDown
		}
		return nil
	}}
	relay := outbox.NewRelay(pool, p, outbox.Config{Topic: "payments.events", BatchSize: 2})

	n, err := relay.RunOnce(ctx)
	require.Equal(t, 2, n, "batch size bounds one cycle")
	require.ErrorIs(t, err, brokerDown)
	require.Equal(t, []string{"b", "c"}, pendingIDs(t, pool), "a is published, b failed, c not read yet")

	p.Fail = nil
	n, err = relay.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Empty(t, pendingIDs(t, pool))

	n, err = relay.RunOnce(ctx)
	require.NoError(t, err)
	require.Zero(t, n, "nothing left")

	recs := p.Records()
	var keys []string
	for _, r := range recs {
		keys = append(keys, string(r.Key))
		require.Equal(t, "payments.events", r.Topic)
		require.Equal(t, event.TypePaymentCaptured, r.Headers[event.HeaderEventType])
	}
	require.Equal(t, []string{"a", "b", "c"}, keys)
}

// F4: 8 workers, 1,000 events. Every event_id is published; in-flight ID sets recorded via
// Hooks.AfterClaim never intersect. Run with -race.
func TestRelay_ConcurrentWorkersNoDoubleClaim(t *testing.T) { t.Skip("M3") }

// F6: env.Kafka.Pause → attempts > 0, published_at IS NULL, last_error set →
// env.Kafka.Unpause → require.Eventually all published.
func TestRelay_BrokerOutage(t *testing.T) { t.Skip("M3") }

// F9: events of one aggregate spread across N relays are published in aggregate_version
// order (topic with several partitions; check order within the key's partition).
func TestRelay_PerAggregateOrdering(t *testing.T) { t.Skip("M3") }
