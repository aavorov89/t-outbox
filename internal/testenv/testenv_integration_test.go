//go:build integration

package testenv_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/kafkax"
	"github.com/aavorov89/t-outbox/internal/outbox"
	"github.com/aavorov89/t-outbox/internal/testenv"
)

var env *testenv.Env

func TestMain(m *testing.M) {
	testenv.Main(m, &env, testenv.WithPostgres(), testenv.WithKafka())
}

// The template must carry the full schema into every per-test database.
func TestPostgres_NewDatabaseIsMigrated(t *testing.T) {
	pool := env.PG.NewPool(t, nil)
	var tables []string
	rows, err := pool.Query(context.Background(),
		`SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY 1`)
	require.NoError(t, err)
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		tables = append(tables, n)
	}
	require.NoError(t, rows.Err())
	require.Subset(t, tables, []string{"payments", "outbox", "processed_events", "ledger_entries", "goose_db_version"})
}

// Parallel tests get disjoint databases.
func TestPostgres_ParallelDatabasesAreIsolated(t *testing.T) {
	for i := range 8 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			pool := env.PG.NewPool(t, nil)
			ctx := context.Background()
			_, err := pool.Exec(ctx, `INSERT INTO processed_events (consumer, event_id) VALUES ('smoke', gen_random_uuid())`)
			require.NoError(t, err)
			var n int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM processed_events`).Scan(&n))
			require.Equal(t, 1, n)
		})
	}
}

// Round trip through the real outbox.Producer implementation.
func TestKafka_ProduceConsume(t *testing.T) {
	testenv.NoLeaks(t)
	topic := env.Kafka.NewTopic(t, 3)
	cl := env.Kafka.NewClient(t, kafkax.ProducerOpts(env.Kafka.Brokers, 5*time.Second)...)
	p := kafkax.NewProducer(cl)

	errs := p.Produce(context.Background(), []outbox.Record{
		{Topic: topic, Key: []byte("a"), Value: []byte(`{"n":1}`), Headers: map[string]string{"event_id": "e1"}},
		{Topic: topic, Key: []byte("a"), Value: []byte(`{"n":2}`), Headers: map[string]string{"event_id": "e2"}},
	})
	require.Equal(t, []error{nil, nil}, errs)

	recs := env.Kafka.ConsumeN(t, topic, 2, 15*time.Second)
	require.Equal(t, "a", string(recs[0].Key))
	require.Equal(t, recs[0].Partition, recs[1].Partition, "same key → same partition")
	require.Equal(t, `{"n":1}`, string(recs[0].Value), "per-key order")
	require.Equal(t, []kgo.RecordHeader{{Key: "event_id", Value: []byte("e1")}}, recs[0].Headers)
}

// The F6 technique: while paused, produce fails within the delivery timeout; after
// unpause, the same client recovers without a new address.
func TestKafka_PauseUnpause(t *testing.T) {
	const deliveryTimeout = 3 * time.Second
	topic := env.Kafka.NewTopic(t, 1)
	cl := env.Kafka.NewClient(t, kafkax.ProducerOpts(env.Kafka.Brokers, deliveryTimeout)...)
	p := kafkax.NewProducer(cl)
	rec := []outbox.Record{{Topic: topic, Key: []byte("k"), Value: []byte("v")}}
	ctx := context.Background()

	require.NoError(t, p.Produce(ctx, rec)[0], "warm-up produce")

	env.Kafka.Pause(t)
	start := time.Now()
	require.Error(t, p.Produce(ctx, rec)[0], "produce must fail while broker is paused")
	elapsed := time.Since(start)
	t.Logf("produce failed after %v while paused", elapsed)
	require.Less(t, elapsed, 2*deliveryTimeout, "failure must be bounded by the delivery timeout")
	env.Kafka.Unpause(t)

	require.Eventually(t, func() bool { return p.Produce(ctx, rec)[0] == nil },
		30*time.Second, 200*time.Millisecond, "produce must recover after unpause")
}
