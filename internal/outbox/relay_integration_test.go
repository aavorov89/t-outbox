//go:build integration

package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aavorov89/t-outbox/internal/event"
	"github.com/aavorov89/t-outbox/internal/kafkax"
	"github.com/aavorov89/t-outbox/internal/outbox"
	"github.com/aavorov89/t-outbox/internal/outbox/outboxtest"
	"github.com/aavorov89/t-outbox/internal/testenv"
)

// ---- helpers ----

func relayPool(t *testing.T, configure ...func(*pgxpool.Config)) *pgxpool.Pool {
	t.Helper()
	return env.PG.NewPool(t, func(c *pgxpool.Config) {
		outbox.ConfigurePool(c)
		for _, f := range configure {
			f(c)
		}
	})
}

func insert(t *testing.T, pool *pgxpool.Pool, msgs ...outbox.Message) {
	t.Helper()
	require.NoError(t, pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error { return queue(t, tx, msgs...) }))
}

func pendingIDs(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT aggregate_id || ':' || aggregate_version FROM outbox WHERE published_at IS NULL ORDER BY id`)
	require.NoError(t, err)
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	return ids
}

type rowState struct {
	Key       string // aggregate_id:version
	Attempts  int
	LastError *string
	Published bool
	DueIn     time.Duration // next_attempt_at - now()
}

func states(t *testing.T, pool *pgxpool.Pool) map[string]rowState {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT aggregate_id || ':' || aggregate_version, attempts, last_error, published_at IS NOT NULL,
		       (extract(epoch FROM next_attempt_at - now()) * 1000)::bigint
		FROM outbox ORDER BY id`)
	require.NoError(t, err)
	out := map[string]rowState{}
	for rows.Next() {
		var s rowState
		var dueMs int64
		require.NoError(t, rows.Scan(&s.Key, &s.Attempts, &s.LastError, &s.Published, &dueMs))
		s.DueIn = time.Duration(dueMs) * time.Millisecond
		out[s.Key] = s
	}
	require.NoError(t, rows.Err())
	return out
}

func keys(recs []outbox.Record) []string {
	var out []string
	for _, r := range recs {
		e, err := event.Decode(r.Value)
		if err != nil {
			panic(err)
		}
		out = append(out, fmt.Sprintf("%s:%d", e.AggregateID, e.AggregateVersion))
	}
	return out
}

func failKeys(err error, aggregateIDs ...string) func(outbox.Record) error {
	return func(rec outbox.Record) error {
		if slices.Contains(aggregateIDs, string(rec.Key)) {
			return err
		}
		return nil
	}
}

type producerFunc func(ctx context.Context, recs []outbox.Record) []error

func (f producerFunc) Produce(ctx context.Context, recs []outbox.Record) []error { return f(ctx, recs) }

var noBackoff = func(int) time.Duration { return 0 }

// ---- single relay ----

// One cycle publishes claimable rows in outbox.id order, bounded by the batch size, and
// marks only the acked ones. A failed record is rescheduled with backoff and its error
// recorded; it is not retried before it is due.
func TestRelay_PublishesMarksAndReschedules(t *testing.T) {
	t.Parallel()
	pool := relayPool(t)
	ctx := context.Background()
	insert(t, pool, message("a", 1, `{"n":1}`), message("b", 1, `{"n":2}`), message("c", 1, `{"n":3}`))

	brokerDown := errors.New("broker down")
	p := &outboxtest.FakeProducer{Fail: failKeys(brokerDown, "b")}
	relay := outbox.NewRelay(pool, p, outbox.Config{
		Topic: "payments.events", BatchSize: 2,
		Backoff: func(attempt int) time.Duration { return time.Duration(attempt) * time.Hour },
	})

	n, err := relay.RunOnce(ctx)
	require.Equal(t, 2, n, "batch size bounds one cycle")
	var pubErr *outbox.PublishError
	require.ErrorAs(t, err, &pubErr)
	require.Equal(t, 1, pubErr.Failed)
	require.ErrorIs(t, err, brokerDown)
	require.Equal(t, []string{"b:1", "c:1"}, pendingIDs(t, pool), "a is published, b failed, c not claimed yet")

	b := states(t, pool)["b:1"]
	require.Equal(t, 1, b.Attempts)
	require.NotNil(t, b.LastError)
	require.Contains(t, *b.LastError, "broker down")
	require.InDelta(t, time.Hour, b.DueIn, float64(time.Minute), "next_attempt_at = now + Backoff(1)")

	p.Fail = nil
	n, err = relay.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "b is not due yet, only c is claimed")
	require.Equal(t, []string{"b:1"}, pendingIDs(t, pool))

	_, err = pool.Exec(ctx, `UPDATE outbox SET next_attempt_at = now()`)
	require.NoError(t, err)
	n, err = relay.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Empty(t, pendingIDs(t, pool))

	n, err = relay.RunOnce(ctx)
	require.NoError(t, err)
	require.Zero(t, n, "nothing left")

	for _, r := range p.Records() {
		require.Equal(t, "payments.events", r.Topic)
	}
	require.Equal(t, []string{"a:1", "c:1", "b:1"}, keys(p.Records()))
}

// The ordering guard: one claim takes only the oldest unpublished event of each
// aggregate, however large the batch. A failing head blocks its own aggregate only.
func TestRelay_OrderingGuard(t *testing.T) {
	t.Parallel()
	pool := relayPool(t)
	ctx := context.Background()
	insert(t, pool, message("x", 1, `{}`), message("x", 2, `{}`), message("y", 1, `{}`), message("x", 3, `{}`))

	p := &outboxtest.FakeProducer{}
	relay := outbox.NewRelay(pool, p, outbox.Config{Topic: "t", BatchSize: 10, Backoff: noBackoff})

	n, err := relay.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []string{"x:1", "y:1"}, keys(p.Records()))

	insert(t, pool, message("y", 2, `{}`))
	p.Fail = failKeys(errors.New("nope"), "x")
	for range 3 {
		_, _ = relay.RunOnce(ctx)
	}
	require.Equal(t, []string{"x:2", "x:3"}, pendingIDs(t, pool), "x:2 keeps failing; x:3 waits behind it; y moves on")
	require.Equal(t, 3, states(t, pool)["x:2"].Attempts)
	require.Zero(t, states(t, pool)["x:3"].Attempts, "never claimed")

	p.Fail = nil
	for range 2 {
		_, err = relay.RunOnce(ctx)
		require.NoError(t, err)
	}
	require.Empty(t, pendingIDs(t, pool))
	require.Equal(t, []string{"x:1", "y:1", "y:2", "x:2", "x:3"}, keys(p.Records()))
}

// A row the relay cannot turn into a record (here: written by hand, bypassing Queue's
// validation) fails alone: it is rescheduled, and the rest of the batch is published.
func TestRelay_UnbuildableRowFailsAlone(t *testing.T) {
	t.Parallel()
	pool := relayPool(t)
	ctx := context.Background()
	insert(t, pool, message("a", 1, `{}`))
	_, err := pool.Exec(ctx, `
		INSERT INTO outbox (event_id, aggregate_type, aggregate_id, aggregate_version, event_type,
		                    event_version, occurred_at, payload)
		VALUES (gen_random_uuid(), 'payment', 'bad', 1, '', 1, now(), '{}')`)
	require.NoError(t, err)

	p := &outboxtest.FakeProducer{}
	n, err := outbox.NewRelay(pool, p, outbox.Config{Topic: "t"}).RunOnce(ctx)
	require.Equal(t, 2, n)
	require.ErrorIs(t, err, event.ErrMalformed)
	require.Equal(t, []string{"a:1"}, keys(p.Records()))
	bad := states(t, pool)["bad:1"]
	require.Equal(t, 1, bad.Attempts)
	require.Contains(t, *bad.LastError, "event_type is empty")
}

// ---- timeouts ----

// Run refuses a pool whose idle_in_transaction_session_timeout would kill the claiming tx
// of a publish that is slow but within PublishTimeout.
func TestRelay_RefusesIdleTimeoutBelowPublishTimeout(t *testing.T) {
	t.Parallel()
	pool := relayPool(t, func(c *pgxpool.Config) {
		c.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "1000"
	})
	relay := outbox.NewRelay(pool, &outboxtest.FakeProducer{}, outbox.Config{Topic: "t", PublishTimeout: 2 * time.Second})
	err := relay.Run(context.Background())
	require.ErrorContains(t, err, "must be greater than PublishTimeout")
}

// Why the timeouts must be ordered: a publish that outlives the server's idle timeout
// lands in Kafka, but the server has killed the claiming session, so the mark is lost and
// the row is re-published by the next cycle. Safe (at-least-once), but a duplicate.
func TestRelay_PublishPastIdleTimeoutRollsBack(t *testing.T) {
	t.Parallel()
	pool := relayPool(t, func(c *pgxpool.Config) {
		c.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "300"
	})
	ctx := context.Background()
	insert(t, pool, message("a", 1, `{}`))

	fake := &outboxtest.FakeProducer{}
	slow := producerFunc(func(ctx context.Context, recs []outbox.Record) []error {
		time.Sleep(time.Second) // a producer that does not honor ctx, slower than the server timeout
		return fake.Produce(ctx, recs)
	})
	_, err := outbox.NewRelay(pool, slow, outbox.Config{Topic: "t"}).RunOnce(ctx)
	require.Error(t, err)
	require.NotErrorAs(t, err, new(*outbox.PublishError), "the tx died: nothing recorded")
	require.Len(t, fake.Records(), 1, "the record did land")
	a := states(t, pool)["a:1"]
	require.False(t, a.Published)
	require.Zero(t, a.Attempts, "rolled back with the session")

	n, err := outbox.NewRelay(pool, fake, outbox.Config{Topic: "t"}).RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []string{"a:1", "a:1"}, keys(fake.Records()), "re-published: a duplicate for the consumer to drop")
}

// ---- graceful shutdown ----

// gatedProducer blocks in Produce until released or ctx is done.
type gatedProducer struct {
	outboxtest.FakeProducer
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatedProducer() *gatedProducer {
	return &gatedProducer{entered: make(chan struct{}), release: make(chan struct{})}
}

func (p *gatedProducer) Produce(ctx context.Context, recs []outbox.Record) []error {
	p.once.Do(func() { close(p.entered) })
	select {
	case <-p.release:
	case <-ctx.Done():
	}
	return p.FakeProducer.Produce(ctx, recs)
}

func startRelay(t *testing.T, relay *outbox.Relay) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() { cancel(); err = <-done })
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// SIGTERM mid-batch: Run stops claiming but lets the in-flight batch publish and commit,
// so a clean shutdown causes neither a loss nor a duplicate.
func TestRelay_ShutdownFinishesInFlightBatch(t *testing.T) {
	testenv.NoLeaks(t)
	pool := relayPool(t)
	insert(t, pool, message("a", 1, `{}`), message("b", 1, `{}`))

	p := newGatedProducer()
	stop := startRelay(t, outbox.NewRelay(pool, p, outbox.Config{Topic: "t", PollInterval: 10 * time.Millisecond}))
	<-p.entered

	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	select {
	case err := <-stopped:
		t.Fatalf("Run returned (%v) while its batch was still publishing", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(p.release)
	require.NoError(t, <-stopped)

	require.Empty(t, pendingIDs(t, pool), "the in-flight batch was committed")
	require.Len(t, p.Records(), 2)
}

// A batch that does not finish within DrainTimeout is abandoned: the tx rolls back, the
// rows stay pending with no attempt recorded, and Run still returns promptly.
func TestRelay_ShutdownDrainTimeoutAbandonsBatch(t *testing.T) {
	testenv.NoLeaks(t)
	pool := relayPool(t)
	insert(t, pool, message("a", 1, `{}`))

	p := newGatedProducer() // never released
	stop := startRelay(t, outbox.NewRelay(pool, p, outbox.Config{Topic: "t", DrainTimeout: 300 * time.Millisecond}))
	<-p.entered

	start := time.Now()
	require.NoError(t, stop())
	require.Less(t, time.Since(start), 3*time.Second)
	a := states(t, pool)["a:1"]
	require.False(t, a.Published)
	require.Zero(t, a.Attempts, "shutdown is not a broker failure")
}

// ---- failure matrix ----

// F4: 8 workers, 1,000 events. No row is ever claimed by two workers at the same time (the
// in-flight sets recorded via Hooks.AfterClaim never intersect), every event is published
// exactly once, and each aggregate's events in version order. Run with -race.
func TestRelay_ConcurrentWorkersNoDoubleClaim(t *testing.T) {
	t.Parallel()
	const workers, aggregates, perAggregate = 8, 200, 5
	const total = aggregates * perAggregate
	pool := relayPool(t, func(c *pgxpool.Config) { c.MaxConns = workers * 2 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var msgs []outbox.Message
	for v := 1; v <= perAggregate; v++ { // interleaved: x1 y1 ... x2 y2 ...
		for a := range aggregates {
			msgs = append(msgs, message(fmt.Sprintf("agg-%03d", a), v, `{}`))
		}
	}
	insert(t, pool, msgs...)

	var (
		mu       sync.Mutex
		inFlight = map[int64]int{} // row id → worker
		overlaps []string
		claims   = make([]int, workers)
	)
	p := &outboxtest.FakeProducer{}
	var wg sync.WaitGroup
	for w := range workers {
		var mine []int64
		relay := outbox.NewRelay(pool, p, outbox.Config{
			Topic: "t", BatchSize: 25,
			Hooks: outbox.Hooks{AfterClaim: func(_ context.Context, ids []int64) {
				mu.Lock()
				for _, id := range ids {
					if other, busy := inFlight[id]; busy {
						overlaps = append(overlaps, fmt.Sprintf("row %d: workers %d and %d", id, other, w))
					}
					inFlight[id] = w
				}
				claims[w] += len(ids)
				mu.Unlock()
				mine = ids
				time.Sleep(time.Duration(rand.IntN(1000)) * time.Microsecond) // widen the window
			}},
		})
		wg.Go(func() {
			for ctx.Err() == nil {
				n, err := relay.RunOnce(ctx)
				if !assert.NoError(t, err) {
					return
				}
				mu.Lock() // after COMMIT: no longer in flight
				for _, id := range mine {
					delete(inFlight, id)
				}
				mu.Unlock()
				mine = nil
				if n > 0 {
					continue
				}
				var pending int
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&pending); err != nil || pending == 0 {
					return
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
	wg.Wait()
	require.NoError(t, ctx.Err())
	t.Logf("rows claimed per worker: %v", claims)

	require.Empty(t, overlaps)
	require.Empty(t, pendingIDs(t, pool))
	got := keys(p.Records())
	require.Len(t, got, total, "each event published exactly once")
	seen := map[string]int{} // aggregate → last version
	for _, k := range got {
		agg, v := splitKey(t, k)
		require.Equal(t, seen[agg]+1, v, "aggregate %s out of order", agg)
		seen[agg] = v
	}
}

// splitKey splits "aggregate:version".
func splitKey(t *testing.T, k string) (string, int) {
	t.Helper()
	i := strings.LastIndexByte(k, ':')
	v, err := strconv.Atoi(k[i+1:])
	require.NoError(t, err)
	return k[:i], v
}

// F6: while the broker is down, rows stay pending; attempts grows, last_error is set and
// the next event of a failing aggregate is not even claimed. After recovery everything is
// published, in order per aggregate.
//
// Not parallel: it pauses the broker shared by this package.
func TestRelay_BrokerOutage(t *testing.T) {
	testenv.NoLeaks(t)
	const deliveryTimeout = 2 * time.Second
	pool := relayPool(t)
	topic := env.Kafka.NewTopic(t, 3)
	cl := env.Kafka.NewClient(t, kafkax.ProducerOpts(env.Kafka.Brokers, deliveryTimeout)...)
	relay := outbox.NewRelay(pool, kafkax.NewProducer(cl), outbox.Config{
		Topic:          topic,
		PollInterval:   50 * time.Millisecond,
		PublishTimeout: deliveryTimeout + time.Second,
		Backoff:        outbox.ExpBackoff(100*time.Millisecond, 500*time.Millisecond, nil),
	})
	ctx := context.Background()

	insert(t, pool, message("warmup", 1, `{}`))
	n, err := relay.RunOnce(ctx) // client has metadata and a producer id before the outage
	require.NoError(t, err)
	require.Equal(t, 1, n)

	env.Kafka.Pause(t)
	insert(t, pool, message("a", 1, `{}`), message("a", 2, `{}`), message("b", 1, `{}`))
	stop := startRelay(t, relay)

	require.Eventually(t, func() bool {
		s := states(t, pool)
		return s["a:1"].Attempts >= 2 && s["b:1"].Attempts >= 2
	}, 30*time.Second, 100*time.Millisecond, "failed publishes must be recorded and retried")
	s := states(t, pool)
	for _, k := range []string{"a:1", "a:2", "b:1"} {
		require.False(t, s[k].Published, k)
	}
	require.NotNil(t, s["a:1"].LastError)
	t.Logf("last_error while paused: %s", *s["a:1"].LastError)
	require.Zero(t, s["a:2"].Attempts, "a:2 waits behind a:1")

	env.Kafka.Unpause(t)
	require.Eventually(t, func() bool { return len(pendingIDs(t, pool)) == 0 },
		60*time.Second, 100*time.Millisecond, "everything is published after recovery")
	require.NoError(t, stop())

	// Records failed in flight may still have landed (see kafkax.ProducerOpts), so Kafka
	// can hold duplicates; the first occurrences must be complete and in order.
	recs := env.Kafka.TryConsumeN(t, topic, 1000, 5*time.Second)
	firstSeen := map[string][]int{}
	dups := map[string]bool{}
	for _, r := range recs {
		e, err := event.Decode(r.Value)
		require.NoError(t, err)
		if dups[e.EventID.String()] {
			continue
		}
		dups[e.EventID.String()] = true
		firstSeen[e.AggregateID] = append(firstSeen[e.AggregateID], e.AggregateVersion)
	}
	t.Logf("%d records in Kafka for 4 events", len(recs))
	require.Equal(t, map[string][]int{"warmup": {1}, "a": {1, 2}, "b": {1}}, firstSeen)
}

// flakyProducer fails a share of records before they reach Kafka.
type flakyProducer struct {
	inner outbox.Producer
	rate  float64
}

func (p flakyProducer) Produce(ctx context.Context, recs []outbox.Record) []error {
	errs := make([]error, len(recs))
	var pass []outbox.Record
	var idx []int
	for i, r := range recs {
		if rand.Float64() < p.rate {
			errs[i] = errors.New("injected failure")
			continue
		}
		pass = append(pass, r)
		idx = append(idx, i)
	}
	if len(pass) > 0 {
		for j, err := range p.inner.Produce(ctx, pass) {
			errs[idx[j]] = err
		}
	}
	return errs
}

// F9: 20 aggregates × 10 events, written while 4 relays run, with 30% of publishes failing
// at random. In Kafka every aggregate's events sit in one partition, in version order,
// each exactly once. Without the NOT EXISTS guard a relay would publish v+1 while v is
// backing off after a failure.
func TestRelay_PerAggregateOrdering(t *testing.T) {
	t.Parallel()
	const relays, aggregates, versions = 4, 20, 10
	pool := relayPool(t, func(c *pgxpool.Config) { c.MaxConns = relays + 4 })
	topic := env.Kafka.NewTopic(t, 4)
	cl := env.Kafka.NewClient(t, kafkax.ProducerOpts(env.Kafka.Brokers, 5*time.Second)...)
	producer := flakyProducer{inner: kafkax.NewProducer(cl), rate: 0.3}

	var stops []func() error
	for range relays {
		stops = append(stops, startRelay(t, outbox.NewRelay(pool, producer, outbox.Config{
			Topic:        topic,
			BatchSize:    8,
			PollInterval: 10 * time.Millisecond,
			Backoff:      outbox.ExpBackoff(5*time.Millisecond, 50*time.Millisecond, nil),
			Logger:       slog.New(slog.DiscardHandler), // the injected failures are noise
		})))
	}

	ctx := context.Background()
	for v := 1; v <= versions; v++ { // one tx per version, committed in order: the writer invariant
		require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			b := &pgx.Batch{}
			for a := range aggregates {
				if err := outbox.Queue(ctx, b, message(fmt.Sprintf("agg-%02d", a), v, `{}`)); err != nil {
					return err
				}
			}
			return tx.SendBatch(ctx, b).Close()
		}))
		time.Sleep(time.Duration(rand.IntN(20)) * time.Millisecond)
	}
	require.Eventually(t, func() bool { return len(pendingIDs(t, pool)) == 0 },
		60*time.Second, 50*time.Millisecond)
	for _, stop := range stops {
		require.NoError(t, stop())
	}

	var retried int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE attempts > 0`).Scan(&retried))
	t.Logf("%d of %d events needed retries", retried, aggregates*versions)
	require.Positive(t, retried, "the test must exercise failures")

	recs := env.Kafka.ConsumeN(t, topic, aggregates*versions, 30*time.Second)
	type pos struct {
		partition int32
		offset    int64
		version   int
	}
	byKey := map[string][]pos{}
	for _, r := range recs {
		e, err := event.Decode(r.Value)
		require.NoError(t, err)
		byKey[string(r.Key)] = append(byKey[string(r.Key)], pos{r.Partition, r.Offset, e.AggregateVersion})
	}
	require.Len(t, byKey, aggregates)
	want := make([]int, versions)
	for i := range want {
		want[i] = i + 1
	}
	for key, ps := range byKey {
		slices.SortFunc(ps, func(a, b pos) int { return int(a.offset - b.offset) })
		var got []int
		for _, p := range ps {
			require.Equal(t, ps[0].partition, p.partition, "%s: one key, one partition", key)
			got = append(got, p.version)
		}
		require.Equal(t, want, got, "%s: versions in partition order", key)
	}
}
