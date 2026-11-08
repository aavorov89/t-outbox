//go:build integration

package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/backoff"
	"github.com/aavorov89/t-outbox/internal/consumer"
	"github.com/aavorov89/t-outbox/internal/consumer/consumertest"
	"github.com/aavorov89/t-outbox/internal/event"
	"github.com/aavorov89/t-outbox/internal/kafkax"
	"github.com/aavorov89/t-outbox/internal/ledger"
	"github.com/aavorov89/t-outbox/internal/testenv"
)

var env *testenv.Env

func TestMain(m *testing.M) {
	testenv.Main(m, &env, testenv.WithPostgres(), testenv.WithKafka())
}

// ---- helpers ----

func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// captured returns a payment.captured record for topic. Its key is the payment id.
func captured(t *testing.T, topic string, amountMinor int64, currency string) (*kgo.Record, uuid.UUID) {
	t.Helper()
	pid := uuid.New()
	data, err := json.Marshal(event.PaymentCapturedV1{PaymentID: pid, MerchantID: "m_42",
		Amount: event.Amount{AmountMinor: amountMinor, Currency: currency}})
	require.NoError(t, err)
	e := event.Envelope{
		EventID: uuid.New(), EventType: event.TypePaymentCaptured, EventVersion: 1,
		AggregateType: event.AggregatePayment, AggregateID: pid.String(), AggregateVersion: 2,
		OccurredAt: time.Now(), Data: data,
	}
	value, err := event.Encode(e)
	require.NoError(t, err)
	return &kgo.Record{Topic: topic, Key: []byte(pid.String()), Value: value, Headers: []kgo.RecordHeader{
		{Key: event.HeaderEventID, Value: []byte(e.EventID.String())},
		{Key: event.HeaderTraceparent, Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
	}}, e.EventID
}

func produce(t *testing.T, recs ...*kgo.Record) {
	t.Helper()
	cl := env.Kafka.NewClient(t, kafkax.ProducerOpts(env.Kafka.Brokers, 5*time.Second)...)
	require.NoError(t, cl.ProduceSync(context.Background(), recs...).FirstErr())
}

type counts struct{ Processed, Transactions, Entries int }

func countRows(t *testing.T, pool *pgxpool.Pool) counts {
	t.Helper()
	var c counts
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT (SELECT count(*) FROM processed_events),
		       (SELECT count(DISTINCT transaction_id) FROM ledger_entries),
		       (SELECT count(*) FROM ledger_entries)`).Scan(&c.Processed, &c.Transactions, &c.Entries))
	return c
}

func requireBalanced(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	require.NoError(t, ledger.CheckBalanced(context.Background(), pool))
}

func fastProcessor(pool *pgxpool.Pool, h consumer.Handler, log *slog.Logger, mod ...func(*consumer.ProcessorConfig)) *consumer.Processor {
	cfg := consumer.ProcessorConfig{
		Consumer:       ledger.ConsumerName,
		MaxAttempts:    3,
		AttemptTimeout: 5 * time.Second,
		Backoff:        backoff.Exp(5*time.Millisecond, 20*time.Millisecond, nil),
		Logger:         log,
	}
	for _, f := range mod {
		f(&cfg)
	}
	return consumer.NewProcessor(pool, h, cfg)
}

// handled collects what a Runner reported through Hooks.Handled.
type handled struct {
	mu   sync.Mutex
	recs []handledRec
}

type handledRec struct {
	Partition int32
	Offset    int64
	Res       consumer.Result
	Err       error
}

func (h *handled) hook(rec *kgo.Record, res consumer.Result, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, handledRec{rec.Partition, rec.Offset, res, err})
}

func (h *handled) all() []handledRec {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]handledRec(nil), h.recs...)
}

func (h *handled) len() int { return len(h.all()) }

func (h *handled) duplicates() int {
	n := 0
	for _, r := range h.all() {
		if r.Res.Duplicate {
			n++
		}
	}
	return n
}

// running is a Runner started in the background.
type running struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when Run returned
	err    error         // Run's result, valid after done
}

// runner starts r.Run in the background; it is stopped on cleanup.
func runner(t *testing.T, r *consumer.Runner) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	rn := &running{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(rn.done)
		rn.err = r.Run(ctx)
	}()
	t.Cleanup(func() { _ = rn.stop() })
	return rn
}

// stop cancels Run and returns its error.
func (rn *running) stop() error {
	rn.cancel()
	return rn.wait(30 * time.Second)
}

// wait returns Run's error once it returned by itself, within timeout.
func (rn *running) wait(timeout time.Duration) error {
	select {
	case <-rn.done:
		return rn.err
	case <-time.After(timeout):
		return errors.New("runner did not return in time")
	}
}

func fastRunner(proc *consumer.Processor, dlq consumer.DLQ, log *slog.Logger, group, topic string, mod ...func(*consumer.RunnerConfig)) *consumer.Runner {
	cfg := consumer.RunnerConfig{
		CommitTimeout: 5 * time.Second,
		DLQTimeout:    5 * time.Second,
		Backoff:       backoff.Exp(50*time.Millisecond, 200*time.Millisecond, nil),
		Logger:        log,
	}
	for _, f := range mod {
		f(&cfg)
	}
	return consumer.NewRunner(proc, dlq, cfg, kafkax.ConsumerOpts(env.Kafka.Brokers, group, topic)...)
}

// flaky wraps a Handler: after the inner handler wrote, err decides whether the attempt
// fails, so a failure also proves the rollback. It counts calls per event.
type flaky struct {
	inner consumer.Handler
	err   func(env event.Envelope, attempt int) error
	mu    sync.Mutex
	calls map[uuid.UUID]int
}

func (f *flaky) Handle(ctx context.Context, tx pgx.Tx, env event.Envelope) error {
	f.mu.Lock()
	if f.calls == nil {
		f.calls = map[uuid.UUID]int{}
	}
	f.calls[env.EventID]++
	attempt := f.calls[env.EventID]
	f.mu.Unlock()
	if err := f.inner.Handle(ctx, tx, env); err != nil {
		return err
	}
	if f.err != nil {
		return f.err(env, attempt)
	}
	return nil
}

func (f *flaky) callsOf(id uuid.UUID) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[id]
}

// ---- Processor (Postgres only) ----

func TestProcessor_AppliesOnceAndSkipsDuplicates(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	proc := fastProcessor(pool, ledger.Handler{}, testLogger(t))
	rec, _ := captured(t, "topic", 1999, "EUR")
	ctx := context.Background()

	res, err := proc.Process(ctx, rec)
	require.NoError(t, err)
	require.Equal(t, consumer.Result{Duplicate: false, Attempts: 1}, res)

	res, err = proc.Process(ctx, rec)
	require.NoError(t, err)
	require.Equal(t, consumer.Result{Duplicate: true, Attempts: 1}, res)

	require.Equal(t, counts{Processed: 1, Transactions: 1, Entries: 2}, countRows(t, pool))
	requireBalanced(t, pool)
}

// Events without a ledger effect are still marked processed.
func TestProcessor_EventWithoutEffectIsMarked(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	proc := fastProcessor(pool, ledger.Handler{}, testLogger(t))
	value, err := event.Encode(event.Envelope{
		EventID: uuid.New(), EventType: event.TypePaymentCreated, EventVersion: 1,
		AggregateType: event.AggregatePayment, AggregateID: "p", AggregateVersion: 1,
		OccurredAt: time.Now(), Data: json.RawMessage(`{}`),
	})
	require.NoError(t, err)
	_, err = proc.Process(context.Background(), &kgo.Record{Value: value})
	require.NoError(t, err)
	require.Equal(t, counts{Processed: 1}, countRows(t, pool))
}

func TestProcessor_TransientErrorIsRetriedAndRolledBack(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	h := &flaky{inner: ledger.Handler{}, err: func(_ event.Envelope, attempt int) error {
		if attempt < 3 {
			return errors.New("connection reset (simulated)")
		}
		return nil
	}}
	proc := fastProcessor(pool, h, testLogger(t), func(c *consumer.ProcessorConfig) { c.MaxAttempts = 3 })
	rec, id := captured(t, "topic", 500, "EUR")

	res, err := proc.Process(context.Background(), rec)
	require.NoError(t, err)
	require.Equal(t, consumer.Result{Attempts: 3}, res)
	require.Equal(t, 3, h.callsOf(id))
	require.Equal(t, counts{Processed: 1, Transactions: 1, Entries: 2}, countRows(t, pool),
		"failed attempts rolled back their entries and marker")
}

func TestProcessor_RetriesExhausted(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	h := &flaky{inner: ledger.Handler{}, err: func(event.Envelope, int) error { return errors.New("db down (simulated)") }}
	proc := fastProcessor(pool, h, testLogger(t), func(c *consumer.ProcessorConfig) { c.MaxAttempts = 4 })
	rec, id := captured(t, "topic", 500, "EUR")

	res, err := proc.Process(context.Background(), rec)
	require.ErrorIs(t, err, consumer.ErrRetriesExhausted)
	require.False(t, consumer.IsPermanent(err))
	require.Equal(t, 4, res.Attempts)
	require.Equal(t, 4, h.callsOf(id))
	require.Equal(t, counts{}, countRows(t, pool), "no marker left behind: a replay can still apply it")
}

func TestProcessor_PermanentErrorsAreNotRetried(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	rec, _ := captured(t, "topic", 500, "EUR")
	badCurrency, _ := captured(t, "topic", 500, "XXX")

	tests := map[string]struct {
		h   consumer.Handler
		rec *kgo.Record
	}{
		"not JSON":         {ledger.Handler{}, &kgo.Record{Value: []byte("{nope")}},
		"no event_id":      {ledger.Handler{}, &kgo.Record{Value: []byte(`{"event_type":"payment.captured"}`)}},
		"unknown currency": {ledger.Handler{}, badCurrency},
		"marked permanent": {consumer.HandlerFunc(func(context.Context, pgx.Tx, event.Envelope) error {
			return consumer.Permanent(errors.New("nope"))
		}), rec},
		"CHECK violation (23514)": {consumer.HandlerFunc(func(ctx context.Context, tx pgx.Tx, env event.Envelope) error {
			_, err := tx.Exec(ctx, `INSERT INTO ledger_entries (transaction_id, account, amount_minor, currency, event_id)
				VALUES ($1, 'a', 0, 'EUR', $1)`, env.EventID)
			return err
		}), rec},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			h := consumer.HandlerFunc(func(ctx context.Context, tx pgx.Tx, env event.Envelope) error {
				calls.Add(1)
				return tt.h.Handle(ctx, tx, env)
			})
			proc := fastProcessor(pool, h, testLogger(t), func(c *consumer.ProcessorConfig) { c.MaxAttempts = 5 })
			res, err := proc.Process(context.Background(), tt.rec)
			require.Error(t, err)
			require.True(t, consumer.IsPermanent(err), "%v", err)
			require.Equal(t, 1, res.Attempts)
			require.LessOrEqual(t, calls.Load(), int32(1))
		})
	}
	require.Equal(t, counts{}, countRows(t, pool))
}

func TestProcessor_PanicIsTransientAndRolledBack(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	h := &flaky{inner: ledger.Handler{}, err: func(event.Envelope, int) error { panic("bug") }}
	proc := fastProcessor(pool, h, testLogger(t), func(c *consumer.ProcessorConfig) { c.MaxAttempts = 2 })
	rec, _ := captured(t, "topic", 500, "EUR")

	res, err := proc.Process(context.Background(), rec)
	require.ErrorIs(t, err, consumer.ErrRetriesExhausted)
	require.ErrorContains(t, err, "handler panicked: bug")
	require.Equal(t, 2, res.Attempts)
	require.Equal(t, counts{}, countRows(t, pool))
}

func TestProcessor_StopsWhenCancelled(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	h := &flaky{inner: ledger.Handler{}, err: func(event.Envelope, int) error { return errors.New("transient") }}
	proc := fastProcessor(pool, h, testLogger(t), func(c *consumer.ProcessorConfig) {
		c.MaxAttempts = 100
		c.Backoff = func(int) time.Duration { return time.Hour }
	})
	rec, _ := captured(t, "topic", 500, "EUR")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	res, err := proc.Process(ctx, rec)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, consumer.ErrRetriesExhausted)
	require.Equal(t, 1, res.Attempts)
	require.Less(t, time.Since(start), 5*time.Second, "the backoff wait must end with ctx")
}

// Two members processing one event at once (rebalance overlap): exactly one applies it.
func TestProcessor_ConcurrentSameEventAppliedOnce(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	proc := fastProcessor(pool, ledger.Handler{}, testLogger(t))
	rec, _ := captured(t, "topic", 700, "EUR")

	const n = 8
	var wg sync.WaitGroup
	var applied, dups atomic.Int32
	for range n {
		wg.Go(func() {
			res, err := proc.Process(context.Background(), rec)
			if !assert.NoError(t, err) {
				return
			}
			if res.Duplicate {
				dups.Add(1)
			} else {
				applied.Add(1)
			}
		})
	}
	wg.Wait()
	require.Equal(t, int32(1), applied.Load())
	require.Equal(t, int32(n-1), dups.Load())
	require.Equal(t, counts{Processed: 1, Transactions: 1, Entries: 2}, countRows(t, pool))
}

// ---- Runner (Postgres + Kafka) ----

var errCrash = errors.New("simulated crash after DB commit, before offset commit")

// F7: the consumer dies after the DB commit, before the offset commit → redelivery is
// skipped via processed_events; exactly one ledger transaction per event.
func TestConsumer_RedeliveryIsIdempotent(t *testing.T) {
	t.Parallel()
	log := testLogger(t)
	pool := env.PG.NewPool(t, nil)
	topic := env.Kafka.NewTopic(t, 1)
	group := "ledger-" + topic

	const n = 5
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i], _ = captured(t, topic, int64(100+i), "EUR")
	}
	produce(t, recs...)

	// Member #1 applies a poll's records, then "crashes" before committing offsets.
	var first handled
	r1 := fastRunner(fastProcessor(pool, ledger.Handler{}, log), &consumertest.FakeDLQ{}, log, group, topic,
		func(c *consumer.RunnerConfig) {
			c.Hooks.Handled = first.hook
			c.Hooks.BeforeCommit = func(context.Context, []*kgo.Record) error { return errCrash }
		})
	require.ErrorIs(t, runner(t, r1).wait(60*time.Second), errCrash, "member #1 must reach the offset commit")
	applied := first.len()
	require.Positive(t, applied)
	require.Empty(t, env.Kafka.CommittedOffsets(t, group, topic), "nothing committed")
	require.Equal(t, applied, countRows(t, pool).Processed, "the DB commit happened")

	// Member #2 gets every record again from the start.
	var second handled
	r2 := fastRunner(fastProcessor(pool, ledger.Handler{}, log), &consumertest.FakeDLQ{}, log, group, topic,
		func(c *consumer.RunnerConfig) { c.Hooks.Handled = second.hook })
	r2run := runner(t, r2)
	require.Eventually(t, func() bool { return second.len() >= n }, 60*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool { return env.Kafka.CommittedOffsets(t, group, topic)[0] == n },
		10*time.Second, 50*time.Millisecond)
	require.NoError(t, r2run.stop())

	require.Equal(t, applied, second.duplicates(), "the redelivered records are skipped")
	require.Equal(t, counts{Processed: n, Transactions: n, Entries: 2 * n}, countRows(t, pool))
	requireBalanced(t, pool)
}

func header(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// F8: poison messages go to the DLQ with dlq.* headers; later records on the same
// partition are still processed.
func TestConsumer_PoisonGoesToDLQ(t *testing.T) {
	t.Parallel()
	log := testLogger(t)
	pool := env.PG.NewPool(t, nil)
	topic := env.Kafka.NewTopic(t, 1)
	dlqTopic := env.Kafka.NewTopic(t, 1)
	group := "ledger-" + topic

	good1, _ := captured(t, topic, 100, "EUR")
	notJSON := &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte("{nope"),
		Headers: []kgo.RecordHeader{{Key: event.HeaderTraceparent, Value: []byte("00-aaaa-bbbb-01")}}}
	badCurrency, badID := captured(t, topic, 100, "XXX")
	exhausted, exhaustedID := captured(t, topic, 300, "EUR")
	good2, _ := captured(t, topic, 200, "EUR")
	produce(t, good1, notJSON, badCurrency, exhausted, good2)

	h := &flaky{inner: ledger.Handler{}, err: func(env event.Envelope, _ int) error {
		if env.EventID == exhaustedID {
			return errors.New("transient forever (simulated)")
		}
		return nil
	}}
	dlqCl := env.Kafka.NewClient(t, kafkax.ProducerOpts(env.Kafka.Brokers, 5*time.Second)...)
	var seen handled
	r := fastRunner(fastProcessor(pool, h, log), kafkax.NewDLQ(dlqCl, dlqTopic), log, group, topic,
		func(c *consumer.RunnerConfig) { c.Hooks.Handled = seen.hook })
	run := runner(t, r)
	require.Eventually(t, func() bool { return seen.len() >= 5 }, 60*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool { return env.Kafka.CommittedOffsets(t, group, topic)[0] == 5 },
		10*time.Second, 50*time.Millisecond, "the partition moved past the poison records")
	require.NoError(t, run.stop())

	require.Equal(t, counts{Processed: 2, Transactions: 2, Entries: 4}, countRows(t, pool))
	requireBalanced(t, pool)

	dead := env.Kafka.ConsumeN(t, dlqTopic, 3, 15*time.Second)
	byOffset := map[string]*kgo.Record{}
	for _, d := range dead {
		require.Equal(t, topic, header(d, consumer.HeaderDLQOriginalTopic))
		require.Equal(t, "0", header(d, consumer.HeaderDLQPartition))
		_, err := time.Parse(time.RFC3339Nano, header(d, consumer.HeaderDLQFailedAt))
		require.NoError(t, err)
		byOffset[header(d, consumer.HeaderDLQOffset)] = d
	}
	require.Len(t, byOffset, 3)

	d := byOffset["1"] // not JSON
	require.Equal(t, notJSON.Value, d.Value)
	require.Equal(t, notJSON.Key, d.Key)
	require.Equal(t, "00-aaaa-bbbb-01", header(d, event.HeaderTraceparent), "original headers kept")
	require.Equal(t, "1", header(d, consumer.HeaderDLQAttempts))
	require.Contains(t, header(d, consumer.HeaderDLQError), "malformed")

	d = byOffset["2"] // unknown currency: permanent, no retries
	require.Equal(t, badCurrency.Value, d.Value)
	require.Equal(t, "1", header(d, consumer.HeaderDLQAttempts))
	require.Equal(t, badID.String(), header(d, event.HeaderEventID))
	require.Contains(t, header(d, consumer.HeaderDLQError), "unknown currency")

	d = byOffset["3"] // transient until retries ran out
	require.Equal(t, "3", header(d, consumer.HeaderDLQAttempts))
	require.Contains(t, header(d, consumer.HeaderDLQError), "retries exhausted")
	require.Equal(t, 3, h.callsOf(exhaustedID))
}

// If the DLQ send fails, the offset is not committed and the partition does not move past
// the record: the next record waits. Once the DLQ is back, both go through.
func TestConsumer_DLQFailureKeepsOffset(t *testing.T) {
	t.Parallel()
	log := testLogger(t)
	pool := env.PG.NewPool(t, nil)
	topic := env.Kafka.NewTopic(t, 1)
	group := "ledger-" + topic

	good0, _ := captured(t, topic, 100, "EUR")
	poison := &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte("{nope")}
	good2, _ := captured(t, topic, 200, "EUR")
	produce(t, good0, poison, good2)

	dlq := &consumertest.FakeDLQ{Err: errors.New("dlq broker down (simulated)")}
	r := fastRunner(fastProcessor(pool, ledger.Handler{}, log), dlq, log, group, topic)
	run := runner(t, r)

	require.Eventually(t, func() bool { return dlq.Failures() >= 3 }, 60*time.Second, 20*time.Millisecond,
		"the record is redelivered and the send retried")
	require.Equal(t, int64(1), env.Kafka.CommittedOffsets(t, group, topic)[0],
		"committed up to the poison record, not past it")
	require.Equal(t, counts{Processed: 1, Transactions: 1, Entries: 2}, countRows(t, pool),
		"the record after the poison one is not processed meanwhile")
	require.Empty(t, dlq.Entries())

	dlq.SetErr(nil)
	require.Eventually(t, func() bool { return env.Kafka.CommittedOffsets(t, group, topic)[0] == 3 },
		30*time.Second, 20*time.Millisecond)
	require.NoError(t, run.stop())
	require.Len(t, dlq.Entries(), 1)
	require.Equal(t, int64(1), dlq.Entries()[0].Record.Offset)
	require.True(t, consumer.IsPermanent(dlq.Entries()[0].Cause))
	require.Equal(t, counts{Processed: 2, Transactions: 2, Entries: 4}, countRows(t, pool))
}

// StopOnExhausted: Run returns at the record that ran out of retries; earlier records are
// committed, the failed one is not, and nothing goes to the DLQ.
func TestConsumer_StopOnExhausted(t *testing.T) {
	t.Parallel()
	log := testLogger(t)
	pool := env.PG.NewPool(t, nil)
	topic := env.Kafka.NewTopic(t, 1)
	group := "ledger-" + topic

	good, _ := captured(t, topic, 100, "EUR")
	stuck, stuckID := captured(t, topic, 200, "EUR")
	after, _ := captured(t, topic, 300, "EUR")
	produce(t, good, stuck, after)

	h := &flaky{inner: ledger.Handler{}, err: func(env event.Envelope, _ int) error {
		if env.EventID == stuckID {
			return errors.New("db down (simulated)")
		}
		return nil
	}}
	dlq := &consumertest.FakeDLQ{}
	r := fastRunner(fastProcessor(pool, h, log), dlq, log, group, topic,
		func(c *consumer.RunnerConfig) { c.StopOnExhausted = true })
	require.ErrorIs(t, runner(t, r).wait(60*time.Second), consumer.ErrRetriesExhausted)
	require.Equal(t, map[int32]int64{0: 1}, env.Kafka.CommittedOffsets(t, group, topic))
	require.Empty(t, dlq.Entries())
	require.Equal(t, counts{Processed: 1, Transactions: 1, Entries: 2}, countRows(t, pool))
}

// Cancelling Run commits what was handled, leaves the group and stops every goroutine.
func TestConsumer_ShutdownCommitsHandled(t *testing.T) {
	testenv.NoLeaks(t)
	log := testLogger(t)
	pool := env.PG.NewPool(t, nil)
	topic := env.Kafka.NewTopic(t, 3)
	group := "ledger-" + topic

	const n = 30
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i], _ = captured(t, topic, int64(i+1), "EUR")
	}
	produce(t, recs...)

	var seen handled
	r := fastRunner(fastProcessor(pool, ledger.Handler{}, log), &consumertest.FakeDLQ{}, log, group, topic,
		func(c *consumer.RunnerConfig) { c.Hooks.Handled = seen.hook })
	run := runner(t, r)
	require.Eventually(t, func() bool { return seen.len() >= n }, 60*time.Second, 20*time.Millisecond)
	require.NoError(t, run.stop())

	total := int64(0)
	for _, off := range env.Kafka.CommittedOffsets(t, group, topic) {
		total += off
	}
	require.Equal(t, int64(n), total, "every handled record is committed")
	require.Equal(t, counts{Processed: n, Transactions: n, Entries: 2 * n}, countRows(t, pool))
}

// Rebalance: members join and leave while events are being processed. Every event is
// applied exactly once, and the group ends fully committed. Processing is slowed down so
// that rebalances hit in-flight polls; the waiting rebalance interrupts them.
func TestConsumer_RebalanceNoLossNoDoubleApply(t *testing.T) {
	t.Parallel()
	log := testLogger(t)
	pool := env.PG.NewPool(t, nil)
	topic := env.Kafka.NewTopic(t, 4)
	group := "ledger-" + topic

	const n = 200
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i], _ = captured(t, topic, int64(i+1), "EUR")
	}
	produce(t, recs...)

	slow := consumer.HandlerFunc(func(ctx context.Context, tx pgx.Tx, env event.Envelope) error {
		select {
		case <-time.After(15 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		return ledger.Handler{}.Handle(ctx, tx, env)
	})
	var seen handled
	member := func() *consumer.Runner {
		return fastRunner(fastProcessor(pool, slow, log), &consumertest.FakeDLQ{}, log, group, topic,
			func(c *consumer.RunnerConfig) {
				c.MaxPollRecords = 20
				c.Hooks.Handled = seen.hook
			})
	}
	processed := func() int { return countRows(t, pool).Processed }

	a := runner(t, member())
	require.Eventually(t, func() bool { return processed() >= 20 }, 60*time.Second, 20*time.Millisecond)
	b := runner(t, member()) // B joins: A gives up partitions mid-stream
	require.Eventually(t, func() bool { return processed() >= 90 }, 60*time.Second, 20*time.Millisecond)
	require.NoError(t, a.stop()) // A leaves: B takes everything over
	runner(t, member())          // C joins
	require.Eventually(t, func() bool { return processed() >= n }, 90*time.Second, 20*time.Millisecond)

	require.Eventually(t, func() bool {
		total := int64(0)
		for _, off := range env.Kafka.CommittedOffsets(t, group, topic) {
			total += off
		}
		return total == n
	}, 30*time.Second, 50*time.Millisecond, "group fully committed")
	require.NoError(t, b.stop())

	require.Equal(t, counts{Processed: n, Transactions: n, Entries: 2 * n}, countRows(t, pool))
	requireBalanced(t, pool)
	for _, h := range seen.all() {
		require.NoError(t, h.Err)
	}
	t.Logf("handled %d records for %d events (%d duplicates skipped)", seen.len(), n, seen.duplicates())
}

// The processed_events key includes the consumer, so two consumers with different names
// each apply every event once.
func TestProcessor_ConsumersAreIndependent(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	rec, _ := captured(t, "topic", 100, "EUR")
	var calls atomic.Int32
	h := consumer.HandlerFunc(func(context.Context, pgx.Tx, event.Envelope) error { calls.Add(1); return nil })
	for i := range 2 {
		name := "c" + strconv.Itoa(i)
		proc := fastProcessor(pool, h, testLogger(t), func(c *consumer.ProcessorConfig) { c.Consumer = name })
		for range 2 {
			_, err := proc.Process(context.Background(), rec)
			require.NoError(t, err)
		}
	}
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, 2, countRows(t, pool).Processed)
}
