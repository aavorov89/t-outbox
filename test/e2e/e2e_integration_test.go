//go:build integration

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/aavorov89/t-outbox/internal/consumer"
	"github.com/aavorov89/t-outbox/internal/event"
	"github.com/aavorov89/t-outbox/internal/httpapi"
	"github.com/aavorov89/t-outbox/internal/kafkax"
	"github.com/aavorov89/t-outbox/internal/ledger"
	"github.com/aavorov89/t-outbox/internal/outbox"
	"github.com/aavorov89/t-outbox/internal/payment/postgres"
	"github.com/aavorov89/t-outbox/internal/telemetry"
	"github.com/aavorov89/t-outbox/internal/testenv"
)

var env *testenv.Env

func TestMain(m *testing.M) {
	testenv.Main(m, &env, testenv.WithPostgres(), testenv.WithKafka())
}

type received struct {
	rec *kgo.Record
	env event.Envelope
}

// The whole chain: POST /payments and POST /payments/{id}/capture → payment + outbox row
// in one tx each → relay → Kafka → ledger consumer → one balanced ledger transaction. All
// three components run in-process against real Postgres and Kafka.
func TestEndToEnd_PaymentEventsReachConsumer(t *testing.T) {
	testenv.NoLeaks(t)
	log := slog.New(slog.NewTextHandler(t.Output(), nil))
	pool := env.PG.NewPool(t, outbox.ConfigurePool)
	topic := env.Kafka.NewTopic(t, 3)

	// API.
	api := httptest.NewServer(httpapi.New(postgres.New(pool), httpapi.Options{Logger: log.With("service", "api")}))
	t.Cleanup(api.Close)

	// Relay and consumer, stopped (and awaited) before their clients and the pool close.
	producerCl := env.Kafka.NewClient(t, kafkax.ProducerOpts(env.Kafka.Brokers, 5*time.Second)...)
	dlqCl := env.Kafka.NewClient(t, kafkax.ProducerOpts(env.Kafka.Brokers, 5*time.Second)...)
	relay := outbox.NewRelay(pool, kafkax.NewProducer(producerCl), outbox.Config{
		Topic:        topic,
		BatchSize:    10,
		PollInterval: 50 * time.Millisecond,
		Logger:       log.With("service", "relay"),
	})
	got := make(chan received, 10)
	consumerLog := log.With("service", "consumer")
	proc := consumer.NewProcessor(pool, ledger.Handler{}, consumer.ProcessorConfig{
		Consumer: ledger.ConsumerName,
		Logger:   consumerLog,
	})
	ledgerConsumer := consumer.NewRunner(proc, kafkax.NewDLQ(dlqCl, topic+".dlq"), consumer.RunnerConfig{
		Logger: consumerLog,
		Hooks: consumer.RunnerHooks{Handled: func(rec *kgo.Record, _ consumer.Result, err error) {
			e, decErr := event.Decode(rec.Value)
			assert.NoError(t, err)
			assert.NoError(t, decErr)
			got <- received{rec, e}
		}},
	}, kafkax.ConsumerOpts(env.Kafka.Brokers, "ledger-"+topic, topic)...)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { assert.NoError(t, relay.Run(ctx)) })
	wg.Go(func() { assert.NoError(t, ledgerConsumer.Run(ctx)) })
	t.Cleanup(func() { cancel(); wg.Wait() })

	// 1. Create a payment.
	created := createPayment(t.Context(), t, api, "m_42", 1999)

	var eventID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT event_id FROM outbox WHERE aggregate_id = $1 AND aggregate_version = 1`, created.ID.String()).Scan(&eventID))

	// 2. The consumer receives exactly that event.
	var r received
	select {
	case r = <-got:
	case <-time.After(30 * time.Second):
		t.Fatal("consumer did not receive the event within 30s")
	}

	require.Equal(t, eventID, r.env.EventID)
	require.Equal(t, event.TypePaymentCreated, r.env.EventType)
	require.Equal(t, 1, r.env.EventVersion)
	require.Equal(t, event.AggregatePayment, r.env.AggregateType)
	require.Equal(t, created.ID.String(), r.env.AggregateID)
	require.Equal(t, 1, r.env.AggregateVersion)
	require.True(t, created.CreatedAt.Equal(r.env.OccurredAt))

	data, err := event.DecodeData[event.PaymentCreatedV1](r.env)
	require.NoError(t, err)
	require.Equal(t, event.PaymentCreatedV1{
		PaymentID:  created.ID,
		MerchantID: "m_42",
		Amount:     event.Amount{AmountMinor: 1999, Currency: "EUR"},
	}, data)

	require.Equal(t, topic, r.rec.Topic)
	require.Equal(t, created.ID.String(), string(r.rec.Key), "key = aggregate_id")
	headers := map[string]string{}
	for _, h := range r.rec.Headers {
		headers[h.Key] = string(h.Value)
	}
	require.Equal(t, eventID.String(), headers[event.HeaderEventID])
	require.Equal(t, event.TypePaymentCreated, headers[event.HeaderEventType])
	require.Equal(t, event.ContentType, headers[event.HeaderContentType])

	// 3. The relay marked the row (it may do so just after the consumer already saw it).
	require.Eventually(t, func() bool {
		var published bool
		err := pool.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM outbox WHERE event_id = $1`, eventID).Scan(&published)
		return err == nil && published
	}, 10*time.Second, 50*time.Millisecond, "outbox row must be marked published")

	// 4. Capture it: payment.captured follows on the same partition.
	capturePayment(t.Context(), t, api, created.ID)

	var c received
	select {
	case c = <-got:
	case <-time.After(30 * time.Second):
		t.Fatal("consumer did not receive payment.captured within 30s")
	}
	require.Equal(t, event.TypePaymentCaptured, c.env.EventType)
	require.Equal(t, created.ID.String(), c.env.AggregateID)
	require.Equal(t, 2, c.env.AggregateVersion)
	require.Equal(t, r.rec.Partition, c.rec.Partition, "one aggregate, one partition")
	require.Greater(t, c.rec.Offset, r.rec.Offset, "captured after created")
	captured, err := event.DecodeData[event.PaymentCapturedV1](c.env)
	require.NoError(t, err)
	require.Equal(t, data.Amount, captured.Amount)

	// 5. Nothing else arrives: one payment, two events.
	select {
	case extra := <-got:
		t.Fatalf("unexpected extra event %s (%s)", extra.env.EventID, extra.env.EventType)
	case <-time.After(500 * time.Millisecond):
	}

	// 6. The ledger: both events marked processed, one balanced transaction for the capture.
	var processed int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE consumer = $1`,
		ledger.ConsumerName).Scan(&processed))
	require.Equal(t, 2, processed)

	rows, err := pool.Query(ctx, `
		SELECT transaction_id, event_id, account, amount_minor, currency
		FROM ledger_entries ORDER BY id`)
	require.NoError(t, err)
	type entry struct {
		TxID, EventID uuid.UUID
		Account       string
		Amount        int64
		Currency      string
	}
	entries, err := pgx.CollectRows(rows, pgx.RowToStructByPos[entry])
	require.NoError(t, err)
	txID := ledger.TransactionID(c.env.EventID)
	require.Equal(t, []entry{
		{txID, c.env.EventID, "psp:receivable", 1999, "EUR"},
		{txID, c.env.EventID, "merchant:m_42:payable", -1999, "EUR"},
	}, entries)
	require.NoError(t, ledger.CheckBalanced(ctx, pool))
}

// F5: a relay dies after Kafka acked its batch, before the rows were marked published.
//
// Two relays run against one outbox. The one that publishes the target event (payment
// #0's payment.captured) first is killed in Hooks.AfterPublish: a side connection
// terminates the backend of its claiming tx (the PID is SELECT pg_backend_pid() on that tx;
// a kill -9 of the process ends the session the same way), and the relay's ctx is cancelled
// so it runs no further cycle. Postgres rolls the tx back and releases the row locks; the
// surviving relay claims the rows and publishes them again.
//
// Kafka then holds two copies of every event of the killed batch, and one of every other
// event. The ledger consumer applies each event once and skips the second copy:
// processed_events holds one row per event, the ledger one transaction per capture.
func TestRelay_CrashAfterPublishBeforeMark(t *testing.T) {
	testenv.NoLeaks(t)
	log := slog.New(slog.NewTextHandler(t.Output(), nil))
	pool := env.PG.NewPool(t, outbox.ConfigurePool)
	topic := env.Kafka.NewTopic(t, 3)
	group := "ledger-" + topic

	// 1. Three payments, created and captured before any relay runs: six outbox rows.
	api := httptest.NewServer(httpapi.New(postgres.New(pool), httpapi.Options{Logger: log.With("service", "api")}))
	t.Cleanup(api.Close)
	const payments = 3
	paymentIDs := make([]uuid.UUID, payments)
	for i := range paymentIDs {
		paymentIDs[i] = createPayment(t.Context(), t, api, fmt.Sprintf("m_%d", i), int64(1000+i)).ID
		capturePayment(t.Context(), t, api, paymentIDs[i])
	}
	eventOf := outboxEvents(t, pool) // outbox.id → event_id
	require.Len(t, eventOf, 2*payments)
	var target int64
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT id FROM outbox WHERE aggregate_id = $1 AND event_type = $2`,
		paymentIDs[0].String(), event.TypePaymentCaptured).Scan(&target))

	// 2. The ledger consumer, running throughout.
	var seen consumed
	dlqCl := env.Kafka.NewClient(t, kafkax.ProducerOpts(env.Kafka.Brokers, 5*time.Second)...)
	consumerLog := log.With("service", "consumer")
	ledgerConsumer := consumer.NewRunner(
		consumer.NewProcessor(pool, ledger.Handler{}, consumer.ProcessorConfig{Consumer: ledger.ConsumerName, Logger: consumerLog}),
		kafkax.NewDLQ(dlqCl, topic+".dlq"),
		consumer.RunnerConfig{Logger: consumerLog, Hooks: consumer.RunnerHooks{Handled: seen.hook(t)}},
		kafkax.ConsumerOpts(env.Kafka.Brokers, group, topic)...)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	wg.Go(func() { assert.NoError(t, ledgerConsumer.Run(ctx)) })

	// 3. Two relays; the first to publish the target dies before its mark.
	side := testenv.Connect(t, pool.Config().ConnConfig.ConnString())
	var c crash
	reclaimed := make(chan string, 1) // the relay that claimed the target after the crash
	relays := map[string]chan struct{}{}
	for _, name := range []string{"relay-1", "relay-2"} {
		relayCtx, die := context.WithCancel(ctx)
		done := make(chan struct{})
		relays[name] = done
		producerCl := env.Kafka.NewClient(t, kafkax.ProducerOpts(env.Kafka.Brokers, 5*time.Second)...)
		relay := outbox.NewRelay(pool, kafkax.NewProducer(producerCl), outbox.Config{
			Topic:        topic,
			BatchSize:    10,
			PollInterval: 20 * time.Millisecond,
			Logger:       log.With("service", name),
			Hooks: outbox.Hooks{
				// How the test learns that the other worker picked the event up: the
				// survivor's claim of the target, after the crash, is signalled here.
				AfterClaim: func(_ context.Context, ids []int64) {
					if slices.Contains(ids, target) && c.happened() {
						select {
						case reclaimed <- name:
						default:
						}
					}
				},
				AfterPublish: func(ctx context.Context, tx pgx.Tx, acked []int64) error {
					if !slices.Contains(acked, target) || !c.record(name, acked) {
						return nil
					}
					// The claiming tx's backend, asked from inside the tx: the server's own
					// answer, right even behind a pooler (a tx pins its server connection),
					// unlike tx.Conn().PgConn().PID(), the BackendKeyData a pooler fakes.
					var pid int32
					if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
						die()
						return fmt.Errorf("pg_backend_pid: %w", err)
					}
					// The timeout makes pg_terminate_backend wait until the backend has
					// exited, so the row locks are released when it returns.
					var killed bool
					err := side.QueryRow(ctx, `SELECT pg_terminate_backend($1, 5000)`, pid).Scan(&killed)
					die() // the process is gone too: no further cycles
					if err != nil || !killed {
						return fmt.Errorf("terminate backend %d: killed=%v: %w", pid, killed, err)
					}
					// Returning nil lets the relay go on to its mark, which must now fail on
					// the dead session: proof that the rows were not marked.
					return nil
				},
			},
		})
		wg.Go(func() {
			defer close(done)
			assert.NoError(t, relay.Run(relayCtx))
		})
	}

	// 4. The survivor claims the target again (it was locked until the kill, SKIP LOCKED
	// hid it), and every row ends up published.
	var survivor string
	select {
	case survivor = <-reclaimed:
	case <-time.After(30 * time.Second):
		t.Fatal("no relay claimed the target event after the crash within 30s")
	}
	crashed, batch := c.result()
	require.NotEqual(t, crashed, survivor, "the dead relay runs no further cycle")
	select {
	case <-relays[crashed]:
	case <-time.After(10 * time.Second):
		t.Fatal("the crashed relay's Run did not return")
	}
	require.Eventually(t, func() bool {
		var pending int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&pending)
		return err == nil && pending == 0
	}, 30*time.Second, 50*time.Millisecond, "the survivor publishes everything")
	var attempts int
	require.NoError(t, pool.QueryRow(ctx, `SELECT attempts FROM outbox WHERE id = $1`, target).Scan(&attempts))
	require.Zero(t, attempts, "the crashed cycle rolled back: no attempt recorded")

	// 5. Kafka holds two copies of each event of the killed batch, one of every other.
	want := map[uuid.UUID]int{}
	for id, ev := range eventOf {
		want[ev] = 1
		if slices.Contains(batch, id) {
			want[ev] = 2
		}
	}
	total := len(eventOf) + len(batch)
	recs := env.Kafka.TryConsumeN(t, topic, total+1, 3*time.Second)
	require.Len(t, recs, total)
	got := map[uuid.UUID]int{}
	ends := map[int32]int64{}
	for _, r := range recs {
		got[uuid.MustParse(headerValue(r, event.HeaderEventID))]++
		ends[r.Partition] = max(ends[r.Partition], r.Offset+1)
	}
	require.Equal(t, want, got)
	require.Equal(t, 2, got[eventOf[target]], "the target was published twice")

	// 6. The consumer handled every copy and applied each event exactly once.
	require.Eventually(t, func() bool { return seen.len() >= total }, 30*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		return maps.Equal(ends, env.Kafka.CommittedOffsets(t, group, topic))
	}, 10*time.Second, 50*time.Millisecond, "the group commits every record")
	applied, duplicates := seen.byEvent()
	for ev, n := range want {
		require.Equal(t, 1, applied[ev], "event %s applied once", ev)
		require.Equal(t, n-1, duplicates[ev], "event %s: every further copy is a duplicate", ev)
	}

	var processed, transactions, targetTx int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM processed_events WHERE consumer = $1),
		       (SELECT count(DISTINCT transaction_id) FROM ledger_entries),
		       (SELECT count(DISTINCT transaction_id) FROM ledger_entries WHERE event_id = $2)`,
		ledger.ConsumerName, eventOf[target]).Scan(&processed, &transactions, &targetTx))
	require.Equal(t, 2*payments, processed, "one processed_events row per event")
	require.Equal(t, payments, transactions, "one ledger transaction per capture")
	require.Equal(t, 1, targetTx)
	require.NoError(t, ledger.CheckBalanced(ctx, pool))
}

// F10: the trace crosses both async gaps. API, relay and consumer run in-process with one
// SDK tracer provider and an in-memory exporter. An upstream caller sends traceparent and
// baggage with both requests (create, then capture); afterwards every span of the system
// is in that caller's trace, and each event forms the chain
//
//	POST /payments[/{id}/capture]  server    (otelhttp)
//	 └─ payment.create|update      client    (the tx; outbox.headers stores its context)
//	     └─ send <topic>           producer  (relay; parent extracted from outbox.headers)
//	         └─ process <topic>    consumer  (parent extracted from the Kafka headers)
//	             └─ apply ledger   client    (processed_events + side effect, one tx)
//	                 └─ ledger.post          (payment.captured only)
func TestTracing_EndToEnd(t *testing.T) {
	testenv.NoLeaks(t)
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(telemetry.Propagator())
	t.Cleanup(func() { // runs last: after the relay and the consumer stopped
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
		assert.NoError(t, tp.Shutdown(context.Background()))
	})

	log := slog.New(telemetry.LogHandler(slog.NewTextHandler(t.Output(), nil)))
	pool := env.PG.NewPool(t, outbox.ConfigurePool)
	topic := env.Kafka.NewTopic(t, 3)

	// The components are built after the global provider is set (otelhttp reads it then).
	api := httptest.NewServer(httpapi.New(postgres.New(pool), httpapi.Options{Logger: log.With("service", "api")}))
	t.Cleanup(api.Close)
	producerCl := env.Kafka.NewClient(t, kafkax.ProducerOpts(env.Kafka.Brokers, 5*time.Second)...)
	dlqCl := env.Kafka.NewClient(t, kafkax.ProducerOpts(env.Kafka.Brokers, 5*time.Second)...)
	relay := outbox.NewRelay(pool, kafkax.NewProducer(producerCl), outbox.Config{
		Topic:        topic,
		BatchSize:    10,
		PollInterval: 20 * time.Millisecond,
		Logger:       log.With("service", "relay"),
	})
	got := make(chan *kgo.Record, 10)
	consumerLog := log.With("service", "consumer")
	ledgerConsumer := consumer.NewRunner(
		consumer.NewProcessor(pool, ledger.Handler{}, consumer.ProcessorConfig{Consumer: ledger.ConsumerName, Logger: consumerLog}),
		kafkax.NewDLQ(dlqCl, topic+".dlq"),
		consumer.RunnerConfig{Logger: consumerLog, Hooks: consumer.RunnerHooks{
			Handled: func(rec *kgo.Record, _ consumer.Result, err error) {
				assert.NoError(t, err)
				got <- rec
			},
		}},
		kafkax.ConsumerOpts(env.Kafka.Brokers, "ledger-"+topic, topic)...)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { assert.NoError(t, relay.Run(ctx)) })
	wg.Go(func() { assert.NoError(t, ledgerConsumer.Run(ctx)) })
	t.Cleanup(func() { cancel(); wg.Wait() })

	// 1. The upstream caller: a client span plus a baggage member, sent with both requests.
	member, err := baggage.NewMember("tenant", "t_42")
	require.NoError(t, err)
	bag, err := baggage.New(member)
	require.NoError(t, err)
	callerCtx, caller := tp.Tracer("upstream").Start(baggage.ContextWithBaggage(t.Context(), bag), "checkout",
		trace.WithSpanKind(trace.SpanKindClient))
	created := createPayment(callerCtx, t, api, "m_42", 1999)
	capturePayment(callerCtx, t, api, created.ID)
	caller.End()
	traceID := caller.SpanContext().TraceID()

	// 2. outbox.headers holds the context of the write tx and the baggage.
	rows, err := pool.Query(ctx, `SELECT event_type, headers FROM outbox WHERE aggregate_id = $1`, created.ID.String())
	require.NoError(t, err)
	stored := map[string]map[string]string{}
	var (
		typ     string
		headers map[string]string
	)
	_, err = pgx.ForEachRow(rows, []any{&typ, &headers}, func() error { stored[typ] = headers; return nil })
	require.NoError(t, err)
	require.Len(t, stored, 2)
	for typ, h := range stored {
		require.Contains(t, h[event.HeaderTraceparent], traceID.String(), "%s: traceparent of the caller's trace", typ)
		require.Equal(t, "tenant=t_42", h[event.HeaderBaggage], typ)
	}

	// 3. Both events reach the consumer, each carrying a traceparent and the baggage.
	recs := map[string]*kgo.Record{}
	for range 2 {
		select {
		case rec := <-got:
			recs[headerValue(rec, event.HeaderEventType)] = rec
		case <-time.After(30 * time.Second):
			t.Fatal("consumer did not handle both events within 30s")
		}
	}
	require.Len(t, recs, 2)
	for typ, rec := range recs {
		require.Equal(t, "tenant=t_42", headerValue(rec, event.HeaderBaggage), "%s: baggage crosses Kafka", typ)
	}

	// 4. The spans: the consumer ends its span right after the Handled hook, so wait for
	// the last ones (the two process spans).
	var spans tracetest.SpanStubs
	require.Eventually(t, func() bool {
		spans = exp.GetSpans()
		return len(spans.Snapshots()) > 0 && len(byName(spans, "process "+topic)) == 2
	}, 10*time.Second, 20*time.Millisecond)

	for _, s := range spans {
		require.Equal(t, traceID, s.SpanContext.TraceID(), "span %q is in the caller's trace", s.Name)
	}

	wantChain := map[string][]string{
		event.TypePaymentCreated: {"POST /payments", "payment.create", "send " + topic, "process " + topic,
			"apply " + ledger.ConsumerName},
		event.TypePaymentCaptured: {"POST /payments/{id}/capture", "payment.update", "send " + topic, "process " + topic,
			"apply " + ledger.ConsumerName, "ledger.post"},
	}
	wantKinds := []trace.SpanKind{trace.SpanKindServer, trace.SpanKindClient, trace.SpanKindProducer,
		trace.SpanKindConsumer, trace.SpanKindClient, trace.SpanKindClient}
	for typ, want := range wantChain {
		eventID := headerValue(recs[typ], event.HeaderEventID)
		leaf := findLeaf(t, spans, want[len(want)-1], eventID, topic)
		chain := ancestry(spans, leaf)
		names := make([]string, len(chain))
		for i, s := range chain {
			names[i] = s.Name
			require.Equal(t, wantKinds[i], s.SpanKind, "%s: kind of %q", typ, s.Name)
		}
		require.Equal(t, want, names, "%s: parent/child chain", typ)
		require.Equal(t, caller.SpanContext().SpanID(), chain[0].Parent.SpanID(), "%s: the server span's parent is the caller", typ)
		require.True(t, chain[0].Parent.IsRemote())

		// The hand-offs: outbox.headers carries the tx span, Kafka the producer span.
		tx, send, process := chain[1], chain[2], chain[3]
		require.Contains(t, stored[typ][event.HeaderTraceparent], tx.SpanContext.SpanID().String(), "%s: outbox.headers", typ)
		require.Contains(t, headerValue(recs[typ], event.HeaderTraceparent), send.SpanContext.SpanID().String(), "%s: Kafka headers", typ)
		require.True(t, send.Parent.IsRemote(), "send's parent comes from outbox.headers")
		require.True(t, process.Parent.IsRemote(), "process' parent comes from the Kafka headers")

		for _, s := range []tracetest.SpanStub{send, process} {
			attrs := attrMap(s)
			require.Equal(t, "kafka", attrs["messaging.system"], s.Name)
			require.Equal(t, topic, attrs["messaging.destination.name"], s.Name)
			require.Equal(t, eventID, attrs["messaging.message.id"], s.Name)
			require.Equal(t, created.ID.String(), attrs["messaging.kafka.message.key"], s.Name)
		}
		require.Equal(t, "false", attrMap(process)["consumer.duplicate"])
	}

	// 5. Nothing else: one trace, no stray spans (an idle relay cycle creates none).
	require.Len(t, spans, 1+len(wantChain[event.TypePaymentCreated])+len(wantChain[event.TypePaymentCaptured]),
		"the caller's span and the two chains")
}

// byName returns the spans called name.
func byName(spans tracetest.SpanStubs, name string) []tracetest.SpanStub {
	var out []tracetest.SpanStub
	for _, s := range spans {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

// findLeaf returns the span called name in the subtree of the process span of eventID.
func findLeaf(t *testing.T, spans tracetest.SpanStubs, name, eventID, topic string) tracetest.SpanStub {
	t.Helper()
	var process []tracetest.SpanStub
	for _, s := range byName(spans, "process "+topic) {
		if attrMap(s)["messaging.message.id"] == eventID {
			process = append(process, s)
		}
	}
	require.Len(t, process, 1, "one process span for event %s", eventID)
	for _, s := range byName(spans, name) {
		for _, a := range ancestry(spans, s) {
			if a.SpanContext.SpanID() == process[0].SpanContext.SpanID() {
				return s
			}
		}
	}
	t.Fatalf("no span %q under the process span of event %s", name, eventID)
	return tracetest.SpanStub{}
}

// ancestry returns s and its exported ancestors, root first.
func ancestry(spans tracetest.SpanStubs, s tracetest.SpanStub) []tracetest.SpanStub {
	byID := map[trace.SpanID]tracetest.SpanStub{}
	for _, sp := range spans {
		byID[sp.SpanContext.SpanID()] = sp
	}
	chain := []tracetest.SpanStub{s}
	for {
		parent, ok := byID[chain[0].Parent.SpanID()]
		if !ok || parent.Name == "checkout" { // stop below the caller's span
			return chain
		}
		chain = append([]tracetest.SpanStub{parent}, chain...)
	}
}

func attrMap(s tracetest.SpanStub) map[string]string {
	m := map[string]string{}
	for _, a := range s.Attributes {
		m[string(a.Key)] = a.Value.String()
	}
	return m
}

// crash records the one simulated relay death of F5.
type crash struct {
	mu    sync.Mutex
	relay string  // who died
	batch []int64 // the rows it had published
}

// record claims the crash for relay; it reports false if another call already did.
func (c *crash) record(relay string, batch []int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.relay != "" {
		return false
	}
	c.relay, c.batch = relay, slices.Clone(batch)
	return true
}

func (c *crash) happened() bool {
	relay, _ := c.result()
	return relay != ""
}

func (c *crash) result() (string, []int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.relay, c.batch
}

// consumed collects what a consumer.Runner handled, by event_id.
type consumed struct {
	mu   sync.Mutex
	recs []consumedRec
}

type consumedRec struct {
	EventID   uuid.UUID
	Duplicate bool
}

func (c *consumed) hook(t *testing.T) func(*kgo.Record, consumer.Result, error) {
	return func(rec *kgo.Record, res consumer.Result, err error) {
		assert.NoError(t, err)
		e, err := event.Decode(rec.Value)
		assert.NoError(t, err)
		c.mu.Lock()
		defer c.mu.Unlock()
		c.recs = append(c.recs, consumedRec{e.EventID, res.Duplicate})
	}
}

func (c *consumed) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.recs)
}

func (c *consumed) byEvent() (applied, duplicates map[uuid.UUID]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	applied, duplicates = map[uuid.UUID]int{}, map[uuid.UUID]int{}
	for _, r := range c.recs {
		if r.Duplicate {
			duplicates[r.EventID]++
		} else {
			applied[r.EventID]++
		}
	}
	return applied, duplicates
}

// ---- helpers ----

// call sends a request to the API and returns the status and body. The trace context of
// ctx, if any, goes along as traceparent/baggage headers (as from an upstream service).
func call(ctx context.Context, t *testing.T, api *httptest.Server, method, path, body string) (int, []byte) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, api.URL+path, r)
	require.NoError(t, err)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(httpapi.HeaderIdempotencyKey, uuid.NewString())
	resp, err := api.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, b
}

func createPayment(ctx context.Context, t *testing.T, api *httptest.Server, merchant string, amountMinor int64) httpapi.PaymentResponse {
	t.Helper()
	status, body := call(ctx, t, api, http.MethodPost, "/payments",
		fmt.Sprintf(`{"merchant_id":%q,"amount":{"amount_minor":%d,"currency":"EUR"}}`, merchant, amountMinor))
	require.Equal(t, http.StatusCreated, status, string(body))
	var p httpapi.PaymentResponse
	require.NoError(t, json.Unmarshal(body, &p))
	return p
}

func capturePayment(ctx context.Context, t *testing.T, api *httptest.Server, id uuid.UUID) {
	t.Helper()
	status, body := call(ctx, t, api, http.MethodPost, "/payments/"+id.String()+"/capture", "")
	require.Equal(t, http.StatusOK, status, string(body))
}

// outboxEvents maps outbox.id to event_id for every row.
func outboxEvents(t *testing.T, pool *pgxpool.Pool) map[int64]uuid.UUID {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT id, event_id FROM outbox`)
	require.NoError(t, err)
	out := map[int64]uuid.UUID{}
	var id int64
	var ev uuid.UUID
	_, err = pgx.ForEachRow(rows, []any{&id, &ev}, func() error { out[id] = ev; return nil })
	require.NoError(t, err)
	return out
}

func headerValue(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
