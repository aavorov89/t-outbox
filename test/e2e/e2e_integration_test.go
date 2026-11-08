//go:build integration

package e2e_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/consumer"
	"github.com/aavorov89/t-outbox/internal/event"
	"github.com/aavorov89/t-outbox/internal/httpapi"
	"github.com/aavorov89/t-outbox/internal/kafkax"
	"github.com/aavorov89/t-outbox/internal/outbox"
	"github.com/aavorov89/t-outbox/internal/payment/postgres"
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
// in one tx each → relay → Kafka → consumer. All three components run in-process against
// real Postgres and Kafka.
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
	consumerCl := env.Kafka.NewClient(t, kafkax.ConsumerOpts(env.Kafka.Brokers, "ledger-"+topic, topic)...)
	relay := outbox.NewRelay(pool, kafkax.NewProducer(producerCl), outbox.Config{
		Topic:        topic,
		BatchSize:    10,
		PollInterval: 50 * time.Millisecond,
		Logger:       log.With("service", "relay"),
	})
	got := make(chan received, 10)
	logEvent := consumer.Log(log.With("service", "consumer"))
	handle := func(ctx context.Context, rec *kgo.Record, e event.Envelope) error {
		got <- received{rec, e}
		return logEvent(ctx, rec, e)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { assert.NoError(t, relay.Run(ctx)) })
	wg.Go(func() { assert.NoError(t, consumer.Poll(ctx, consumerCl, handle, log)) })
	t.Cleanup(func() { cancel(); wg.Wait() })

	// 1. Create a payment.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, api.URL+"/payments",
		strings.NewReader(`{"merchant_id":"m_42","amount":{"amount_minor":1999,"currency":"EUR"}}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(httpapi.HeaderIdempotencyKey, uuid.NewString())
	resp, err := api.Client().Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusCreated, resp.StatusCode, string(body))
	var created httpapi.PaymentResponse
	require.NoError(t, json.Unmarshal(body, &created))

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
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, api.URL+"/payments/"+created.ID.String()+"/capture", nil)
	require.NoError(t, err)
	req.Header.Set(httpapi.HeaderIdempotencyKey, uuid.NewString())
	resp, err = api.Client().Do(req)
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

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
}

// F5: relay #1 publishes, then Hooks.AfterPublish kills its own backend
// (pg_terminate_backend from a side connection using the tx's pg_backend_pid()).
// Relay #2 re-publishes. Kafka holds 2 records with the event_id; the ledger has exactly
// 1 transaction and 1 processed_events row.
func TestRelay_CrashAfterPublishBeforeMark(t *testing.T) { t.Skip("M5") }

// F10: API, relay and consumer in-process with tracetest.NewInMemoryExporter; all spans
// share one trace ID and form the HTTP → producer → consumer → db chain.
func TestTracing_EndToEnd(t *testing.T) { t.Skip("M6") }
