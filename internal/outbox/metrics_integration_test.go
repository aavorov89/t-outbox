//go:build integration

package outbox_test

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aavorov89/t-outbox/internal/outbox"
	"github.com/aavorov89/t-outbox/internal/outbox/outboxtest"
	"github.com/aavorov89/t-outbox/internal/telemetry"
)

// scrape returns the samples of a Prometheus text exposition: "name{labels}" → value.
// Label sets are kept as the exporter printed them, so tests match them by substring.
func scrape(t *testing.T, h http.Handler) map[string]float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	out := map[string]float64{}
	sc := bufio.NewScanner(rec.Body)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		v, err := strconv.ParseFloat(line[i+1:], 64)
		require.NoError(t, err, line)
		out[line[:i]] = v
	}
	require.NoError(t, sc.Err())
	return out
}

// sample is the value of the one series of name whose labels contain every label in
// want ("k=\"v\"" each); it fails if there is none or more than one.
func sample(t *testing.T, samples map[string]float64, name string, want ...string) float64 {
	t.Helper()
	var (
		found []string
		v     float64
	)
	for series, val := range samples {
		n, labels, _ := strings.Cut(series, "{")
		if n != name {
			continue
		}
		ok := true
		for _, w := range want {
			ok = ok && strings.Contains(labels, w)
		}
		if ok {
			found = append(found, series)
			v = val
		}
	}
	require.Len(t, found, 1, "series %s%v", name, want)
	return v
}

// The relay's metrics, as Prometheus sees them (names after the OTel → Prometheus
// translation): published and failed rows by cause, one duration sample per Produce
// call, rolled-back cycles; and the backlog gauges, read from the table per scrape.
func TestRelay_Metrics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := relayPool(t)
	mp, h, err := telemetry.NewMetrics(ctx, "outbox-relay")
	require.NoError(t, err)
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })
	unobserve, err := outbox.ObserveBacklog(pool, mp)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unobserve() })

	m := scrape(t, h)
	require.Zero(t, sample(t, m, "outbox_pending"), "empty table")
	require.Zero(t, sample(t, m, "outbox_oldest_pending_age_seconds"), "no pending row: 0, not absent")

	// a:1 publishes, a:2 waits behind it (ordering guard), b:1 fails at the producer,
	// bad:1 cannot be built. The oldest row is an hour old.
	insert(t, pool, message("a", 1, `{}`), message("a", 2, `{}`), message("b", 1, `{}`))
	_, err = pool.Exec(ctx, `
		INSERT INTO outbox (event_id, aggregate_type, aggregate_id, aggregate_version, event_type,
		                    event_version, occurred_at, payload)
		VALUES (gen_random_uuid(), 'payment', 'bad', 1, '', 1, now(), '{}')`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE outbox SET created_at = now() - interval '1 hour'
		WHERE id = (SELECT min(id) FROM outbox)`)
	require.NoError(t, err)

	m = scrape(t, h)
	require.Equal(t, 4.0, sample(t, m, "outbox_pending"))
	require.InDelta(t, time.Hour.Seconds(), sample(t, m, "outbox_oldest_pending_age_seconds"), 60)

	p := &outboxtest.FakeProducer{Fail: failKeys(errors.New("broker down"), "b")}
	relay := outbox.NewRelay(pool, p, outbox.Config{
		Topic: "payments.events", MeterProvider: mp,
		Backoff: func(int) time.Duration { return time.Hour },
	})
	n, err := relay.RunOnce(ctx)
	require.Equal(t, 3, n, "a:1, b:1, bad:1; a:2 is behind a:1")
	var pubErr *outbox.PublishError
	require.ErrorAs(t, err, &pubErr)

	topic := `messaging_destination_name="payments.events"`
	m = scrape(t, h)
	require.Equal(t, 1.0, sample(t, m, "outbox_published_total", topic))
	require.Equal(t, 1.0, sample(t, m, "outbox_publish_failures_total", topic, `error_type="publish"`))
	require.Equal(t, 1.0, sample(t, m, "outbox_publish_failures_total", topic, `error_type="build"`))
	require.Equal(t, 1.0, sample(t, m, "outbox_publish_duration_seconds_count", topic),
		"one Produce call per cycle, whatever the batch size")
	require.Equal(t, 1.0, sample(t, m, "outbox_publish_duration_seconds_bucket", topic, `le="15"`))
	require.Equal(t, 3.0, sample(t, m, "outbox_pending"), "a:2, b:1 (backing off), bad:1")
	require.Less(t, sample(t, m, "outbox_oldest_pending_age_seconds"), 60.0,
		"the hour-old row is published; the oldest pending one is new")
	require.Equal(t, 1.0, sample(t, m, "target_info", `service_name="outbox-relay"`))

	// A cycle that rolls back is counted as such and publishes nothing.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = relay.RunOnce(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	m = scrape(t, h)
	require.Equal(t, 1.0, sample(t, m, "outbox_cycle_failures_total", topic))
	require.Equal(t, 1.0, sample(t, m, "outbox_published_total", topic))

	// The next cycle publishes a:2; the rest is backing off. Then the table is drained.
	_, err = relay.RunOnce(ctx)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE published_at IS NULL`)
	require.NoError(t, err)
	m = scrape(t, h)
	require.Equal(t, 2.0, sample(t, m, "outbox_published_total", topic))
	require.Equal(t, 2.0, sample(t, m, "outbox_publish_duration_seconds_count", topic))
	require.Zero(t, sample(t, m, "outbox_pending"))
	require.Zero(t, sample(t, m, "outbox_oldest_pending_age_seconds"))
}

// A scrape while the database is unreachable has no backlog samples (and does not hang):
// alerts on the backlog must also fire on absence.
func TestObserveBacklog_DatabaseDown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := relayPool(t)
	mp, h, err := telemetry.NewMetrics(ctx, "outbox-relay")
	require.NoError(t, err)
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })
	_, err = outbox.ObserveBacklog(pool, mp)
	require.NoError(t, err)
	require.Contains(t, scrape(t, h), "outbox_pending")

	pool.Close()
	start := time.Now()
	m := scrape(t, h)
	require.Less(t, time.Since(start), 5*time.Second)
	require.NotContains(t, m, "outbox_pending")
	require.NotContains(t, m, "outbox_oldest_pending_age_seconds")
}
