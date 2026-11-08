package telemetry_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"

	"github.com/aavorov89/t-outbox/internal/telemetry"
)

// OTel names come out in Prometheus form: unit suffix, _total on counters, annotations
// such as {event} dropped; plus target_info and the Go runtime collector.
func TestNewMetrics_PrometheusNames(t *testing.T) {
	ctx := context.Background()
	mp, h, err := telemetry.NewMetrics(ctx, "svc")
	require.NoError(t, err)
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })
	m := mp.Meter("test")
	c, err := m.Int64Counter("outbox.published", metric.WithUnit("{event}"))
	require.NoError(t, err)
	c.Add(ctx, 3)
	hist, err := m.Float64Histogram("outbox.publish.duration", metric.WithUnit("s"))
	require.NoError(t, err)
	hist.Record(ctx, 0.01)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	require.Contains(t, body, "\noutbox_published_total 3\n")
	require.Contains(t, body, "\noutbox_publish_duration_seconds_count 1\n")
	require.Contains(t, body, `target_info{`)
	require.Contains(t, body, `service_name="svc"`)
	require.Contains(t, body, "\ngo_goroutines ")
}

// ServeMetrics fails at start-up when the port is taken, and serves GET /metrics.
func TestServeMetrics(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	_, err = telemetry.ServeMetrics(addr, http.NotFoundHandler(), slog.Default())
	require.Error(t, err, "port taken")
	require.NoError(t, ln.Close())

	stop, err := telemetry.ServeMetrics(addr, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}), slog.Default())
	require.NoError(t, err)
	defer stop()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr+"/metrics", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(b))
}
