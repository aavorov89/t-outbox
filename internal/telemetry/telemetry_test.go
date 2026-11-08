package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/aavorov89/t-outbox/internal/telemetry"
)

func TestKafkaHeaders_RoundTrip(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "send")
	defer span.End()
	m, err := baggage.NewMember("tenant", "t_42")
	require.NoError(t, err)
	b, err := baggage.New(m)
	require.NoError(t, err)
	ctx = baggage.ContextWithBaggage(ctx, b)

	rec := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: "event_id", Value: []byte("e1")},
		{Key: "traceparent", Value: []byte("00-stale-stale-01")}, // replaced, not duplicated
	}}
	prop := telemetry.Propagator()
	prop.Inject(ctx, telemetry.KafkaHeaders{Record: rec})

	keys := telemetry.KafkaHeaders{Record: rec}.Keys()
	require.ElementsMatch(t, []string{"event_id", "traceparent", "baggage"}, keys)

	got := prop.Extract(context.Background(), telemetry.KafkaHeaders{Record: rec})
	sc := trace.SpanContextFromContext(got)
	require.True(t, sc.IsRemote())
	require.Equal(t, span.SpanContext().TraceID(), sc.TraceID())
	require.Equal(t, span.SpanContext().SpanID(), sc.SpanID())
	require.Equal(t, "t_42", baggage.FromContext(got).Member("tenant").Value())
}

func TestKafkaHeaders_GetLastWins(t *testing.T) {
	rec := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: "k", Value: []byte("first")},
		{Key: "k", Value: []byte("last")},
	}}
	require.Equal(t, "last", telemetry.KafkaHeaders{Record: rec}.Get("k"))
	require.Empty(t, telemetry.KafkaHeaders{Record: rec}.Get("missing"))
}

func TestLogHandler_AddsTraceIDs(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(telemetry.LogHandler(slog.NewJSONHandler(&buf, nil))).With("service", "x")

	log.InfoContext(context.Background(), "no span")
	var line map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &line))
	require.NotContains(t, line, "trace_id")

	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "op")
	defer span.End()
	buf.Reset()
	log.InfoContext(ctx, "in span")
	line = nil
	require.NoError(t, json.Unmarshal(buf.Bytes(), &line))
	require.Equal(t, span.SpanContext().TraceID().String(), line["trace_id"])
	require.Equal(t, span.SpanContext().SpanID().String(), line["span_id"])
	require.Equal(t, "x", line["service"])
}

// Without an OTLP endpoint, Setup still creates spans (trace IDs for logs and headers)
// and installs the propagator.
func TestSetup_NoEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_SDK_DISABLED", "")
	shutdown, err := telemetry.Setup(context.Background(), "test", slog.Default())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, shutdown(context.Background())) })
}
