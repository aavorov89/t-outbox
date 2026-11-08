package kafkax

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDLQRecord(t *testing.T) {
	failedAt := time.Date(2026, 11, 8, 12, 0, 0, 5, time.FixedZone("CET", 3600))
	rec := &kgo.Record{
		Topic: "payments.events", Partition: 3, Offset: 42,
		Key: []byte("agg"), Value: []byte(`{"x":1}`),
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte("e1")},
			{Key: "traceparent", Value: []byte("00-abc-def-01")},
			{Key: "dlq.error", Value: []byte("from an earlier dead-lettering")},
		},
	}
	got := DLQRecord("payments.events.dlq", rec, errors.New("permanent: boom"), 5, failedAt)

	require.Equal(t, "payments.events.dlq", got.Topic)
	require.Equal(t, rec.Key, got.Key)
	require.Equal(t, rec.Value, got.Value)
	require.Equal(t, []kgo.RecordHeader{
		{Key: "event_id", Value: []byte("e1")},
		{Key: "traceparent", Value: []byte("00-abc-def-01")},
		{Key: "dlq.original_topic", Value: []byte("payments.events")},
		{Key: "dlq.partition", Value: []byte("3")},
		{Key: "dlq.offset", Value: []byte("42")},
		{Key: "dlq.error", Value: []byte("permanent: boom")},
		{Key: "dlq.attempts", Value: []byte("5")},
		{Key: "dlq.failed_at", Value: []byte("2026-11-08T11:00:00.000000005Z")},
	}, got.Headers)
}

func TestDLQRecordTruncatesError(t *testing.T) {
	got := DLQRecord("dlq", &kgo.Record{Topic: "t"}, errors.New(strings.Repeat("é", 2000)), 1, time.Now())
	for _, h := range got.Headers {
		if h.Key == "dlq.error" {
			require.LessOrEqual(t, len(h.Value), maxDLQErrorLen)
			require.True(t, strings.HasPrefix(string(h.Value), "éé"))
			require.NotContains(t, string(h.Value), "�")
			return
		}
	}
	t.Fatal("no dlq.error header")
}
