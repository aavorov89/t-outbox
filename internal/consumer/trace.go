package consumer

import (
	"context"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/aavorov89/t-outbox/internal/event"
	"github.com/aavorov89/t-outbox/internal/telemetry"
)

const scopeName = "github.com/aavorov89/t-outbox/internal/consumer"

// startProcessSpan starts the consumer span of rec. Its parent is the relay's producer
// span, extracted from the record's headers (traceparent, tracestate, baggage), so the
// span joins the trace of the API request that wrote the event. ctx keeps its
// cancellation: Extract only adds the remote span context.
//
// A record published twice (F5) has the same parent both times: the trace then shows two
// send spans and two process spans, the second one marked consumer.duplicate.
func startProcessSpan(ctx context.Context, rec *kgo.Record, consumer string) (context.Context, trace.Span) {
	carrier := telemetry.KafkaHeaders{Record: rec}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	return otel.Tracer(scopeName).Start(ctx, "process "+rec.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeProcess,
			semconv.MessagingOperationName("process"),
			semconv.MessagingDestinationName(rec.Topic),
			semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(rec.Partition), 10)),
			semconv.MessagingKafkaOffset(int(rec.Offset)),
			semconv.MessagingKafkaMessageKey(string(rec.Key)),
			semconv.MessagingMessageID(carrier.Get(event.HeaderEventID)),
			attribute.String("event.type", carrier.Get(event.HeaderEventType)),
			attribute.String("consumer.name", consumer),
		))
}

// startAttemptSpan starts the span of one attempt: the transaction with the
// processed_events marker and the Handler's side effect.
func startAttemptSpan(ctx context.Context, consumer string, attempt int) (context.Context, trace.Span) {
	return otel.Tracer(scopeName).Start(ctx, "apply "+consumer,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBSystemNamePostgreSQL,
			attribute.String("consumer.name", consumer),
			attribute.Int("consumer.attempt", attempt),
		))
}

func failSpan(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
