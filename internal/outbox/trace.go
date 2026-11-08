package outbox

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

const scopeName = "github.com/aavorov89/t-outbox/internal/outbox"

// startSendSpans starts one producer span per record and injects its context into the
// record's headers, so the consumer's span becomes its child.
//
// The parent of each span is the trace context the API stored in the row's headers
// (outbox.Queue), i.e. the request that wrote the event: the trace crosses the async gap
// between the API's commit and the relay's claim. A row without one starts a new trace.
// The relay's own context is never a parent: one batch carries events of many unrelated
// requests.
//
// recs[j] belongs to rows[idx[j]], as in RunOnce. The record headers are the row headers
// (BuildRecord copied them), so the injected traceparent replaces the API's.
func startSendSpans(rows []Row, recs []Record, idx []int) []trace.Span {
	tracer := otel.Tracer(scopeName)
	prop := otel.GetTextMapPropagator()
	spans := make([]trace.Span, len(recs))
	for j := range recs {
		row := rows[idx[j]]
		parent := prop.Extract(context.Background(), propagation.MapCarrier(row.Headers))
		spanCtx, span := tracer.Start(parent, "send "+recs[j].Topic,
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				semconv.MessagingSystemKafka,
				semconv.MessagingOperationTypeSend,
				semconv.MessagingOperationName("send"),
				semconv.MessagingDestinationName(recs[j].Topic),
				semconv.MessagingKafkaMessageKey(string(recs[j].Key)),
				semconv.MessagingMessageID(row.EventID.String()),
				attribute.String("event.type", row.EventType),
				attribute.Int64("outbox.id", row.ID),
				attribute.Int("outbox.attempt", row.Attempts+1),
			))
		prop.Inject(spanCtx, propagation.MapCarrier(recs[j].Headers))
		spans[j] = span
	}
	return spans
}

// endSpan ends a send span with the record's publish result.
func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
