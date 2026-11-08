package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/aavorov89/t-outbox/internal/payment"
)

const scopeName = "github.com/aavorov89/t-outbox/internal/payment/postgres"

// startSpan starts the span of one write transaction (payment row + outbox row).
func startSpan(ctx context.Context, name string, id uuid.UUID) (context.Context, trace.Span) {
	return otel.Tracer(scopeName).Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBSystemNamePostgreSQL,
			attribute.String("payment.id", id.String()),
		))
}

// endSpan ends a write span. Outcomes the API answers normally (an idempotent replay, a
// payment that does not exist, a refused state transition) are attributes, not errors.
func endSpan(span trace.Span, err error) {
	switch {
	case err == nil:
	case errors.Is(err, payment.ErrDuplicateIdempotencyKey):
		span.SetAttributes(attribute.Bool("payment.duplicate_idempotency_key", true))
	case errors.Is(err, payment.ErrNotFound), errors.Is(err, payment.ErrInvalidTransition):
		span.SetAttributes(attribute.String("payment.rejected", err.Error()))
	default:
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
