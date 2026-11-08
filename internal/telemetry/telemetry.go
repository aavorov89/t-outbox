// Package telemetry sets up OpenTelemetry tracing for the three binaries: the tracer
// provider, the OTLP exporter and the global propagator (W3C TraceContext + Baggage).
//
// The instrumented packages (httpapi, payment/postgres, outbox, consumer, ledger, kafkax)
// never import this package's setup: they use the global otel.GetTracerProvider and
// otel.GetTextMapPropagator, which are no-ops until Setup runs. So unit tests and code
// paths without Setup pay nothing, and nothing changes in their signatures.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// Propagator is the propagator of the whole system: W3C traceparent/tracestate plus W3C
// baggage. It writes these headers into outbox.headers (API), from there into the Kafka
// headers (relay), and reads them back in the consumer.
func Propagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
}

// Setup installs the global propagator and an SDK tracer provider for service, and
// returns the function that flushes and stops it (call it on exit, with a timeout).
//
// Export is OTLP over HTTP, straight to Jaeger, which accepts OTLP natively (no collector
// in between). It is on when OTEL_EXPORTER_OTLP_ENDPOINT or
// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT is set, e.g. http://localhost:4318. Without an
// endpoint, spans are still created (so trace IDs reach the logs and the Kafka headers)
// but not exported. OTEL_SDK_DISABLED=true installs only the propagator.
//
// The usual OTEL_* variables apply: OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES
// override the resource (compose sets service.instance.id per relay), OTEL_TRACES_SAMPLER
// the sampler (default: parent-based, always on).
//
// Errors of the SDK itself (an export that failed because Jaeger is down) go to log.
func Setup(ctx context.Context, service string, log *slog.Logger) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(Propagator())
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Warn("telemetry: otel error", "err", err)
	}))
	noop := func(context.Context) error { return nil }
	if disabled, _ := strconv.ParseBool(os.Getenv("OTEL_SDK_DISABLED")); disabled {
		return noop, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(service)),
		resource.WithFromEnv(), // after the default name: OTEL_SERVICE_NAME wins
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return noop, fmt.Errorf("telemetry: resource: %w", err)
	}

	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		// Reads the endpoint (http:// means no TLS), headers and timeout from the env.
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return noop, fmt.Errorf("telemetry: otlp exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
