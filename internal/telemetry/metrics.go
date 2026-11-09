package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// SetupMetrics installs a global SDK meter provider read by a Prometheus exporter
// (NewMetrics) and returns the handler that serves its metrics in the Prometheus text
// format (mount it on /metrics, see ServeMetrics), and the function that stops the
// provider. With OTEL_SDK_DISABLED=true it installs nothing and the handler serves only
// the Go runtime and process collectors.
//
// Metrics are pulled, not pushed: Jaeger takes traces only, so Prometheus scrapes each
// process.
func SetupMetrics(ctx context.Context, service string) (http.Handler, func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if disabled, _ := strconv.ParseBool(os.Getenv("OTEL_SDK_DISABLED")); disabled {
		return promhttp.HandlerFor(newRegistry(), promhttp.HandlerOpts{}), noop, nil
	}
	mp, h, err := NewMetrics(ctx, service)
	if err != nil {
		return nil, noop, err
	}
	otel.SetMeterProvider(mp)
	return h, mp.Shutdown, nil
}

// NewMetrics returns a meter provider whose instruments are served by h in the
// Prometheus text format, without touching the global provider. Tests pass mp to the code
// under test (outbox.Config.MeterProvider) and scrape h, so they see the names exactly as
// Prometheus does.
//
// The instrumented code uses the OTel metric API only; names are translated on the way
// out: dots become underscores, the unit is appended (outbox.publish.duration in seconds
// is outbox_publish_duration_seconds; {annotations} such as {event} are dropped) and
// counters get _total. The resource (service.name, service.instance.id) is exported once
// as target_info. h also serves the Go runtime and process collectors.
func NewMetrics(ctx context.Context, service string) (*sdkmetric.MeterProvider, http.Handler, error) {
	res, err := newResource(ctx, service)
	if err != nil {
		return nil, nil, err
	}
	reg := newRegistry()
	exp, err := otelprom.New(otelprom.WithRegisterer(reg), otelprom.WithoutScopeInfo())
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry: prometheus exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(exp))
	return mp, promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}), nil
}

func newRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// ServeMetrics serves h on addr at /metrics until stop is called (ctx bounds only the listen). It listens before it
// returns, so a port that is taken fails start-up instead of leaving a process nobody
// can scrape. Serving errors after that are logged.
func ServeMetrics(ctx context.Context, addr string, h http.Handler, log *slog.Logger) (stop func(), err error) {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("telemetry: metrics listener: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", h)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("telemetry: metrics server", "err", err)
		}
	}()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
	}, nil
}
