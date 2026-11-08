// Command outbox-relay publishes outbox rows to Kafka. Run as many instances as you like:
// they split the work with FOR UPDATE SKIP LOCKED and keep per-aggregate order.
//
//	DATABASE_URL=postgres://... KAFKA_BROKERS=localhost:19092 go run ./cmd/outbox-relay
//
// Optional: KAFKA_TOPIC (payments.events), RELAY_BATCH_SIZE (100),
// RELAY_POLL_INTERVAL (200ms), METRICS_ADDR (:9464, Prometheus /metrics). SIGTERM/SIGINT
// finish the in-flight batch, then exit.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/kafkax"
	"github.com/aavorov89/t-outbox/internal/outbox"
	"github.com/aavorov89/t-outbox/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		slog.Error("outbox-relay failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(telemetry.LogHandler(slog.NewJSONHandler(os.Stdout, nil))).With("service", "outbox-relay")
	shutdownTracing, err := telemetry.Setup(ctx, "outbox-relay", log)
	if err != nil {
		return err
	}
	defer func() { // flush the last spans; a slow Jaeger must not hold up the exit for long
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(ctx); err != nil {
			log.Warn("telemetry shutdown", "err", err)
		}
	}()
	metrics, shutdownMetrics, err := telemetry.SetupMetrics(ctx, "outbox-relay")
	if err != nil {
		return err
	}
	defer func() { _ = shutdownMetrics(context.Background()) }() // pull exporter: nothing to flush

	cfg := outbox.DefaultConfig(getenv("KAFKA_TOPIC", "payments.events"))
	cfg.Logger = log
	if v := os.Getenv("RELAY_BATCH_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return fmt.Errorf("RELAY_BATCH_SIZE=%q: want a positive integer", v)
		}
		cfg.BatchSize = n
	}
	if v := os.Getenv("RELAY_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return fmt.Errorf("RELAY_POLL_INTERVAL=%q: want a positive duration such as 200ms", v)
		}
		cfg.PollInterval = d
	}

	poolCfg, err := pgxpool.ParseConfig(getenv("DATABASE_URL", "postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable"))
	if err != nil {
		return err
	}
	outbox.ConfigurePool(poolCfg) // idle_in_transaction_session_timeout > PublishTimeout
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	// Backlog gauges, read from the table per scrape: every relay reports the same values.
	unobserve, err := outbox.ObserveBacklog(pool, nil)
	if err != nil {
		return err
	}
	defer func() { _ = unobserve() }()
	metricsAddr := getenv("METRICS_ADDR", ":9464")
	stopMetrics, err := telemetry.ServeMetrics(metricsAddr, metrics, log)
	if err != nil {
		return err
	}
	defer stopMetrics()

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:19092"), ",")
	cl, err := kgo.NewClient(kafkax.ProducerOpts(brokers, kafkax.DefaultDeliveryTimeout)...)
	if err != nil {
		return err
	}
	defer cl.Close()

	log.Info("relaying", "topic", cfg.Topic, "brokers", brokers,
		"batch_size", cfg.BatchSize, "poll_interval", cfg.PollInterval.String(),
		"publish_timeout", cfg.PublishTimeout.String(), "drain_timeout", cfg.DrainTimeout.String(),
		"metrics_addr", metricsAddr)
	err = outbox.NewRelay(pool, kafkax.NewProducer(cl), cfg).Run(ctx)
	log.Info("stopped")
	return err
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
