// Command outbox-relay publishes outbox rows to Kafka.
//
// M2: the relay takes no row locks, so run a single instance (see outbox.Relay).
//
//	DATABASE_URL=postgres://... KAFKA_BROKERS=localhost:19092 go run ./cmd/outbox-relay
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/kafkax"
	"github.com/aavorov89/t-outbox/internal/outbox"
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
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "outbox-relay")

	pool, err := pgxpool.New(ctx, getenv("DATABASE_URL", "postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable"))
	if err != nil {
		return err
	}
	defer pool.Close()

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:19092"), ",")
	cl, err := kgo.NewClient(kafkax.ProducerOpts(brokers, kafkax.DefaultDeliveryTimeout)...)
	if err != nil {
		return err
	}
	defer cl.Close()

	cfg := outbox.DefaultConfig(getenv("KAFKA_TOPIC", "payments.events"))
	cfg.Logger = log
	log.Info("relaying", "topic", cfg.Topic, "brokers", brokers)
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
