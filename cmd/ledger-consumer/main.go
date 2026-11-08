// Command ledger-consumer consumes payment events.
//
// M2: it only logs each event. Ledger postings, processed_events dedup and the DLQ come
// in M4.
//
//	KAFKA_BROKERS=localhost:19092 go run ./cmd/ledger-consumer
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/consumer"
	"github.com/aavorov89/t-outbox/internal/kafkax"
)

func main() {
	if err := run(); err != nil {
		slog.Error("ledger-consumer failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "ledger-consumer")

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:19092"), ",")
	topic := getenv("KAFKA_TOPIC", "payments.events")
	group := getenv("KAFKA_GROUP", "ledger")
	cl, err := kgo.NewClient(kafkax.ConsumerOpts(brokers, group, topic)...)
	if err != nil {
		return err
	}
	defer cl.Close() // leaves the group and commits marked offsets

	log.Info("consuming", "topic", topic, "group", group, "brokers", brokers)
	err = consumer.Poll(ctx, cl, consumer.Log(log), log)
	log.Info("stopped")
	return err
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
