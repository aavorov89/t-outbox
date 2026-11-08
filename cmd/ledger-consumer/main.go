// Command ledger-consumer consumes payment events and posts double-entry ledger
// transactions for captured payments, exactly once per event (processed_events in the same
// transaction). Records that fail permanently or run out of retries go to the DLQ topic.
//
//	DATABASE_URL=postgres://... KAFKA_BROKERS=localhost:19092 go run ./cmd/ledger-consumer
//
// Optional: KAFKA_TOPIC (payments.events), KAFKA_DLQ_TOPIC (payments.events.dlq),
// KAFKA_GROUP (ledger), CONSUMER_MAX_ATTEMPTS (5), CONSUMER_STOP_ON_EXHAUSTED (false:
// exhausted records go to the DLQ; true: the consumer exits and resumes at that record on
// restart). SIGTERM/SIGINT commit the handled records and leave the group.
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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/consumer"
	"github.com/aavorov89/t-outbox/internal/kafkax"
	"github.com/aavorov89/t-outbox/internal/ledger"
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
	dlqTopic := getenv("KAFKA_DLQ_TOPIC", "payments.events.dlq")
	group := getenv("KAFKA_GROUP", "ledger")

	pcfg := consumer.DefaultProcessorConfig(ledger.ConsumerName)
	pcfg.Logger = log
	if v := os.Getenv("CONSUMER_MAX_ATTEMPTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return fmt.Errorf("CONSUMER_MAX_ATTEMPTS=%q: want a positive integer", v)
		}
		pcfg.MaxAttempts = n
	}
	rcfg := consumer.DefaultRunnerConfig()
	rcfg.Logger = log
	if v := os.Getenv("CONSUMER_STOP_ON_EXHAUSTED"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("CONSUMER_STOP_ON_EXHAUSTED=%q: want true or false", v)
		}
		rcfg.StopOnExhausted = b
	}

	poolCfg, err := pgxpool.ParseConfig(getenv("DATABASE_URL", "postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable"))
	if err != nil {
		return err
	}
	if poolCfg.ConnConfig.RuntimeParams["application_name"] == "" {
		poolCfg.ConnConfig.RuntimeParams["application_name"] = "ledger-consumer"
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	dlqCl, err := kgo.NewClient(kafkax.ProducerOpts(brokers, kafkax.DefaultDeliveryTimeout)...)
	if err != nil {
		return err
	}
	defer dlqCl.Close()

	proc := consumer.NewProcessor(pool, ledger.Handler{}, pcfg)
	runner := consumer.NewRunner(proc, kafkax.NewDLQ(dlqCl, dlqTopic), rcfg, kafkax.ConsumerOpts(brokers, group, topic)...)

	log.Info("consuming", "topic", topic, "dlq_topic", dlqTopic, "group", group, "brokers", brokers,
		"max_attempts", pcfg.MaxAttempts, "stop_on_exhausted", rcfg.StopOnExhausted)
	err = runner.Run(ctx) // closes its client: commits nothing more, leaves the group
	log.Info("stopped", "err", err)
	return err
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
