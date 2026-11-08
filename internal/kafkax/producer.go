// Package kafkax holds franz-go setup: producer and consumer options and the
// outbox.Producer and consumer.DLQ implementations.
//
// Trace context needs nothing here: the relay injects it into outbox.Record.Headers, which
// Produce copies into the Kafka headers as they are; the consumer reads it back with
// telemetry.KafkaHeaders.
package kafkax

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/outbox"
)

const scopeName = "github.com/aavorov89/t-outbox/internal/kafkax"

// DefaultDeliveryTimeout bounds how long Produce waits for a record. It is below
// outbox.DefaultPublishTimeout, so the producer reports its own timeout error before the
// relay's ctx deadline fires (see the timeout chain in package outbox).
const DefaultDeliveryTimeout = 10 * time.Second

// ProducerOpts are the client options the relay needs: idempotent producer (franz-go's
// default), acks=all, and a delivery timeout that is actually enforced.
//
// By default franz-go never fails an idempotent record that is already in flight: if the
// broker hangs, ProduceSync blocks until it answers, ignoring RecordDeliveryTimeout and
// ctx. The relay holds a DB transaction (and row locks) while publishing, so it needs a
// hard bound. AllowIdempotentProduceCancellation provides it; the cost is that a record
// failed in flight may still have landed, so the relay's retry can duplicate it. That is
// the same at-least-once duplicate as F5, and the consumer dedups by event_id.
//
// The delivery timeout is only checked between requests, and an in-flight request is
// deadlined at ProduceRequestTimeout + RequestTimeoutOverhead (10s + 10s by default).
// Both are set to half the delivery timeout so a hung broker fails Produce in about
// deliveryTimeout instead of 20s+.
func ProducerOpts(brokers []string, deliveryTimeout time.Duration) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
		kgo.AllowIdempotentProduceCancellation(),
		kgo.ProduceRequestTimeout(deliveryTimeout / 2),
		kgo.RequestTimeoutOverhead(deliveryTimeout / 2),
		kgo.ProducerLinger(0),
	}
}

// Producer implements outbox.Producer over a *kgo.Client.
type Producer struct {
	cl *kgo.Client
}

var _ outbox.Producer = (*Producer)(nil)

// NewProducer wraps cl. The caller owns cl and closes it.
func NewProducer(cl *kgo.Client) *Producer { return &Producer{cl: cl} }

func (p *Producer) Produce(ctx context.Context, recs []outbox.Record) []error {
	krecs := make([]*kgo.Record, len(recs))
	for i, r := range recs {
		kr := &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value}
		for k, v := range r.Headers {
			kr.Headers = append(kr.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		krecs[i] = kr
	}
	results := p.cl.ProduceSync(ctx, krecs...)
	// ProduceSync returns results in input order.
	errs := make([]error, len(recs))
	for i, res := range results {
		if res.Err != nil {
			errs[i] = fmt.Errorf("kafkax: produce %s: %w", res.Record.Topic, res.Err)
		}
	}
	return errs
}
