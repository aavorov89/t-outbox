// Package kafkax holds franz-go setup: producer and consumer options, the outbox.Producer
// implementation and, from M6, the OTel header carrier.
package kafkax

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/outbox"
)

// DefaultDeliveryTimeout bounds how long Produce waits for a record. The relay's
// idle_in_transaction_session_timeout must be larger (see outbox.Relay).
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

// ConsumerOpts are the client options of a consumer-group member of group reading topic.
// A new group starts at the beginning of the topic, so no event published before the first
// start is missed. Offsets are auto-committed only for records the application marked
// (kgo.AutoCommitMarks), i.e. after they were handled.
func ConsumerOpts(brokers []string, group, topic string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(),
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
