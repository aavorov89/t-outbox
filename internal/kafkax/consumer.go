package kafkax

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/consumer"
)

// ConsumerOpts are the client options of a consumer-group member of group reading topic,
// for consumer.NewRunner. A new group starts at the beginning of the topic, so no event
// published before the first start is missed. Offset handling (auto-commit off, commit
// after the DB commit, blocked rebalances while a poll is handled) is set by the Runner.
func ConsumerOpts(brokers []string, group, topic string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
}

// maxDLQErrorLen bounds the dlq.error header.
const maxDLQErrorLen = 1024

// DLQ implements consumer.DLQ by producing to a dead-letter topic. Build its client with
// ProducerOpts, so a send is bounded by the delivery timeout.
type DLQ struct {
	cl    *kgo.Client
	topic string
	now   func() time.Time
}

var _ consumer.DLQ = (*DLQ)(nil)

// NewDLQ wraps cl. The caller owns cl and closes it.
func NewDLQ(cl *kgo.Client, topic string) *DLQ {
	return &DLQ{cl: cl, topic: topic, now: time.Now}
}

// Send produces DLQRecord(rec, ...) and waits for the ack. Only a nil error lets the
// consumer commit rec's offset.
func (d *DLQ) Send(ctx context.Context, rec *kgo.Record, cause error, attempts int) error {
	out := DLQRecord(d.topic, rec, cause, attempts, d.now())
	if err := d.cl.ProduceSync(ctx, out).FirstErr(); err != nil {
		return fmt.Errorf("kafkax: dlq %s: %w", d.topic, err)
	}
	return nil
}

// DLQRecord is the dead-letter copy of rec: the same key and value, the original headers
// (event_id, traceparent, ...), plus dlq.* headers saying where it came from and why. On a
// record that was already dead-lettered once (replayed and failed again), the dlq.*
// headers are replaced, not duplicated.
func DLQRecord(topic string, rec *kgo.Record, cause error, attempts int, failedAt time.Time) *kgo.Record {
	msg := "<nil>"
	if cause != nil {
		msg = cause.Error()
	}
	if len(msg) > maxDLQErrorLen {
		msg = strings.ToValidUTF8(msg[:maxDLQErrorLen], "")
	}
	dlq := []kgo.RecordHeader{
		{Key: consumer.HeaderDLQOriginalTopic, Value: []byte(rec.Topic)},
		{Key: consumer.HeaderDLQPartition, Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
		{Key: consumer.HeaderDLQOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		{Key: consumer.HeaderDLQError, Value: []byte(msg)},
		{Key: consumer.HeaderDLQAttempts, Value: []byte(strconv.Itoa(attempts))},
		{Key: consumer.HeaderDLQFailedAt, Value: []byte(failedAt.UTC().Format(time.RFC3339Nano))},
	}
	headers := make([]kgo.RecordHeader, 0, len(rec.Headers)+len(dlq))
	for _, h := range rec.Headers {
		if !isDLQHeader(h.Key) {
			headers = append(headers, h)
		}
	}
	return &kgo.Record{
		Topic:   topic,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: append(headers, dlq...),
	}
}

func isDLQHeader(key string) bool {
	switch key {
	case consumer.HeaderDLQOriginalTopic, consumer.HeaderDLQPartition, consumer.HeaderDLQOffset,
		consumer.HeaderDLQError, consumer.HeaderDLQAttempts, consumer.HeaderDLQFailedAt:
		return true
	}
	return false
}
