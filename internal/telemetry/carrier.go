package telemetry

import (
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
)

// KafkaHeaders is a propagation.TextMapCarrier over a Kafka record's headers. It is
// written by hand so the propagation stays visible in code: the relay injects into the
// outbox.Record's map (propagation.MapCarrier), kafkax copies the map into the record's
// headers, and the consumer extracts from them with this carrier:
//
//	ctx := otel.GetTextMapPropagator().Extract(ctx, telemetry.KafkaHeaders{Record: rec})
//
// Kafka allows a key to repeat. Get returns the last value, so a header appended later
// wins; Set replaces every header with that key.
type KafkaHeaders struct {
	Record *kgo.Record
}

var _ propagation.TextMapCarrier = KafkaHeaders{}

func (c KafkaHeaders) Get(key string) string {
	for i := len(c.Record.Headers) - 1; i >= 0; i-- {
		if c.Record.Headers[i].Key == key {
			return string(c.Record.Headers[i].Value)
		}
	}
	return ""
}

func (c KafkaHeaders) Set(key, value string) {
	c.Record.Headers = slices.DeleteFunc(c.Record.Headers, func(h kgo.RecordHeader) bool { return h.Key == key })
	c.Record.Headers = append(c.Record.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c KafkaHeaders) Keys() []string {
	keys := make([]string, 0, len(c.Record.Headers))
	for _, h := range c.Record.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}
