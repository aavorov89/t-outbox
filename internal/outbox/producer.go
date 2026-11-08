package outbox

import "context"

// Record is a Kafka record independent of the client library.
type Record struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// Producer publishes records synchronously. The kafkax package implements it over
// franz-go; outboxtest.FakeProducer implements it for tests.
type Producer interface {
	// Produce returns one error per record, aligned by index with recs (nil = acked).
	// It returns only after every record is acked or has failed.
	Produce(ctx context.Context, recs []Record) []error
}
