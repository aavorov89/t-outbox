// Package consumertest provides fakes for the consumer package's interfaces.
package consumertest

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aavorov89/t-outbox/internal/consumer"
)

// DLQEntry is one record sent to FakeDLQ.
type DLQEntry struct {
	Record   *kgo.Record
	Cause    error
	Attempts int
}

// FakeDLQ records what was sent to it. Err, if set, is returned from Send.
type FakeDLQ struct {
	Err error

	mu      sync.Mutex
	entries []DLQEntry
}

var _ consumer.DLQ = (*FakeDLQ)(nil)

func (d *FakeDLQ) Send(_ context.Context, rec *kgo.Record, cause error, attempts int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Err != nil {
		return d.Err
	}
	d.entries = append(d.entries, DLQEntry{Record: rec, Cause: cause, Attempts: attempts})
	return nil
}

func (d *FakeDLQ) Entries() []DLQEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]DLQEntry(nil), d.entries...)
}
