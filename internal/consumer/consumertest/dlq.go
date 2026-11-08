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

// FakeDLQ records what was sent to it. While an error is set (Err before first use, or
// SetErr at any time), Send fails with it and records nothing.
type FakeDLQ struct {
	Err error

	mu      sync.Mutex
	entries []DLQEntry
	fails   int
}

var _ consumer.DLQ = (*FakeDLQ)(nil)

func (d *FakeDLQ) Send(_ context.Context, rec *kgo.Record, cause error, attempts int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Err != nil {
		d.fails++
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

// SetErr sets the error Send returns; nil makes it succeed again.
func (d *FakeDLQ) SetErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Err = err
}

// Failures is the number of Send calls that returned an error.
func (d *FakeDLQ) Failures() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.fails
}
