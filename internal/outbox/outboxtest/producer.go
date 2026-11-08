// Package outboxtest provides fakes for the outbox package's interfaces.
package outboxtest

import (
	"context"
	"sync"

	"github.com/aavorov89/t-outbox/internal/outbox"
)

// FakeProducer records produced records. Fail, if set, decides a per-record error.
type FakeProducer struct {
	Fail func(rec outbox.Record) error

	mu   sync.Mutex
	recs []outbox.Record
}

var _ outbox.Producer = (*FakeProducer)(nil)

func (p *FakeProducer) Produce(ctx context.Context, recs []outbox.Record) []error {
	errs := make([]error, len(recs))
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, r := range recs {
		if err := ctx.Err(); err != nil {
			errs[i] = err
			continue
		}
		if p.Fail != nil {
			if err := p.Fail(r); err != nil {
				errs[i] = err
				continue
			}
		}
		p.recs = append(p.recs, r)
	}
	return errs
}

// Records returns a copy of all successfully produced records.
func (p *FakeProducer) Records() []outbox.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]outbox.Record(nil), p.recs...)
}
