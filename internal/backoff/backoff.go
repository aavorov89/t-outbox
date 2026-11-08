// Package backoff holds the retry delay policy shared by the outbox relay and the consumer.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Func returns the delay before retry number attempt (attempt >= 1).
type Func func(attempt int) time.Duration

// Exp is capped exponential backoff with "equal jitter": the delay for attempt n is in
// [d/2, d], where d = min(maxDelay, base*2^(n-1)). rnd returns a value in [0, 1); nil
// means math/rand/v2. Pass a fixed rnd in tests.
func Exp(base, maxDelay time.Duration, rnd func() float64) Func {
	if rnd == nil {
		rnd = rand.Float64
	}
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		d := maxDelay
		// Shift only while it can't overflow or pass the cap.
		if shift := attempt - 1; shift < 62 && base <= maxDelay>>shift {
			d = base << shift
		}
		half := d / 2
		return half + time.Duration(rnd()*float64(d-half))
	}
}
