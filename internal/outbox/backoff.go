package outbox

import (
	"time"

	"github.com/aavorov89/t-outbox/internal/backoff"
)

// Backoff returns the delay before retry number attempt (attempt >= 1).
type Backoff = backoff.Func

// ExpBackoff is backoff.Exp: capped exponential backoff with equal jitter.
func ExpBackoff(base, maxDelay time.Duration, rnd func() float64) Backoff {
	return backoff.Exp(base, maxDelay, rnd)
}
