package backoff

import (
	"testing"
	"time"
)

func TestExp(t *testing.T) {
	const base, maxDelay = 100 * time.Millisecond, 10 * time.Second
	lo := Exp(base, maxDelay, func() float64 { return 0 })
	hi := Exp(base, maxDelay, func() float64 { return 0.999999999 })

	tests := []struct {
		attempt int
		full    time.Duration // un-jittered delay
	}{
		{0, base}, // clamped to attempt 1
		{1, base},
		{2, 2 * base},
		{3, 4 * base},
		{7, 6400 * time.Millisecond},
		{8, maxDelay}, // 12.8s capped
		{63, maxDelay},
		{1000, maxDelay}, // no overflow
	}
	for _, tt := range tests {
		if got := lo(tt.attempt); got != tt.full/2 {
			t.Errorf("attempt %d: low = %v, want %v", tt.attempt, got, tt.full/2)
		}
		if got := hi(tt.attempt); got > tt.full || got < tt.full-time.Microsecond {
			t.Errorf("attempt %d: high = %v, want ≈%v", tt.attempt, got, tt.full)
		}
	}
}
