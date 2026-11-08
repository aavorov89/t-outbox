package consumer

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aavorov89/t-outbox/internal/event"
)

func TestIsPermanent(t *testing.T) {
	base := errors.New("boom")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"plain error is transient", base, false},
		{"marked", Permanent(base), true},
		{"marked and wrapped", fmt.Errorf("handle: %w", Permanent(base)), true},
		{"malformed envelope", fmt.Errorf("decode: %w", event.ErrMalformed), true},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		if got := IsPermanent(tt.err); got != tt.want {
			t.Errorf("%s: IsPermanent = %v, want %v", tt.name, got, tt.want)
		}
	}
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) must be nil")
	}
	if !errors.Is(Permanent(base), base) {
		t.Error("Permanent must unwrap to the cause")
	}
}
