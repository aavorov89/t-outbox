package consumer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

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
		{"pg numeric out of range (22003)", &pgconn.PgError{Code: "22003"}, true},
		{"pg check violation (23514)", fmt.Errorf("post: %w", &pgconn.PgError{Code: "23514"}), true},
		{"pg not null violation (23502)", &pgconn.PgError{Code: "23502"}, true},
		{"pg unique violation (23505) may be a race", &pgconn.PgError{Code: "23505"}, false},
		{"pg serialization failure (40001)", &pgconn.PgError{Code: "40001"}, false},
		{"pg deadlock (40P01)", &pgconn.PgError{Code: "40P01"}, false},
		{"pg admin shutdown (57P01)", &pgconn.PgError{Code: "57P01"}, false},
		{"pg syntax error (42601) is a bug: retry, then DLQ", &pgconn.PgError{Code: "42601"}, false},
		{"deadline", context.DeadlineExceeded, false},
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
