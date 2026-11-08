//go:build integration

package httpapi_test

import "testing"

// F3: a client retries POST /payments with the same Idempotency-Key → 200 with the same
// payment, exactly one payment row and one outbox row.
func TestAPI_IdempotencyKey(t *testing.T) { t.Skip("M2") }
