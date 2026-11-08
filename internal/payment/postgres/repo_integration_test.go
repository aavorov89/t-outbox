//go:build integration

package postgres_test

import "testing"

// F1: the API dies before COMMIT → neither the payment nor the outbox row exists.
func TestWrite_Atomicity(t *testing.T) { t.Skip("M2") }

// F2: the outbox insert fails (e.g. duplicate aggregate_version) → the payment rolls back too.
func TestWrite_OutboxFailureRollsBackPayment(t *testing.T) { t.Skip("M2") }
