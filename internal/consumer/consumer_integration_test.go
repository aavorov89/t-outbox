//go:build integration

package consumer_test

import "testing"

// F7: the consumer dies after the DB commit, before the offset commit → redelivery is
// skipped via processed_events; exactly one ledger transaction.
func TestConsumer_RedeliveryIsIdempotent(t *testing.T) { t.Skip("M4") }

// F8: a poison message goes to the DLQ with dlq.* headers; later records on the same
// partition are still processed.
func TestConsumer_PoisonGoesToDLQ(t *testing.T) { t.Skip("M4") }
