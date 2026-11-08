//go:build integration

package outbox_test

import "testing"

// F4: 8 workers, 1,000 events. Every event_id is published; in-flight ID sets recorded via
// Hooks.AfterClaim never intersect. Run with -race.
func TestRelay_ConcurrentWorkersNoDoubleClaim(t *testing.T) { t.Skip("M3") }

// F6: env.Kafka.Pause → attempts > 0, published_at IS NULL, last_error set →
// env.Kafka.Unpause → require.Eventually all published.
func TestRelay_BrokerOutage(t *testing.T) { t.Skip("M3") }

// F9: events of one aggregate spread across N relays are published in aggregate_version
// order (topic with several partitions; check order within the key's partition).
func TestRelay_PerAggregateOrdering(t *testing.T) { t.Skip("M3") }
