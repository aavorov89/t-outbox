//go:build integration

package e2e_test

import "testing"

// F5: relay #1 publishes, then Hooks.AfterPublish kills its own backend
// (pg_terminate_backend from a side connection using the tx's pg_backend_pid()).
// Relay #2 re-publishes. Kafka holds 2 records with the event_id; the ledger has exactly
// 1 transaction and 1 processed_events row.
func TestRelay_CrashAfterPublishBeforeMark(t *testing.T) { t.Skip("M5") }

// F10: API, relay and consumer in-process with tracetest.NewInMemoryExporter; all spans
// share one trace ID and form the HTTP → producer → consumer → db chain.
func TestTracing_EndToEnd(t *testing.T) { t.Skip("M6") }
