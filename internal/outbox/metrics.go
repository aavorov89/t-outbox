package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// Metric names. The Prometheus exporter turns dots into underscores and appends the unit
// and, for counters, _total:
//
//	outbox.pending              gauge      outbox_pending
//	outbox.oldest_pending.age   gauge      outbox_oldest_pending_age_seconds
//	outbox.published            counter    outbox_published_total
//	outbox.publish.failures     counter    outbox_publish_failures_total{error_type}
//	outbox.publish.duration     histogram  outbox_publish_duration_seconds
//	outbox.cycle.failures       counter    outbox_cycle_failures_total
//
// The two gauges describe the table (ObserveBacklog); the rest are per relay process.
const (
	MetricPending          = "outbox.pending"
	MetricOldestPendingAge = "outbox.oldest_pending.age"
	MetricPublished        = "outbox.published"
	MetricPublishFailures  = "outbox.publish.failures"
	MetricPublishDuration  = "outbox.publish.duration"
	MetricCycleFailures    = "outbox.cycle.failures"
)

// Values of the error.type attribute on outbox.publish.failures.
const (
	FailureBuild   = "build"   // the row could not be turned into a record (envelope validation)
	FailurePublish = "publish" // the producer returned an error for the record
)

// publishBuckets cover a healthy publish (milliseconds) up to PublishTimeout (15s), so a
// batch that waits for the delivery timeout lands in its own bucket, not in +Inf.
var publishBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 20}

// relayMetrics are the instruments of one Relay.
type relayMetrics struct {
	published     metric.Int64Counter
	failures      metric.Int64Counter
	duration      metric.Float64Histogram
	cycleFailures metric.Int64Counter
	topic         metric.MeasurementOption
}

func newRelayMetrics(mp metric.MeterProvider, topic string) (*relayMetrics, error) {
	m := mp.Meter(scopeName)
	var (
		rm  = &relayMetrics{topic: metric.WithAttributes(semconv.MessagingDestinationName(topic))}
		err error
	)
	if rm.published, err = m.Int64Counter(MetricPublished, metric.WithUnit("{event}"),
		metric.WithDescription("Outbox rows published to Kafka and marked published.")); err != nil {
		return nil, err
	}
	if rm.failures, err = m.Int64Counter(MetricPublishFailures, metric.WithUnit("{event}"),
		metric.WithDescription("Outbox rows that failed to publish and were rescheduled with backoff.")); err != nil {
		return nil, err
	}
	if rm.duration, err = m.Float64Histogram(MetricPublishDuration, metric.WithUnit("s"),
		metric.WithDescription("Duration of one Produce call (a batch), bounded by PublishTimeout."),
		metric.WithExplicitBucketBoundaries(publishBuckets...)); err != nil {
		return nil, err
	}
	if rm.cycleFailures, err = m.Int64Counter(MetricCycleFailures, metric.WithUnit("{cycle}"),
		metric.WithDescription("Relay cycles that rolled back (database down, tx aborted); nothing was marked.")); err != nil {
		return nil, err
	}
	return rm, nil
}

// cycle records the outcome of a committed cycle. Called after COMMIT only: a cycle that
// rolled back published (from the table's point of view) nothing.
func (m *relayMetrics) cycle(ctx context.Context, published, buildFailed, publishFailed int) {
	if published > 0 {
		m.published.Add(ctx, int64(published), m.topic)
	}
	if buildFailed > 0 {
		m.failures.Add(ctx, int64(buildFailed), m.topic, metric.WithAttributes(semconv.ErrorTypeKey.String(FailureBuild)))
	}
	if publishFailed > 0 {
		m.failures.Add(ctx, int64(publishFailed), m.topic, metric.WithAttributes(semconv.ErrorTypeKey.String(FailurePublish)))
	}
}

// backlogQueryTimeout bounds the query behind one collection of the backlog gauges, so a
// slow database makes a scrape miss the gauges instead of hanging it.
const backlogQueryTimeout = 2 * time.Second

const backlogSQL = `
SELECT count(*),
       coalesce(extract(epoch FROM now() - min(created_at)), 0)::float8
FROM outbox
WHERE published_at IS NULL`

// ObserveBacklog registers the two backlog gauges on mp (nil: the global provider):
//
//   - outbox.pending: unpublished rows, due or backing off.
//   - outbox.oldest_pending.age: now() - created_at of the oldest unpublished row, in
//     seconds, 0 when there is none. This is the main SLO signal: it grows when the
//     relays are down, Kafka is down or one aggregate is stuck behind a failing event,
//     and it does not depend on the write rate the way the pending count does.
//
// Both are read from the table at collection time, one query per scrape (a scan of the
// outbox_pending partial index), with the database's clock. They describe the table, not
// a relay: every relay that calls this reports the same values, so aggregate with max(),
// not sum(). If the query fails, the scrape has no samples for them; alert on absence too.
//
// The returned function unregisters the callback.
func ObserveBacklog(pool *pgxpool.Pool, mp metric.MeterProvider) (unregister func() error, err error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	m := mp.Meter(scopeName)
	pending, err := m.Int64ObservableGauge(MetricPending, metric.WithUnit("{event}"),
		metric.WithDescription("Unpublished outbox rows (due or backing off)."))
	if err != nil {
		return nil, err
	}
	age, err := m.Float64ObservableGauge(MetricOldestPendingAge, metric.WithUnit("s"),
		metric.WithDescription("Age of the oldest unpublished outbox row; 0 when there is none."))
	if err != nil {
		return nil, err
	}
	reg, err := m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, backlogQueryTimeout)
		defer cancel()
		var (
			n    int64
			secs float64
		)
		if err := pool.QueryRow(ctx, backlogSQL).Scan(&n, &secs); err != nil {
			return fmt.Errorf("outbox: backlog metrics: %w", err)
		}
		o.ObserveInt64(pending, n)
		o.ObserveFloat64(age, secs)
		return nil
	}, pending, age)
	if err != nil {
		return nil, err
	}
	return reg.Unregister, nil
}
