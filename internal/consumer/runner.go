package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"

	"github.com/aavorov89/t-outbox/internal/backoff"
)

// RunnerConfig configures a Runner. Zero values get the DefaultRunnerConfig values.
type RunnerConfig struct {
	// MaxPollRecords bounds the records of one poll, i.e. the work between two offset
	// commits and the work a rebalance may have to wait for.
	MaxPollRecords int
	// CommitTimeout bounds the offset commit after a poll's records were handled.
	CommitTimeout time.Duration
	// DLQTimeout bounds one DLQ.Send.
	DLQTimeout time.Duration
	// Backoff is the pause after a poll in which a DLQ send failed, by consecutive
	// failures, before the record is redelivered and tried again.
	Backoff backoff.Func
	// StopOnExhausted makes Run return (without committing the record) when a record
	// runs out of retries, instead of sending it to the DLQ. Use it when the side effect
	// must never be skipped and a stuck consumer, which alerts on lag, is the better
	// failure. Permanent errors still go to the DLQ.
	StopOnExhausted bool
	Hooks           RunnerHooks
	Logger          *slog.Logger
}

// RunnerHooks are test-only seams. All fields are nil in production.
type RunnerHooks struct {
	// Handled runs for each record once it is handled: applied, a duplicate, or sent to
	// the DLQ (err is then the processing error).
	Handled func(rec *kgo.Record, res Result, err error)
	// BeforeCommit runs after a poll's records were handled, before their offsets are
	// committed. A non-nil error makes Run return it at once, committing nothing: F7 uses it
	// as a crash between the DB commit and the offset commit.
	BeforeCommit func(ctx context.Context, recs []*kgo.Record) error
}

// DefaultRunnerConfig returns production defaults.
func DefaultRunnerConfig() RunnerConfig {
	return RunnerConfig{
		MaxPollRecords: 100,
		CommitTimeout:  10 * time.Second,
		DLQTimeout:     15 * time.Second,
		Backoff:        backoff.Exp(500*time.Millisecond, 30*time.Second, nil),
		Logger:         slog.Default(),
	}
}

// Runner is the consumer-group poll loop around a Processor.
//
//	loop:
//	  poll ≤ MaxPollRecords                   rebalances are blocked from here...
//	  per partition, in offset order:
//	    Process(rec)
//	    failed → DLQ.Send                     permanent, or retries exhausted
//	    DLQ.Send failed → stop the partition  this record and the rest are not handled
//	  commit the handled records' offsets     after their DB commit (and DLQ send)
//	  rewind partitions that stopped early    redelivered by the next poll
//	  allow rebalance                         ...to here
//
// Offsets: auto-commit is off. An offset is committed only for a record that is
// handled, and a partition's later records are never handled before an earlier one, so a
// committed offset never skips a record. A record whose DLQ send failed is not committed:
// the partition stops at it, is rewound, and after Backoff the record is processed again.
// A crash after the DB commit but before the offset commit redelivers records that are
// already applied; the Processor skips them as duplicates (F7).
//
// Rebalance: kgo.BlockRebalanceOnPoll keeps the assignment fixed while a poll is being
// handled, so offsets are committed only for partitions this member still owns. When a
// rebalance is waiting (kgo.OnPartitionsCallbackBlocked), the in-flight work is cancelled
// so the group is not held up: the record being processed rolls back, the records not yet
// handled are rewound, and the handled ones are committed before the rebalance proceeds.
// If a partition moves, its new owner starts exactly after the last committed record.
//
// Shutdown: cancelling ctx works the same way. Handled records are committed (on a
// context that is not cancelled, bounded by CommitTimeout), the rest is left for the next
// owner, and the client leaves the group.
//
// Partitions of one poll are handled one after another; scale out with more members.
type Runner struct {
	proc *Processor
	dlq  DLQ
	cfg  RunnerConfig
	opts []kgo.Opt

	mu          sync.Mutex
	cancelBatch context.CancelCauseFunc // the in-flight batch, nil between polls
}

// NewRunner returns a Runner for a client built from opts (seed brokers, group, topics:
// see kafkax.ConsumerOpts). Run adds the options it relies on itself:
// kgo.DisableAutoCommit, kgo.BlockRebalanceOnPoll and kgo.OnPartitionsCallbackBlocked.
func NewRunner(p *Processor, dlq DLQ, cfg RunnerConfig, opts ...kgo.Opt) *Runner {
	def := DefaultRunnerConfig()
	if cfg.MaxPollRecords <= 0 {
		cfg.MaxPollRecords = def.MaxPollRecords
	}
	if cfg.CommitTimeout <= 0 {
		cfg.CommitTimeout = def.CommitTimeout
	}
	if cfg.DLQTimeout <= 0 {
		cfg.DLQTimeout = def.DLQTimeout
	}
	if cfg.Backoff == nil {
		cfg.Backoff = def.Backoff
	}
	if cfg.Logger == nil {
		cfg.Logger = def.Logger
	}
	return &Runner{proc: p, dlq: dlq, cfg: cfg, opts: slices.Clone(opts)}
}

var errRebalance = errors.New("consumer: rebalance waiting")

// Run joins the group and consumes until ctx is cancelled; it returns nil then. It
// returns an error if the client cannot be created, if a record exhausted its retries
// with StopOnExhausted set, or if Hooks.BeforeCommit fails. The client is closed (and
// the group left) before Run returns. Run may be called once.
func (r *Runner) Run(ctx context.Context) error {
	opts := append(slices.Clone(r.opts),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsCallbackBlocked(func(context.Context, *kgo.Client) { r.interrupt(errRebalance) }),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("consumer: runner: client: %w", err)
	}
	defer cl.CloseAllowingRebalance()

	dlqFailures := 0
	for {
		fetches := cl.PollRecords(ctx, r.cfg.MaxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			// Polled records, if any, are neither handled nor committed: the next owner
			// gets them from the committed offset.
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			r.cfg.Logger.WarnContext(ctx, "consumer: fetch failed", "topic", topic, "partition", partition, "err", err)
		})

		out, err := r.handlePoll(ctx, cl, fetches)
		cl.AllowRebalance()
		if err != nil {
			return err
		}
		if !out.dlqFailed {
			dlqFailures = 0
			continue
		}
		dlqFailures++
		delay := r.cfg.Backoff(dlqFailures)
		r.cfg.Logger.WarnContext(ctx, "consumer: DLQ unavailable; partition stopped at the failed record",
			"consecutive_failures", dlqFailures, "retry_in", delay)
		if !sleep(ctx, delay) {
			return nil
		}
	}
}

type pollOutcome struct {
	dlqFailed bool // a DLQ send failed: its partition was rewound to that record
}

// handlePoll handles one poll's records, commits the handled ones and rewinds the rest.
// It runs while rebalances are blocked; the caller allows them afterwards.
func (r *Runner) handlePoll(ctx context.Context, cl *kgo.Client, fetches kgo.Fetches) (pollOutcome, error) {
	batch, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	r.mu.Lock()
	r.cancelBatch = cancel
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.cancelBatch = nil
		r.mu.Unlock()
	}()

	var (
		out     pollOutcome
		handled []*kgo.Record
		rewind  = map[string]map[int32]kgo.EpochOffset{}
		fatal   error
	)
	// stopAt marks rec as the first record of its partition that is not handled.
	stopAt := func(rec *kgo.Record) {
		if rewind[rec.Topic] == nil {
			rewind[rec.Topic] = map[int32]kgo.EpochOffset{}
		}
		rewind[rec.Topic][rec.Partition] = kgo.EpochOffset{Epoch: rec.LeaderEpoch, Offset: rec.Offset}
	}

	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		for _, rec := range p.Records {
			if fatal != nil || batch.Err() != nil {
				stopAt(rec)
				return
			}
			ok, err := r.handle(batch, rec)
			if err != nil {
				fatal = err
			}
			if !ok {
				if batch.Err() == nil && fatal == nil {
					out.dlqFailed = true
				}
				stopAt(rec)
				return
			}
			handled = append(handled, rec)
		}
	})

	if cause := context.Cause(batch); errors.Is(cause, errRebalance) {
		r.cfg.Logger.InfoContext(ctx, "consumer: rebalance waiting; committing handled records, rewinding the rest",
			"handled", len(handled), "rewound_partitions", countPartitions(rewind))
	}

	if len(handled) > 0 {
		if h := r.cfg.Hooks.BeforeCommit; h != nil {
			if err := h(ctx, handled); err != nil {
				return out, fmt.Errorf("consumer: before commit hook: %w", err)
			}
		}
		// Committed even at shutdown: these records are in the database.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.CommitTimeout)
		err := cl.CommitRecords(commitCtx, handled...)
		cancel()
		if err != nil {
			r.cfg.Logger.WarnContext(ctx, "consumer: offset commit failed; the records will be redelivered and skipped as duplicates",
				"records", len(handled), "err", err)
		}
	}
	if fatal != nil {
		return out, fatal
	}
	if len(rewind) > 0 && ctx.Err() == nil {
		// Still inside the blocked-rebalance window, after the synchronous commit: the
		// partitions are ours and no commit runs concurrently, as SetOffsets requires. It
		// also drops buffered fetches, so the next poll starts exactly at these records.
		cl.SetOffsets(rewind)
	}
	return out, nil
}

// handle processes rec and, if that failed for good, sends it to the DLQ. It reports
// whether rec is handled, i.e. its offset may be committed. A non-nil error stops Run.
//
// It runs in the record's consumer span (see startProcessSpan): the Processor's attempts
// and the DLQ send are its children.
func (r *Runner) handle(ctx context.Context, rec *kgo.Record) (bool, error) {
	ctx, span := startProcessSpan(ctx, rec, r.proc.cfg.Consumer)
	defer span.End()

	res, err := r.proc.Process(ctx, rec)
	span.SetAttributes(attribute.Int("consumer.attempts", res.Attempts), attribute.Bool("consumer.duplicate", res.Duplicate))
	if err == nil {
		r.handled(rec, res, nil)
		return true, nil
	}
	failSpan(span, err)
	if ctx.Err() != nil {
		span.SetAttributes(attribute.Bool("consumer.interrupted", true))
		return false, nil //nolint:nilerr // interrupted (rebalance or shutdown): not handled, not failed
	}
	log := r.cfg.Logger.With("topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset,
		"attempts", res.Attempts, "err", err)
	if errors.Is(err, ErrRetriesExhausted) && r.cfg.StopOnExhausted {
		log.ErrorContext(ctx, "consumer: retries exhausted; stopping (StopOnExhausted)")
		return false, err
	}

	dlqCtx, cancel := context.WithTimeout(ctx, r.cfg.DLQTimeout)
	dlqErr := r.dlq.Send(dlqCtx, rec, err, res.Attempts)
	cancel()
	if dlqErr != nil {
		span.RecordError(dlqErr)
		if ctx.Err() == nil {
			log.ErrorContext(ctx, "consumer: DLQ send failed; offset not committed", "dlq_err", dlqErr)
		}
		return false, nil
	}
	span.SetAttributes(attribute.Bool("consumer.dead_lettered", true))
	log.WarnContext(ctx, "consumer: record sent to DLQ", "permanent", IsPermanent(err))
	r.handled(rec, res, err)
	return true, nil
}

func (r *Runner) handled(rec *kgo.Record, res Result, err error) {
	if h := r.cfg.Hooks.Handled; h != nil {
		h(rec, res, err)
	}
}

// interrupt cancels the in-flight batch, if any.
func (r *Runner) interrupt(cause error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancelBatch != nil {
		r.cancelBatch(cause)
	}
}

func countPartitions(m map[string]map[int32]kgo.EpochOffset) int {
	n := 0
	for _, ps := range m {
		n += len(ps)
	}
	return n
}
