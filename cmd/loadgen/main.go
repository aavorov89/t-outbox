// Command loadgen puts load on the running system and reports what it measured: API
// latency, and the lag of the asynchronous pipeline read from the database's own
// timestamps (outbox.created_at → published_at → processed_events.processed_at).
//
//	go run ./cmd/loadgen -duration 30s -concurrency 16   # load, wait for the pipeline, report
//	go run ./cmd/loadgen -duration 60s -rate 100         # 100 payments/s (200 events/s), for lag
//	go run ./cmd/loadgen -payments 20000 -wait=false     # only write; prints the outbox id to report from
//	go run ./cmd/loadgen -report-since 12345             # no load: wait, then report rows after that id
//
// Each client creates a payment and captures it (two requests, two outbox events), as
// fast as the API answers. `make load` (scripts/load.sh) runs the scenarios of the README.
// Needs the app running (make app or the run-* targets); API_URL and DATABASE_URL as for
// the other tools.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

type options struct {
	api, db      string
	duration     time.Duration
	payments     int
	rate         float64
	concurrency  int
	wait         bool
	reportSince  int64
	drainTimeout time.Duration
}

func run() error {
	var o options
	flag.StringVar(&o.api, "api", getenv("API_URL", "http://localhost:8080"), "payments-api base URL")
	flag.StringVar(&o.db, "db", getenv("DATABASE_URL", "postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable"), "Postgres DSN")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "how long to generate load (ignored with -payments)")
	flag.IntVar(&o.payments, "payments", 0, "create and capture exactly this many payments instead of running for -duration")
	flag.Float64Var(&o.rate, "rate", 0, "payments per second over all clients (0: as fast as the API answers)")
	flag.IntVar(&o.concurrency, "concurrency", 16, "parallel clients")
	flag.BoolVar(&o.wait, "wait", true, "wait until the relays and the consumer caught up, then report the pipeline")
	flag.Int64Var(&o.reportSince, "report-since", -1, "no load: report the pipeline for outbox rows with id > this")
	flag.DurationVar(&o.drainTimeout, "drain-timeout", 5*time.Minute, "how long to wait for the pipeline")
	flag.Parse()
	if o.concurrency < 1 {
		return errors.New("-concurrency must be positive")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, o.db)
	if err != nil {
		return err
	}
	defer pool.Close()

	since := o.reportSince
	var loadErr error
	if since < 0 {
		if err := pool.QueryRow(ctx, `SELECT coalesce(max(id), 0) FROM outbox`).Scan(&since); err != nil {
			return fmt.Errorf("read outbox position: %w", err)
		}
		res, err := generate(ctx, o)
		if err != nil {
			return err
		}
		res.print(o)
		if res.errors > 0 {
			loadErr = fmt.Errorf("%d payments failed (first: %w)", res.errors, res.firstErr)
		}
		if !o.wait {
			fmt.Printf("\nreport later with: -report-since %d\n", since)
			return loadErr
		}
	}
	return errors.Join(report(ctx, pool, since, o.drainTimeout), loadErr)
}

// ---- load ----

type result struct {
	elapsed         time.Duration
	create, capture []time.Duration
	errors          int // failed payments (create or capture)
	firstErr        error
	payments        int
}

func generate(ctx context.Context, o options) (*result, error) {
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: o.concurrency, MaxConnsPerHost: o.concurrency},
	}
	var (
		mu    sync.Mutex
		total = &result{}
		wg    sync.WaitGroup
		left  = make(chan struct{}, max(o.payments, 0))
	)
	for range o.payments {
		left <- struct{}{}
	}
	close(left)
	loadCtx := ctx
	if o.payments == 0 {
		var cancel context.CancelFunc
		loadCtx, cancel = context.WithTimeout(ctx, o.duration)
		defer cancel()
	}

	var tick <-chan time.Time // nil: no rate limit
	if o.rate > 0 {
		t := time.NewTicker(time.Duration(float64(time.Second) / o.rate))
		defer t.Stop()
		tick = t.C
	}

	start := time.Now()
	for range o.concurrency {
		wg.Go(func() {
			r := &result{}
			for loadCtx.Err() == nil {
				if o.payments > 0 {
					if _, ok := <-left; !ok {
						break
					}
				}
				if tick != nil {
					select {
					case <-tick:
					case <-loadCtx.Done():
						continue
					}
				}
				if err := onePayment(loadCtx, client, o.api, r); err != nil && loadCtx.Err() == nil {
					r.errors++
					if r.firstErr == nil {
						r.firstErr = err
					}
				}
			}
			mu.Lock()
			defer mu.Unlock()
			total.create = append(total.create, r.create...)
			total.capture = append(total.capture, r.capture...)
			total.payments += r.payments
			total.errors += r.errors
			if total.firstErr == nil {
				total.firstErr = r.firstErr
			}
		})
	}
	wg.Wait()
	total.elapsed = time.Since(start)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return total, nil
}

// onePayment creates and captures one payment, recording the latency of each request.
func onePayment(ctx context.Context, c *http.Client, api string, r *result) error {
	body := fmt.Sprintf(`{"merchant_id":"m_%d","amount":{"amount_minor":%d,"currency":"EUR"}}`,
		rand.IntN(5)+1, rand.IntN(9900)+100)
	var created struct {
		ID string `json:"id"`
	}
	d, err := call(ctx, c, api+"/payments", body, &created)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	r.create = append(r.create, d)
	d, err = call(ctx, c, api+"/payments/"+created.ID+"/capture", "", nil)
	if err != nil {
		return fmt.Errorf("capture: %w", err)
	}
	r.capture = append(r.capture, d)
	r.payments++
	return nil
}

func call(ctx context.Context, c *http.Client, url, body string, out any) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Idempotency-Key", uuid.NewString())
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	start := time.Now()
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	d := time.Since(start)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode/100 != 2 {
		return 0, fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(b))
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			return 0, err
		}
	}
	return d, nil
}

func (r *result) print(o options) {
	fmt.Printf("load: %d clients for %v: %d payments created and captured (%.0f payments/s, %.0f requests/s)\n",
		o.concurrency, r.elapsed.Round(time.Millisecond), r.payments,
		float64(r.payments)/r.elapsed.Seconds(), float64(len(r.create)+len(r.capture))/r.elapsed.Seconds())
	fmt.Printf("\n%-10s %8s %8s %8s %8s %8s\n", "API", "requests", "p50", "p95", "p99", "max")
	for _, row := range []struct {
		name string
		d    []time.Duration
	}{{"create", r.create}, {"capture", r.capture}} {
		slices.Sort(row.d)
		fmt.Printf("%-10s %8d %8s %8s %8s %8s\n", row.name, len(row.d),
			ms(pct(row.d, 0.50)), ms(pct(row.d, 0.95)), ms(pct(row.d, 0.99)), ms(pct(row.d, 1)))
	}
	if r.errors > 0 {
		fmt.Printf("\nerrors: %d (first: %v)\n", r.errors, r.firstErr)
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, int(p*float64(len(sorted))))]
}

func ms(d time.Duration) string { return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000) }

// ---- pipeline report ----

// report waits until every outbox row after since is published and applied by the
// ledger consumer, then prints the lag percentiles and the relays' throughput, all from
// the database's timestamps (one clock, no skew between hosts).
func report(ctx context.Context, pool *pgxpool.Pool, since int64, timeout time.Duration) error {
	fmt.Printf("\nwaiting for the relays and the consumer (outbox id > %d) ...\n", since)
	deadline := time.Now().Add(timeout)
	for {
		var pending, unprocessed int64
		err := pool.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE published_at IS NULL),
			       count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM processed_events p
			                                          WHERE p.consumer = 'ledger' AND p.event_id = o.event_id))
			FROM outbox o WHERE id > $1`, since).Scan(&pending, &unprocessed)
		if err != nil {
			return err
		}
		if pending == 0 && unprocessed == 0 {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not drained after %v: %d unpublished, %d unprocessed", timeout, pending, unprocessed)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}

	const lagSQL = `
	SELECT count(*),
	       percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY lag),
	       max(lag)
	FROM (SELECT extract(epoch FROM %s)::float8 AS lag
	      FROM outbox o JOIN processed_events p ON p.consumer = 'ledger' AND p.event_id = o.event_id
	      WHERE o.id > $1) t`
	fmt.Printf("\n%-28s %8s %8s %8s %8s %8s\n", "pipeline (DB timestamps)", "events", "p50", "p95", "p99", "max")
	for _, row := range []struct{ name, expr string }{
		{"write → published", "o.published_at - o.created_at"},
		{"published → applied", "p.processed_at - o.published_at"},
		{"write → applied (e2e)", "p.processed_at - o.created_at"},
	} {
		var (
			n   int64
			ps  []float64
			top float64
		)
		if err := pool.QueryRow(ctx, fmt.Sprintf(lagSQL, row.expr), since).Scan(&n, &ps, &top); err != nil {
			return err
		}
		if n == 0 {
			fmt.Println("no events")
			return nil
		}
		fmt.Printf("%-28s %8d %8s %8s %8s %8s\n", row.name, n, sec(ps[0]), sec(ps[1]), sec(ps[2]), sec(top))
	}

	var (
		n      int64
		window float64
	)
	if err := pool.QueryRow(ctx, `
		SELECT count(*), extract(epoch FROM max(published_at) - min(published_at))::float8
		FROM outbox WHERE id > $1`, since).Scan(&n, &window); err != nil {
		return err
	}
	if window > 0 {
		fmt.Printf("\nrelays:   %d events published in %.2fs (first → last published_at) = %.0f events/s\n",
			n, window, float64(n)/window)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*), extract(epoch FROM max(p.processed_at) - min(p.processed_at))::float8
		FROM outbox o JOIN processed_events p ON p.consumer = 'ledger' AND p.event_id = o.event_id
		WHERE o.id > $1`, since).Scan(&n, &window); err != nil {
		return err
	}
	if window > 0 {
		fmt.Printf("consumer: %d events applied in %.2fs (first → last processed_at) = %.0f events/s\n",
			n, window, float64(n)/window)
	}
	return nil
}

func sec(s float64) string { return ms(time.Duration(s * float64(time.Second))) }

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
