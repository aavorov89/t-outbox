//go:build integration

package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aavorov89/t-outbox/internal/httpapi"
	"github.com/aavorov89/t-outbox/internal/payment/postgres"
	"github.com/aavorov89/t-outbox/internal/testenv"
)

var env *testenv.Env

func TestMain(m *testing.M) {
	testenv.Main(m, &env, testenv.WithPostgres())
}

type response struct {
	status   int
	replayed string
	body     string
}

func doPost(srv *httptest.Server, key, body string) (response, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/payments", strings.NewReader(body))
	if err != nil {
		return response{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(httpapi.HeaderIdempotencyKey, key)
	resp, err := srv.Client().Do(req)
	if err != nil {
		return response{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return response{resp.StatusCode, resp.Header.Get(httpapi.HeaderIdempotentReplayed), string(b)}, err
}

func doCapture(srv *httptest.Server, id, key string) (response, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/payments/"+id+"/capture", nil)
	if err != nil {
		return response{}, err
	}
	req.Header.Set(httpapi.HeaderIdempotencyKey, key)
	resp, err := srv.Client().Do(req)
	if err != nil {
		return response{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return response{resp.StatusCode, resp.Header.Get(httpapi.HeaderIdempotentReplayed), string(b)}, err
}

func postPayment(t *testing.T, srv *httptest.Server, key, body string) response {
	t.Helper()
	r, err := doPost(srv, key, body)
	require.NoError(t, err)
	return r
}

// F3: a client retries POST /payments with the same Idempotency-Key → 200 with the same
// payment, exactly one payment row and one outbox row. Concurrent retries behave the same:
// exactly one 201, the rest are replays.
func TestAPI_IdempotencyKey(t *testing.T) {
	pool := env.PG.NewPool(t, nil)
	srv := httptest.NewServer(httpapi.New(postgres.New(pool), httpapi.Options{Logger: slog.New(slog.DiscardHandler)}))
	t.Cleanup(srv.Close)
	ctx := context.Background()

	countRows := func(key string) (payments, events int) {
		t.Helper()
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM payments WHERE idempotency_key = $1),
			       (SELECT count(*) FROM outbox o JOIN payments p ON o.aggregate_id = p.id::text
			         WHERE p.idempotency_key = $1)`, key).Scan(&payments, &events))
		return payments, events
	}

	t.Run("sequential retry", func(t *testing.T) {
		first := postPayment(t, srv, "seq", validBody)
		require.Equal(t, http.StatusCreated, first.status, first.body)
		second := postPayment(t, srv, "seq", validBody)
		require.Equal(t, http.StatusOK, second.status, second.body)
		require.Equal(t, "true", second.replayed)
		require.JSONEq(t, first.body, second.body, "replay returns the stored payment unchanged")

		nPay, nEvt := countRows("seq")
		require.Equal(t, 1, nPay)
		require.Equal(t, 1, nEvt)
	})

	t.Run("concurrent retries", func(t *testing.T) {
		const n = 16
		results := make([]response, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() { results[i], errs[i] = doPost(srv, "conc", validBody) })
		}
		wg.Wait()
		require.NoError(t, errors.Join(errs...))

		var created int
		ids := map[string]bool{}
		for _, r := range results {
			require.Contains(t, []int{http.StatusCreated, http.StatusOK}, r.status, r.body)
			if r.status == http.StatusCreated {
				created++
			}
			var p httpapi.PaymentResponse
			require.NoError(t, json.Unmarshal([]byte(r.body), &p))
			ids[p.ID.String()] = true
		}
		require.Equal(t, 1, created, "exactly one request creates the payment")
		require.Len(t, ids, 1, "all responses describe the same payment")

		nPay, nEvt := countRows("conc")
		require.Equal(t, 1, nPay)
		require.Equal(t, 1, nEvt)
	})

	t.Run("same key, different body", func(t *testing.T) {
		require.Equal(t, http.StatusCreated, postPayment(t, srv, "diff", validBody).status)
		r := postPayment(t, srv, "diff", `{"merchant_id":"m_42","amount":{"amount_minor":1,"currency":"EUR"}}`)
		require.Equal(t, http.StatusUnprocessableEntity, r.status)
		var p httpapi.Problem
		require.NoError(t, json.Unmarshal([]byte(r.body), &p))
		require.Equal(t, httpapi.TypeIdempotencyKeyReused, p.Type)

		nPay, nEvt := countRows("diff")
		require.Equal(t, 1, nPay)
		require.Equal(t, 1, nEvt)
	})
}

// F3 for capture: retries with the capture's Idempotency-Key replay it; a capture with
// another key conflicts. Concurrent captures of one payment are serialized by the row
// lock: exactly one of them emits payment.captured.
func TestAPI_CaptureIdempotencyKey(t *testing.T) {
	pool := env.PG.NewPool(t, nil)
	srv := httptest.NewServer(httpapi.New(postgres.New(pool), httpapi.Options{Logger: slog.New(slog.DiscardHandler)}))
	t.Cleanup(srv.Close)
	ctx := context.Background()

	newPayment := func(t *testing.T, key string) string {
		t.Helper()
		r := postPayment(t, srv, key, validBody)
		require.Equal(t, http.StatusCreated, r.status, r.body)
		var p httpapi.PaymentResponse
		require.NoError(t, json.Unmarshal([]byte(r.body), &p))
		return p.ID.String()
	}
	capturedEvents := func(t *testing.T, id string) int {
		t.Helper()
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox WHERE aggregate_id = $1 AND event_type = 'payment.captured'`, id).Scan(&n))
		return n
	}
	concurrently := func(t *testing.T, n int, f func(i int) (response, error)) []response {
		t.Helper()
		results := make([]response, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() { results[i], errs[i] = f(i) })
		}
		wg.Wait()
		require.NoError(t, errors.Join(errs...))
		return results
	}

	t.Run("sequential retry", func(t *testing.T) {
		id := newPayment(t, "seq")
		first, err := doCapture(srv, id, "cap")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, first.status, first.body)
		require.Empty(t, first.replayed)

		second, err := doCapture(srv, id, "cap")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, second.status)
		require.Equal(t, "true", second.replayed)
		require.JSONEq(t, first.body, second.body)

		other, err := doCapture(srv, id, "cap-other")
		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, other.status)

		require.Equal(t, 1, capturedEvents(t, id))
	})

	t.Run("concurrent retries, same key", func(t *testing.T) {
		id := newPayment(t, "conc-same")
		results := concurrently(t, 16, func(int) (response, error) { return doCapture(srv, id, "cap") })
		var fresh int
		for _, r := range results {
			require.Equal(t, http.StatusOK, r.status, r.body)
			if r.replayed == "" {
				fresh++
			}
		}
		require.Equal(t, 1, fresh, "exactly one request performs the capture")
		require.Equal(t, 1, capturedEvents(t, id))
	})

	t.Run("concurrent captures, different keys", func(t *testing.T) {
		id := newPayment(t, "conc-diff")
		results := concurrently(t, 16, func(i int) (response, error) { return doCapture(srv, id, fmt.Sprint("cap-", i)) })
		statuses := map[int]int{}
		for _, r := range results {
			statuses[r.status]++
		}
		require.Equal(t, map[int]int{http.StatusOK: 1, http.StatusConflict: 15}, statuses)
		require.Equal(t, 1, capturedEvents(t, id))
	})
}
