//go:build integration

package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
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
