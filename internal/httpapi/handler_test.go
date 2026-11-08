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
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/aavorov89/t-outbox/internal/httpapi"
	"github.com/aavorov89/t-outbox/internal/outbox"
	"github.com/aavorov89/t-outbox/internal/payment"
)

// memStore is an in-memory httpapi.Store.
type memStore struct {
	mu        sync.Mutex
	byKey     map[string]payment.Payment
	events    []outbox.Message // written by Update
	createErr error
}

func (s *memStore) Create(_ context.Context, p payment.Payment, _ outbox.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return s.createErr
	}
	if s.byKey == nil {
		s.byKey = map[string]payment.Payment{}
	}
	if _, ok := s.byKey[p.IdempotencyKey]; ok {
		return payment.ErrDuplicateIdempotencyKey
	}
	s.byKey[p.IdempotencyKey] = p
	return nil
}

func (s *memStore) Get(_ context.Context, id uuid.UUID) (payment.Payment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.byKey {
		if p.ID == id {
			return p, nil
		}
	}
	return payment.Payment{}, payment.ErrNotFound
}

func (s *memStore) Update(_ context.Context, id uuid.UUID, fn payment.UpdateFunc) (payment.Payment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, p := range s.byKey {
		if p.ID != id {
			continue
		}
		next, msg, err := fn(p)
		if err != nil {
			return payment.Payment{}, err
		}
		if msg == nil {
			return p, nil
		}
		s.byKey[key] = next
		s.events = append(s.events, *msg)
		return next, nil
	}
	return payment.Payment{}, payment.ErrNotFound
}

func (s *memStore) GetByIdempotencyKey(_ context.Context, key string) (payment.Payment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.byKey[key]; ok {
		return p, nil
	}
	return payment.Payment{}, payment.ErrNotFound
}

var fixedNow = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

func newHandler(s httpapi.Store) http.Handler {
	return httpapi.New(s, httpapi.Options{
		Logger: slog.New(slog.DiscardHandler),
		Now:    func() time.Time { return fixedNow },
	})
}

const validBody = `{"merchant_id":"m_42","amount":{"amount_minor":1999,"currency":"EUR"}}`

func post(t *testing.T, h http.Handler, key, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/payments", strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if key != "" {
		req.Header.Set(httpapi.HeaderIdempotencyKey, key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeProblem(t *testing.T, rec *httptest.ResponseRecorder) httpapi.Problem {
	t.Helper()
	require.Equal(t, httpapi.ProblemContentType, rec.Header().Get("Content-Type"))
	var p httpapi.Problem
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
	require.Equal(t, rec.Code, p.Status, "status member must match the HTTP status")
	require.NotEmpty(t, p.Title)
	return p
}

func TestCreatePayment_Created(t *testing.T) {
	h := newHandler(&memStore{})
	rec := post(t, h, "k1", "application/json; charset=utf-8", validBody)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var got httpapi.PaymentResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, "/payments/"+got.ID.String(), rec.Header().Get("Location"))
	require.Equal(t, "m_42", got.MerchantID)
	require.Equal(t, int64(1999), got.Amount.AmountMinor)
	require.Equal(t, "created", got.Status)
	require.Equal(t, 1, got.Version)
	require.Equal(t, fixedNow, got.CreatedAt)
	require.Empty(t, rec.Header().Get(httpapi.HeaderIdempotentReplayed))
}

func TestCreatePayment_Replay(t *testing.T) {
	h := newHandler(&memStore{})
	first := post(t, h, "k1", "application/json", validBody)
	require.Equal(t, http.StatusCreated, first.Code)

	// Same request, different JSON formatting: still the same request.
	second := post(t, h, "k1", "application/json", `{ "amount": {"currency":"EUR","amount_minor":1999}, "merchant_id":"m_42" }`)
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	require.Equal(t, "true", second.Header().Get(httpapi.HeaderIdempotentReplayed))
	require.JSONEq(t, first.Body.String(), second.Body.String())
}

func TestCreatePayment_KeyReusedWithDifferentBody(t *testing.T) {
	h := newHandler(&memStore{})
	require.Equal(t, http.StatusCreated, post(t, h, "k1", "application/json", validBody).Code)

	for name, body := range map[string]string{
		"amount":   `{"merchant_id":"m_42","amount":{"amount_minor":2000,"currency":"EUR"}}`,
		"currency": `{"merchant_id":"m_42","amount":{"amount_minor":1999,"currency":"USD"}}`,
		"merchant": `{"merchant_id":"m_43","amount":{"amount_minor":1999,"currency":"EUR"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			rec := post(t, h, "k1", "application/json", body)
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
			p := decodeProblem(t, rec)
			require.Equal(t, httpapi.TypeIdempotencyKeyReused, p.Type)
			require.Equal(t, "/payments", p.Instance)
		})
	}
}

func TestCreatePayment_Problems(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		contentType string
		body        string
		status      int
		typ         string
		pointers    []string
	}{
		{"missing key", "", "application/json", validBody, 400, httpapi.TypeIdempotencyKeyMissing, nil},
		{"key with space", "a b", "application/json", validBody, 400, httpapi.TypeIdempotencyKeyMissing, nil},
		{"key too long", strings.Repeat("k", 256), "application/json", validBody, 400, httpapi.TypeIdempotencyKeyMissing, nil},
		{"no content type", "k", "", validBody, 415, httpapi.TypeAboutBlank, nil},
		{"wrong content type", "k", "text/plain", validBody, 415, httpapi.TypeAboutBlank, nil},
		{"not json", "k", "application/json", `{nope`, 400, httpapi.TypeMalformedRequest, nil},
		{"unknown field", "k", "application/json", `{"merchant_id":"m","amount":{"amount_minor":1,"currency":"EUR"},"x":1}`, 400, httpapi.TypeMalformedRequest, nil},
		{"trailing data", "k", "application/json", validBody + `{}`, 400, httpapi.TypeMalformedRequest, nil},
		{"fractional amount", "k", "application/json", `{"merchant_id":"m","amount":{"amount_minor":19.99,"currency":"EUR"}}`, 400, httpapi.TypeMalformedRequest, nil},
		{"amount as string", "k", "application/json", `{"merchant_id":"m","amount":{"amount_minor":"1999","currency":"EUR"}}`, 400, httpapi.TypeMalformedRequest, nil},
		{"too large", "k", "application/json", `{"merchant_id":"` + strings.Repeat("m", 70<<10) + `"}`, 413, httpapi.TypeAboutBlank, nil},
		{"empty object", "k", "application/json", `{}`, 422, httpapi.TypeValidation, []string{"/merchant_id", "/amount"}},
		{"bad fields", "k", "application/json", `{"merchant_id":"m","amount":{"amount_minor":0,"currency":"eur"}}`, 422, httpapi.TypeValidation, []string{"/amount/amount_minor", "/amount/currency"}},
		{"long merchant", "k", "application/json", `{"merchant_id":"` + strings.Repeat("m", 65) + `","amount":{"amount_minor":1,"currency":"EUR"}}`, 422, httpapi.TypeValidation, []string{"/merchant_id"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &memStore{}
			rec := post(t, newHandler(store), tt.key, tt.contentType, tt.body)
			require.Equal(t, tt.status, rec.Code, rec.Body.String())
			p := decodeProblem(t, rec)
			require.Equal(t, tt.typ, p.Type)
			var pointers []string
			for _, e := range p.Errors {
				pointers = append(pointers, e.Pointer)
			}
			require.Equal(t, tt.pointers, pointers)
			require.Empty(t, store.byKey, "nothing must be stored")
		})
	}
}

func TestCreatePayment_StoreFailureIs500WithoutDetails(t *testing.T) {
	rec := post(t, newHandler(&memStore{createErr: errors.New("pg: connection reset, password=hunter2")}),
		"k", "application/json", validBody)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	p := decodeProblem(t, rec)
	require.Equal(t, httpapi.TypeAboutBlank, p.Type)
	require.NotContains(t, rec.Body.String(), "hunter2")
}

func TestGetPayment(t *testing.T) {
	store := &memStore{}
	h := newHandler(store)
	created := post(t, h, "k1", "application/json", validBody)
	require.Equal(t, http.StatusCreated, created.Code)

	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
		return rec
	}
	rec := get(created.Header().Get("Location"))
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, created.Body.String(), rec.Body.String())

	for _, path := range []string{"/payments/" + uuid.NewString(), "/payments/not-a-uuid"} {
		rec := get(path)
		require.Equal(t, http.StatusNotFound, rec.Code, path)
		require.Equal(t, httpapi.TypeAboutBlank, decodeProblem(t, rec).Type)
	}
}

func TestRecoverer(t *testing.T) {
	h := newHandler(&panicStore{})
	rec := post(t, h, "k", "application/json", validBody)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	require.NotContains(t, string(body), "boom")
}

type panicStore struct{ memStore }

func (*panicStore) Create(context.Context, payment.Payment, outbox.Message) error { panic("boom") }

func capture(t *testing.T, h http.Handler, path, key string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path+"/capture", nil)
	if key != "" {
		req.Header.Set(httpapi.HeaderIdempotencyKey, key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCapturePayment(t *testing.T) {
	store := &memStore{}
	h := newHandler(store)
	created := post(t, h, "k1", "application/json", validBody)
	require.Equal(t, http.StatusCreated, created.Code)
	path := created.Header().Get("Location")

	first := capture(t, h, path, "cap-1")
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	require.Empty(t, first.Header().Get(httpapi.HeaderIdempotentReplayed))
	var got httpapi.PaymentResponse
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &got))
	require.Equal(t, "captured", got.Status)
	require.Equal(t, 2, got.Version)
	require.Len(t, store.events, 1)
	require.Equal(t, 2, store.events[0].AggregateVersion)

	t.Run("retry with the same key replays", func(t *testing.T) {
		rec := capture(t, h, path, "cap-1")
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "true", rec.Header().Get(httpapi.HeaderIdempotentReplayed))
		require.JSONEq(t, first.Body.String(), rec.Body.String())
		require.Len(t, store.events, 1, "no second event")
	})

	t.Run("another key conflicts", func(t *testing.T) {
		rec := capture(t, h, path, "cap-2")
		require.Equal(t, http.StatusConflict, rec.Code)
		require.Equal(t, httpapi.TypeInvalidStateTransition, decodeProblem(t, rec).Type)
		require.Len(t, store.events, 1)
	})
}

func TestCapturePayment_Problems(t *testing.T) {
	store := &memStore{}
	h := newHandler(store)
	created := post(t, h, "k1", "application/json", validBody)
	require.Equal(t, http.StatusCreated, created.Code)
	path := created.Header().Get("Location")

	tests := []struct {
		name, path, key string
		status          int
		typ             string
	}{
		{"missing key", path, "", 400, httpapi.TypeIdempotencyKeyMissing},
		{"bad key", path, "a b", 400, httpapi.TypeIdempotencyKeyMissing},
		{"unknown payment", "/payments/" + uuid.NewString(), "k", 404, httpapi.TypeAboutBlank},
		{"not a uuid", "/payments/nope", "k", 404, httpapi.TypeAboutBlank},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := capture(t, h, tt.path, tt.key)
			require.Equal(t, tt.status, rec.Code, rec.Body.String())
			require.Equal(t, tt.typ, decodeProblem(t, rec).Type)
		})
	}
	require.Empty(t, store.events)
}
