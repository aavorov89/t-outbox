// Package httpapi holds the HTTP handlers and middleware of payments-api.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/aavorov89/t-outbox/internal/event"
	"github.com/aavorov89/t-outbox/internal/money"
	"github.com/aavorov89/t-outbox/internal/outbox"
	"github.com/aavorov89/t-outbox/internal/payment"
)

const (
	// HeaderIdempotencyKey is required on POST /payments.
	HeaderIdempotencyKey = "Idempotency-Key"
	// HeaderIdempotentReplayed is "true" on a response that returns an existing payment.
	HeaderIdempotentReplayed = "Idempotent-Replayed"

	maxIdempotencyKeyLen = 255
	maxMerchantIDLen     = 64
	maxBodyBytes         = 64 << 10
)

// Store is what the handlers need from the payment repository.
type Store interface {
	// Create writes the payment and its event atomically. It returns
	// payment.ErrDuplicateIdempotencyKey if the key is taken and writes nothing.
	Create(ctx context.Context, p payment.Payment, msg outbox.Message) error
	Get(ctx context.Context, id uuid.UUID) (payment.Payment, error)
	GetByIdempotencyKey(ctx context.Context, key string) (payment.Payment, error)
}

// Options configures the API. Zero values get defaults.
type Options struct {
	Logger *slog.Logger     // default slog.Default()
	Now    func() time.Time // default time.Now
}

type api struct {
	store Store
	log   *slog.Logger
	now   func() time.Time
}

// New returns the payments-api handler with middleware applied.
func New(store Store, opts Options) http.Handler {
	a := &api{store: store, log: opts.Logger, now: opts.Now}
	if a.log == nil {
		a.log = slog.Default()
	}
	if a.now == nil {
		a.now = time.Now
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", a.createPayment)
	mux.HandleFunc("GET /payments/{id}", a.getPayment)
	return a.recoverer(a.logRequests(mux))
}

// CreatePaymentRequest is the body of POST /payments.
type CreatePaymentRequest struct {
	MerchantID string        `json:"merchant_id"`
	Amount     *event.Amount `json:"amount"`
}

// PaymentResponse is the representation of a payment.
type PaymentResponse struct {
	ID         uuid.UUID    `json:"id"`
	MerchantID string       `json:"merchant_id"`
	Amount     event.Amount `json:"amount"`
	Status     string       `json:"status"`
	Version    int          `json:"version"`
	CreatedAt  time.Time    `json:"created_at"`
	UpdatedAt  time.Time    `json:"updated_at"`
}

func toResponse(p payment.Payment) PaymentResponse {
	return PaymentResponse{
		ID:         p.ID,
		MerchantID: p.MerchantID,
		Amount:     event.Amount{AmountMinor: p.Amount.Amount, Currency: p.Amount.Currency.Code()},
		Status:     string(p.Status),
		Version:    p.Version,
		CreatedAt:  p.CreatedAt.UTC(),
		UpdatedAt:  p.UpdatedAt.UTC(),
	}
}

// createPayment implements POST /payments.
//
// Idempotency (draft-ietf-httpapi-idempotency-key-header):
//   - new key → 201 Created;
//   - same key, same body → 200 OK with the existing payment, Idempotent-Replayed: true;
//   - same key, different body → 422 idempotency-key-reused.
//
// The draft's 409 Conflict ("a request with this key is still being processed") cannot
// happen here: the key is claimed by the same transaction that writes the payment, so a
// concurrent duplicate waits on the unique index and then sees the committed payment.
func (a *api) createPayment(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get(HeaderIdempotencyKey)
	if problem, ok := checkIdempotencyKey(key); !ok {
		writeProblem(w, r, problem)
		return
	}
	var req CreatePaymentRequest
	if !a.decodeJSON(w, r, &req) {
		return
	}
	amount, fieldErrs := req.validate()
	if len(fieldErrs) > 0 {
		writeProblem(w, r, Problem{
			Type:   TypeValidation,
			Title:  "Request validation failed",
			Status: http.StatusUnprocessableEntity,
			Errors: fieldErrs,
		})
		return
	}

	p, msg, err := payment.New(key, req.MerchantID, amount, a.now())
	if err != nil {
		// The request was validated above; reaching this is a bug, not a client error.
		a.internalError(w, r, err)
		return
	}

	err = a.store.Create(r.Context(), p, msg)
	switch {
	case err == nil:
		w.Header().Set("Location", "/payments/"+p.ID.String())
		a.writeJSON(w, http.StatusCreated, toResponse(p))
	case errors.Is(err, payment.ErrDuplicateIdempotencyKey):
		a.replay(w, r, key, req.MerchantID, amount)
	default:
		a.internalError(w, r, err)
	}
}

func (a *api) replay(w http.ResponseWriter, r *http.Request, key, merchantID string, amount money.Money) {
	existing, err := a.store.GetByIdempotencyKey(r.Context(), key)
	if err != nil {
		// ErrNotFound included: the key was taken a moment ago, payments are never deleted.
		a.internalError(w, r, err)
		return
	}
	if !existing.SameRequest(merchantID, amount) {
		writeProblem(w, r, Problem{
			Type:   TypeIdempotencyKeyReused,
			Title:  "Idempotency key reused with a different request",
			Status: http.StatusUnprocessableEntity,
			Detail: "This Idempotency-Key was already used for a payment with different parameters. Use a new key for a new payment.",
		})
		return
	}
	w.Header().Set(HeaderIdempotentReplayed, "true")
	w.Header().Set("Location", "/payments/"+existing.ID.String())
	a.writeJSON(w, http.StatusOK, toResponse(existing))
}

// getPayment implements GET /payments/{id}.
func (a *api) getPayment(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeStatus(w, r, http.StatusNotFound, "")
		return
	}
	p, err := a.store.Get(r.Context(), id)
	switch {
	case errors.Is(err, payment.ErrNotFound):
		writeStatus(w, r, http.StatusNotFound, "")
	case err != nil:
		a.internalError(w, r, err)
	default:
		a.writeJSON(w, http.StatusOK, toResponse(p))
	}
}

func checkIdempotencyKey(key string) (Problem, bool) {
	bad := func(detail string) (Problem, bool) {
		return Problem{
			Type:   TypeIdempotencyKeyMissing,
			Title:  "Idempotency-Key header missing or invalid",
			Status: http.StatusBadRequest,
			Detail: detail,
		}, false
	}
	if key == "" {
		return bad("POST /payments requires an Idempotency-Key header, e.g. a UUID generated by the client per payment.")
	}
	if len(key) > maxIdempotencyKeyLen {
		return bad("Idempotency-Key must be at most 255 characters.")
	}
	for _, c := range []byte(key) {
		if c < 0x21 || c > 0x7e {
			return bad("Idempotency-Key must be printable ASCII without spaces.")
		}
	}
	return Problem{}, true
}

func (req CreatePaymentRequest) validate() (money.Money, []FieldError) {
	var errs []FieldError
	switch {
	case strings.TrimSpace(req.MerchantID) == "":
		errs = append(errs, FieldError{"/merchant_id", "is required"})
	case len(req.MerchantID) > maxMerchantIDLen:
		errs = append(errs, FieldError{"/merchant_id", "must be at most 64 characters"})
	}
	if req.Amount == nil {
		return money.Money{}, append(errs, FieldError{"/amount", "is required"})
	}
	if req.Amount.AmountMinor <= 0 {
		errs = append(errs, FieldError{"/amount/amount_minor", "must be a positive integer (minor units)"})
	}
	amount, err := money.Parse(req.Amount.AmountMinor, req.Amount.Currency)
	if err != nil {
		errs = append(errs, FieldError{"/amount/currency", "must be a supported ISO 4217 code"})
	}
	return amount, errs
}

// decodeJSON reads exactly one JSON object with known fields only. On failure it writes
// the problem response and returns false.
func (a *api) decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		writeStatus(w, r, http.StatusUnsupportedMediaType, "Content-Type must be application/json.")
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	err = dec.Decode(dst)
	if err == nil && !errors.Is(dec.Decode(&struct{}{}), io.EOF) {
		err = errors.New("unexpected data after the JSON object")
	}
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeStatus(w, r, http.StatusRequestEntityTooLarge, "")
			return false
		}
		writeProblem(w, r, Problem{
			Type:   TypeMalformedRequest,
			Title:  "Malformed request body",
			Status: http.StatusBadRequest,
			Detail: err.Error(),
		})
		return false
	}
	return true
}

func (a *api) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		a.log.Warn("write response", "err", err)
	}
}

// internalError logs err and answers 500 without leaking it.
func (a *api) internalError(w http.ResponseWriter, r *http.Request, err error) {
	a.log.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeStatus(w, r, http.StatusInternalServerError, "")
}
