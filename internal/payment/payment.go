// Package payment is the payment aggregate: its state and the events it emits.
package payment

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/aavorov89/t-outbox/internal/event"
	"github.com/aavorov89/t-outbox/internal/money"
	"github.com/aavorov89/t-outbox/internal/outbox"
)

// Status is the payment's lifecycle state.
type Status string

const (
	StatusCreated  Status = "created"
	StatusCaptured Status = "captured"
	StatusFailed   Status = "failed"
)

var (
	// ErrInvalid wraps every validation failure of New.
	ErrInvalid = errors.New("payment: invalid")
	// ErrNotFound is returned by repositories when no payment matches.
	ErrNotFound = errors.New("payment: not found")
	// ErrDuplicateIdempotencyKey is returned by repositories when a payment with the
	// same idempotency key already exists. Nothing was written.
	ErrDuplicateIdempotencyKey = errors.New("payment: duplicate idempotency key")
	// ErrInvalidTransition matches every *TransitionError.
	ErrInvalidTransition = errors.New("payment: invalid state transition")
)

// TransitionError is returned when an action is not allowed in the payment's current
// status. errors.Is(err, ErrInvalidTransition) is true for it.
type TransitionError struct {
	Action string // e.g. "capture"
	From   Status
	// AlreadyDone is true if the payment already is in the action's target state, reached
	// by a different request (another idempotency key).
	AlreadyDone bool
}

func (e *TransitionError) Error() string {
	if e.AlreadyDone {
		return fmt.Sprintf("payment: cannot %s: already %s by another request", e.Action, e.From)
	}
	return fmt.Sprintf("payment: cannot %s a payment in status %s", e.Action, e.From)
}

func (e *TransitionError) Is(target error) bool { return target == ErrInvalidTransition }

// Payment is the aggregate. Version is +1 per emitted event, so it equals the
// aggregate_version of the latest outbox event.
type Payment struct {
	ID             uuid.UUID
	IdempotencyKey string
	MerchantID     string
	Amount         money.Money
	Status         Status
	Version        int
	// CaptureIdempotencyKey is the Idempotency-Key of the request that captured the
	// payment; empty until then.
	CaptureIdempotencyKey string
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// New creates a payment in status created and its payment.created event (aggregate
// version 1). Both get UUIDv7 ids: time-ordered, so B-tree inserts stay at the right edge
// of the index instead of landing on random pages as with v4. The id leaks the creation
// time, which is acceptable here.
//
// now is normalized to UTC microseconds, the precision of TIMESTAMPTZ, so the payment
// returned now and the one read back from the database later compare equal.
func New(idempotencyKey, merchantID string, amount money.Money, now time.Time) (Payment, outbox.Message, error) {
	var errs []error
	if idempotencyKey == "" {
		errs = append(errs, errors.New("idempotency key is empty"))
	}
	if merchantID == "" {
		errs = append(errs, errors.New("merchant id is empty"))
	}
	if amount.Currency.IsZero() {
		errs = append(errs, errors.New("currency is not set"))
	}
	if amount.Amount <= 0 {
		errs = append(errs, fmt.Errorf("amount %d is not positive", amount.Amount))
	}
	if len(errs) > 0 {
		return Payment{}, outbox.Message{}, fmt.Errorf("%w: %w", ErrInvalid, errors.Join(errs...))
	}

	id, err := uuid.NewV7()
	if err != nil {
		return Payment{}, outbox.Message{}, fmt.Errorf("payment: new id: %w", err)
	}
	eventID, err := uuid.NewV7()
	if err != nil {
		return Payment{}, outbox.Message{}, fmt.Errorf("payment: new event id: %w", err)
	}
	now = now.UTC().Truncate(time.Microsecond)

	p := Payment{
		ID:             id,
		IdempotencyKey: idempotencyKey,
		MerchantID:     merchantID,
		Amount:         amount,
		Status:         StatusCreated,
		Version:        1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	data, err := json.Marshal(event.PaymentCreatedV1{
		PaymentID:  p.ID,
		MerchantID: p.MerchantID,
		Amount:     wireAmount(p.Amount),
	})
	if err != nil {
		return Payment{}, outbox.Message{}, fmt.Errorf("payment: encode event: %w", err)
	}
	msg := outbox.Message{
		EventID:          eventID,
		AggregateType:    event.AggregatePayment,
		AggregateID:      p.ID.String(),
		AggregateVersion: p.Version,
		EventType:        event.TypePaymentCreated,
		EventVersion:     1,
		OccurredAt:       now,
		Payload:          data,
	}
	return p, msg, nil
}

// Capture moves the payment from created to captured and returns the new state with its
// payment.captured event (aggregate version +1). Callers must hold a lock on the payment
// (see outbox.Message); the repository's Update does.
//
// Capture is idempotent per key: if the payment was already captured with idempotencyKey,
// it returns p unchanged and a nil event, so a retried request writes nothing. Capturing
// with another key, or from any status but created, is a *TransitionError.
func (p Payment) Capture(idempotencyKey string, now time.Time) (Payment, *outbox.Message, error) {
	if idempotencyKey == "" {
		return Payment{}, nil, fmt.Errorf("%w: idempotency key is empty", ErrInvalid)
	}
	switch p.Status {
	case StatusCreated:
	case StatusCaptured:
		if p.CaptureIdempotencyKey == idempotencyKey {
			return p, nil, nil
		}
		return Payment{}, nil, &TransitionError{Action: "capture", From: p.Status, AlreadyDone: true}
	default:
		return Payment{}, nil, &TransitionError{Action: "capture", From: p.Status}
	}

	eventID, err := uuid.NewV7()
	if err != nil {
		return Payment{}, nil, fmt.Errorf("payment: new event id: %w", err)
	}
	now = now.UTC().Truncate(time.Microsecond)
	next := p
	next.Status = StatusCaptured
	next.Version++
	next.CaptureIdempotencyKey = idempotencyKey
	next.UpdatedAt = now

	data, err := json.Marshal(event.PaymentCapturedV1{
		PaymentID:  p.ID,
		MerchantID: p.MerchantID,
		Amount:     wireAmount(p.Amount),
	})
	if err != nil {
		return Payment{}, nil, fmt.Errorf("payment: encode event: %w", err)
	}
	return next, &outbox.Message{
		EventID:          eventID,
		AggregateType:    event.AggregatePayment,
		AggregateID:      p.ID.String(),
		AggregateVersion: next.Version,
		EventType:        event.TypePaymentCaptured,
		EventVersion:     1,
		OccurredAt:       now,
		Payload:          data,
	}, nil
}

// UpdateFunc decides a state change of a locked payment p (see the repository's Update).
// It returns the new state and the event that records it, or (p, nil, nil) to change
// nothing, e.g. for an idempotent replay. A non-nil error aborts the update.
type UpdateFunc func(p Payment) (Payment, *outbox.Message, error)

// SameRequest reports whether a create request with these fields is the one that created
// p. A retry with the same idempotency key must match it; otherwise the key was reused for
// a different payment.
func (p Payment) SameRequest(merchantID string, amount money.Money) bool {
	return p.MerchantID == merchantID && p.Amount == amount
}

func wireAmount(m money.Money) event.Amount {
	return event.Amount{AmountMinor: m.Amount, Currency: m.Currency.Code()}
}
