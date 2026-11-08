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
)

// Payment is the aggregate. Version is +1 per emitted event, so it equals the
// aggregate_version of the latest outbox event.
type Payment struct {
	ID             uuid.UUID
	IdempotencyKey string
	MerchantID     string
	Amount         money.Money
	Status         Status
	Version        int
	CreatedAt      time.Time
	UpdatedAt      time.Time
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

// SameRequest reports whether a create request with these fields is the one that created
// p. A retry with the same idempotency key must match it; otherwise the key was reused for
// a different payment.
func (p Payment) SameRequest(merchantID string, amount money.Money) bool {
	return p.MerchantID == merchantID && p.Amount == amount
}

func wireAmount(m money.Money) event.Amount {
	return event.Amount{AmountMinor: m.Amount, Currency: m.Currency.Code()}
}
