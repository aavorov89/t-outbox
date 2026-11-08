package ledger

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/aavorov89/t-outbox/internal/consumer"
	"github.com/aavorov89/t-outbox/internal/event"
	"github.com/aavorov89/t-outbox/internal/money"
)

// ConsumerName is the ledger's key in processed_events.
const ConsumerName = "ledger"

// Handler posts ledger transactions for payment events. It implements consumer.Handler.
//
//	payment.captured v1   debit psp:receivable, credit merchant:{id}:payable
//	payment.created       nothing to post: no money moved yet
//	other event types     ignored, so new event types don't stop the ledger
//
// Data that can never be posted (an unknown event_version of payment.captured, a bad
// currency, a non-positive amount) is a permanent error: the record goes to the DLQ.
type Handler struct{}

var _ consumer.Handler = Handler{}

func (Handler) Handle(ctx context.Context, tx pgx.Tx, env event.Envelope) error {
	if env.EventType != event.TypePaymentCaptured {
		return nil
	}
	t, err := CaptureTransaction(env)
	if err != nil {
		return err
	}
	return Post(ctx, tx, t)
}

// CaptureTransaction builds the transaction of a payment.captured event. Every error is
// permanent: the event itself is wrong, a retry would fail the same way.
func CaptureTransaction(env event.Envelope) (Transaction, error) {
	if env.EventVersion != 1 {
		return Transaction{}, consumer.Permanent(fmt.Errorf("ledger: %s: unknown event_version %d", env.EventType, env.EventVersion))
	}
	data, err := event.DecodeData[event.PaymentCapturedV1](env)
	if err != nil {
		return Transaction{}, err // wraps event.ErrMalformed: permanent
	}
	amount, err := money.Parse(data.Amount.AmountMinor, data.Amount.Currency)
	switch {
	case err != nil:
		return Transaction{}, consumer.Permanent(fmt.Errorf("ledger: payment %s: %w", data.PaymentID, err))
	case amount.Amount <= 0:
		return Transaction{}, consumer.Permanent(fmt.Errorf("ledger: payment %s: amount %s, want > 0", data.PaymentID, amount))
	case data.MerchantID == "":
		return Transaction{}, consumer.Permanent(fmt.Errorf("ledger: payment %s: empty merchant_id", data.PaymentID))
	}
	credit, err := amount.Neg() // cannot fail for a positive amount
	if err != nil {
		return Transaction{}, consumer.Permanent(err)
	}
	t := Transaction{
		ID:      TransactionID(env.EventID),
		EventID: env.EventID,
		Entries: []Entry{
			{Account: AccountPSPReceivable, Amount: amount},
			{Account: MerchantPayable(data.MerchantID), Amount: credit},
		},
	}
	if err := t.Validate(); err != nil {
		return Transaction{}, consumer.Permanent(err)
	}
	return t, nil
}
