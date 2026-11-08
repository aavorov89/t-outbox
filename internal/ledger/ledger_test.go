package ledger

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/aavorov89/t-outbox/internal/consumer"
	"github.com/aavorov89/t-outbox/internal/event"
	"github.com/aavorov89/t-outbox/internal/money"
)

func m(t *testing.T, amount int64, code string) money.Money {
	t.Helper()
	v, err := money.Parse(amount, code)
	require.NoError(t, err)
	return v
}

func TestTransactionValidate(t *testing.T) {
	id, ev := uuid.New(), uuid.New()
	tx := func(entries ...Entry) Transaction { return Transaction{ID: id, EventID: ev, Entries: entries} }
	a, b := "a", "b"

	tests := []struct {
		name string
		tx   Transaction
		want error // nil, ErrInvalid or ErrUnbalanced
	}{
		{"balanced pair", tx(Entry{a, m(t, 1999, "EUR")}, Entry{b, m(t, -1999, "EUR")}), nil},
		{"balanced split", tx(Entry{a, m(t, 1000, "EUR")}, Entry{b, m(t, -600, "EUR")}, Entry{b, m(t, -400, "EUR")}), nil},
		{"balanced per currency", tx(
			Entry{a, m(t, 5, "JPY")}, Entry{b, m(t, -5, "JPY")},
			Entry{a, m(t, 7, "EUR")}, Entry{b, m(t, -7, "EUR")}), nil},
		{"unbalanced", tx(Entry{a, m(t, 1999, "EUR")}, Entry{b, m(t, -1998, "EUR")}), ErrUnbalanced},
		{"sums to zero only across currencies", tx(Entry{a, m(t, 5, "EUR")}, Entry{b, m(t, -5, "USD")}), ErrUnbalanced},
		{"overflow cannot fake a balance", tx(
			Entry{a, m(t, math.MaxInt64, "EUR")}, Entry{a, m(t, math.MaxInt64, "EUR")}, Entry{b, m(t, 2, "EUR")}), ErrUnbalanced},
		{"one entry", tx(Entry{a, m(t, 1, "EUR")}), ErrInvalid},
		{"zero amount", tx(Entry{a, m(t, 0, "EUR")}, Entry{b, m(t, 0, "EUR")}), ErrInvalid},
		{"empty account", tx(Entry{"", m(t, 1, "EUR")}, Entry{b, m(t, -1, "EUR")}), ErrInvalid},
		{"no currency", tx(Entry{a, money.Money{Amount: 1}}, Entry{b, money.Money{Amount: -1}}), ErrInvalid},
		{"no id", Transaction{EventID: ev, Entries: tx(Entry{a, m(t, 1, "EUR")}, Entry{b, m(t, -1, "EUR")}).Entries}, ErrInvalid},
		{"no event", Transaction{ID: id, Entries: tx(Entry{a, m(t, 1, "EUR")}, Entry{b, m(t, -1, "EUR")}).Entries}, ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.tx.Validate()
			if tt.want == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestTransactionIDIsDerivedFromEvent(t *testing.T) {
	ev := uuid.New()
	require.Equal(t, TransactionID(ev), TransactionID(ev))
	require.NotEqual(t, TransactionID(ev), TransactionID(uuid.New()))
	require.NotEqual(t, ev, TransactionID(ev))
}

func captured(t *testing.T, version int, data any) event.Envelope {
	t.Helper()
	b, err := json.Marshal(data)
	require.NoError(t, err)
	return event.Envelope{
		EventID: uuid.New(), EventType: event.TypePaymentCaptured, EventVersion: version,
		AggregateType: event.AggregatePayment, AggregateID: uuid.NewString(), AggregateVersion: 2,
		OccurredAt: time.Now(), Data: b,
	}
}

func TestCaptureTransaction(t *testing.T) {
	pid := uuid.New()
	env := captured(t, 1, event.PaymentCapturedV1{PaymentID: pid, MerchantID: "m_42",
		Amount: event.Amount{AmountMinor: 1999, Currency: "EUR"}})

	got, err := CaptureTransaction(env)
	require.NoError(t, err)
	require.Equal(t, Transaction{
		ID:      TransactionID(env.EventID),
		EventID: env.EventID,
		Entries: []Entry{
			{Account: "psp:receivable", Amount: m(t, 1999, "EUR")},
			{Account: "merchant:m_42:payable", Amount: m(t, -1999, "EUR")},
		},
	}, got)
}

func TestCaptureTransactionRejectsBadEventsPermanently(t *testing.T) {
	pid := uuid.New()
	good := event.PaymentCapturedV1{PaymentID: pid, MerchantID: "m", Amount: event.Amount{AmountMinor: 1, Currency: "EUR"}}
	with := func(f func(d *event.PaymentCapturedV1)) event.PaymentCapturedV1 { d := good; f(&d); return d }

	tests := map[string]event.Envelope{
		"unknown event_version": captured(t, 2, good),
		"unknown currency":      captured(t, 1, with(func(d *event.PaymentCapturedV1) { d.Amount.Currency = "XXX" })),
		"zero amount":           captured(t, 1, with(func(d *event.PaymentCapturedV1) { d.Amount.AmountMinor = 0 })),
		"negative amount":       captured(t, 1, with(func(d *event.PaymentCapturedV1) { d.Amount.AmountMinor = -5 })),
		"min int64 amount":      captured(t, 1, with(func(d *event.PaymentCapturedV1) { d.Amount.AmountMinor = math.MinInt64 })),
		"empty merchant":        captured(t, 1, with(func(d *event.PaymentCapturedV1) { d.MerchantID = "" })),
		"data of wrong shape":   captured(t, 1, map[string]any{"amount": "19.99"}),
	}
	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := CaptureTransaction(env)
			require.Error(t, err)
			require.True(t, consumer.IsPermanent(err), "must be permanent: %v", err)
			// The Handler fails before touching the transaction.
			require.True(t, consumer.IsPermanent(Handler{}.Handle(context.Background(), nil, env)))
		})
	}
}

// Events without ledger effect are accepted without touching the transaction (nil here).
func TestHandlerIgnoresOtherEvents(t *testing.T) {
	for _, typ := range []string{event.TypePaymentCreated, "payment.refunded"} {
		env := captured(t, 7, map[string]any{})
		env.EventType = typ
		require.NoError(t, Handler{}.Handle(context.Background(), nil, env), typ)
	}
}
