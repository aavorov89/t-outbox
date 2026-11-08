package event

import "github.com/google/uuid"

// Aggregate and event types for payments.
const (
	AggregatePayment    = "payment"
	TypePaymentCreated  = "payment.created"
	TypePaymentCaptured = "payment.captured"
)

// Amount is the wire form of money: minor units as a JSON integer plus an ISO 4217 code.
// It is intentionally separate from money.Money so the domain type can change without
// changing the contract.
type Amount struct {
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

// PaymentCreatedV1 is the data of payment.created, event_version 1.
type PaymentCreatedV1 struct {
	PaymentID  uuid.UUID `json:"payment_id"`
	MerchantID string    `json:"merchant_id"`
	Amount     Amount    `json:"amount"`
}

// PaymentCapturedV1 is the data of payment.captured, event_version 1.
type PaymentCapturedV1 struct {
	PaymentID  uuid.UUID `json:"payment_id"`
	MerchantID string    `json:"merchant_id"`
	Amount     Amount    `json:"amount"`
}
