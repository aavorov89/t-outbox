package payment

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aavorov89/t-outbox/internal/event"
	"github.com/aavorov89/t-outbox/internal/money"
)

func eur(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.Parse(minor, "EUR")
	require.NoError(t, err)
	return m
}

func TestNew(t *testing.T) {
	now := time.Date(2026, 11, 8, 13, 0, 0, 123456789, time.FixedZone("CET", 3600))
	p, msg, err := New("key-1", "m_42", eur(t, 1999), now)
	require.NoError(t, err)

	require.Equal(t, 7, int(p.ID.Version()), "payment id must be UUIDv7")
	require.Equal(t, 7, int(msg.EventID.Version()), "event id must be UUIDv7")
	require.Equal(t, StatusCreated, p.Status)
	require.Equal(t, 1, p.Version)
	wantTime := time.Date(2026, 11, 8, 12, 0, 0, 123456000, time.UTC)
	require.Equal(t, wantTime, p.CreatedAt, "UTC, truncated to µs")
	require.Equal(t, wantTime, msg.OccurredAt)

	require.Equal(t, event.TypePaymentCreated, msg.EventType)
	require.Equal(t, p.ID.String(), msg.AggregateID)
	require.Equal(t, p.Version, msg.AggregateVersion)
	require.JSONEq(t,
		`{"payment_id":"`+p.ID.String()+`","merchant_id":"m_42","amount":{"amount_minor":1999,"currency":"EUR"}}`,
		string(msg.Payload))
}

func TestNewRejectsInvalid(t *testing.T) {
	now := time.Now()
	tests := map[string]func() error{
		"empty key":       func() error { _, _, err := New("", "m", eur(t, 1), now); return err },
		"empty merchant":  func() error { _, _, err := New("k", "", eur(t, 1), now); return err },
		"zero amount":     func() error { _, _, err := New("k", "m", eur(t, 0), now); return err },
		"negative amount": func() error { _, _, err := New("k", "m", eur(t, -5), now); return err },
		"no currency":     func() error { _, _, err := New("k", "m", money.Money{Amount: 1}, now); return err },
	}
	for name, f := range tests {
		t.Run(name, func(t *testing.T) {
			require.True(t, errors.Is(f(), ErrInvalid))
		})
	}
}

func TestSameRequest(t *testing.T) {
	p, _, err := New("k", "m_42", eur(t, 1999), time.Now())
	require.NoError(t, err)
	usd, err := money.Parse(1999, "USD")
	require.NoError(t, err)

	require.True(t, p.SameRequest("m_42", eur(t, 1999)))
	require.False(t, p.SameRequest("m_43", eur(t, 1999)))
	require.False(t, p.SameRequest("m_42", eur(t, 2000)))
	require.False(t, p.SameRequest("m_42", usd))
}
