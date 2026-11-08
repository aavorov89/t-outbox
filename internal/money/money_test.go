package money

import (
	"errors"
	"math"
	"testing"
)

func mustParse(t *testing.T, amount int64, code string) Money {
	t.Helper()
	m, err := Parse(amount, code)
	if err != nil {
		t.Fatalf("Parse(%d, %q): %v", amount, code, err)
	}
	return m
}

func TestParse(t *testing.T) {
	tests := []struct {
		code    string
		wantExp uint8
		wantErr error
	}{
		{"EUR", 2, nil},
		{"JPY", 0, nil},
		{"KWD", 3, nil},
		{"eur", 0, ErrUnknownCurrency},
		{"XXX", 0, ErrUnknownCurrency},
		{"", 0, ErrUnknownCurrency},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			m, err := Parse(100, tt.code)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && m.Currency.Exponent() != tt.wantExp {
				t.Fatalf("exponent = %d, want %d", m.Currency.Exponent(), tt.wantExp)
			}
		})
	}
}

func TestAdd(t *testing.T) {
	tests := []struct {
		name    string
		a, b    int64
		bCode   string
		want    int64
		wantErr error
	}{
		{"simple", 1999, 1, "EUR", 2000, nil},
		{"negative", 100, -250, "EUR", -150, nil},
		{"max boundary", math.MaxInt64 - 1, 1, "EUR", math.MaxInt64, nil},
		{"min boundary", math.MinInt64 + 1, -1, "EUR", math.MinInt64, nil},
		{"overflow", math.MaxInt64, 1, "EUR", 0, ErrOverflow},
		{"underflow", math.MinInt64, -1, "EUR", 0, ErrOverflow},
		{"currency mismatch", 1, 1, "USD", 0, ErrCurrencyMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mustParse(t, tt.a, "EUR").Add(mustParse(t, tt.b, tt.bCode))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil && got.Amount != tt.want {
				t.Fatalf("amount = %d, want %d", got.Amount, tt.want)
			}
		})
	}
}

func TestNeg(t *testing.T) {
	tests := []struct {
		in      int64
		want    int64
		wantErr error
	}{
		{1999, -1999, nil},
		{0, 0, nil},
		{math.MaxInt64, -math.MaxInt64, nil},
		{math.MinInt64, 0, ErrOverflow},
	}
	for _, tt := range tests {
		got, err := mustParse(t, tt.in, "EUR").Neg()
		if !errors.Is(err, tt.wantErr) {
			t.Fatalf("Neg(%d) err = %v, want %v", tt.in, err, tt.wantErr)
		}
		if err == nil && got.Amount != tt.want {
			t.Fatalf("Neg(%d) = %d, want %d", tt.in, got.Amount, tt.want)
		}
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		amount int64
		code   string
		want   string
	}{
		{1999, "EUR", "19.99 EUR"},
		{5, "EUR", "0.05 EUR"},
		{-5, "EUR", "-0.05 EUR"},
		{0, "EUR", "0.00 EUR"},
		{1500, "KWD", "1.500 KWD"},
		{-42, "JPY", "-42 JPY"},
		{math.MinInt64, "EUR", "-92233720368547758.08 EUR"},
	}
	for _, tt := range tests {
		if got := mustParse(t, tt.amount, tt.code).String(); got != tt.want {
			t.Errorf("String(%d %s) = %q, want %q", tt.amount, tt.code, got, tt.want)
		}
	}
}
