// Package money implements amounts as int64 minor units with an ISO 4217 currency.
// There is no float64 anywhere in this package, and all arithmetic is overflow-checked.
package money

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

var (
	ErrUnknownCurrency  = errors.New("money: unknown currency")
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrOverflow         = errors.New("money: int64 overflow")
)

// Currency is an ISO 4217 currency with its minor-unit exponent.
// The zero value is invalid; obtain one via CurrencyOf.
type Currency struct {
	code     string
	exponent uint8
}

func (c Currency) Code() string    { return c.code }
func (c Currency) Exponent() uint8 { return c.exponent }
func (c Currency) IsZero() bool    { return c.code == "" }
func (c Currency) String() string  { return c.code }

// currencies is deliberately small: only what this system accepts.
var currencies = map[string]uint8{
	"EUR": 2,
	"USD": 2,
	"GBP": 2,
	"CHF": 2,
	"SEK": 2,
	"JPY": 0,
	"KWD": 3,
}

// CurrencyOf returns the currency for an upper-case ISO 4217 alpha-3 code.
func CurrencyOf(code string) (Currency, error) {
	exp, ok := currencies[code]
	if !ok {
		return Currency{}, fmt.Errorf("%w: %q", ErrUnknownCurrency, code)
	}
	return Currency{code: code, exponent: exp}, nil
}

// Money is an amount in minor units: Amount=1999, EUR means €19.99.
type Money struct {
	Amount   int64
	Currency Currency
}

// Parse builds Money from minor units and a currency code.
func Parse(amountMinor int64, code string) (Money, error) {
	c, err := CurrencyOf(code)
	if err != nil {
		return Money{}, err
	}
	return Money{Amount: amountMinor, Currency: c}, nil
}

// Add returns m+o. Both must have the same currency.
func (m Money) Add(o Money) (Money, error) {
	if m.Currency != o.Currency {
		return Money{}, fmt.Errorf("%w: %s + %s", ErrCurrencyMismatch, m.Currency, o.Currency)
	}
	if (o.Amount > 0 && m.Amount > math.MaxInt64-o.Amount) ||
		(o.Amount < 0 && m.Amount < math.MinInt64-o.Amount) {
		return Money{}, ErrOverflow
	}
	return Money{Amount: m.Amount + o.Amount, Currency: m.Currency}, nil
}

// Neg returns -m. It fails for math.MinInt64, which has no positive counterpart.
func (m Money) Neg() (Money, error) {
	if m.Amount == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{Amount: -m.Amount, Currency: m.Currency}, nil
}

// String formats as "19.99 EUR", "-5 JPY", "1.500 KWD". For logs and tests, not for UIs.
func (m Money) String() string {
	neg := m.Amount < 0
	// Convert to uint64 so math.MinInt64 has a magnitude.
	mag := uint64(m.Amount)
	if neg {
		mag = -mag
	}
	digits := strconv.FormatUint(mag, 10)
	exp := int(m.Currency.exponent)
	if exp > 0 {
		if len(digits) <= exp {
			digits = strings.Repeat("0", exp-len(digits)+1) + digits
		}
		digits = digits[:len(digits)-exp] + "." + digits[len(digits)-exp:]
	}
	if neg {
		digits = "-" + digits
	}
	return digits + " " + m.Currency.code
}
