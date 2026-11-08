// Package ledger is double-entry bookkeeping for captured payments: the posting rules, the
// balance invariant, and the repository that writes ledger_entries.
//
// Amounts are signed minor units: + is a debit, - is a credit. Every transaction must
// balance: for each currency, the entries sum to zero. Validate enforces it before any
// write, and CheckBalanced verifies it over the whole table.
package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/aavorov89/t-outbox/internal/money"
)

// ErrUnbalanced means a transaction's entries do not sum to zero in some currency.
var ErrUnbalanced = errors.New("ledger: unbalanced transaction")

// ErrInvalid means a transaction is malformed (no ID, too few entries, a zero amount...).
var ErrInvalid = errors.New("ledger: invalid transaction")

// Accounts.
const (
	// AccountPSPReceivable is what the payment service provider owes us for captured
	// payments (an asset: debited on capture).
	AccountPSPReceivable = "psp:receivable"
)

// MerchantPayable is what we owe merchant for its captured payments (a liability:
// credited on capture).
func MerchantPayable(merchantID string) string { return "merchant:" + merchantID + ":payable" }

// Entry is one leg of a transaction. Amount is signed: + debit, - credit.
type Entry struct {
	Account string
	Amount  money.Money
}

// Transaction is a balanced group of entries caused by one event.
type Transaction struct {
	ID      uuid.UUID
	EventID uuid.UUID
	Entries []Entry
}

// Validate checks the double-entry rules: an ID and an event, at least two entries, no
// empty account or zero amount, and per currency the amounts sum to exactly zero. The sum
// is overflow-checked, so entries that would wrap around int64 cannot fake a balance.
func (t Transaction) Validate() error {
	switch {
	case t.ID == uuid.Nil:
		return fmt.Errorf("%w: empty transaction id", ErrInvalid)
	case t.EventID == uuid.Nil:
		return fmt.Errorf("%w: empty event id", ErrInvalid)
	case len(t.Entries) < 2:
		return fmt.Errorf("%w: %d entries, want at least 2", ErrInvalid, len(t.Entries))
	}
	sums := map[money.Currency]money.Money{}
	var order []money.Currency
	for i, e := range t.Entries {
		switch {
		case e.Account == "":
			return fmt.Errorf("%w: entry %d: empty account", ErrInvalid, i)
		case e.Amount.Currency.IsZero():
			return fmt.Errorf("%w: entry %d: no currency", ErrInvalid, i)
		case e.Amount.Amount == 0:
			return fmt.Errorf("%w: entry %d: zero amount", ErrInvalid, i)
		}
		sum, ok := sums[e.Amount.Currency]
		if !ok {
			sum = money.Money{Currency: e.Amount.Currency}
			order = append(order, e.Amount.Currency)
		}
		next, err := sum.Add(e.Amount)
		if err != nil {
			return fmt.Errorf("%w: entry %d: %w", ErrUnbalanced, i, err)
		}
		sums[e.Amount.Currency] = next
	}
	for _, c := range order {
		if s := sums[c]; s.Amount != 0 {
			return fmt.Errorf("%w: %s sums to %s", ErrUnbalanced, c, s)
		}
	}
	return nil
}

// transactionNS is the UUIDv5 namespace of ledger transaction ids.
var transactionNS = uuid.MustParse("6f1c2b0e-3a59-4d0f-9c55-1d0b8f6c2a71")

// TransactionID derives the ledger transaction id from the event that caused it. An event
// maps to one transaction id by construction, so a double application (should dedup ever
// fail) shows up as the same transaction_id posted twice.
func TransactionID(eventID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(transactionNS, eventID[:])
}

const insertEntriesSQL = `
INSERT INTO ledger_entries (transaction_id, account, amount_minor, currency, event_id)
SELECT $1, e.account, e.amount_minor, e.currency, $2
FROM unnest($3::text[], $4::bigint[], $5::text[]) AS e(account, amount_minor, currency)`

// Post validates t and inserts its entries in tx with one statement. The caller owns tx;
// in the consumer it also holds the processed_events marker, so the entries and the
// marker commit together.
func Post(ctx context.Context, tx pgx.Tx, t Transaction) error {
	if err := t.Validate(); err != nil {
		return err
	}
	accounts := make([]string, len(t.Entries))
	amounts := make([]int64, len(t.Entries))
	currencies := make([]string, len(t.Entries))
	for i, e := range t.Entries {
		accounts[i], amounts[i], currencies[i] = e.Account, e.Amount.Amount, e.Amount.Currency.Code()
	}
	if _, err := tx.Exec(ctx, insertEntriesSQL, t.ID, t.EventID, accounts, amounts, currencies); err != nil {
		return fmt.Errorf("ledger: post %s: %w", t.ID, err)
	}
	return nil
}

// Querier is satisfied by *pgxpool.Pool, *pgx.Conn and pgx.Tx.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// CheckBalanced verifies the invariant over the whole table: for every transaction and
// currency, SUM(amount_minor) = 0. It returns an ErrUnbalanced error naming the offenders.
// The sum is NUMERIC, so it cannot overflow.
func CheckBalanced(ctx context.Context, q Querier) error {
	rows, err := q.Query(ctx, `
		SELECT transaction_id, currency, sum(amount_minor::numeric)::text
		FROM ledger_entries
		GROUP BY transaction_id, currency
		HAVING sum(amount_minor::numeric) <> 0
		ORDER BY transaction_id, currency
		LIMIT 10`)
	if err != nil {
		return fmt.Errorf("ledger: check balanced: %w", err)
	}
	var bad []error
	for rows.Next() {
		var (
			id       uuid.UUID
			currency string
			sum      string
		)
		if err := rows.Scan(&id, &currency, &sum); err != nil {
			rows.Close()
			return fmt.Errorf("ledger: check balanced: %w", err)
		}
		bad = append(bad, fmt.Errorf("transaction %s: %s sums to %s minor units", id, currency, sum))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ledger: check balanced: %w", err)
	}
	if len(bad) > 0 {
		return fmt.Errorf("%w: %w", ErrUnbalanced, errors.Join(bad...))
	}
	return nil
}
