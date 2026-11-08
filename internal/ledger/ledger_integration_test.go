//go:build integration

package ledger_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/aavorov89/t-outbox/internal/ledger"
	"github.com/aavorov89/t-outbox/internal/money"
	"github.com/aavorov89/t-outbox/internal/testenv"
)

var env *testenv.Env

func TestMain(m *testing.M) { testenv.Main(m, &env, testenv.WithPostgres()) }

func eur(t *testing.T, amount int64) money.Money {
	t.Helper()
	v, err := money.Parse(amount, "EUR")
	require.NoError(t, err)
	return v
}

func TestPost_WritesBalancedEntries(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	ctx := context.Background()
	ev := uuid.New()
	txn := ledger.Transaction{ID: ledger.TransactionID(ev), EventID: ev, Entries: []ledger.Entry{
		{Account: ledger.AccountPSPReceivable, Amount: eur(t, 1<<53+1)},
		{Account: ledger.MerchantPayable("m_1"), Amount: eur(t, -(1<<53 + 1))},
	}}
	require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return ledger.Post(ctx, tx, txn) }))

	rows, err := pool.Query(ctx, `SELECT transaction_id, event_id, account, amount_minor, currency FROM ledger_entries ORDER BY id`)
	require.NoError(t, err)
	type row struct {
		TxID, EventID uuid.UUID
		Account       string
		Amount        int64
		Currency      string
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[row])
	require.NoError(t, err)
	require.Equal(t, []row{
		{txn.ID, ev, "psp:receivable", 1<<53 + 1, "EUR"},
		{txn.ID, ev, "merchant:m_1:payable", -(1<<53 + 1), "EUR"},
	}, got, "amounts above 2^53 survive exactly")
	require.NoError(t, ledger.CheckBalanced(ctx, pool))
}

func TestPost_RejectsUnbalancedBeforeWriting(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	ctx := context.Background()
	ev := uuid.New()
	txn := ledger.Transaction{ID: uuid.New(), EventID: ev, Entries: []ledger.Entry{
		{Account: "a", Amount: eur(t, 100)}, {Account: "b", Amount: eur(t, -99)},
	}}
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return ledger.Post(ctx, tx, txn) })
	require.ErrorIs(t, err, ledger.ErrUnbalanced)
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries`).Scan(&n))
	require.Zero(t, n)
}

// CheckBalanced is the table-wide invariant check: it finds rows written around Post.
func TestCheckBalanced_FindsImbalance(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	ctx := context.Background()
	require.NoError(t, ledger.CheckBalanced(ctx, pool), "empty ledger is balanced")

	bad := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO ledger_entries (transaction_id, account, amount_minor, currency, event_id) VALUES
		($1, 'a',  9223372036854775807, 'EUR', $2),
		($1, 'a',  9223372036854775807, 'EUR', $2),
		($1, 'b', -2,                   'EUR', $2),
		($1, 'c',  5,                   'JPY', $2),
		($1, 'd', -5,                   'JPY', $2)`, bad, uuid.New())
	require.NoError(t, err)

	err = ledger.CheckBalanced(ctx, pool)
	require.ErrorIs(t, err, ledger.ErrUnbalanced)
	require.Contains(t, err.Error(), bad.String())
	require.Contains(t, err.Error(), "EUR sums to 18446744073709551612", "NUMERIC sum: no int64 wrap-around")
	require.NotContains(t, err.Error(), "JPY")
}
