//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/aavorov89/t-outbox/internal/money"
	"github.com/aavorov89/t-outbox/internal/outbox"
	"github.com/aavorov89/t-outbox/internal/payment"
	"github.com/aavorov89/t-outbox/internal/payment/postgres"
	"github.com/aavorov89/t-outbox/internal/testenv"
)

var env *testenv.Env

func TestMain(m *testing.M) {
	testenv.Main(m, &env, testenv.WithPostgres())
}

func newPayment(t *testing.T, key string) (payment.Payment, outbox.Message) {
	t.Helper()
	amount, err := money.Parse(1999, "EUR")
	require.NoError(t, err)
	p, msg, err := payment.New(key, "m_42", amount, time.Now())
	require.NoError(t, err)
	return p, msg
}

func counts(t *testing.T, pool *pgxpool.Pool) (payments, outboxRows int) {
	t.Helper()
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT (SELECT count(*) FROM payments), (SELECT count(*) FROM outbox)`).Scan(&payments, &outboxRows))
	return payments, outboxRows
}

func TestRepo_CreateAndGet(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	repo := postgres.New(pool)
	ctx := context.Background()
	p, msg := newPayment(t, "key-1")

	require.NoError(t, repo.Create(ctx, p, msg))

	got, err := repo.Get(ctx, p.ID)
	require.NoError(t, err)
	require.Equal(t, p, got, "read back exactly what was written")
	got, err = repo.GetByIdempotencyKey(ctx, "key-1")
	require.NoError(t, err)
	require.Equal(t, p, got)

	var eventID, aggregateID string
	var version int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT event_id::text, aggregate_id, aggregate_version FROM outbox`).Scan(&eventID, &aggregateID, &version))
	require.Equal(t, msg.EventID.String(), eventID)
	require.Equal(t, p.ID.String(), aggregateID)
	require.Equal(t, 1, version)

	_, err = repo.Get(ctx, msg.EventID) // some other uuid
	require.ErrorIs(t, err, payment.ErrNotFound)
}

func TestRepo_DuplicateIdempotencyKey(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	repo := postgres.New(pool)
	ctx := context.Background()

	p1, m1 := newPayment(t, "same-key")
	require.NoError(t, repo.Create(ctx, p1, m1))
	p2, m2 := newPayment(t, "same-key")
	require.ErrorIs(t, repo.Create(ctx, p2, m2), payment.ErrDuplicateIdempotencyKey)

	nPay, nOut := counts(t, pool)
	require.Equal(t, 1, nPay)
	require.Equal(t, 1, nOut, "the duplicate's event must not be written")
}

// F1: the API dies before COMMIT → neither the payment nor the outbox row exists.
// The hook kills the transaction's own backend from a side connection, as a crash of the
// API process would: the server aborts the tx, nothing is committed.
func TestWrite_Atomicity(t *testing.T) {
	t.Parallel()
	dsn := env.PG.NewDatabase(t)
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	side := testenv.Connect(t, dsn)

	repo := postgres.New(pool)
	postgres.SetBeforeCommit(repo, func(ctx context.Context, tx pgx.Tx) error {
		var pid int
		if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			return err
		}
		var ok bool
		// The timeout makes it wait until the backend is really gone.
		if err := side.QueryRow(ctx, `SELECT pg_terminate_backend($1, 5000)`, pid).Scan(&ok); err != nil {
			return err
		}
		if !ok {
			return errors.New("backend not terminated")
		}
		return nil // proceed to COMMIT on a dead connection
	})

	p, msg := newPayment(t, "key-f1")
	require.Error(t, repo.Create(context.Background(), p, msg))

	nPay, nOut := counts(t, pool)
	require.Zero(t, nPay)
	require.Zero(t, nOut)
}

// F2: the outbox insert fails (here: an event_id that already exists) → the payment
// rolls back too.
func TestWrite_OutboxFailureRollsBackPayment(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	repo := postgres.New(pool)
	ctx := context.Background()

	p1, m1 := newPayment(t, "key-a")
	require.NoError(t, repo.Create(ctx, p1, m1))

	p2, m2 := newPayment(t, "key-b")
	m2.EventID = m1.EventID // outbox UNIQUE (event_id) violation
	err := repo.Create(ctx, p2, m2)
	require.Error(t, err)
	require.NotErrorIs(t, err, payment.ErrDuplicateIdempotencyKey)

	_, err = repo.Get(ctx, p2.ID)
	require.ErrorIs(t, err, payment.ErrNotFound, "payment must roll back with its event")
	nPay, nOut := counts(t, pool)
	require.Equal(t, 1, nPay)
	require.Equal(t, 1, nOut)
}
