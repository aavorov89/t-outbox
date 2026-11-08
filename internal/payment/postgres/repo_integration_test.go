//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
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

// killBeforeCommit makes the repo's next transactions die before COMMIT: the hook kills
// the transaction's own backend from a side connection, as a crash of the API process
// would. The server aborts the tx, nothing is committed.
func killBeforeCommit(t *testing.T, repo *postgres.Repo, side *pgx.Conn) {
	t.Helper()
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
}

func capture(key string) payment.UpdateFunc {
	return func(p payment.Payment) (payment.Payment, *outbox.Message, error) {
		return p.Capture(key, time.Now())
	}
}

// F1: the API dies before COMMIT → nothing of that request is persisted: neither the
// payment and its event on create, nor the status change and its event on capture.
func TestWrite_Atomicity(t *testing.T) {
	t.Parallel()
	dsn := env.PG.NewDatabase(t)
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	side := testenv.Connect(t, dsn)
	ctx := context.Background()

	t.Run("create", func(t *testing.T) {
		repo := postgres.New(pool)
		killBeforeCommit(t, repo, side)
		p, msg := newPayment(t, "key-f1")
		require.Error(t, repo.Create(ctx, p, msg))

		nPay, nOut := counts(t, pool)
		require.Zero(t, nPay)
		require.Zero(t, nOut)
	})

	t.Run("capture", func(t *testing.T) {
		repo := postgres.New(pool)
		p, msg := newPayment(t, "key-f1-capture")
		require.NoError(t, repo.Create(ctx, p, msg))
		killBeforeCommit(t, repo, side)

		_, err := repo.Update(ctx, p.ID, capture("cap"))
		require.Error(t, err)

		got, err := postgres.New(pool).Get(ctx, p.ID)
		require.NoError(t, err)
		require.Equal(t, p, got, "status change rolled back")
		nPay, nOut := counts(t, pool)
		require.Equal(t, 1, nPay)
		require.Equal(t, 1, nOut, "no payment.captured row")
	})
}

// F2: the outbox insert fails (here: an event_id that already exists) → the payment
// write rolls back too, on create and on capture.
func TestWrite_OutboxFailureRollsBackPayment(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	repo := postgres.New(pool)
	ctx := context.Background()

	p1, m1 := newPayment(t, "key-a")
	require.NoError(t, repo.Create(ctx, p1, m1))

	t.Run("create", func(t *testing.T) {
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
	})

	t.Run("capture", func(t *testing.T) {
		_, err := repo.Update(ctx, p1.ID, func(p payment.Payment) (payment.Payment, *outbox.Message, error) {
			next, msg, err := p.Capture("cap", time.Now())
			if err == nil {
				msg.EventID = m1.EventID
			}
			return next, msg, err
		})
		require.Error(t, err)

		got, err := repo.Get(ctx, p1.ID)
		require.NoError(t, err)
		require.Equal(t, p1, got, "status change must roll back with its event")
		_, nOut := counts(t, pool)
		require.Equal(t, 1, nOut)
	})
}

func TestRepo_UpdateCapture(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	repo := postgres.New(pool)
	ctx := context.Background()
	p, msg := newPayment(t, "key-1")
	require.NoError(t, repo.Create(ctx, p, msg))

	captured, err := repo.Update(ctx, p.ID, capture("cap-1"))
	require.NoError(t, err)
	require.Equal(t, payment.StatusCaptured, captured.Status)
	require.Equal(t, 2, captured.Version)
	got, err := repo.Get(ctx, p.ID)
	require.NoError(t, err)
	require.Equal(t, captured, got, "read back exactly what was written")

	replayed, err := repo.Update(ctx, p.ID, capture("cap-1"))
	require.NoError(t, err)
	require.Equal(t, captured, replayed)

	_, err = repo.Update(ctx, p.ID, capture("cap-2"))
	require.ErrorIs(t, err, payment.ErrInvalidTransition)

	rows, err := pool.Query(ctx, `SELECT event_type || ':' || aggregate_version FROM outbox ORDER BY id`)
	require.NoError(t, err)
	events, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	require.Equal(t, []string{"payment.created:1", "payment.captured:2"}, events)

	_, err = repo.Update(ctx, uuid.New(), capture("cap-1"))
	require.ErrorIs(t, err, payment.ErrNotFound)
}

// Update refuses a change that breaks the aggregate's version contract, before writing.
func TestRepo_UpdateRejectsVersionSkip(t *testing.T) {
	t.Parallel()
	pool := env.PG.NewPool(t, nil)
	repo := postgres.New(pool)
	ctx := context.Background()
	p, msg := newPayment(t, "key-1")
	require.NoError(t, repo.Create(ctx, p, msg))

	_, err := repo.Update(ctx, p.ID, func(p payment.Payment) (payment.Payment, *outbox.Message, error) {
		next, msg, err := p.Capture("cap", time.Now())
		next.Version++
		msg.AggregateVersion++
		return next, msg, err
	})
	require.ErrorContains(t, err, "want +1")
	_, nOut := counts(t, pool)
	require.Equal(t, 1, nOut)
}
