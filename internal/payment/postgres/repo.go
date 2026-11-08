// Package postgres is the payment repository: it writes the payment row and its outbox
// event in one pgx.Batch inside one transaction.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aavorov89/t-outbox/internal/money"
	"github.com/aavorov89/t-outbox/internal/outbox"
	"github.com/aavorov89/t-outbox/internal/payment"
)

// idempotencyKeyConstraint is Postgres' default name for UNIQUE (idempotency_key).
const idempotencyKeyConstraint = "payments_idempotency_key_key"

const insertPaymentSQL = `
INSERT INTO payments (id, idempotency_key, merchant_id, amount_minor, currency,
                      status, version, capture_idempotency_key, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), $9, $10)`

const selectPaymentSQL = `
SELECT id, idempotency_key, merchant_id, amount_minor, currency, status, version,
       COALESCE(capture_idempotency_key, ''), created_at, updated_at
FROM payments`

// Only the mutable fields; id, key, merchant and amount never change.
const updatePaymentSQL = `
UPDATE payments
SET status = $2, version = $3, capture_idempotency_key = NULLIF($4, ''), updated_at = $5
WHERE id = $1`

// Repo stores payments in Postgres.
type Repo struct {
	pool *pgxpool.Pool

	// beforeCommit is a test seam (see export_test.go): it runs after the batch, before
	// COMMIT. A non-nil error rolls back.
	beforeCommit func(ctx context.Context, tx pgx.Tx) error
}

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// Create inserts p and its event msg in one transaction and one round trip. If a payment
// with p.IdempotencyKey exists, it returns payment.ErrDuplicateIdempotencyKey and writes
// nothing.
//
// A concurrent Create with the same key blocks on the unique index until the first
// transaction ends, then gets the duplicate error (or succeeds if the first rolled back).
// So a caller that then reads the key always sees a committed payment.
func (r *Repo) Create(ctx context.Context, p payment.Payment, msg outbox.Message) error {
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		b := &pgx.Batch{}
		b.Queue(insertPaymentSQL,
			p.ID, p.IdempotencyKey, p.MerchantID, p.Amount.Amount, p.Amount.Currency.Code(),
			string(p.Status), p.Version, p.CaptureIdempotencyKey, p.CreatedAt, p.UpdatedAt)
		if err := outbox.Queue(ctx, b, msg); err != nil {
			return err
		}
		return r.finish(ctx, tx, b)
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == idempotencyKeyConstraint {
		return payment.ErrDuplicateIdempotencyKey
	}
	if err != nil {
		return fmt.Errorf("payment/postgres: create: %w", err)
	}
	return nil
}

// Update loads the payment with id under SELECT ... FOR UPDATE, calls fn, and writes the
// new state and its event in the same transaction (one more round trip). It returns the
// payment as stored afterwards. Errors wrap payment.ErrNotFound or fn's error.
//
// The row lock is what the outbox writer invariant needs: two concurrent updates of one
// payment are serialized, so their events get outbox ids in commit order. fn sees the
// latest committed state and may be called with a payment another request just changed.
func (r *Repo) Update(ctx context.Context, id uuid.UUID, fn payment.UpdateFunc) (payment.Payment, error) {
	var out payment.Payment
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		cur, err := scanPayment(tx.QueryRow(ctx, selectPaymentSQL+` WHERE id = $1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		next, msg, err := fn(cur)
		if err != nil {
			return err
		}
		if msg == nil {
			out = cur
			return nil // nothing to write; COMMIT just releases the lock
		}
		if err := checkUpdate(cur, next, *msg); err != nil {
			return err
		}
		b := &pgx.Batch{}
		b.Queue(updatePaymentSQL, next.ID, string(next.Status), next.Version, next.CaptureIdempotencyKey, next.UpdatedAt)
		if err := outbox.Queue(ctx, b, *msg); err != nil {
			return err
		}
		if err := r.finish(ctx, tx, b); err != nil {
			return err
		}
		out = next
		return nil
	})
	if err != nil {
		return payment.Payment{}, fmt.Errorf("payment/postgres: update %s: %w", id, err)
	}
	return out, nil
}

// checkUpdate rejects a change fn was not allowed to make: the aggregate version must go
// up by exactly one, and the event must belong to this payment and carry that version.
func checkUpdate(cur, next payment.Payment, msg outbox.Message) error {
	switch {
	case next.ID != cur.ID || next.IdempotencyKey != cur.IdempotencyKey ||
		next.MerchantID != cur.MerchantID || next.Amount != cur.Amount:
		return errors.New("payment/postgres: update changed an immutable field")
	case next.Version != cur.Version+1:
		return fmt.Errorf("payment/postgres: update: version %d → %d, want +1", cur.Version, next.Version)
	case msg.AggregateID != cur.ID.String() || msg.AggregateVersion != next.Version:
		return fmt.Errorf("payment/postgres: update: event %s v%d does not match payment %s v%d",
			msg.AggregateID, msg.AggregateVersion, cur.ID, next.Version)
	}
	return nil
}

// finish sends the batch and runs the test hook before COMMIT.
func (r *Repo) finish(ctx context.Context, tx pgx.Tx, b *pgx.Batch) error {
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		return err
	}
	if r.beforeCommit != nil {
		return r.beforeCommit(ctx, tx)
	}
	return nil
}

// Get returns the payment with id, or payment.ErrNotFound.
func (r *Repo) Get(ctx context.Context, id uuid.UUID) (payment.Payment, error) {
	return r.getOne(ctx, selectPaymentSQL+` WHERE id = $1`, id)
}

// GetByIdempotencyKey returns the payment created with key, or payment.ErrNotFound.
func (r *Repo) GetByIdempotencyKey(ctx context.Context, key string) (payment.Payment, error) {
	return r.getOne(ctx, selectPaymentSQL+` WHERE idempotency_key = $1`, key)
}

func (r *Repo) getOne(ctx context.Context, sql string, arg any) (payment.Payment, error) {
	p, err := scanPayment(r.pool.QueryRow(ctx, sql, arg))
	if err != nil && !errors.Is(err, payment.ErrNotFound) {
		return payment.Payment{}, fmt.Errorf("payment/postgres: get: %w", err)
	}
	return p, err
}

// scanPayment scans one row of selectPaymentSQL. No row is payment.ErrNotFound.
func scanPayment(row pgx.Row) (payment.Payment, error) {
	var (
		p        payment.Payment
		amount   int64
		currency string
		status   string
	)
	err := row.Scan(
		&p.ID, &p.IdempotencyKey, &p.MerchantID, &amount, &currency, &status, &p.Version,
		&p.CaptureIdempotencyKey, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return payment.Payment{}, payment.ErrNotFound
	}
	if err != nil {
		return payment.Payment{}, err
	}
	if p.Amount, err = money.Parse(amount, currency); err != nil {
		return payment.Payment{}, fmt.Errorf("payment/postgres: payment %s: %w", p.ID, err)
	}
	p.Status = payment.Status(status)
	p.CreatedAt = p.CreatedAt.UTC()
	p.UpdatedAt = p.UpdatedAt.UTC()
	return p, nil
}
