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
                      status, version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

const selectPaymentSQL = `
SELECT id, idempotency_key, merchant_id, amount_minor, currency, status, version,
       created_at, updated_at
FROM payments`

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
			string(p.Status), p.Version, p.CreatedAt, p.UpdatedAt)
		if err := outbox.Queue(ctx, b, msg); err != nil {
			return err
		}
		if err := tx.SendBatch(ctx, b).Close(); err != nil {
			return err
		}
		if r.beforeCommit != nil {
			return r.beforeCommit(ctx, tx)
		}
		return nil
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

// Get returns the payment with id, or payment.ErrNotFound.
func (r *Repo) Get(ctx context.Context, id uuid.UUID) (payment.Payment, error) {
	return r.getOne(ctx, selectPaymentSQL+` WHERE id = $1`, id)
}

// GetByIdempotencyKey returns the payment created with key, or payment.ErrNotFound.
func (r *Repo) GetByIdempotencyKey(ctx context.Context, key string) (payment.Payment, error) {
	return r.getOne(ctx, selectPaymentSQL+` WHERE idempotency_key = $1`, key)
}

func (r *Repo) getOne(ctx context.Context, sql string, arg any) (payment.Payment, error) {
	var (
		p        payment.Payment
		amount   int64
		currency string
		status   string
	)
	err := r.pool.QueryRow(ctx, sql, arg).Scan(
		&p.ID, &p.IdempotencyKey, &p.MerchantID, &amount, &currency, &status, &p.Version,
		&p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return payment.Payment{}, payment.ErrNotFound
	}
	if err != nil {
		return payment.Payment{}, fmt.Errorf("payment/postgres: get: %w", err)
	}
	if p.Amount, err = money.Parse(amount, currency); err != nil {
		return payment.Payment{}, fmt.Errorf("payment/postgres: payment %s: %w", p.ID, err)
	}
	p.Status = payment.Status(status)
	p.CreatedAt = p.CreatedAt.UTC()
	p.UpdatedAt = p.UpdatedAt.UTC()
	return p, nil
}
