package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// SetBeforeCommit installs a hook that runs after the batch, before COMMIT (F1).
func SetBeforeCommit(r *Repo, f func(ctx context.Context, tx pgx.Tx) error) { r.beforeCommit = f }
