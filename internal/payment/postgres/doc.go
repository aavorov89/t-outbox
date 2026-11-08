// Package postgres is the payment repository: it writes the payment row and its outbox
// event in one pgx.Batch inside one transaction. Implemented in M2.
package postgres
