//go:build integration

package testenv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/aavorov89/t-outbox/migrations"
)

const templateDB = "outbox_template"

// Postgres is a running container with a migrated template database.
type Postgres struct {
	ctr      *tcpostgres.PostgresContainer
	adminDSN string // connects to the "postgres" maintenance DB
	admin    *pgxpool.Pool
}

// StartPostgres starts Postgres tuned for tests (no fsync, data on tmpfs), creates the
// template database, and runs the migrations on it.
func StartPostgres(ctx context.Context) (*Postgres, error) {
	ctr, err := tcpostgres.Run(ctx, image("POSTGRES_IMAGE", "postgres:18.6-alpine"),
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.WithCmd("postgres",
			"-c", "fsync=off",
			"-c", "synchronous_commit=off",
			"-c", "full_page_writes=off",
			"-c", "max_connections=500",
		),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.Tmpfs = map[string]string{"/var/lib/postgresql": "rw"}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("testenv: start postgres: %w", err)
	}
	p := &Postgres{ctr: ctr}
	if err := p.init(ctx); err != nil {
		_ = p.Terminate(context.Background())
		return nil, err
	}
	return p, nil
}

func (p *Postgres) init(ctx context.Context) error {
	dsn, err := p.ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("testenv: postgres dsn: %w", err)
	}
	p.adminDSN = dsn
	if p.admin, err = pgxpool.New(ctx, dsn); err != nil {
		return fmt.Errorf("testenv: admin pool: %w", err)
	}
	if _, err := p.admin.Exec(ctx, "CREATE DATABASE "+templateDB); err != nil {
		return fmt.Errorf("testenv: create template: %w", err)
	}
	if err := migrations.Up(ctx, p.dsnFor(templateDB)); err != nil {
		return fmt.Errorf("testenv: migrate template: %w", err)
	}
	// No connections allowed: CREATE DATABASE ... TEMPLATE fails if anyone is connected.
	if _, err := p.admin.Exec(ctx, "ALTER DATABASE "+templateDB+" WITH IS_TEMPLATE true ALLOW_CONNECTIONS false"); err != nil {
		return fmt.Errorf("testenv: freeze template: %w", err)
	}
	return nil
}

// Terminate stops the container.
func (p *Postgres) Terminate(ctx context.Context) error {
	if p.admin != nil {
		p.admin.Close()
	}
	return testcontainers.TerminateContainer(p.ctr)
}

// NewDatabase creates a fresh, migrated database for t and returns its DSN.
// The database is dropped when t finishes.
func (p *Postgres) NewDatabase(t testing.TB) string {
	t.Helper()
	name := "t_" + randHex(8)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := p.admin.Exec(ctx, "CREATE DATABASE "+name+" TEMPLATE "+templateDB); err != nil {
		t.Fatalf("testenv: create database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := p.admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("testenv: drop database %s: %v", name, err)
		}
	})
	return p.dsnFor(name)
}

// NewPool creates a fresh database for t and returns a pool to it. configure, if non-nil,
// can adjust the pool config (e.g. RuntimeParams for idle_in_transaction_session_timeout).
func (p *Postgres) NewPool(t testing.TB, configure func(*pgxpool.Config)) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(p.NewDatabase(t))
	if err != nil {
		t.Fatalf("testenv: parse dsn: %v", err)
	}
	if configure != nil {
		configure(cfg)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("testenv: new pool: %v", err)
	}
	t.Cleanup(pool.Close) // runs before the DROP registered above (LIFO)
	return pool
}

// Connect opens a single connection to dsn, closed when t finishes. Useful as the
// "side" connection that kills another backend in crash tests.
func Connect(t testing.TB, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("testenv: connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func (p *Postgres) dsnFor(db string) string {
	u, err := url.Parse(p.adminDSN)
	if err != nil {
		panic(err) // adminDSN comes from testcontainers; always a URL
	}
	u.Path = "/" + db
	return u.String()
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
