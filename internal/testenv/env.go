//go:build integration

// Package testenv starts Postgres and Kafka (Redpanda) with testcontainers-go and hands out
// isolated resources: a fresh database per test (cloned from a migrated template) and a
// fresh topic per test. Tests in one package share the containers and can run in parallel.
//
// Typical TestMain:
//
//	var env *testenv.Env
//
//	func TestMain(m *testing.M) { testenv.Main(m, &env, testenv.WithPostgres(), testenv.WithKafka()) }
package testenv

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Env holds the started containers. Unrequested ones are nil.
type Env struct {
	PG    *Postgres
	Kafka *Kafka
}

type options struct{ postgres, kafka bool }

type Option func(*options)

func WithPostgres() Option { return func(o *options) { o.postgres = true } }
func WithKafka() Option    { return func(o *options) { o.kafka = true } }

// Start starts the requested containers. Call Close when done.
func Start(ctx context.Context, opts ...Option) (*Env, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	env := &Env{}
	if o.postgres {
		pg, err := StartPostgres(ctx)
		if err != nil {
			return nil, err
		}
		env.PG = pg
	}
	if o.kafka {
		k, err := StartKafka(ctx)
		if err != nil {
			_ = env.Close(ctx)
			return nil, err
		}
		env.Kafka = k
	}
	return env, nil
}

// Close terminates all containers.
func (e *Env) Close(ctx context.Context) error {
	var errs []error
	if e.Kafka != nil {
		errs = append(errs, e.Kafka.Terminate(ctx))
	}
	if e.PG != nil {
		errs = append(errs, e.PG.Terminate(ctx))
	}
	return errors.Join(errs...)
}

// Main is a TestMain helper: start containers, store them in *dst, run tests, clean up, exit.
func Main(m *testing.M, dst **Env, opts ...Option) {
	os.Exit(run(m, dst, opts...))
}

func run(m *testing.M, dst **Env, opts ...Option) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	env, err := Start(ctx, opts...)
	cancel()
	if err != nil {
		log.Printf("testenv: start: %v", err)
		return 1
	}
	*dst = env
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := env.Close(ctx); err != nil {
			log.Printf("testenv: close: %v", err)
		}
	}()
	return m.Run()
}

// image returns the image for key: env var, else deploy/.env, else fallback.
func image(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	if v, ok := dotenv()[key]; ok && v != "" {
		return v
	}
	return fallback
}

// dotenv parses deploy/.env at the repo root (found by walking up to go.mod).
func dotenv() map[string]string {
	vals := map[string]string{}
	root, err := repoRoot()
	if err != nil {
		return vals
	}
	f, err := os.Open(filepath.Join(root, "deploy", ".env"))
	if err != nil {
		return vals
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			vals[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
		}
	}
	return vals
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found")
		}
		dir = parent
	}
}
