// Command payments-api serves the payments HTTP API. It writes each payment and its
// outbox event in one transaction.
//
//	DATABASE_URL=postgres://... HTTP_ADDR=:8080 go run ./cmd/payments-api
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aavorov89/t-outbox/internal/httpapi"
	"github.com/aavorov89/t-outbox/internal/payment/postgres"
	"github.com/aavorov89/t-outbox/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		slog.Error("payments-api failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(telemetry.LogHandler(slog.NewJSONHandler(os.Stdout, nil))).With("service", "payments-api")
	shutdownTracing, err := telemetry.Setup(ctx, "payments-api", log)
	if err != nil {
		return err
	}
	defer func() { // flush the last spans; a slow Jaeger must not hold up the exit for long
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(ctx); err != nil {
			log.Warn("telemetry shutdown", "err", err)
		}
	}()

	pool, err := pgxpool.New(ctx, getenv("DATABASE_URL", "postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable"))
	if err != nil {
		return err
	}
	defer pool.Close()

	srv := &http.Server{
		Addr:              getenv("HTTP_ADDR", ":8080"),
		Handler:           httpapi.New(postgres.New(pool), httpapi.Options{Logger: log}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("listening", "addr", srv.Addr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Info("stopped")
	return nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
