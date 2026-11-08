package httpapi

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (a *api) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		// The mux sets r.Pattern ("POST /payments/{id}"); otelhttp names the span after it
		// but reads http.route only before routing, so it is added here.
		route := r.Pattern
		if _, path, ok := strings.Cut(route, " "); ok {
			route = path
		}
		if route != "" {
			trace.SpanFromContext(r.Context()).SetAttributes(semconv.HTTPRoute(route))
		}
		a.log.InfoContext(r.Context(), "http request",
			"method", r.Method, "path", r.URL.Path, "route", route, "status", rec.status,
			"duration", time.Since(start))
	})
}

// recoverer turns a panic into a 500 problem response. http.ErrAbortHandler is re-panicked,
// as net/http expects.
func (a *api) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if v == http.ErrAbortHandler { //nolint:errorlint // sentinel compared as net/http does
				panic(v)
			}
			a.internalError(w, r, fmt.Errorf("panic: %v", v))
		}()
		next.ServeHTTP(w, r)
	})
}
