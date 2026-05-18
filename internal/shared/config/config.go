// Package config provides startup-time helpers shared by the binaries
// in cmd/. The functions here are intentionally fail-fast: a missing
// required env var calls os.Exit so misconfiguration surfaces loudly
// at process start, never as a confusing 500 mid-traffic.
//
// Tests that exercise the cmd binaries' wiring should inject their
// dependencies directly rather than calling these helpers, since
// os.Exit is hostile to test runners.
package config

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// IsLambda reports whether the current process is running inside an
// AWS Lambda runtime. The check is `AWS_LAMBDA_FUNCTION_NAME != ""`
// because every Lambda execution environment sets that variable and
// nothing else does — sufficient and stable as a transport-selection
// sentinel.
func IsLambda() bool {
	return os.Getenv("AWS_LAMBDA_FUNCTION_NAME") != ""
}

// MustEnv returns the value of the given env var. If unset or empty,
// it logs at ERROR level and calls os.Exit(1). Use this for required
// configuration at startup — never inside a request handler.
func MustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required env var not set", "key", key)
		os.Exit(1)
	}
	return v
}

// Port resolves a local HTTP listener port through a three-step chain:
// the service-specific env var, then the generic PORT, then the
// fallback. Matches api-platform's convention so the same `PORT=…`
// works across both monorepos. Production Lambda doesn't bind to a
// port — the runtime handles transport.
func Port(envKey, fallback string) string {
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	if v := os.Getenv("PORT"); v != "" {
		return v
	}
	return fallback
}

// MustPgxPool reads DATABASE_URL via MustEnv, opens a pgxpool, and
// exits on construction failure. The returned pool is lazily
// connected — call pool.Ping if you want to fail fast on the database
// being unreachable at startup.
func MustPgxPool() *pgxpool.Pool {
	dsn := MustEnv("DATABASE_URL")
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		slog.Error("pgxpool init failed", "error", err)
		os.Exit(1)
	}
	return pool
}
