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
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Auth mode values for the DB_AUTH env var, read by MustPgxPool.
const (
	// AuthPassword authenticates with the password embedded in
	// DATABASE_URL. Local dev default.
	AuthPassword = "password"
	// AuthRDSIAM authenticates through RDS Proxy with a locally-signed
	// 15-minute RDS-IAM token minted per new connection (any password in
	// DATABASE_URL is overwritten before each dial).
	AuthRDSIAM = "rds-iam"
)

// IsLambda reports whether the current process is running inside an
// AWS Lambda runtime. The check is `AWS_LAMBDA_FUNCTION_NAME != ""`
// because every Lambda execution environment sets that variable and
// nothing else does — sufficient and stable as a transport-selection
// sentinel.
func IsLambda() bool {
	return os.Getenv("AWS_LAMBDA_FUNCTION_NAME") != ""
}

// CreditWalletEnabled reports the CREDIT_WALLET_ENABLED env flag, fail-closed:
// only "1"/"true" enable it; absent/empty/anything else = OFF.
func CreditWalletEnabled() bool {
	switch os.Getenv("CREDIT_WALLET_ENABLED") {
	case "1", "true", "TRUE", "True":
		return true
	default:
		return false
	}
}

// CreditWalletSchemaReady probes (once, at boot) that migration 048's objects
// exist and are readable by the app role. Missing objects are the fail-closed
// "not ready yet" signal; every other database error is returned so the caller
// can fail fast. LIMIT 0 plans and validates the columns without moving rows.
func CreditWalletSchemaReady(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	return creditWalletSchemaReady(ctx, pool)
}

type creditWalletSchemaExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func creditWalletSchemaReady(ctx context.Context, db creditWalletSchemaExecutor) (bool, error) {
	if _, err := db.Exec(ctx, "SELECT billing_mode FROM ms_billing.accounts LIMIT 0"); err != nil {
		var pg *pgconn.PgError
		if errors.As(err, &pg) && (pg.Code == "42P01" || pg.Code == "42703") {
			return false, nil
		}
		return false, err
	}
	if _, err := db.Exec(ctx, "SELECT 1 FROM ms_billing.credit_ledger LIMIT 0"); err != nil {
		var pg *pgconn.PgError
		if errors.As(err, &pg) && pg.Code == "42P01" {
			return false, nil
		}
		return false, err
	}
	return true, nil
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
// exits on construction failure. DB_AUTH selects the auth mode:
// "password" (default) sends the DATABASE_URL password; "rds-iam"
// (production, through RDS Proxy) signs a fresh RDS-IAM token per new
// connection and presents it as the password over TLS. The returned
// pool is lazily connected — call pool.Ping if you want to fail fast
// on the database being unreachable at startup.
func MustPgxPool() *pgxpool.Pool {
	dsn := MustEnv("DATABASE_URL")
	poolCfg, err := pgxPoolConfig(dsn, os.Getenv("DB_AUTH"))
	if err != nil {
		slog.Error("pgxpool config failed", "error", err)
		os.Exit(1)
	}
	// NewWithConfig, not New(ctx, poolCfg.ConnString()): ConnString()
	// returns the ORIGINAL DSN, so re-parsing it would silently drop the
	// programmatic BeforeConnect hook set in rds-iam mode.
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		slog.Error("pgxpool init failed", "error", err)
		os.Exit(1)
	}
	return pool
}

// pgxPoolConfig parses the DSN and applies the DB_AUTH mode. It rejects
// combinations that would otherwise fail at dial time with an opaque
// server error: RDS-IAM tokens are only accepted over TLS, so a non-TLS
// DATABASE_URL would be a silent downgrade that surfaces as a generic
// auth failure — and would put the token on the wire in cleartext.
func pgxPoolConfig(dsn, authMode string) (*pgxpool.Config, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	switch authMode {
	case "", AuthPassword:
		return poolCfg, nil
	case AuthRDSIAM:
		// TLSConfig == nil means the parsed DSN resolved to a plaintext
		// primary connection (sslmode=disable or allow) — reject loudly.
		if poolCfg.ConnConfig.TLSConfig == nil {
			return nil, fmt.Errorf("config: DB_AUTH=rds-iam requires TLS, set sslmode=require in DATABASE_URL")
		}
		poolCfg.BeforeConnect = newRDSIAMBeforeConnect()
		return poolCfg, nil
	default:
		return nil, fmt.Errorf("config: unknown DB_AUTH %q (want %q or %q)", authMode, AuthPassword, AuthRDSIAM)
	}
}
