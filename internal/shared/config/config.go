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

	"github.com/jackc/pgx/v5"
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

// CreditRuntimeSchemaReady extends the migration-048 capability probe with the
// migration-049 fields required by durable automatic top-ups. Rollout roots
// must use this stricter probe before shadow or enforce; the legacy/off path
// does not call it.
func CreditRuntimeSchemaReady(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	ready, err := creditWalletSchemaReady(ctx, pool)
	if err != nil || !ready {
		return ready, err
	}
	return creditAutoTopUpSchemaReady(ctx, pool)
}

// CreditRecoverySchemaReady is the narrow request-time capability probe for
// already-authorized manual-purchase and auto-top-up recovery. Unlike the
// startup rollout probe, it is catalog-only: a schema-047 database therefore
// returns false without preparing or executing a statement that directly
// names migration-048/049 relations or columns.
func CreditRecoverySchemaReady(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	return creditRecoverySchemaReady(ctx, pool)
}

type creditWalletSchemaExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type creditRuntimeSchemaExecutor interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func warnCreditSchemaCapabilitiesNotReady(ctx context.Context, missing []string) {
	slog.WarnContext(
		ctx,
		"credit runtime schema capability probe is not ready",
		"missing_or_incompatible_capabilities",
		missing,
	)
}

// creditWalletCatalogCapabilityProbe checks the base objects needed before the
// migration-049 capabilities can be meaningful. Every relation/column name is
// data filtered from pg_catalog; none appears in a direct FROM/SELECT target.
const creditWalletCatalogCapabilityProbe = `
WITH schema_relations AS (
    SELECT relation.oid, relation.relname, relation.relkind
    FROM pg_catalog.pg_namespace AS namespace
    JOIN pg_catalog.pg_class AS relation
      ON relation.relnamespace = namespace.oid
    WHERE namespace.nspname = 'ms_billing'
),
schema_columns AS (
    SELECT relation.relname AS table_name,
           attribute.attname AS column_name,
           attribute.atttypid AS type_id,
           attribute.attnotnull AS not_null
    FROM schema_relations AS relation
    JOIN pg_catalog.pg_attribute AS attribute
      ON attribute.attrelid = relation.oid
    WHERE attribute.attnum > 0
      AND NOT attribute.attisdropped
)
SELECT
    EXISTS (
        SELECT 1
        FROM schema_columns
        WHERE table_name = 'accounts'
          AND column_name = 'billing_mode'
          AND type_id = 'text'::regtype
          AND not_null
    ),
    EXISTS (
        SELECT 1
        FROM schema_relations
        WHERE relname = 'credit_ledger'
          AND relkind = 'r'
    ),
    EXISTS (
        SELECT 1
        FROM schema_relations
        WHERE relname = 'credit_auto_topup_configs'
          AND relkind = 'r'
    )
`

func creditRecoverySchemaReady(
	ctx context.Context,
	db creditRuntimeSchemaExecutor,
) (bool, error) {
	var billingMode, ledger, autoTopUpConfig bool
	if err := db.QueryRow(ctx, creditWalletCatalogCapabilityProbe).Scan(
		&billingMode,
		&ledger,
		&autoTopUpConfig,
	); err != nil {
		return false, err
	}
	if !billingMode || !ledger || !autoTopUpConfig {
		var missing []string
		if !billingMode {
			missing = append(
				missing,
				"ms_billing.accounts.billing_mode (non-null text column)",
			)
		}
		if !ledger {
			missing = append(missing, "ms_billing.credit_ledger (ordinary table)")
		}
		if !autoTopUpConfig {
			missing = append(
				missing,
				"ms_billing.credit_auto_topup_configs (ordinary table)",
			)
		}
		warnCreditSchemaCapabilitiesNotReady(ctx, missing)
		return false, nil
	}
	return creditAutoTopUpSchemaReady(ctx, db)
}

func creditWalletSchemaReady(ctx context.Context, db creditWalletSchemaExecutor) (bool, error) {
	if _, err := db.Exec(ctx, "SELECT billing_mode FROM ms_billing.accounts LIMIT 0"); err != nil {
		var pg *pgconn.PgError
		if errors.As(err, &pg) && (pg.Code == "42P01" || pg.Code == "42703") {
			warnCreditSchemaCapabilitiesNotReady(
				ctx,
				[]string{"ms_billing.accounts.billing_mode (readable column)"},
			)
			return false, nil
		}
		return false, err
	}
	if _, err := db.Exec(ctx, "SELECT 1 FROM ms_billing.credit_ledger LIMIT 0"); err != nil {
		var pg *pgconn.PgError
		if errors.As(err, &pg) && pg.Code == "42P01" {
			warnCreditSchemaCapabilitiesNotReady(
				ctx,
				[]string{"ms_billing.credit_ledger (readable table)"},
			)
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// creditAutoTopUpSchemaContract is deliberately one boolean per runtime
// dependency. Keeping the fields separate makes a partial migration fail
// closed, identifies every missing or incompatible capability in diagnostics,
// and lets tests remove each capability independently.
type creditAutoTopUpSchemaContract struct {
	configPaymentMethodColumn      bool
	attemptPaymentMethodColumn     bool
	attemptStripePMColumn          bool
	attemptStripeCustomerColumn    bool
	attemptExpiresColumn           bool
	failureCodeColumn              bool
	configPaymentMethodForeignKey  bool
	attemptPaymentMethodForeignKey bool
	attemptFieldsCheck             bool
	failureStateCheck              bool
	attemptWindowCheck             bool
	pendingAttemptUniqueIndex      bool
}

func (c creditAutoTopUpSchemaContract) ready() bool {
	return len(c.missingCapabilities()) == 0
}

func (c creditAutoTopUpSchemaContract) missingCapabilities() []string {
	checks := []struct {
		name  string
		ready bool
	}{
		{
			name:  "ms_billing.credit_auto_topup_configs.payment_method_id (nullable uuid column)",
			ready: c.configPaymentMethodColumn,
		},
		{
			name:  "ms_billing.credit_ledger.attempt_payment_method_id (nullable uuid column)",
			ready: c.attemptPaymentMethodColumn,
		},
		{
			name:  "ms_billing.credit_ledger.attempt_stripe_payment_method_id (nullable text column)",
			ready: c.attemptStripePMColumn,
		},
		{
			name:  "ms_billing.credit_ledger.attempt_stripe_customer_id (nullable text column)",
			ready: c.attemptStripeCustomerColumn,
		},
		{
			name:  "ms_billing.credit_ledger.attempt_expires_at (nullable timestamptz column)",
			ready: c.attemptExpiresColumn,
		},
		{
			name:  "ms_billing.credit_ledger.failure_code (nullable text column)",
			ready: c.failureCodeColumn,
		},
		{
			name: "ms_billing.credit_auto_topup_configs constraint " +
				"credit_auto_topup_configs_payment_method_fkey (validated foreign key)",
			ready: c.configPaymentMethodForeignKey,
		},
		{
			name: "ms_billing.credit_ledger constraint " +
				"credit_ledger_attempt_payment_method_fkey (validated foreign key)",
			ready: c.attemptPaymentMethodForeignKey,
		},
		{
			name: "ms_billing.credit_ledger constraint " +
				"credit_ledger_auto_topup_attempt_fields_check (validated check constraint)",
			ready: c.attemptFieldsCheck,
		},
		{
			name: "ms_billing.credit_ledger constraint " +
				"credit_ledger_auto_topup_failure_state_check (validated check constraint)",
			ready: c.failureStateCheck,
		},
		{
			name: "ms_billing.credit_ledger constraint " +
				"credit_ledger_auto_topup_attempt_window_check (validated check constraint)",
			ready: c.attemptWindowCheck,
		},
		{
			name: "ms_billing.credit_ledger_auto_topup_pending_uidx " +
				"(valid unique index on ms_billing.credit_ledger)",
			ready: c.pendingAttemptUniqueIndex,
		},
	}

	var missing []string
	for _, check := range checks {
		if !check.ready {
			missing = append(missing, check.name)
		}
	}
	return missing
}

// creditAutoTopUpSchemaContractProbe verifies that migration 049's runtime
// capabilities exist with the required catalog-level kind, type, nullability,
// and validity. It deliberately does not compare pg_get_* output: PostgreSQL's
// rendered definitions are not a stable runtime interface. The exact
// constraint and index shapes remain pinned by the PostgreSQL integration
// suite (and the production migration verifier).
const creditAutoTopUpSchemaContractProbe = `
WITH schema_columns AS (
    SELECT
        relation.relname AS table_name,
        attribute.attname AS column_name,
        attribute.atttypid AS type_id,
        attribute.attnotnull AS not_null
    FROM pg_catalog.pg_namespace AS namespace
    JOIN pg_catalog.pg_class AS relation
      ON relation.relnamespace = namespace.oid
    JOIN pg_catalog.pg_attribute AS attribute
      ON attribute.attrelid = relation.oid
    WHERE namespace.nspname = 'ms_billing'
      AND relation.relkind = 'r'
      AND attribute.attnum > 0
      AND NOT attribute.attisdropped
),
schema_constraints AS (
    SELECT
        relation.relname AS table_name,
        catalog_constraint.conname AS constraint_name,
        catalog_constraint.contype AS constraint_type,
        catalog_constraint.convalidated AS validated
    FROM pg_catalog.pg_namespace AS namespace
    JOIN pg_catalog.pg_class AS relation
      ON relation.relnamespace = namespace.oid
    JOIN pg_catalog.pg_constraint AS catalog_constraint
      ON catalog_constraint.conrelid = relation.oid
    WHERE namespace.nspname = 'ms_billing'
),
schema_indexes AS (
    SELECT
        relation.relname AS table_name,
        index_relation.relname AS index_name,
        catalog_index.indisunique AS is_unique,
        catalog_index.indisvalid AS is_valid,
        catalog_index.indisready AS is_ready,
        catalog_index.indislive AS is_live
    FROM pg_catalog.pg_namespace AS namespace
    JOIN pg_catalog.pg_class AS relation
      ON relation.relnamespace = namespace.oid
    JOIN pg_catalog.pg_index AS catalog_index
      ON catalog_index.indrelid = relation.oid
    JOIN pg_catalog.pg_class AS index_relation
      ON index_relation.oid = catalog_index.indexrelid
    WHERE namespace.nspname = 'ms_billing'
)
SELECT
    EXISTS (
        SELECT 1
        FROM schema_columns
        WHERE table_name = 'credit_auto_topup_configs'
          AND column_name = 'payment_method_id'
          AND type_id = 'uuid'::regtype
          AND NOT not_null
    ),
    EXISTS (
        SELECT 1
        FROM schema_columns
        WHERE table_name = 'credit_ledger'
          AND column_name = 'attempt_payment_method_id'
          AND type_id = 'uuid'::regtype
          AND NOT not_null
    ),
    EXISTS (
        SELECT 1
        FROM schema_columns
        WHERE table_name = 'credit_ledger'
          AND column_name = 'attempt_stripe_payment_method_id'
          AND type_id = 'text'::regtype
          AND NOT not_null
    ),
    EXISTS (
        SELECT 1
        FROM schema_columns
        WHERE table_name = 'credit_ledger'
          AND column_name = 'attempt_stripe_customer_id'
          AND type_id = 'text'::regtype
          AND NOT not_null
    ),
    EXISTS (
        SELECT 1
        FROM schema_columns
        WHERE table_name = 'credit_ledger'
          AND column_name = 'attempt_expires_at'
          AND type_id = 'timestamptz'::regtype
          AND NOT not_null
    ),
    EXISTS (
        SELECT 1
        FROM schema_columns
        WHERE table_name = 'credit_ledger'
          AND column_name = 'failure_code'
          AND type_id = 'text'::regtype
          AND NOT not_null
    ),
    EXISTS (
        SELECT 1
        FROM schema_constraints
        WHERE table_name = 'credit_auto_topup_configs'
          AND constraint_name = 'credit_auto_topup_configs_payment_method_fkey'
          AND constraint_type = 'f'
          AND validated
    ),
    EXISTS (
        SELECT 1
        FROM schema_constraints
        WHERE table_name = 'credit_ledger'
          AND constraint_name = 'credit_ledger_attempt_payment_method_fkey'
          AND constraint_type = 'f'
          AND validated
    ),
    EXISTS (
        SELECT 1
        FROM schema_constraints
        WHERE table_name = 'credit_ledger'
          AND constraint_name = 'credit_ledger_auto_topup_attempt_fields_check'
          AND constraint_type = 'c'
          AND validated
    ),
    EXISTS (
        SELECT 1
        FROM schema_constraints
        WHERE table_name = 'credit_ledger'
          AND constraint_name = 'credit_ledger_auto_topup_failure_state_check'
          AND constraint_type = 'c'
          AND validated
    ),
    EXISTS (
        SELECT 1
        FROM schema_constraints
        WHERE table_name = 'credit_ledger'
          AND constraint_name = 'credit_ledger_auto_topup_attempt_window_check'
          AND constraint_type = 'c'
          AND validated
    ),
    EXISTS (
        SELECT 1
        FROM schema_indexes
        WHERE table_name = 'credit_ledger'
          AND index_name = 'credit_ledger_auto_topup_pending_uidx'
          AND is_unique
          AND is_valid
          AND is_ready
          AND is_live
    )
`

func creditAutoTopUpSchemaReady(ctx context.Context, db creditRuntimeSchemaExecutor) (bool, error) {
	var contract creditAutoTopUpSchemaContract
	err := db.QueryRow(ctx, creditAutoTopUpSchemaContractProbe).Scan(
		&contract.configPaymentMethodColumn,
		&contract.attemptPaymentMethodColumn,
		&contract.attemptStripePMColumn,
		&contract.attemptStripeCustomerColumn,
		&contract.attemptExpiresColumn,
		&contract.failureCodeColumn,
		&contract.configPaymentMethodForeignKey,
		&contract.attemptPaymentMethodForeignKey,
		&contract.attemptFieldsCheck,
		&contract.failureStateCheck,
		&contract.attemptWindowCheck,
		&contract.pendingAttemptUniqueIndex,
	)
	if err != nil {
		return false, err
	}
	if contract.ready() {
		return true, nil
	}
	warnCreditSchemaCapabilitiesNotReady(ctx, contract.missingCapabilities())
	return false, nil
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
