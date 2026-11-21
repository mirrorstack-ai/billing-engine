package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Copied from cmd/intent-shadow/readonly.go (the same proof-before-use
// read-only gate), not imported — the two are separate main packages. See
// that file's doc comment for why each of the three layers matters.
const (
	statementTimeout = "30s"
	lockTimeout      = "2s"
	idleTimeout      = "60s"
	txDeadline       = 4 * time.Minute
)

// withReadOnlyTx opens a transaction the server has confirmed cannot write,
// and runs fn while holding it, then always rolls back. fn never writes
// through tx — this binary's own reads go through usage.Service against the
// pool directly (see main.go's doc comment for why) — but the open
// transaction still proves, before anything runs, that the credentials
// handed to this binary cannot write.
func withReadOnlyTx(ctx context.Context, pool *pgxpool.Pool, fn func(context.Context, pgx.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, txDeadline)
	defer cancel()

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
		IsoLevel:   pgx.RepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("begin read-only transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}()

	if _, err := tx.Exec(ctx, `SELECT set_config('statement_timeout',$1,true),
	                                  set_config('lock_timeout',$2,true),
	                                  set_config('idle_in_transaction_session_timeout',$3,true)`,
		statementTimeout, lockTimeout, idleTimeout); err != nil {
		return fmt.Errorf("set read-only timeouts: %w", err)
	}

	var readOnly string
	if err := tx.QueryRow(ctx, `SELECT current_setting('transaction_read_only')`).Scan(&readOnly); err != nil {
		return fmt.Errorf("read back transaction_read_only: %w", err)
	}
	if readOnly != "on" {
		return fmt.Errorf(
			"refusing to run: the server reports transaction_read_only=%q, not \"on\"", readOnly)
	}

	return fn(ctx, tx)
}
