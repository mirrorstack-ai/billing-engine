package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Timeouts, copied from the deployed migration-048 verifier
// (mirrorstack-infra/assets/migration-verifier) with one deliberate change.
//
// The verifier runs fixed catalog probes and bounds statements at 8s. This runs
// 2+N queries over real billing tables, so 8s would refuse legitimate work on a
// large account. The lock timeout is unchanged: a diagnostic that waits on a
// lock is a diagnostic competing with production traffic.
const (
	statementTimeout = "30s"
	lockTimeout      = "2s"
	idleTimeout      = "60s"

	// txDeadline bounds the whole transaction. A REPEATABLE READ snapshot held
	// open pins the xmin horizon and blocks vacuum, so an ops function that
	// hangs is not merely slow — it degrades the database it is inspecting.
	txDeadline = 4 * time.Minute
)

// withReadOnlyTx runs fn inside a transaction that cannot write, and proves it
// before handing the transaction over.
//
// Three layers, and the third is the one that matters:
//
//  1. AccessMode: pgx.ReadOnly asks the server for a read-only transaction.
//  2. IsoLevel: pgx.RepeatableRead makes the 2+N queries see ONE snapshot. Under
//     READ COMMITTED this reconciliation could count a period in one query and
//     not in another, and that torn read is indistinguishable from a real
//     discrepancy — the tool would invent the thing it exists to detect.
//  3. The server is ASKED whether it agrees. current_setting('transaction_read_only')
//     is read back and anything other than 'on' aborts. A flag we set and never
//     verify is a belief, not a guarantee, and this one is load-bearing: it is
//     what makes running against the production billing ledger safe.
//
// Never commits. A read-only transaction has nothing to commit, and Rollback is
// the honest end.
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
		// context.WithoutCancel so an expired deadline still releases the
		// snapshot promptly instead of waiting for pgx to tear the connection
		// down — the snapshot is what blocks vacuum.
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
