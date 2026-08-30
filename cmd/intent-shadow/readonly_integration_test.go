//go:build integration

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// 🔴 The guarantee that makes running against the production billing ledger
// safe, asserted against a real server rather than assumed from the flag.
//
// A write inside this transaction must FAIL. If it does not, every other
// safety claim about this tool is worthless.
func TestAWriteInsideTheReadOnlyTransactionFails(t *testing.T) {
	pool := testutil.NewTestDB(t)

	err := withReadOnlyTx(context.Background(), pool, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO ms_billing.billing_periods
			(account_id, period_start, period_end)
			VALUES (gen_random_uuid(), now(), now() + interval '1 day')`)
		return err
	})

	require.Error(t, err, "a write succeeded inside the read-only transaction")
	require.Contains(t, strings.ToLower(err.Error()), "read-only",
		"the write failed for some reason OTHER than the transaction being read-only, "+
			"so this test does not prove the guard works: %v", err)
}

// The server must AGREE it is read-only, not merely be asked to be. A flag we
// set and never verify is a belief, not a guarantee.
func TestTheServerConfirmsTheTransactionIsReadOnly(t *testing.T) {
	pool := testutil.NewTestDB(t)

	var reported string
	err := withReadOnlyTx(context.Background(), pool, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT current_setting('transaction_read_only')`).Scan(&reported)
	})
	require.NoError(t, err)
	require.Equal(t, "on", reported)
}

// Reads must still work, or the guard is just an outage.
func TestReadsStillWorkInsideTheReadOnlyTransaction(t *testing.T) {
	pool := testutil.NewTestDB(t)

	var n int
	err := withReadOnlyTx(context.Background(), pool, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM ms_billing.billing_periods`).Scan(&n)
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 0)
}

// The timeouts are set INSIDE the transaction (local = true), so they bound
// this work and do not leak to the next user of a pooled connection.
func TestTheTimeoutsAreLocalToTheTransaction(t *testing.T) {
	pool := testutil.NewTestDB(t)

	var stmt string
	require.NoError(t, withReadOnlyTx(context.Background(), pool, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT current_setting('statement_timeout')`).Scan(&stmt)
	}))
	require.Equal(t, statementTimeout, stmt)

	// Outside the transaction the pool's default is back.
	var after string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT current_setting('statement_timeout')`).Scan(&after))
	require.NotEqual(t, statementTimeout, after,
		"the statement timeout leaked out of the transaction onto a pooled connection")
}
