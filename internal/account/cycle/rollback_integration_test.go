//go:build integration

package cycle

// White-box (package cycle, not cycle_test) so this can exercise the
// unexported deferredRollback directly (finding 3, store.go): the deferred
// Rollback in ChargeProrationLocked's phases must reach Postgres even when the
// context passed to the surrounding call is already cancelled/expired (e.g.
// the enclosing Lambda invocation timed out while awaiting a stalled Stripe
// call) — reusing that dead context verbatim in the Rollback call can fail
// silently, leaving the transaction (and its row lock) open until Postgres's
// own dead-connection detection eventually reclaims it.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestDeferredRollback_Integration_SurvivesAlreadyCancelledContext(t *testing.T) {
	pool := testutil.NewTestDB(t)

	// Sanity check: confirms the bug this test guards against is real — pgx
	// refuses to run a network round-trip (including ROLLBACK) against an
	// already-cancelled context, so a NAIVE `defer func() { tx.Rollback(ctx) }()`
	// that reuses a dead ctx verbatim is a silent no-op.
	ctx, cancel := context.WithCancel(context.Background())
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	cancel()
	naiveErr := tx.Rollback(ctx)
	require.Error(t, naiveErr, "sanity: rolling back with an already-cancelled context fails")
	// Clean up the still-open transaction/connection from the sanity check with
	// a fresh context so it doesn't leak into the real assertion below.
	_ = tx.Rollback(context.Background())

	// The fix: deferredRollback is handed the SAME dead ctx but must still reach
	// Postgres via its own short-lived detached context.
	tx2, err := pool.Begin(context.Background())
	require.NoError(t, err)
	deferredRollback(ctx, tx2) // ctx here is STILL the cancelled context from above

	// Prove the rollback actually completed and the connection is healthy again
	// (a leaked/open transaction would eventually starve the pool; this bounds
	// how long we're willing to wait for the pool to hand back a working conn).
	require.Eventually(t, func() bool {
		var one int
		return pool.QueryRow(context.Background(), "SELECT 1").Scan(&one) == nil && one == 1
	}, 3*time.Second, 50*time.Millisecond, "deferredRollback must release the transaction/connection despite the dead ctx")
}
