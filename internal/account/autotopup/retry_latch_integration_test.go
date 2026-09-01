//go:build integration

package autotopup_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// 🔴 Two executor-driven latch tests were DELETED with the legacy collector.
//
// TestExecutorTrigger_DeterministicDeclineLatchesUntilConfigRevisionIntegration
// and TestExecutorTrigger_ClockBehindPolicyCannotBypassFailureLatchIntegration
// both produced their "failed" attempt by letting the collector create,
// finalize and pay an invoice that Stripe declined. There is no create,
// finalize or pay port left, so neither could reach the state it asserted.
//
// The latch itself is a STORE property and is still covered without a
// collector by TestStoreAcquire_FailedPolicyRevisionLatchesUntilConfigResubmission in
// store_integration_test.go, which drives it through Store.Fail directly.

func TestStoreAcquire_DatabaseClockBoundsPendingGraceAcrossApplicationSkewIntegration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, _ := seedEligibleAutoTopUp(t, pool, 0, true)
	store := autotopup.NewStore(pool)

	var dbBefore time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT CURRENT_TIMESTAMP`).Scan(&dbBefore))
	appClockAhead := dbBefore.Add(6 * time.Hour)
	attempt, kind, err := store.Acquire(ctx, accountID, 1, appClockAhead)
	require.NoError(t, err)
	require.Equal(t, autotopup.AcquireNew, kind)
	require.False(t, attempt.Expired(appClockAhead),
		"an application clock ahead of Aurora must not expire a fresh attempt")
	require.False(t, attempt.CreatedAt.Before(dbBefore))
	require.Less(t, attempt.CreatedAt, appClockAhead.Add(-5*time.Hour),
		"created_at must come from Aurora rather than the future application clock")
	require.Equal(t, autotopup.PendingGrace, attempt.ExpiresAt.Sub(attempt.CreatedAt))

	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.credit_ledger
		SET created_at = CURRENT_TIMESTAMP - INTERVAL '11 minutes',
		    attempt_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 minute'
		WHERE id = $1`,
		attempt.ID,
	)
	require.NoError(t, err)

	expired, found, err := store.Pending(ctx, accountID)
	require.NoError(t, err)
	require.True(t, found)
	appClockBehind := dbBefore.Add(-6 * time.Hour)
	require.True(t, expired.Expired(appClockBehind),
		"an application clock behind Aurora must not extend an expired durable attempt")
}

func TestStoreAcquire_GraceClockSampleOccursAfterAccountLockWaitIntegration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, _ := seedEligibleAutoTopUp(t, pool, 0, true)
	store := autotopup.NewStore(pool)

	lockTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lockTx.Rollback(ctx) })
	_, err = lockTx.Exec(
		ctx,
		`SELECT id FROM ms_billing.accounts WHERE id = $1 FOR UPDATE`,
		accountID,
	)
	require.NoError(t, err)

	type acquireResult struct {
		attempt autotopup.Attempt
		kind    autotopup.AcquireKind
		err     error
	}
	resultCh := make(chan acquireResult, 1)
	go func() {
		attempt, kind, acquireErr := store.Acquire(
			ctx,
			accountID,
			1,
			time.Now().UTC().Add(6*time.Hour),
		)
		resultCh <- acquireResult{attempt: attempt, kind: kind, err: acquireErr}
	}()

	require.Eventually(t, func() bool {
		var waiting bool
		queryErr := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE pid <> pg_backend_pid()
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%LockAutoTopUpAccount%'
			)`,
		).Scan(&waiting)
		return queryErr == nil && waiting
	}, 5*time.Second, 10*time.Millisecond,
		"Acquire must be observed waiting on the account lock")

	releasedAt := time.Now().UTC()
	require.NoError(t, lockTx.Commit(ctx))
	result := <-resultCh
	require.NoError(t, result.err)
	require.Equal(t, autotopup.AcquireNew, result.kind)
	require.False(t, result.attempt.CreatedAt.Before(releasedAt.Add(-25*time.Millisecond)),
		"created_at must sample DB wall time after the account lock wait")
	require.Equal(
		t,
		autotopup.PendingGrace,
		result.attempt.ExpiresAt.Sub(result.attempt.CreatedAt),
	)
}
