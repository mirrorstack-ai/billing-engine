//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 033 (ms_billing.app_module_overage_timers) — exercises the per-module
// install-timer SQL against a real Postgres: the generate_series bulk insert, the
// live-count reconcile input, the FIFO rank row-comparison, the past-grace sweep
// work-list join (activated_at gate), the terminal-mark first-write-wins guards,
// and LIFO / all soft-removal.
func TestModuleOverageTimers_Integration_SynthesisFIFOAndSweep(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	// Activate so the sweep's activated_at IS NOT NULL gate passes.
	_, err := pool.Exec(ctx, `UPDATE ms_billing.accounts SET activated_at = $2 WHERE id = $1`,
		acct.String(), mustTime(t, "2026-05-04T00:00:00Z"))
	require.NoError(t, err)

	app := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, app, acct, 0, mustTime(t, "2026-06-01T00:00:00Z")))

	// 5 "included" installs anchored early + 1 "over" install anchored June 10.
	early := mustTime(t, "2026-05-04T00:00:00Z")
	require.NoError(t, store.InsertModuleOverageTimers(ctx, acct, app, early, early.AddDate(0, 0, 3), 5))
	over := mustTime(t, "2026-06-10T00:00:00Z")
	require.NoError(t, store.InsertModuleOverageTimers(ctx, acct, app, over, over.AddDate(0, 0, 3), 1))

	n, err := store.LiveModuleTimerCountForApp(ctx, app)
	require.NoError(t, err)
	require.Equal(t, 6, n)

	// Work list as of June 20 (all graces elapsed): all 6 candidates, activated.
	cands, err := store.ModuleOverageTimersPastGrace(ctx, mustTime(t, "2026-06-20T00:00:00Z"))
	require.NoError(t, err)
	require.Len(t, cands, 6)

	// LIVE FIFO rank: the 5 early installs rank 0-4 (included); the June-10 install
	// ranks 5 (over) — verifying the (installed_at, id) row-comparison in SQL.
	var overCand cycle.ModuleOverageCandidate
	included := 0
	for _, c := range cands {
		rank, err := store.LiveModuleTimerRankBefore(ctx, c.AccountID, c.ID, c.InstalledAt)
		require.NoError(t, err)
		if rank < usage.IncludedModules {
			included++
		} else {
			overCand = c
		}
	}
	require.Equal(t, usage.IncludedModules, included)
	require.True(t, over.Equal(overCand.InstalledAt), "the June-10 install is the over one")

	// Terminal marks drop rows out of the work list.
	require.NoError(t, store.MarkModuleTimerCharged(ctx, overCand.ID, mustTime(t, "2026-06-20T00:00:00Z"), "in_real_stripe", "ii_real_stripe"))
	for _, c := range cands {
		if c.ID != overCand.ID {
			require.NoError(t, store.MarkModuleTimerIncluded(ctx, c.ID))
		}
	}
	cands2, err := store.ModuleOverageTimersPastGrace(ctx, mustTime(t, "2026-06-20T00:00:00Z"))
	require.NoError(t, err)
	require.Empty(t, cands2, "resolved timers drop out of the work list")

	// Resolved rows stay LIVE (removed_at NULL) — resolution ≠ removal.
	n, err = store.LiveModuleTimerCountForApp(ctx, app)
	require.NoError(t, err)
	require.Equal(t, 6, n)

	// LIFO soft-removal: add 3 more, remove the newest 2 → 6 + 3 − 2 = 7 live.
	late := mustTime(t, "2026-07-01T00:00:00Z")
	require.NoError(t, store.InsertModuleOverageTimers(ctx, acct, app, late, late.AddDate(0, 0, 3), 3))
	require.NoError(t, store.SoftRemoveNewestModuleTimers(ctx, app, 2, late))
	n, err = store.LiveModuleTimerCountForApp(ctx, app)
	require.NoError(t, err)
	require.Equal(t, 7, n)

	// SoftRemoveAll clears every remaining live timer.
	require.NoError(t, store.SoftRemoveAllModuleTimersForApp(ctx, app, late))
	n, err = store.LiveModuleTimerCountForApp(ctx, app)
	require.NoError(t, err)
	require.Zero(t, n)
}

// Regression (review 2026-07-06, H7): timer synthesis is a count-then-insert
// reconcile in a fire-and-forget-with-retry RPC environment — two CONCURRENT
// executions for the same app used to both read the same live count and both
// insert the full deficit, minting phantom timers wrongfully charged $3 each at
// every boundary. ReconcileModuleTimersToTarget serializes per app under a
// pg_advisory_xact_lock; hammer it concurrently and assert the invariant.
func TestModuleOverageTimers_Integration_ConcurrentReconcileNeverDoubleInserts(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app := uuid.New()
	created := mustTime(t, "2026-06-19T12:00:00Z")
	require.NoError(t, store.InsertAppMirror(ctx, app, acct, 7, created))

	const workers = 8
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			errs <- store.ReconcileModuleTimersToTarget(ctx, acct, app, 7, created, created.AddDate(0, 0, 3), created)
		}()
	}
	for i := 0; i < workers; i++ {
		require.NoError(t, <-errs)
	}

	n, err := store.LiveModuleTimerCountForApp(ctx, app)
	require.NoError(t, err)
	require.Equal(t, 7, n, "concurrent reconciles must never double-insert the deficit")

	// And a concurrent grow/shrink mix still converges to the LAST target
	// applied — each reconcile is atomic, so no interleaving can overshoot.
	require.NoError(t, store.ReconcileModuleTimersToTarget(ctx, acct, app, 3, created, created.AddDate(0, 0, 3), mustTime(t, "2026-06-20T00:00:00Z")))
	n, err = store.LiveModuleTimerCountForApp(ctx, app)
	require.NoError(t, err)
	require.Equal(t, 3, n)
}

// Stage B: the row_number()-windowed reads backing scenario 3 (CoCreatedOverModuleTimers),
// scenario 6 / Leg 2 (CountOngoingOverModuleTimers), and the display
// (CountLiveModuleTimersForAccount) — validated against real Postgres, since the
// unit fakes only approximate the FIFO window function in Go.
func TestModuleOverageTimers_Integration_OverQueries(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	_, err := pool.Exec(ctx, `UPDATE ms_billing.accounts SET activated_at = $2 WHERE id = $1`,
		acct.String(), mustTime(t, "2026-05-04T00:00:00Z"))
	require.NoError(t, err)

	appA, appB := uuid.New(), uuid.New()
	created := mustTime(t, "2026-06-19T12:00:00Z")
	require.NoError(t, store.InsertAppMirror(ctx, appA, acct, 7, created))
	require.NoError(t, store.InsertAppMirror(ctx, appB, acct, 0, created))

	// appA: 7 co-created install timers at created_at → FIFO ranks 0-6, so 2 are
	// "over" (rank ≥ IncludedModules=5).
	require.NoError(t, store.InsertModuleOverageTimers(ctx, acct, appA, created, created.AddDate(0, 0, 3), 7))

	// CountLiveModuleTimersForAccount (via the usage store, the display's real
	// consumer) = the account-overage over-count input (7 live).
	usageStore := usage.NewStore(pool)
	liveN, err := usageStore.LiveModuleTimerCountForAccount(ctx, acct)
	require.NoError(t, err)
	require.Equal(t, 7, liveN)

	// CoCreatedOverModuleTimers: the 2 co-created over-modules (rank ≥ 5), unresolved.
	over, err := store.CoCreatedOverModuleTimers(ctx, acct, appA, created, usage.IncludedModules)
	require.NoError(t, err)
	require.Len(t, over, 2, "7 co-created → 2 over the included 5")

	// The boundary that opens the account's [Jul 4, Aug 4) period (anchor day 4).
	newPeriodStart := mustTime(t, "2026-07-04T00:00:00Z")

	// Before any charge, none are "ongoing" (grace_resolved all false).
	ongoing, err := store.CountOngoingOverModuleTimers(ctx, acct, usage.IncludedModules, newPeriodStart)
	require.NoError(t, err)
	require.Zero(t, ongoing, "nothing resolved yet → no ongoing over-module")

	// Charge the 2 co-created over-modules (scenario 3) → they become "ongoing".
	for _, id := range over {
		require.NoError(t, store.MarkModuleTimerCharged(ctx, id, created.AddDate(0, 0, 3), "in_x", "ii_x"))
	}
	ongoing, err = store.CountOngoingOverModuleTimers(ctx, acct, usage.IncludedModules, newPeriodStart)
	require.NoError(t, err)
	require.Equal(t, 2, ongoing, "the 2 charged over-modules are ongoing")
	// And they're no longer co-created candidates (grace_resolved now true).
	over, err = store.CoCreatedOverModuleTimers(ctx, acct, appA, created, usage.IncludedModules)
	require.NoError(t, err)
	require.Empty(t, over)

	// installed_at < period_end cutoff (review 2026-07-06, H1): at a RECLAIMED
	// earlier boundary (new period opening Jun 4, BEFORE these Jun-19 installs)
	// the charged over-modules must NOT be counted — their own grace charge
	// covered the period they were installed into; precharging it again is the
	// reclaimed skipped_no_pm/failed-run double-charge.
	ongoing, err = store.CountOngoingOverModuleTimers(ctx, acct, usage.IncludedModules, mustTime(t, "2026-06-04T00:00:00Z"))
	require.NoError(t, err)
	require.Zero(t, ongoing, "a module installed inside the new period is never precharged for it")

	// appB adds one LATER over-module (rank 7) still in its own grace (unresolved):
	// live count rises to 8, but the ongoing count stays 2 (unresolved excluded).
	late := mustTime(t, "2026-06-28T00:00:00Z")
	require.NoError(t, store.InsertModuleOverageTimers(ctx, acct, appB, late, late.AddDate(0, 0, 3), 1))
	liveN, err = usageStore.LiveModuleTimerCountForAccount(ctx, acct)
	require.NoError(t, err)
	require.Equal(t, 8, liveN)
	ongoing, err = store.CountOngoingOverModuleTimers(ctx, acct, usage.IncludedModules, newPeriodStart)
	require.NoError(t, err)
	require.Equal(t, 2, ongoing, "the in-grace (unresolved) over-module is NOT ongoing")

	// D1d resolved-WITHOUT-charge (review 2026-07-06, C1): resolving the rank-7
	// timer uncharged (the period-closed posture) must still make it "ongoing"
	// from the next boundary on — the old grace_charged_at IS NOT NULL proxy
	// exempted such modules from ALL overage billing forever.
	lateCands, err := store.ModuleOverageTimersPastGrace(ctx, mustTime(t, "2026-07-02T00:00:00Z"))
	require.NoError(t, err)
	var rank7 cycle.ModuleOverageCandidate
	for _, c := range lateCands {
		if c.AppID == appB {
			rank7 = c
		}
	}
	require.NotEqual(t, uuid.Nil, rank7.ID, "the rank-7 appB timer is in the late work list")
	require.NoError(t, store.MarkModuleTimerIncluded(ctx, rank7.ID))
	ongoing, err = store.CountOngoingOverModuleTimers(ctx, acct, usage.IncludedModules, newPeriodStart)
	require.NoError(t, err)
	require.Equal(t, 3, ongoing, "a resolved-uncharged (D1d) over-module still owes every later period")

	// grace_expires_at < period_end cutoff (review 2026-07-06, M1): a charged
	// over-module whose grace STRADDLES the boundary (installed Jul 2, expiry
	// Jul 5 >= Jul 4) is excluded — its own Leg 1 charge covers the new period
	// (coverage runs through the END of the period its grace elapses into).
	straddle := mustTime(t, "2026-07-02T00:00:00Z")
	require.NoError(t, store.InsertModuleOverageTimers(ctx, acct, appB, straddle, straddle.AddDate(0, 0, 3), 1))
	straddleCands, err := store.ModuleOverageTimersPastGrace(ctx, mustTime(t, "2026-07-06T00:00:00Z"))
	require.NoError(t, err)
	var straddleTimer cycle.ModuleOverageCandidate
	for _, c := range straddleCands {
		if c.InstalledAt.Equal(straddle) {
			straddleTimer = c
		}
	}
	require.NotEqual(t, uuid.Nil, straddleTimer.ID, "the Jul-2 straddle timer is in the late work list")
	require.NoError(t, store.MarkModuleTimerCharged(ctx, straddleTimer.ID, mustTime(t, "2026-07-05T00:00:00Z"), "in_straddle", "ii_straddle"))
	ongoing, err = store.CountOngoingOverModuleTimers(ctx, acct, usage.IncludedModules, newPeriodStart)
	require.NoError(t, err)
	require.Equal(t, 3, ongoing, "a boundary-straddling grace is Leg 1's coverage, never the precharge's")
}
