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
