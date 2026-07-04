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

// Migration 028 (ms_billing.app_base_snapshots) — verifies the SQL-level
// conflict semantics the unit fakes only model: the proration upsert's
// retry-idempotence and win-over-advance, the advance insert's ON CONFLICT DO
// NOTHING, and the exact-period_start display read (usage.Store's
// AppBaseSnapshot) both legs feed.

func TestAppBaseSnapshots_Integration_ConflictSemanticsAndRead(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	usageStore := usage.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	appID := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, appID, acct, 5, mustTime(t, "2026-06-10T08:00:00Z")))

	periodStart := mustTime(t, "2026-06-01T00:00:00Z")
	periodEnd := mustTime(t, "2026-07-01T00:00:00Z")

	// Proration write + identical retry (idempotent upsert).
	pro := cycle.AppBaseSnapshot{
		AppID: appID, PeriodStart: periodStart, PeriodEnd: periodEnd,
		ModuleCount: 5, BaseMicros: 6_451_613,
	}
	require.NoError(t, store.UpsertProrationBaseSnapshot(ctx, pro))
	require.NoError(t, store.UpsertProrationBaseSnapshot(ctx, pro))

	// A conflicting advance insert is a silent no-op — the proration row wins.
	require.NoError(t, store.InsertAdvanceBaseSnapshot(ctx, cycle.AppBaseSnapshot{
		AppID: appID, PeriodStart: periodStart, PeriodEnd: periodEnd,
		ModuleCount: 5, BaseMicros: 20_000_000,
	}))

	snap, found, err := usageStore.AppBaseSnapshot(ctx, appID, periodStart)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "proration", snap.Source, "proration wins over a conflicting advance write")
	require.EqualValues(t, 6_451_613, snap.BaseMicros)
	require.Equal(t, 5, snap.ModuleCount)

	// A DIFFERENT period inserts cleanly (advance) and a re-run keeps the
	// FIRST write (DO NOTHING).
	nextStart := periodEnd
	nextEnd := mustTime(t, "2026-08-01T00:00:00Z")
	require.NoError(t, store.InsertAdvanceBaseSnapshot(ctx, cycle.AppBaseSnapshot{
		AppID: appID, PeriodStart: nextStart, PeriodEnd: nextEnd,
		ModuleCount: 5, BaseMicros: 20_000_000,
	}))
	require.NoError(t, store.InsertAdvanceBaseSnapshot(ctx, cycle.AppBaseSnapshot{
		AppID: appID, PeriodStart: nextStart, PeriodEnd: nextEnd,
		ModuleCount: 9, BaseMicros: 32_000_000, // must NOT overwrite
	}))
	snap, found, err = usageStore.AppBaseSnapshot(ctx, appID, nextStart)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "advance", snap.Source)
	require.EqualValues(t, 20_000_000, snap.BaseMicros, "a reclaimed re-run never rewrites the recorded charge")

	// Exact-match read: an unknown period start resolves found=false, never an
	// error (the display then falls back to the live estimate).
	_, found, err = usageStore.AppBaseSnapshot(ctx, appID, mustTime(t, "2026-05-01T00:00:00Z"))
	require.NoError(t, err)
	require.False(t, found)
}
