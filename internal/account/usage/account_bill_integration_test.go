//go:build integration

package usage_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// These exercise GetAccountBill's two app-roster enumeration queries
// (AppIDsWithUsage / MirroredAppIDsOverlappingWindow) against a real Postgres.
// They verify what the fake-store unit tests can't: the rolled ∪ live UNION
// dedup, the account gate, the window bounds on the live half, and the mirror's
// half-open [created_at, deleted_at) overlap arithmetic (both boundary edges).
// Reuses the seed helpers + appPeriodStart/End constants from
// app_usage_integration_test.go (same package).

// seedMirrorApp inserts a ms_billing.apps roster row (migration 027) directly;
// deletedAt == "" seeds a live row (NULL deleted_at).
func seedMirrorApp(t *testing.T, pool *pgxpool.Pool, acct, app uuid.UUID, createdAt, deletedAt string) {
	t.Helper()
	var del any
	if deletedAt != "" {
		del = deletedAt
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.apps (app_id, account_id, module_count, created_at, deleted_at)
		 VALUES ($1, $2, 0, $3, $4)`,
		app.String(), acct.String(), createdAt, del)
	require.NoError(t, err)
}

// TestAppIDsWithUsage_Integration: the usage half enumerates the UNION of
// rolled (usage_aggregates for the period) and live (usage_events in the
// window) app_ids, deduped, account-gated, window-bounded on the live half.
func TestAppIDsWithUsage_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	foreign := appSeedAccount(t, pool)
	appLive, appRolled, appBoth := uuid.New(), uuid.New(), uuid.New()
	appForeign, appOutside := uuid.New(), uuid.New()
	mod := uuid.New()
	appSeedMetricDef(t, pool, mod, "orders.placed", usage.KindCount, 100)

	// Live-only app: raw events in the window.
	appSeedEvent(t, pool, acct, appLive, mod, "orders.placed", usage.KindCount, 4, "2026-06-05T00:00:00Z", "", "")
	// Rolled-only app: a frozen aggregate row for the period, no live events.
	periodID := appSeedPeriod(t, pool, acct, appPeriodStart, appPeriodEnd)
	appSeedAggregate(t, pool, periodID, acct, appRolled, mod, "orders.placed", usage.KindCount, "", "", 10, 100, 1000, 1000)
	// Both halves: must dedup to ONE entry.
	appSeedEvent(t, pool, acct, appBoth, mod, "orders.placed", usage.KindCount, 1, "2026-06-06T00:00:00Z", "", "")
	appSeedAggregate(t, pool, periodID, acct, appBoth, mod, "orders.placed", usage.KindCount, "", "", 2, 100, 200, 200)
	// Another account's event in the window: the account gate must drop it.
	appSeedEvent(t, pool, foreign, appForeign, mod, "orders.placed", usage.KindCount, 9, "2026-06-07T00:00:00Z", "", "")
	// Same account, event OUTSIDE the window: the live bounds must drop it.
	appSeedEvent(t, pool, acct, appOutside, mod, "orders.placed", usage.KindCount, 9, "2026-07-05T00:00:00Z", "", "")

	ids, err := store.AppIDsWithUsage(ctx, acct,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.ElementsMatch(t, []uuid.UUID{appLive, appRolled, appBoth}, ids)
}

// TestMirroredAppIDs_Integration: the mirror half enumerates rows whose
// [created_at, deleted_at) overlaps the half-open window — created-inside and
// deleted-inside rows are in; rows created AT/after period_end or deleted
// AT/before period_start are out (both edges exclusive per half-open math);
// other accounts' rows never appear.
func TestMirroredAppIDs_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	foreign := appSeedAccount(t, pool)

	longLived, createdInside, deletedInside := uuid.New(), uuid.New(), uuid.New()
	createdAtEnd, deletedAtStart, foreignApp := uuid.New(), uuid.New(), uuid.New()
	seedMirrorApp(t, pool, acct, longLived, "2026-01-10T00:00:00Z", "")                         // spans the window → in
	seedMirrorApp(t, pool, acct, createdInside, "2026-06-22T14:30:00Z", "")                     // created mid-window → in
	seedMirrorApp(t, pool, acct, deletedInside, "2026-01-10T00:00:00Z", "2026-06-20T00:00:00Z") // deleted mid-window → in (spent base)
	seedMirrorApp(t, pool, acct, createdAtEnd, appPeriodEnd, "")                                // created exactly at period_end → out
	seedMirrorApp(t, pool, acct, deletedAtStart, "2026-01-10T00:00:00Z", appPeriodStart)        // deleted exactly at period_start → out
	seedMirrorApp(t, pool, foreign, foreignApp, "2026-06-05T00:00:00Z", "")                     // other account → out

	ids, err := store.MirroredAppIDs(ctx, acct,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.ElementsMatch(t, []uuid.UUID{longLived, createdInside, deletedInside}, ids)
}
