//go:build integration

package usage_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// These exercise the AppBillLines / ListBillingPeriods / BillingPeriodWindow
// sqlc queries against a real Postgres (gated by the `integration` build tag).
// They verify what the fake-store unit tests can't: the LIVE infra 1.2× markup
// applied INLINE in SQL, the reserved-vs-custom split at the query level, and the
// period listing / window resolution. Reuses the seed helpers +
// appPeriodStart/End constants from app_usage_integration_test.go (same package).

// TestAppBill_Integration_LiveInfraMarkupAndSplit: on the LIVE branch a custom
// metric is charged qty × price (NO markup) while a reserved infra.* metric is
// charged qty × price × 12/10 (the 1.2× infra plane) — both returned by one read
// so the service can split them.
func TestAppBill_Integration_LiveInfraMarkupAndSplit(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	app := uuid.New()
	modCustom := uuid.New()
	modInfra := uuid.New() // a distinct module carrying the infra metric catalog row

	appSeedMetricDef(t, pool, modCustom, "orders.placed", usage.KindCount, 100)       // 100 µ$/unit
	appSeedMetricDef(t, pool, modInfra, "infra.egress.api.bytes", usage.KindSum, 100) // 100 µ$/unit COGS

	// Custom: 4 units → 400 µ$ (no markup).
	appSeedEvent(t, pool, acct, app, modCustom, "orders.placed", usage.KindCount, 4, "2026-06-05T00:00:00Z", "", "")
	// Infra: 10 units × 100 = 1000 raw → ×1.2 = 1200 µ$ charged.
	appSeedEvent(t, pool, acct, app, modInfra, "infra.egress.api.bytes", usage.KindSum, 10, "2026-06-06T00:00:00Z", "", "")

	rows, err := store.AppBill(ctx, acct, app,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Len(t, rows, 2)

	custom, ok := findAppRow(rows, "orders.placed", "", "")
	require.True(t, ok)
	require.EqualValues(t, 400, custom.ChargedMicros, "custom metric: 4 × 100, NO markup")

	infra, ok := findAppRow(rows, "infra.egress.api.bytes", "", "")
	require.True(t, ok)
	require.EqualValues(t, 1200, infra.ChargedMicros, "reserved infra metric: 10 × 100 × 1.2")
}

// TestAppBill_Integration_RolledInfraFrozenNotRemarkedUp: once rolled up, the
// frozen usage_aggregates.charged_micros is read verbatim — the rolled branch
// must NOT re-apply the 1.2× (the markup was already snapshotted at rollup).
func TestAppBill_Integration_RolledInfraFrozenNotRemarkedUp(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	app := uuid.New()
	modInfra := uuid.New()

	// A live event that would show on the live path — suppressed once rolled.
	appSeedMetricDef(t, pool, modInfra, "infra.egress.api.bytes", usage.KindSum, 100)
	appSeedEvent(t, pool, acct, app, modInfra, "infra.egress.api.bytes", usage.KindSum, 999, "2026-06-05T00:00:00Z", "", "")

	// Frozen record: charged 1200 already includes the 1.2× markup.
	periodID := appSeedPeriod(t, pool, acct, appPeriodStart, appPeriodEnd)
	appSeedAggregate(t, pool, periodID, acct, app, modInfra, "infra.egress.api.bytes", usage.KindSum, "", "",
		10, 100, 1000, 1200)

	rows, err := store.AppBill(ctx, acct, app,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Len(t, rows, 1, "rolled branch wins; live events suppressed")
	require.EqualValues(t, 1200, rows[0].ChargedMicros, "frozen charged, not re-marked-up")
}

// TestListBillingPeriods_Integration: an account's real periods list newest-first
// with is_current flagging the row equal to current_month_start; another
// account's periods are excluded.
func TestListBillingPeriods_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	other := appSeedAccount(t, pool)

	appSeedPeriod(t, pool, acct, "2026-05-01T00:00:00Z", "2026-06-01T00:00:00Z")
	junID := appSeedPeriod(t, pool, acct, "2026-06-01T00:00:00Z", "2026-07-01T00:00:00Z")
	appSeedPeriod(t, pool, other, "2026-06-01T00:00:00Z", "2026-07-01T00:00:00Z") // excluded

	currentMonthStart := appMustTime(t, "2026-06-01T00:00:00Z")
	rows, err := store.ListBillingPeriods(ctx, acct, currentMonthStart)
	require.NoError(t, err)
	require.Len(t, rows, 2, "only this account's periods")
	require.True(t, rows[0].PeriodStart.After(rows[1].PeriodStart), "newest-first")
	require.Equal(t, junID, rows[0].ID)
	require.True(t, rows[0].IsCurrent, "June == current_month_start")
	require.False(t, rows[1].IsCurrent, "May is a past period")
}

// TestBillingPeriodWindow_Integration: resolves a period's window by (account,
// id); wrong account or unknown id → found=false.
func TestBillingPeriodWindow_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	other := appSeedAccount(t, pool)
	pid := appSeedPeriod(t, pool, acct, appPeriodStart, appPeriodEnd)

	start, end, found, err := store.BillingPeriodWindow(ctx, acct, pid)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, start.Equal(appMustTime(t, appPeriodStart)))
	require.True(t, end.Equal(appMustTime(t, appPeriodEnd)))

	// Wrong account → not found (no cross-account resolution).
	_, _, found, err = store.BillingPeriodWindow(ctx, other, pid)
	require.NoError(t, err)
	require.False(t, found)

	// Unknown id → not found.
	_, _, found, err = store.BillingPeriodWindow(ctx, acct, uuid.New())
	require.NoError(t, err)
	require.False(t, found)
}
