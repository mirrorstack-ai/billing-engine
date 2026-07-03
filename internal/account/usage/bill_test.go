package usage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// customLine builds one non-reserved (module-usage) bill line.
func customLine(mod uuid.UUID, metric, version string, charged int64) usage.AppMetricUsageRaw {
	return usage.AppMetricUsageRaw{
		ModuleID: mod, Metric: metric, Kind: usage.KindCount, ModuleVersion: version, ChargedMicros: charged,
	}
}

// infraLine builds one reserved (infrastructure) bill line. Its ChargedMicros is
// already the 1.2×-marked-up value the store returns (the markup lives in the
// AppBillLines SQL, exercised by the integration suite, not here).
func infraLine(metric string, charged int64) usage.AppMetricUsageRaw {
	return usage.AppMetricUsageRaw{
		ModuleID: uuid.Nil, Metric: metric, Kind: usage.KindSum, ChargedMicros: charged,
	}
}

// --- GetAppBill: base-fee tiering -----------------------------------------

func TestGetAppBill_BaseFeeBundlesUpToIncludedModules(t *testing.T) {
	// Exactly IncludedModules (5) distinct modules with usage → the base fee is
	// the flat BaseFeeMicros, no per-module surcharge.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	for i := 0; i < usage.IncludedModules; i++ {
		store.appBillRows = append(store.appBillRows, customLine(uuid.New(), "orders.placed", "", 100))
	}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.Equal(t, usage.BaseFeeMicros, resp.BaseFeeMicros, "5 modules is within the bundle → no surcharge")
}

func TestGetAppBill_BaseFeeSurchargesModulesBeyondIncluded(t *testing.T) {
	// 7 distinct modules → base = BaseFeeMicros + ExtraModuleMicros × (7 − 5).
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	for i := 0; i < 7; i++ {
		store.appBillRows = append(store.appBillRows, customLine(uuid.New(), "orders.placed", "", 0))
	}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	want := usage.BaseFeeMicros + usage.ExtraModuleMicros*2
	require.Equal(t, want, resp.BaseFeeMicros)
}

func TestGetAppBill_ModuleCountIsDistinctModulesNotLines(t *testing.T) {
	// Many lines across only 2 distinct modules (multiple metrics/versions per
	// module) → the installed-module proxy counts 2, still within the bundle.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	modA, modB := uuid.New(), uuid.New()
	store.appBillRows = []usage.AppMetricUsageRaw{
		customLine(modA, "orders.placed", "1.0.0", 100),
		customLine(modA, "orders.placed", "2.0.0", 100),
		customLine(modA, "orders.shipped", "", 100),
		customLine(modB, "views.count", "", 100),
	}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.Equal(t, usage.BaseFeeMicros, resp.BaseFeeMicros, "2 distinct modules → no surcharge despite 4 lines")
}

// --- GetAppBill: infra vs module split + PaaS credit ----------------------

func TestGetAppBill_SplitsInfraFromModuleUsage(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()
	store.appBillRows = []usage.AppMetricUsageRaw{
		customLine(mod, "orders.placed", "", 1000),
		infraLine("infra.egress.api.bytes", 12),
		infraLine("platform.tokens", 8),
	}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)

	// Module usage EXCLUDES the reserved infra./platform. lines.
	require.Len(t, resp.ModuleUsage, 1)
	require.Equal(t, "orders.placed", resp.ModuleUsage[0].Metric)
	require.EqualValues(t, 1000, resp.ModuleUsageTotalMicros)

	// Infra is its own total (12 + 8), the 1.2× already folded into each line's
	// ChargedMicros by the store — GetAppBill never re-marks-up.
	require.EqualValues(t, 20, resp.InfraTotalMicros)

	// PaaS credit is subscription-gated OFF (no subscription system in v1) → 0,
	// NOT a default 30% of infra.
	require.Zero(t, resp.PaasCreditMicros)

	// Only the one non-reserved module counts → flat base fee.
	require.Equal(t, usage.BaseFeeMicros, resp.BaseFeeMicros)

	// Total = base + module usage + infra − credit (credit 0).
	require.Equal(t, usage.BaseFeeMicros+1000+20, resp.TotalMicros)
}

func TestGetAppBill_PaasCreditSubscriptionGatedOff(t *testing.T) {
	// Infra present, but with no SaaS subscription the credit is NOT applied by
	// default (it is EARNED only via a subscription, which v1 has no system for).
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.appBillRows = []usage.AppMetricUsageRaw{infraLine("infra.request.count", 5)}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.EqualValues(t, 5, resp.InfraTotalMicros)
	require.Zero(t, resp.PaasCreditMicros, "credit gated off with no subscription")
	require.Empty(t, resp.ModuleUsage)
	require.Equal(t, usage.BaseFeeMicros+5, resp.TotalMicros)
}

func TestGetAppBill_NoInfraMeansNoCredit(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.appBillRows = []usage.AppMetricUsageRaw{customLine(uuid.New(), "orders.placed", "", 500)}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.Zero(t, resp.InfraTotalMicros)
	require.Zero(t, resp.PaasCreditMicros)
	require.Equal(t, usage.BaseFeeMicros+500, resp.TotalMicros)
}

// --- GetAppBill: per-version lines ----------------------------------------

func TestGetAppBill_KeepsPerModuleVersionLines(t *testing.T) {
	// When >1 version of a module was used in the period the bill carries a line
	// PER version (migration 023 data) so the UI can render per-version sub-lines.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()
	store.appBillRows = []usage.AppMetricUsageRaw{
		customLine(mod, "orders.placed", "1.0.0", 400),
		customLine(mod, "orders.placed", "2.0.0", 600),
	}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.Len(t, resp.ModuleUsage, 2)
	require.Equal(t, "1.0.0", resp.ModuleUsage[0].ModuleVersion)
	require.Equal(t, "2.0.0", resp.ModuleUsage[1].ModuleVersion)
	require.EqualValues(t, 1000, resp.ModuleUsageTotalMicros)
	require.Equal(t, usage.BaseFeeMicros, resp.BaseFeeMicros, "both versions are one installed module")
}

// --- GetAppBill: account / period resolution ------------------------------

func TestGetAppBill_NoAccountReturnsBaseFeeOnly(t *testing.T) {
	// No billing account yet → the bill is base-fee-only (a per-app platform fee
	// still applies), with zero usage / infra / credit and no module lines.
	app := uuid.New()
	resp, err := newService(newFakeStore()).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: uuid.New(), AppID: app})
	require.NoError(t, err)
	require.Equal(t, app, resp.AppID)
	require.Equal(t, usage.BaseFeeMicros, resp.BaseFeeMicros)
	require.Empty(t, resp.ModuleUsage)
	require.Zero(t, resp.ModuleUsageTotalMicros)
	require.Zero(t, resp.InfraTotalMicros)
	require.Zero(t, resp.PaasCreditMicros)
	require.Equal(t, usage.BaseFeeMicros, resp.TotalMicros)
	require.Empty(t, resp.PeriodID, "current live period has no billing_periods id")
}

func TestGetAppBill_DefaultsToCurrentLivePeriod(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.Empty(t, resp.PeriodID, "no period_id echoed for the current live period")
	require.False(t, resp.PeriodStart.IsZero())
	require.True(t, resp.PeriodEnd.After(resp.PeriodStart))
}

func TestGetAppBill_PastPeriodResolvesFrozenWindow(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	acct := uuid.New()
	store.accounts[owner] = acct
	pid := uuid.New()
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	store.periodWindows = map[uuid.UUID]periodWindow{pid: {start: start, end: end}}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New(), PeriodID: pid})
	require.NoError(t, err)
	require.Equal(t, pid.String(), resp.PeriodID)
	require.True(t, resp.PeriodStart.Equal(start))
	require.True(t, resp.PeriodEnd.Equal(end))
	require.Equal(t, acct, store.gotAppBillAccountID, "the payer account gates the bill query")
}

func TestGetAppBill_UnknownPeriodIsNotFound(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	// periodWindows empty → the requested id resolves to no row.

	_, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New(), PeriodID: uuid.New()})
	requireCode(t, err, billing.CodeNotFound)
}

func TestGetAppBill_PeriodWithoutAccountIsNotFound(t *testing.T) {
	// A period id is account-scoped; with no billing account the caller owns no
	// periods → NOT_FOUND (never a cross-account resolve).
	_, err := newService(newFakeStore()).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: uuid.New(), AppID: uuid.New(), PeriodID: uuid.New()})
	requireCode(t, err, billing.CodeNotFound)
}

func TestGetAppBill_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).GetAppBill(context.Background(), usage.GetAppBillRequest{AppID: uuid.New()})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetAppBill_RejectsBothOwners(t *testing.T) {
	_, err := newService(newFakeStore()).GetAppBill(context.Background(), usage.GetAppBillRequest{
		OwnerUserID: uuid.New(), OwnerOrgID: uuid.New(), AppID: uuid.New(),
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetAppBill_RequiresAppID(t *testing.T) {
	_, err := newService(newFakeStore()).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: uuid.New()})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetAppBill_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.errAppBill = errors.New("boom")
	_, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	requireCode(t, err, billing.CodeInternal)
}

// --- GetBillingPeriods -----------------------------------------------------

func TestGetBillingPeriods_PrependsSyntheticCurrentNewestFirst(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	apr := uuid.New()
	may := uuid.New()
	store.periodListRows = []usage.BillingPeriodRaw{
		{ID: may, PeriodStart: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), PeriodEnd: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), IsCurrent: false},
		{ID: apr, PeriodStart: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC), PeriodEnd: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), IsCurrent: false},
	}

	resp, err := newService(store).GetBillingPeriods(context.Background(), usage.GetBillingPeriodsRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Periods, 3, "synthetic current + 2 closed periods")
	require.True(t, resp.Periods[0].IsCurrent, "current period first")
	require.Empty(t, resp.Periods[0].PeriodID, "current live period has no id")
	require.Equal(t, may.String(), resp.Periods[1].PeriodID)
	require.Equal(t, apr.String(), resp.Periods[2].PeriodID)
}

func TestGetBillingPeriods_DoesNotDuplicateCurrentWhenRowFlagged(t *testing.T) {
	// If a billing_periods row already covers the current month (IsCurrent=true),
	// no synthetic current is prepended.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	cur := uuid.New()
	store.periodListRows = []usage.BillingPeriodRaw{
		{ID: cur, PeriodStart: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), PeriodEnd: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), IsCurrent: true},
	}

	resp, err := newService(store).GetBillingPeriods(context.Background(), usage.GetBillingPeriodsRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Periods, 1, "no synthetic prepend when a row is already current")
	require.True(t, resp.Periods[0].IsCurrent)
	require.Equal(t, cur.String(), resp.Periods[0].PeriodID)
}

func TestGetBillingPeriods_NoAccountReturnsOnlyCurrent(t *testing.T) {
	resp, err := newService(newFakeStore()).GetBillingPeriods(context.Background(), usage.GetBillingPeriodsRequest{OwnerUserID: uuid.New()})
	require.NoError(t, err)
	require.Len(t, resp.Periods, 1)
	require.True(t, resp.Periods[0].IsCurrent)
	require.Empty(t, resp.Periods[0].PeriodID)
}

func TestGetBillingPeriods_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).GetBillingPeriods(context.Background(), usage.GetBillingPeriodsRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetBillingPeriods_RejectsBothOwners(t *testing.T) {
	_, err := newService(newFakeStore()).GetBillingPeriods(context.Background(), usage.GetBillingPeriodsRequest{
		OwnerUserID: uuid.New(), OwnerOrgID: uuid.New(),
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetBillingPeriods_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.errPeriodList = errors.New("boom")
	_, err := newService(store).GetBillingPeriods(context.Background(), usage.GetBillingPeriodsRequest{OwnerUserID: owner})
	requireCode(t, err, billing.CodeInternal)
}
