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

// infraLine builds one reserved (infrastructure) row as it would appear on the
// AppBill (module-usage) read. These are now DROPPED by GetAppBill (infra is
// sourced from AppInfraBill instead), so a test uses them to prove the reserved
// rows are ignored on the AppBill scan and never double-counted.
func infraLine(metric string, charged int64) usage.AppMetricUsageRaw {
	return usage.AppMetricUsageRaw{
		ModuleID: uuid.Nil, Metric: metric, Kind: usage.KindSum, ChargedMicros: charged,
	}
}

// appInfraLine builds one catalog-anchored infra bill line as AppInfraBill
// returns it. ChargedMicros is already the 1.2×-marked-up value (the markup lives
// in the AppInfraBillLines SQL, exercised by the integration suite, not here);
// UnitPriceMicros is the raw catalog COGS. A zero charged/quantity line models a
// declared-but-unused infra metric (shown at $0 under "show all").
func appInfraLine(metric, group string, unitPrice int64, qty float64, charged int64) usage.AppInfraUsage {
	return usage.AppInfraUsage{
		Metric: metric, Kind: usage.KindSum, Unit: "unit", Group: group,
		UnitPriceMicros: unitPrice, BillableQuantity: qty, ChargedMicros: charged,
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
	// The AppBill (module-usage) read carries a custom line PLUS reserved infra
	// lines with DELIBERATELY large charges — those reserved rows must be dropped
	// (never summed) by GetAppBill, so if the double-count guard regressed this
	// test would blow past the expected 20.
	store.appBillRows = []usage.AppMetricUsageRaw{
		customLine(mod, "orders.placed", "", 1000),
		infraLine("infra.egress.api.bytes", 9999),
		infraLine("platform.tokens", 9999),
	}
	// Infra is sourced AUTHORITATIVELY from the catalog-anchored AppInfraBill: two
	// metrics that charged (12 + 8) plus one declared-but-unused metric at $0.
	store.appInfraBillRows = []usage.AppInfraUsage{
		appInfraLine("infra.egress.api.bytes", "network", 90000, 0.0001, 12),
		appInfraLine("platform.tokens", "ai", 1000, 8, 8),
		appInfraLine("infra.request.count", "requests", 1, 0, 0), // unused → $0
	}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)

	// Module usage EXCLUDES the reserved infra./platform. lines.
	require.Len(t, resp.ModuleUsage, 1)
	require.Equal(t, "orders.placed", resp.ModuleUsage[0].Metric)
	require.EqualValues(t, 1000, resp.ModuleUsageTotalMicros)

	// Infra breakdown is the catalog-anchored lines verbatim (incl. the $0 one),
	// and the scalar total is their sum (12 + 8 + 0) — NOT the 2×9999 reserved rows
	// on the AppBill read, which are dropped (no double-count).
	require.Len(t, resp.InfraLines, 3)
	require.EqualValues(t, 20, resp.InfraTotalMicros)
	var lineSum int64
	for _, l := range resp.InfraLines {
		lineSum += l.ChargedMicros
	}
	require.Equal(t, resp.InfraTotalMicros, lineSum, "infra_total == Σ infra_lines[].charged")

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
	store.appInfraBillRows = []usage.AppInfraUsage{appInfraLine("infra.request.count", "requests", 1, 5, 5)}

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

// --- GetAppBill: catalog-anchored infra breakdown -------------------------

func TestGetAppBill_ForwardsInfraLinesIncludingZero(t *testing.T) {
	// GetAppBill forwards the catalog-anchored infra breakdown verbatim — one line
	// per declared infra metric INCLUDING declared-but-unused ones at qty 0 · $0
	// (the "show all" symmetry with unused module meters) — and the scalar
	// InfraTotalMicros reconciles as the exact sum of the lines' charged.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.appInfraBillRows = []usage.AppInfraUsage{
		appInfraLine("infra.ai.input.tokens", "ai", 1000, 3, 3600), // 3 × 1000 × 1.2
		appInfraLine("infra.request.count", "requests", 1, 0, 0),   // declared, unused → $0
		appInfraLine("infra.storage.gib_hours", "storage", 32, 0, 0),
	}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.Len(t, resp.InfraLines, 3, "every declared infra metric appears, incl. the $0 ones")
	require.Equal(t, "infra.ai.input.tokens", resp.InfraLines[0].Metric)
	require.EqualValues(t, 1000, resp.InfraLines[0].UnitPriceMicros, "unit price is raw catalog COGS (pre-markup)")
	require.EqualValues(t, 3600, resp.InfraLines[0].ChargedMicros, "charged includes the ×1.2 markup, once")
	require.EqualValues(t, 3600, resp.InfraTotalMicros, "infra_total == Σ infra_lines[].charged")
}

func TestGetAppBill_InfraLinesRenderForLazyNoAccountApp(t *testing.T) {
	// AppInfraBill is queried UNCONDITIONALLY — even a lazy app with no billing
	// account still shows every declared infra metric (at $0 in reality; the fake
	// returns the catalog rows). The store receives uuid.Nil as the account gate.
	store := newFakeStore()
	store.appInfraBillRows = []usage.AppInfraUsage{
		appInfraLine("infra.request.count", "requests", 1, 0, 0),
		appInfraLine("infra.cron.count", "compute", 1, 0, 0),
	}
	app := uuid.New()

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: uuid.New(), AppID: app})
	require.NoError(t, err)
	require.Len(t, resp.InfraLines, 2, "declared infra metrics render even with no account")
	require.Zero(t, resp.InfraTotalMicros)
	require.Equal(t, uuid.Nil, store.gotAppInfraBillAccountID, "no account → uuid.Nil gates the infra query")
	require.Equal(t, app, store.gotAppInfraBillAppID)
	// Still base-fee-only otherwise.
	require.Equal(t, usage.BaseFeeMicros, resp.TotalMicros)
}

func TestGetAppBill_InternalOnInfraStoreError(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.errAppInfraBill = errors.New("boom")
	_, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	requireCode(t, err, billing.CodeInternal)
}

// --- GetAppBill: per-module infra split (decision 19) ---------------------

// moduleInfraLine builds one per-module infra line as AppModuleInfraBill returns it.
// modulePrice nil models a module with NO override (plain line at the default);
// a non-nil pointer models an ms.Price(n) override. ChargedMicros is the already
// ×1.2-marked-up line total (the markup lives in the AppModuleInfraBillLines SQL,
// exercised by the integration suite, not here).
func moduleInfraLine(mod uuid.UUID, metric, version string, defaultPrice int64, modulePrice *int64, qty float64, charged int64) usage.AppModuleInfraUsage {
	return usage.AppModuleInfraUsage{
		ModuleID: mod, ModuleVersion: version, Metric: metric, Label: metric,
		Kind: usage.KindSum, Unit: "ms", Group: "compute",
		BillableQuantity: qty, DefaultUnitPriceMicros: defaultPrice,
		ModuleUnitPriceMicros: modulePrice, ChargedMicros: charged,
	}
}

func i64(v int64) *int64 { return &v }

func TestGetAppBill_ModuleInfraLinesReconcileWithResidual(t *testing.T) {
	// The per-module split is a pure DISPLAY re-partition of the same infra total:
	// InfraTotalMicros == Σ module_infra_lines.charged + Σ infra_lines.charged. Both
	// the residual (sentinel-attributed) and the per-module (attributed) lines feed the
	// scalar so downstream base-fee / credit / total math is unchanged.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	// Residual: an unattributable sentinel AI line charged 8.
	store.appInfraBillRows = []usage.AppInfraUsage{
		appInfraLine("infra.ai.input.tokens", "ai", 1000, 0.008, 8),
	}
	// Per-module: two modules incurred compute. modA absorbed (override 0 → charged 0),
	// modB has no override → plain at the sentinel default (charged 24).
	modA, modB := uuid.New(), uuid.New()
	store.appModuleInfraBillRows = []usage.AppModuleInfraUsage{
		moduleInfraLine(modA, "infra.compute.walltime.ms", "", 20, i64(0), 100, 0),
		moduleInfraLine(modB, "infra.compute.walltime.ms", "", 20, nil, 100, 24),
	}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)

	require.Len(t, resp.ModuleInfraLines, 2, "module_infra_lines forwarded verbatim")
	require.Len(t, resp.InfraLines, 1, "residual retained")
	// Reconciliation: 8 (residual) + 0 (modA absorb) + 24 (modB plain) = 32.
	require.EqualValues(t, 32, resp.InfraTotalMicros)
	var sum int64
	for _, l := range resp.InfraLines {
		sum += l.ChargedMicros
	}
	for _, l := range resp.ModuleInfraLines {
		sum += l.ChargedMicros
	}
	require.Equal(t, resp.InfraTotalMicros, sum, "infra_total == Σ module_infra + Σ residual")

	// Dual-price passthrough: modA carries an explicit 0 override (full absorb, NOT nil);
	// modB carries nil (no override → UI plain mode). No revenue leak — modB with no
	// override still prices at the sentinel default (charged 24 > 0).
	require.NotNil(t, resp.ModuleInfraLines[0].ModuleUnitPriceMicros)
	require.EqualValues(t, 0, *resp.ModuleInfraLines[0].ModuleUnitPriceMicros)
	require.Nil(t, resp.ModuleInfraLines[1].ModuleUnitPriceMicros, "no override → nil (plain mode)")
	require.EqualValues(t, 20, resp.ModuleInfraLines[1].DefaultUnitPriceMicros)
	require.EqualValues(t, 24, resp.ModuleInfraLines[1].ChargedMicros)

	// The total folds the whole infra scalar in: base + 0 module usage + 32 − 0.
	require.Equal(t, usage.BaseFeeMicros+32, resp.TotalMicros)
}

func TestGetAppBill_ModuleInfraSkippedForLazyNoAccountApp(t *testing.T) {
	// AppModuleInfraBill is USAGE-anchored: with no billing account there is no
	// attributed usage, so it is skipped (unlike the catalog-anchored residual, which
	// still renders every declared metric at $0). ModuleInfraLines is a non-nil empty slice.
	store := newFakeStore()
	store.appInfraBillRows = []usage.AppInfraUsage{appInfraLine("infra.request.count", "requests", 1, 0, 0)}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: uuid.New(), AppID: uuid.New()})
	require.NoError(t, err)
	require.False(t, store.appModuleInfraBillCalled, "no account → the per-module infra read is skipped")
	require.NotNil(t, resp.ModuleInfraLines)
	require.Empty(t, resp.ModuleInfraLines)
}

func TestGetAppBill_InternalOnModuleInfraStoreError(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.errAppModuleInfraBill = errors.New("boom")
	_, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	requireCode(t, err, billing.CodeInternal)
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
