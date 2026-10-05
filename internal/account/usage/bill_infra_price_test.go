package usage_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// The sentinel infra catalog the fake serves: four active metrics, one per
// display group the page renders (compute, network, storage, requests).
var infraCatalogMetrics = []struct {
	metric, group string
	price         int64
}{
	{"infra.compute.walltime.ms", "compute", 20},
	{"infra.egress.bytes", "network", 100_000},
	{"infra.storage.gib_hours", "storage", 37},
	{"infra.requests.count", "requests", 5},
}

// catalogRows is the raw cross join ModuleInfraPriceCatalog returns for the
// given modules; overrides[module][metric] is the stored per-module price.
func catalogRows(overrides map[uuid.UUID]map[string]int64, mods ...uuid.UUID) []usage.ModuleInfraCatalogRow {
	var rows []usage.ModuleInfraCatalogRow
	for _, m := range mods {
		for _, c := range infraCatalogMetrics {
			r := usage.ModuleInfraCatalogRow{
				ModuleID: m, Metric: c.metric, Kind: usage.KindSum, Unit: "unit",
				Group: c.group, DefaultUnitPriceMicros: c.price,
			}
			if p, ok := overrides[m][c.metric]; ok {
				p := p
				r.ModuleUnitPriceMicros = &p
			}
			rows = append(rows, r)
		}
	}
	return rows
}

func billWithInstalled(t *testing.T, store *fakeStore, owner uuid.UUID, mods ...uuid.UUID) *usage.GetAppBillResponse {
	t.Helper()
	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{
		OwnerUserID: owner, AppID: uuid.New(), InstalledModuleIDs: mods,
	})
	require.NoError(t, err)
	return resp
}

func zeroRowsFor(resp *usage.GetAppBillResponse, mod uuid.UUID) map[string]usage.AppModuleInfraUsage {
	out := map[string]usage.AppModuleInfraUsage{}
	for _, l := range resp.ModuleInfraLines {
		if l.ModuleID == mod {
			out[l.Metric] = l
		}
	}
	return out
}

func TestGetAppBill_InfraLinesCarryTheCustomerUnitPrice(t *testing.T) {
	// 12/10 is the same markup usage.sql applies to the charge; the web must not
	// re-derive it. 37 → 44.4 is fractional ON PURPOSE: rounding it to 44 would
	// read $0.0321/GiB-month instead of $0.0324.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.appInfraBillRows = []usage.AppInfraUsage{
		appInfraLine("infra.storage.gib_hours", "storage", 37, 0, 0),
		appInfraLine("infra.ai.input.tokens", "ai", 1000, 10, 12_000),
		appInfraLine("infra.requests.count", "requests", 0, 0, 0),
	}
	resp := billWithInstalled(t, store, owner)

	require.InDelta(t, 44.4, resp.InfraLines[0].CustomerUnitPriceMicros, 1e-9)
	require.InDelta(t, 1200, resp.InfraLines[1].CustomerUnitPriceMicros, 1e-9)
	require.Zero(t, resp.InfraLines[2].CustomerUnitPriceMicros)
	require.EqualValues(t, 37, resp.InfraLines[0].UnitPriceMicros, "the raw field is unchanged")
}

func TestGetAppBill_ModuleInfraLinesCarryCustomerPricesAndPriceSource(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	plain, override, zero := uuid.New(), uuid.New(), uuid.New()
	store.appModuleInfraBillRows = []usage.AppModuleInfraUsage{
		moduleInfraLine(plain, "infra.compute.walltime.ms", "1.0.0", 20, nil, 100, 2),
		moduleInfraLine(override, "infra.compute.walltime.ms", "1.0.0", 20, i64(50), 100, 6),
		moduleInfraLine(zero, "infra.compute.walltime.ms", "1.0.0", 20, i64(0), 100, 0),
	}
	store.moduleInfraCatalog = catalogRows(map[uuid.UUID]map[string]int64{
		override: {"infra.compute.walltime.ms": 50},
		zero:     {"infra.compute.walltime.ms": 0}, // ONE explicit zero, not the whole catalog
	}, plain, override, zero)

	resp := billWithInstalled(t, store, owner)

	byMod := map[uuid.UUID]usage.AppModuleInfraUsage{}
	for _, l := range resp.ModuleInfraLines {
		if l.Metric == "infra.compute.walltime.ms" && l.ModuleVersion == "1.0.0" {
			byMod[l.ModuleID] = l
		}
	}
	require.InDelta(t, 24, byMod[plain].CustomerUnitPriceMicros, 1e-9, "default 20 × 12/10")
	require.Nil(t, byMod[plain].ModuleCustomerUnitPriceMicros, "no override stays nil on the wire")
	require.Equal(t, usage.PriceSourceDefault, byMod[plain].PriceSource)

	require.NotNil(t, byMod[override].ModuleCustomerUnitPriceMicros)
	require.InDelta(t, 60, *byMod[override].ModuleCustomerUnitPriceMicros, 1e-9, "override 50 × 12/10")
	require.Equal(t, usage.PriceSourceOverride, byMod[override].PriceSource)

	require.NotNil(t, byMod[zero].ModuleCustomerUnitPriceMicros)
	require.Zero(t, *byMod[zero].ModuleCustomerUnitPriceMicros)
	require.Equal(t, usage.PriceSourceOverride, byMod[zero].PriceSource,
		"a single explicit ms.Price(0) is an override, not an AbsorbInfra declaration")
}

func TestGetAppBill_InstalledModulesGetZeroQuantityInfraRows(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	plain, partial, absorbAll, absorbPlusEgress := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	store.moduleInfraCatalog = catalogRows(map[uuid.UUID]map[string]int64{
		partial: {"infra.storage.gib_hours": 37},
		absorbAll: {
			"infra.compute.walltime.ms": 0, "infra.egress.bytes": 0,
			"infra.storage.gib_hours": 0, "infra.requests.count": 0,
		},
		// AbsorbInfra + an explicit ms.Price(120000) for egress: the named metric
		// wins, everything else stays absorbed.
		absorbPlusEgress: {
			"infra.compute.walltime.ms": 0, "infra.egress.bytes": 120_000,
			"infra.storage.gib_hours": 0, "infra.requests.count": 0,
		},
	}, plain, partial, absorbAll, absorbPlusEgress)

	resp := billWithInstalled(t, store, owner, plain, partial, absorbAll, absorbPlusEgress)

	for _, m := range []uuid.UUID{plain, partial, absorbAll, absorbPlusEgress} {
		require.Len(t, zeroRowsFor(resp, m), len(infraCatalogMetrics), "one row per active infra metric")
	}
	for _, l := range resp.ModuleInfraLines {
		require.Zero(t, l.BillableQuantity)
		require.Zero(t, l.ChargedMicros)
		require.Empty(t, l.ModuleVersion, "a zero row belongs to no version")
		require.Equal(t, l.Metric, l.Label)
		require.NotEmpty(t, l.Unit)
		require.NotEmpty(t, l.Group)
	}

	for _, l := range zeroRowsFor(resp, plain) {
		require.Nil(t, l.ModuleUnitPriceMicros)
		require.Equal(t, usage.PriceSourceDefault, l.PriceSource)
	}
	p := zeroRowsFor(resp, partial)
	require.Equal(t, usage.PriceSourceOverride, p["infra.storage.gib_hours"].PriceSource)
	require.InDelta(t, 44.4, *p["infra.storage.gib_hours"].ModuleCustomerUnitPriceMicros, 1e-9)
	require.InDelta(t, 44.4, p["infra.storage.gib_hours"].CustomerUnitPriceMicros, 1e-9,
		"the default 37 and the override 37 price the same, only the source differs")
	require.Equal(t, usage.PriceSourceDefault, p["infra.compute.walltime.ms"].PriceSource)
	require.Nil(t, p["infra.compute.walltime.ms"].ModuleUnitPriceMicros)

	for _, l := range zeroRowsFor(resp, absorbAll) {
		require.NotNil(t, l.ModuleUnitPriceMicros)
		require.Zero(t, *l.ModuleUnitPriceMicros)
		require.Equal(t, usage.PriceSourceAbsorbed, l.PriceSource)
	}
	ae := zeroRowsFor(resp, absorbPlusEgress)
	require.Equal(t, usage.PriceSourceOverride, ae["infra.egress.bytes"].PriceSource)
	require.InDelta(t, 144_000, *ae["infra.egress.bytes"].ModuleCustomerUnitPriceMicros, 1e-9)
	require.Equal(t, usage.PriceSourceAbsorbed, ae["infra.compute.walltime.ms"].PriceSource)
}

func TestGetAppBill_ZeroInfraRowsDoNotDuplicateUsageRowsOrMoveTheTotals(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()
	store.appInfraBillRows = []usage.AppInfraUsage{appInfraLine("infra.ai.input.tokens", "ai", 1000, 0.008, 8)}
	store.appModuleInfraBillRows = []usage.AppModuleInfraUsage{
		moduleInfraLine(mod, "infra.compute.walltime.ms", "1.0.0", 20, nil, 100, 2),
		moduleInfraLine(mod, "infra.compute.walltime.ms", "1.1.0", 20, nil, 50, 1),
	}
	store.moduleInfraCatalog = catalogRows(nil, mod)

	without := billWithInstalled(t, store, owner)
	with := billWithInstalled(t, store, owner, mod)

	require.EqualValues(t, 8+2+1, with.InfraTotalMicros)
	require.Equal(t, without.InfraTotalMicros, with.InfraTotalMicros, "zero rows move no total")
	require.Equal(t, without.TotalMicros, with.TotalMicros)
	require.Equal(t, without.DeployUsageMicros, with.DeployUsageMicros)

	var compute, zero int
	for _, l := range with.ModuleInfraLines {
		if l.Metric == "infra.compute.walltime.ms" {
			compute++
			require.NotEmpty(t, l.ModuleVersion, "the metric with usage keeps its versioned rows and gets no zero row")
		} else {
			zero++
		}
	}
	require.Equal(t, 2, compute)
	require.Equal(t, len(infraCatalogMetrics)-1, zero)

	var sum int64
	for _, l := range with.InfraLines {
		sum += l.ChargedMicros
	}
	for _, l := range with.ModuleInfraLines {
		sum += l.ChargedMicros
	}
	require.Equal(t, with.InfraTotalMicros, sum, "infra_total == Σ module_infra + Σ residual still holds")
}

func TestGetAppBill_NoInstalledModulesAndNoUsageSkipsTheCatalogRead(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()

	resp := billWithInstalled(t, store, owner)

	require.False(t, store.moduleInfraCatalogCalled, "no module → nothing to price")
	require.NotNil(t, resp.ModuleInfraLines)
	require.Empty(t, resp.ModuleInfraLines)
}

func TestGetAppBill_ZeroInfraRowsWorkWithoutABillingAccount(t *testing.T) {
	// The catalog is account-independent, and a brand-new app is exactly when the
	// page needs every module's meters listed.
	store := newFakeStore()
	mod := uuid.New()
	store.moduleInfraCatalog = catalogRows(nil, mod)

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{
		OwnerUserID: uuid.New(), AppID: uuid.New(), InstalledModuleIDs: []uuid.UUID{mod},
	})
	require.NoError(t, err)
	require.Len(t, zeroRowsFor(resp, mod), len(infraCatalogMetrics))
	require.Zero(t, resp.InfraTotalMicros)
}

func TestGetAppBill_DevServedInfraLinesGetPriceSourceButNoZeroRows(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()
	store.moduleInfraDevServed = []usage.AppModuleInfraUsage{
		moduleInfraLine(mod, "infra.compute.walltime.ms", "1.0.0", 20, i64(50), 100, 6),
	}
	store.moduleInfraCatalog = catalogRows(map[uuid.UUID]map[string]int64{mod: {"infra.compute.walltime.ms": 50}}, mod)

	resp := billWithInstalled(t, store, owner, mod)

	require.Len(t, resp.ModuleInfraDevServedLines, 1, "the display partition never grows zero rows")
	require.Equal(t, usage.PriceSourceOverride, resp.ModuleInfraDevServedLines[0].PriceSource)
	require.InDelta(t, 60, *resp.ModuleInfraDevServedLines[0].ModuleCustomerUnitPriceMicros, 1e-9)
	require.Zero(t, resp.InfraTotalMicros, "dev-served stays a term of no total")
	require.Len(t, resp.ModuleInfraLines, len(infraCatalogMetrics), "the charged half lists the installed module's meters")
}

func TestGetAppBill_InstalledModuleIDsAreDedupedAndNilIgnored(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()
	store.moduleInfraCatalog = catalogRows(nil, mod)

	resp := billWithInstalled(t, store, owner, mod, uuid.Nil, mod)

	require.ElementsMatch(t, []uuid.UUID{mod}, store.gotModuleInfraCatalogIDs)
	require.Len(t, resp.ModuleInfraLines, len(infraCatalogMetrics))
}

func TestGetAppBill_TooManyInstalledModuleIDsIsInvalidInput(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	ids := make([]uuid.UUID, usage.MaxInstalledModuleIDs+1)
	for i := range ids {
		ids[i] = uuid.New()
	}
	_, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{
		OwnerUserID: owner, AppID: uuid.New(), InstalledModuleIDs: ids,
	})
	require.Error(t, err)
}

func TestAppBillWire_NewInfraFieldsAreAdditive(t *testing.T) {
	// The proxy re-encodes a decoded struct, so a renamed key silently vanishes
	// downstream. Pin the exact keys, and that a nil override stays absent.
	line := usage.AppModuleInfraUsage{
		ModuleID: uuid.New(), Metric: "infra.compute.walltime.ms",
		DefaultUnitPriceMicros: 20, CustomerUnitPriceMicros: 24, PriceSource: usage.PriceSourceDefault,
	}
	b, err := json.Marshal(line)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	require.EqualValues(t, 24, m["customer_unit_price_micros"])
	require.Equal(t, "default", m["price_source"])
	require.NotContains(t, m, "module_customer_unit_price_micros")
	require.Contains(t, m, "default_unit_price_micros", "the existing keys are untouched")

	b, err = json.Marshal(usage.AppInfraUsage{Metric: "infra.storage.gib_hours", UnitPriceMicros: 37, CustomerUnitPriceMicros: 44.4})
	require.NoError(t, err)
	m = map[string]any{}
	require.NoError(t, json.Unmarshal(b, &m))
	require.InDelta(t, 44.4, m["customer_unit_price_micros"], 1e-9)
	require.EqualValues(t, 37, m["unit_price_micros"])

	var req usage.GetAppBillRequest
	require.NoError(t, json.Unmarshal([]byte(`{"app_id":"`+uuid.NewString()+`"}`), &req))
	require.Empty(t, req.InstalledModuleIDs, "an old caller that omits installed_module_ids still decodes")
}
