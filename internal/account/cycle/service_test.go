package cycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// --- in-memory Store fake -------------------------------------------------

type aggKey struct {
	period, app, module, metric string
}

type fakeStore struct {
	// rollup inputs
	raws   []cycle.RawAggregate
	prices map[string]int64 // module/metric → price; absent = unpriced (0)
	// settlement inputs
	incomes    []cycle.ModuleIncome
	visibility map[uuid.UUID]cycle.Visibility

	// captured writes
	periodID    uuid.UUID
	aggregates  map[aggKey]cycle.MetricAggregate
	settlements map[string]cycle.ModuleSettlement // period/module → settlement

	// injected errors
	errOpen   error
	errRaw    error
	errPrice  error
	errUpsert error
	errIncome error
	errVis    error
	errSettle error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		prices:      map[string]int64{},
		visibility:  map[uuid.UUID]cycle.Visibility{},
		periodID:    uuid.New(),
		aggregates:  map[aggKey]cycle.MetricAggregate{},
		settlements: map[string]cycle.ModuleSettlement{},
	}
}

func priceKey(moduleID uuid.UUID, metric string) string { return moduleID.String() + "/" + metric }

func (f *fakeStore) OpenPeriodForAccount(_ context.Context, _ uuid.UUID, _, _ time.Time) (uuid.UUID, error) {
	if f.errOpen != nil {
		return uuid.Nil, f.errOpen
	}
	return f.periodID, nil
}

func (f *fakeStore) RawAggregates(_ context.Context, _ uuid.UUID, _, _ time.Time) ([]cycle.RawAggregate, error) {
	if f.errRaw != nil {
		return nil, f.errRaw
	}
	return f.raws, nil
}

func (f *fakeStore) MetricPriceMicros(_ context.Context, moduleID uuid.UUID, metric string) (int64, bool, error) {
	if f.errPrice != nil {
		return 0, false, f.errPrice
	}
	p, ok := f.prices[priceKey(moduleID, metric)]
	return p, ok, nil
}

func (f *fakeStore) UpsertUsageAggregate(_ context.Context, periodID, _ uuid.UUID, agg cycle.MetricAggregate) error {
	if f.errUpsert != nil {
		return f.errUpsert
	}
	f.aggregates[aggKey{periodID.String(), agg.AppID.String(), agg.ModuleID.String(), agg.Metric}] = agg
	return nil
}

func (f *fakeStore) ModuleIncome(_ context.Context, _ uuid.UUID) ([]cycle.ModuleIncome, error) {
	if f.errIncome != nil {
		return nil, f.errIncome
	}
	return f.incomes, nil
}

func (f *fakeStore) ModuleVisibility(_ context.Context, moduleID uuid.UUID) (cycle.Visibility, bool, error) {
	if f.errVis != nil {
		return "", false, f.errVis
	}
	v, ok := f.visibility[moduleID]
	return v, ok, nil
}

func (f *fakeStore) UpsertDeveloperSettlement(_ context.Context, periodID, _ uuid.UUID, s cycle.ModuleSettlement) error {
	if f.errSettle != nil {
		return f.errSettle
	}
	f.settlements[periodID.String()+"/"+s.ModuleID.String()] = s
	return nil
}

// --- helpers --------------------------------------------------------------

func requireCode(t *testing.T, err error, want billing.Code) {
	t.Helper()
	require.Error(t, err)
	var be *billing.Error
	require.True(t, errors.As(err, &be), "want *billing.Error, got %T", err)
	require.Equal(t, want, be.Code)
}

var (
	periodStart = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	periodEnd   = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
)

func rawAgg(app, mod uuid.UUID, metric string, kind cycle.Kind, qty string) cycle.RawAggregate {
	return cycle.RawAggregate{AppID: app, ModuleID: mod, Metric: metric, Kind: kind, BillableQuantity: qty}
}

// --- RollupPeriod: pricing + aggregation ----------------------------------

func TestRollupPeriod_CustomMetricNoMarkup(t *testing.T) {
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "orders.placed", usage.KindSum, "10")}
	store.prices[priceKey(mod, "orders.placed")] = 50_000 // $0.05/unit

	resp, err := cycle.NewService(store).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 1)

	a := resp.Aggregates[0]
	require.Equal(t, 10, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 500_000, a.RawCostMicros) // 10 × 50_000
	require.EqualValues(t, 500_000, a.ChargedMicros) // no markup → charged == raw
	require.EqualValues(t, 500_000, resp.TotalChargedMicros)
}

func TestRollupPeriod_ReservedMetricInfraMarkup(t *testing.T) {
	// A reserved infra.* / platform.* name takes the 12/10 (1.2×) markup plane.
	// These don't ingest until PR #10, but the plane logic is implemented.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "infra.compute.ms", usage.KindSum, "100")}
	store.prices[priceKey(mod, "infra.compute.ms")] = 1_000

	resp, err := cycle.NewService(store).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)

	a := resp.Aggregates[0]
	require.Equal(t, 12, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 100_000, a.RawCostMicros) // 100 × 1_000
	require.EqualValues(t, 120_000, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_PlatformReservedPrefix(t *testing.T) {
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "platform.tokens", usage.KindSum, "5")}
	store.prices[priceKey(mod, "platform.tokens")] = 2_000

	resp, err := cycle.NewService(store).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Equal(t, 12, resp.Aggregates[0].MarkupNum)
	require.EqualValues(t, 12_000, resp.Aggregates[0].ChargedMicros) // 5×2_000×1.2
}

func TestRollupPeriod_NullPriceZeroCharge(t *testing.T) {
	// A metered-but-unpriced metric (no catalog price) prices to 0.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "orders.placed", usage.KindCount, "42")}
	// no price registered → unpriced

	resp, err := cycle.NewService(store).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.EqualValues(t, 0, a.UnitPriceMicros)
	require.EqualValues(t, 0, a.RawCostMicros)
	require.EqualValues(t, 0, a.ChargedMicros)
}

func TestRollupPeriod_FractionalQuantityRoundHalfUp(t *testing.T) {
	// A time-weighted integral can be fractional (byte-hours). raw_cost =
	// round_half_up(quantity × unit_price). 2.5 × 3 = 7.5 → 8 (half-up).
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "myapp.objects.byte_hours", usage.KindTimeWeighted, "2.5")}
	store.prices[priceKey(mod, "myapp.objects.byte_hours")] = 3

	resp, err := cycle.NewService(store).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.EqualValues(t, 8, resp.Aggregates[0].RawCostMicros)
	require.EqualValues(t, 8, resp.Aggregates[0].ChargedMicros)
}

func TestRollupPeriod_HalfUpExactBoundary(t *testing.T) {
	// Exactly .5 rounds UP deterministically. 0.5 × 1 = 0.5 → 1.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "m", usage.KindTimeWeighted, "0.5")}
	store.prices[priceKey(mod, "m")] = 1

	resp, err := cycle.NewService(store).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.Aggregates[0].RawCostMicros)
}

func TestRollupPeriod_InfraMarkupSingleRound(t *testing.T) {
	// B1 regression: the 12/10 markup must round ONCE over the whole product,
	// not twice (round raw_cost, then round raw_cost×12/10). For qty=0.1,
	// price=13: single-pass charged = round_half_up(0.1×13×1.2) =
	// round_half_up(1.56) = 2. A two-step path gives round_half_up(1.3)=1 then
	// round_half_up(1.2)=1 — under-billing by 1 micro.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "infra.compute.ms", usage.KindTimeWeighted, "0.1")}
	store.prices[priceKey(mod, "infra.compute.ms")] = 13

	resp, err := cycle.NewService(store).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.EqualValues(t, 1, a.RawCostMicros) // round_half_up(0.1×13)=round(1.3)=1
	require.EqualValues(t, 2, a.ChargedMicros) // round_half_up(0.1×13×12/10)=round(1.56)=2
}

func TestRollupPeriod_OverflowRejected(t *testing.T) {
	// B2 regression: a quantity × price that exceeds int64 micros must error,
	// not silently wrap to a wrong (possibly negative) charge. 1e12 × 50_000_000
	// = 5e19 > int64 max (~9.22e18).
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "orders.placed", usage.KindSum, "1000000000000")}
	store.prices[priceKey(mod, "orders.placed")] = 50_000_000

	_, err := cycle.NewService(store).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	requireCode(t, err, billing.CodeInternal)
}

func TestRollupPeriod_KindsCarriedThrough(t *testing.T) {
	// Each aggregate snapshots the kind it rolled up under.
	store := newFakeStore()
	app := uuid.New()
	m1, m2, m3 := uuid.New(), uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{
		rawAgg(app, m1, "a", usage.KindCount, "3"),
		rawAgg(app, m2, "b", usage.KindPeak, "9"),
		rawAgg(app, m3, "c", usage.KindTimeWeighted, "4"),
	}
	resp, err := cycle.NewService(store).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	kinds := map[string]cycle.Kind{}
	for _, a := range resp.Aggregates {
		kinds[a.Metric] = a.Kind
	}
	require.Equal(t, usage.KindCount, kinds["a"])
	require.Equal(t, usage.KindPeak, kinds["b"])
	require.Equal(t, usage.KindTimeWeighted, kinds["c"])
}

func TestRollupPeriod_NoEventsEmpty(t *testing.T) {
	store := newFakeStore() // no raws (no-sample period → 0 aggregates)
	resp, err := cycle.NewService(store).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Empty(t, resp.Aggregates)
	require.EqualValues(t, 0, resp.TotalChargedMicros)
	require.Empty(t, store.aggregates)
}

// --- RollupPeriod: idempotency --------------------------------------------

func TestRollupPeriod_IdempotentReRun(t *testing.T) {
	// Re-running the same period upserts the IDENTICAL aggregate, never a
	// duplicate (the fake keys on (period, app, module, metric) like the DB
	// UNIQUE constraint).
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "orders.placed", usage.KindSum, "10")}
	store.prices[priceKey(mod, "orders.placed")] = 50_000
	acct := uuid.New()
	svc := cycle.NewService(store)

	first, err := svc.RollupPeriod(context.Background(), acct, periodStart, periodEnd)
	require.NoError(t, err)
	second, err := svc.RollupPeriod(context.Background(), acct, periodStart, periodEnd)
	require.NoError(t, err)

	require.Len(t, store.aggregates, 1, "re-run upserts, never duplicates")
	require.Equal(t, first.Aggregates[0].ChargedMicros, second.Aggregates[0].ChargedMicros)
	require.Equal(t, first.TotalChargedMicros, second.TotalChargedMicros)
}

// --- RollupPeriod: validation + error propagation -------------------------

func TestRollupPeriod_RejectsNilAccount(t *testing.T) {
	_, err := cycle.NewService(newFakeStore()).RollupPeriod(context.Background(), uuid.Nil, periodStart, periodEnd)
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestRollupPeriod_RejectsBadWindow(t *testing.T) {
	for _, tc := range []struct {
		name       string
		start, end time.Time
	}{
		{"zero start", time.Time{}, periodEnd},
		{"zero end", periodStart, time.Time{}},
		{"end before start", periodEnd, periodStart},
		{"equal", periodStart, periodStart},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := cycle.NewService(newFakeStore()).RollupPeriod(context.Background(), uuid.New(), tc.start, tc.end)
			requireCode(t, err, billing.CodeInvalidInput)
		})
	}
}

func TestRollupPeriod_PropagatesStoreErrors(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name  string
		setup func(*fakeStore)
	}{
		{"open", func(f *fakeStore) { f.errOpen = boom }},
		{"raw", func(f *fakeStore) { f.errRaw = boom }},
		{"price", func(f *fakeStore) {
			f.raws = []cycle.RawAggregate{rawAgg(uuid.New(), uuid.New(), "m", usage.KindSum, "1")}
			f.errPrice = boom
		}},
		{"upsert", func(f *fakeStore) {
			f.raws = []cycle.RawAggregate{rawAgg(uuid.New(), uuid.New(), "m", usage.KindSum, "1")}
			f.errUpsert = boom
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			tc.setup(store)
			_, err := cycle.NewService(store).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
			requireCode(t, err, billing.CodeInternal)
		})
	}
}

// --- SettleDevelopers: margin-share math ----------------------------------

func TestSettleDevelopers_PrivateThirtyPercent(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: 1_000_000}}
	store.visibility[mod] = usage.VisibilityPrivate

	resp, err := cycle.NewService(store).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	require.Len(t, resp.Settlements, 1)
	s := resp.Settlements[0]
	require.Equal(t, usage.VisibilityPrivate, s.MarginShareClass)
	require.EqualValues(t, 300_000, s.PlatformTakeMicros)  // 30% of 1_000_000
	require.EqualValues(t, 700_000, s.DeveloperOwedMicros) // remainder
	require.EqualValues(t, 0, s.InfraMicros)
}

func TestSettleDevelopers_PublishedFifteenPercent(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: 1_000_000}}
	store.visibility[mod] = usage.VisibilityPublished

	resp, err := cycle.NewService(store).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	s := resp.Settlements[0]
	require.Equal(t, usage.VisibilityPublished, s.MarginShareClass)
	require.EqualValues(t, 150_000, s.PlatformTakeMicros) // 15%
	require.EqualValues(t, 850_000, s.DeveloperOwedMicros)
}

func TestSettleDevelopers_UnknownVisibilityDefaultsPrivate(t *testing.T) {
	// No visibility row → default to private (30%, the higher take) so the
	// platform never under-collects on a lagging publish.
	store := newFakeStore()
	mod := uuid.New()
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: 1_000_000}}
	// no visibility registered

	resp, err := cycle.NewService(store).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	s := resp.Settlements[0]
	require.Equal(t, usage.VisibilityPrivate, s.MarginShareClass)
	require.EqualValues(t, 300_000, s.PlatformTakeMicros)
}

func TestSettleDevelopers_ZeroIncomeZeroOwed(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: 0}}
	store.visibility[mod] = usage.VisibilityPublished

	resp, err := cycle.NewService(store).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	s := resp.Settlements[0]
	require.EqualValues(t, 0, s.PlatformTakeMicros)
	require.EqualValues(t, 0, s.DeveloperOwedMicros)
}

func TestSettleDevelopers_RoundHalfUpTake(t *testing.T) {
	// 30% of 5 = 1.5 → take rounds half-up to 2; owed = 5 − 2 = 3.
	store := newFakeStore()
	mod := uuid.New()
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: 5}}
	store.visibility[mod] = usage.VisibilityPrivate

	resp, err := cycle.NewService(store).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	s := resp.Settlements[0]
	require.EqualValues(t, 2, s.PlatformTakeMicros)
	require.EqualValues(t, 3, s.DeveloperOwedMicros)
}

func TestSettleDevelopers_TakePlusOwedEqualsIncome(t *testing.T) {
	// Invariant: with infra=0, take + owed == income exactly (no money lost).
	store := newFakeStore()
	for _, income := range []int64{1, 7, 333_333, 1_000_001, 999_999_999} {
		mod := uuid.New()
		store.incomes = append(store.incomes, cycle.ModuleIncome{ModuleID: mod, IncomeMicros: income})
		store.visibility[mod] = usage.VisibilityPrivate
	}
	resp, err := cycle.NewService(store).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	for _, s := range resp.Settlements {
		require.Equal(t, s.IncomeMicros, s.PlatformTakeMicros+s.DeveloperOwedMicros,
			"take + owed must equal income for module %s", s.ModuleID)
	}
}

func TestSettleDevelopers_MultipleModules(t *testing.T) {
	store := newFakeStore()
	mPub, mPriv := uuid.New(), uuid.New()
	store.incomes = []cycle.ModuleIncome{
		{ModuleID: mPub, IncomeMicros: 1_000_000},
		{ModuleID: mPriv, IncomeMicros: 1_000_000},
	}
	store.visibility[mPub] = usage.VisibilityPublished
	store.visibility[mPriv] = usage.VisibilityPrivate

	resp, err := cycle.NewService(store).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
	require.NoError(t, err)
	require.Len(t, store.settlements, 2)
	byMod := map[uuid.UUID]cycle.ModuleSettlement{}
	for _, s := range resp.Settlements {
		byMod[s.ModuleID] = s
	}
	require.EqualValues(t, 150_000, byMod[mPub].PlatformTakeMicros)
	require.EqualValues(t, 300_000, byMod[mPriv].PlatformTakeMicros)
}

func TestSettleDevelopers_IdempotentReRun(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: 1_000_000}}
	store.visibility[mod] = usage.VisibilityPrivate
	acct := uuid.New()
	svc := cycle.NewService(store)

	_, err := svc.SettleDevelopers(context.Background(), acct, store.periodID)
	require.NoError(t, err)
	_, err = svc.SettleDevelopers(context.Background(), acct, store.periodID)
	require.NoError(t, err)
	require.Len(t, store.settlements, 1, "re-run upserts, never duplicates")
}

func TestSettleDevelopers_Validation(t *testing.T) {
	_, err := cycle.NewService(newFakeStore()).SettleDevelopers(context.Background(), uuid.Nil, uuid.New())
	requireCode(t, err, billing.CodeInvalidInput)
	_, err = cycle.NewService(newFakeStore()).SettleDevelopers(context.Background(), uuid.New(), uuid.Nil)
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSettleDevelopers_PropagatesStoreErrors(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name  string
		setup func(*fakeStore)
	}{
		{"income", func(f *fakeStore) { f.errIncome = boom }},
		{"visibility", func(f *fakeStore) {
			f.incomes = []cycle.ModuleIncome{{ModuleID: uuid.New(), IncomeMicros: 1}}
			f.errVis = boom
		}},
		{"settle", func(f *fakeStore) {
			f.incomes = []cycle.ModuleIncome{{ModuleID: uuid.New(), IncomeMicros: 1}}
			f.errSettle = boom
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			tc.setup(store)
			_, err := cycle.NewService(store).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
			requireCode(t, err, billing.CodeInternal)
		})
	}
}

// --- end-to-end: rollup feeds settlement ----------------------------------

func TestRollupThenSettle_IncomeFromAggregates(t *testing.T) {
	// The realistic flow: RollupPeriod writes aggregates, then SettleDevelopers
	// reads the per-module charged income from them. Here we wire the fake's
	// income to mirror what the rollup charged for the module.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "orders.placed", usage.KindSum, "20")}
	store.prices[priceKey(mod, "orders.placed")] = 50_000
	acct := uuid.New()
	svc := cycle.NewService(store)

	roll, err := svc.RollupPeriod(context.Background(), acct, periodStart, periodEnd)
	require.NoError(t, err)
	require.EqualValues(t, 1_000_000, roll.TotalChargedMicros) // 20 × 50_000

	// Feed the rolled income (as the DB ModuleIncome query would) and settle.
	store.incomes = []cycle.ModuleIncome{{ModuleID: mod, IncomeMicros: roll.TotalChargedMicros}}
	store.visibility[mod] = usage.VisibilityPublished
	set, err := svc.SettleDevelopers(context.Background(), acct, roll.PeriodID)
	require.NoError(t, err)
	require.EqualValues(t, 150_000, set.Settlements[0].PlatformTakeMicros)
	require.EqualValues(t, 850_000, set.Settlements[0].DeveloperOwedMicros)
}
