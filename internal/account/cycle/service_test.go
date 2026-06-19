package cycle_test

import (
	"context"
	"errors"
	"math"
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
	period, app, module, metric, model string
}

type fakeStore struct {
	// rollup inputs
	raws        []cycle.RawAggregate
	prices      map[string]int64 // module/metric → price; absent = unpriced (0)
	modelPrices map[string]int64 // metric/model → per-model price (migration 018); checked before prices when a model is carried
	// settlement inputs
	incomes    []cycle.ModuleIncome
	visibility map[uuid.UUID]cycle.Visibility

	// captured writes
	periodID    uuid.UUID
	aggregates  map[aggKey]cycle.MetricAggregate
	settlements map[string]cycle.ModuleSettlement // period/module → settlement

	// charge-cycle inputs
	chargedTotal     int64       // PeriodChargedTotal return
	hasPM            bool        // HasUsableDefaultPM return
	stripeCustomer   string      // AccountStripeCustomer return
	unbilledAccounts []uuid.UUID // AccountsWithUnbilledUsage return
	usageEventAccts  []uuid.UUID // AccountsWithUsageEvents return

	// risk-graded collection inputs (PR #9)
	collection    cycle.AccountCollection // AccountCollection return
	unpaidInvoice bool                    // HasUnpaidInvoice return (delinquency signal)

	// captured collection writes
	updatedCollection *cycle.AccountCollection // last UpdateAccountCollection arg

	// captured charge writes
	insertedRuns map[string]uuid.UUID                 // (account/start/end) → run id (the idempotency gate state)
	runStatus    map[uuid.UUID]cycle.BillingRunStatus // run id → current status (models the DB row's terminal state)
	markedRuns   map[uuid.UUID]markedRun              // run id → terminal mark
	invoices     map[string]cycle.InvoiceMirror       // stripe_invoice_id → mirror

	// injected errors
	errOpen        error
	errRaw         error
	errPrice       error
	errUpsert      error
	errIncome      error
	errVis         error
	errSettle      error
	errInsertRun   error
	errTotal       error
	errPM          error
	errCustomer    error
	errInvoice     error
	errMarkRun     error
	errUnbilled    error
	errUsageEvents error
	errCollection  error // AccountCollection
	errUpdateColl  error // UpdateAccountCollection
	errUnpaid      error // HasUnpaidInvoice
}

// markedRun records a MarkBillingRun call so a test can assert the terminal
// status + invoice id + charged cents the cycle wrote.
type markedRun struct {
	status     cycle.BillingRunStatus
	invoiceID  string
	totalCents int64
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		prices:       map[string]int64{},
		modelPrices:  map[string]int64{},
		visibility:   map[uuid.UUID]cycle.Visibility{},
		periodID:     uuid.New(),
		aggregates:   map[aggKey]cycle.MetricAggregate{},
		settlements:  map[string]cycle.ModuleSettlement{},
		insertedRuns: map[string]uuid.UUID{},
		runStatus:    map[uuid.UUID]cycle.BillingRunStatus{},
		markedRuns:   map[uuid.UUID]markedRun{},
		invoices:     map[string]cycle.InvoiceMirror{},
		// Default collection state: arrears mode with a high credit limit + no
		// spend ceiling, so the existing charge tests (which don't set risk
		// fields) flow through the gate to the charge path unchanged. Risk tests
		// override these explicitly.
		collection: cycle.AccountCollection{
			Mode:              cycle.BillingModeArrears,
			CreditLimitMicros: math.MaxInt64, // effectively unlimited so legacy charge tests never tighten
			HasSpendCeiling:   false,
		},
	}
}

// runKey is the idempotency key the fake's InsertBillingRun dedupes on, mirroring
// the DB UNIQUE(account_id, period_start, period_end).
func runKey(accountID uuid.UUID, start, end time.Time) string {
	return accountID.String() + "/" + start.Format(time.RFC3339Nano) + "/" + end.Format(time.RFC3339Nano)
}

func priceKey(moduleID uuid.UUID, metric string) string { return moduleID.String() + "/" + metric }

// modelPriceKey mirrors the metric_model_prices PRIMARY KEY (metric, model).
func modelPriceKey(metric, model string) string { return metric + "/" + model }

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

func (f *fakeStore) MetricPriceMicros(_ context.Context, moduleID uuid.UUID, metric, model string) (int64, bool, error) {
	if f.errPrice != nil {
		return 0, false, f.errPrice
	}
	// Per-model price wins when the event carries a model (migration 018); a miss
	// falls back to the (module, metric) catalog price, mirroring the pgxStore.
	if model != "" {
		if p, ok := f.modelPrices[modelPriceKey(metric, model)]; ok {
			return p, true, nil
		}
	}
	p, ok := f.prices[priceKey(moduleID, metric)]
	return p, ok, nil
}

func (f *fakeStore) UpsertUsageAggregate(_ context.Context, periodID, _ uuid.UUID, agg cycle.MetricAggregate) error {
	if f.errUpsert != nil {
		return f.errUpsert
	}
	f.aggregates[aggKey{periodID.String(), agg.AppID.String(), agg.ModuleID.String(), agg.Metric, agg.Model}] = agg
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

func (f *fakeStore) InsertBillingRun(_ context.Context, accountID uuid.UUID, start, end time.Time) (uuid.UUID, bool, error) {
	if f.errInsertRun != nil {
		return uuid.Nil, false, f.errInsertRun
	}
	k := runKey(accountID, start, end)
	if id, exists := f.insertedRuns[k]; exists {
		// Conflict on an existing row. Mirrors the DB ON CONFLICT DO UPDATE …
		// WHERE status <> 'invoiced': an 'invoiced' row blocks (shouldCharge=
		// false); any non-terminal row (skipped_no_pm / failed / pending) is
		// RECLAIMED — same id, reset to pending, shouldCharge=true.
		if f.runStatus[id] == cycle.RunStatusInvoiced {
			return id, false, nil
		}
		f.runStatus[id] = "pending"
		return id, true, nil
	}
	id := uuid.New()
	f.insertedRuns[k] = id
	f.runStatus[id] = "pending"
	return id, true, nil
}

func (f *fakeStore) PeriodChargedTotal(_ context.Context, _ uuid.UUID, _, _ time.Time) (int64, error) {
	if f.errTotal != nil {
		return 0, f.errTotal
	}
	return f.chargedTotal, nil
}

func (f *fakeStore) HasUsableDefaultPM(_ context.Context, _ uuid.UUID) (bool, error) {
	if f.errPM != nil {
		return false, f.errPM
	}
	return f.hasPM, nil
}

func (f *fakeStore) AccountStripeCustomer(_ context.Context, _ uuid.UUID) (string, error) {
	if f.errCustomer != nil {
		return "", f.errCustomer
	}
	return f.stripeCustomer, nil
}

func (f *fakeStore) AccountCollection(_ context.Context, _ uuid.UUID) (cycle.AccountCollection, error) {
	if f.errCollection != nil {
		return cycle.AccountCollection{}, f.errCollection
	}
	return f.collection, nil
}

func (f *fakeStore) UpdateAccountCollection(_ context.Context, _ uuid.UUID, c cycle.AccountCollection) error {
	if f.errUpdateColl != nil {
		return f.errUpdateColl
	}
	f.collection = c // persist so a re-run reads the transitioned mode
	cp := c
	f.updatedCollection = &cp
	return nil
}

func (f *fakeStore) TightenAndMarkRun(_ context.Context, _ uuid.UUID, c cycle.AccountCollection, runID uuid.UUID, status cycle.BillingRunStatus) error {
	// Models the atomic tx: persist the mode transition AND mark the run skipped.
	// An injected error on EITHER underlying op fails the whole call (all-or-
	// nothing) so a test can assert the cycle surfaces a tighten-tx failure.
	if f.errUpdateColl != nil {
		return f.errUpdateColl
	}
	if f.errMarkRun != nil {
		return f.errMarkRun
	}
	f.collection = c
	cp := c
	f.updatedCollection = &cp
	f.markedRuns[runID] = markedRun{status: status, totalCents: 0}
	f.runStatus[runID] = status
	return nil
}

func (f *fakeStore) HasUnpaidInvoice(_ context.Context, _ uuid.UUID) (bool, error) {
	if f.errUnpaid != nil {
		return false, f.errUnpaid
	}
	return f.unpaidInvoice, nil
}

func (f *fakeStore) UpsertInvoice(_ context.Context, inv cycle.InvoiceMirror) error {
	if f.errInvoice != nil {
		return f.errInvoice
	}
	f.invoices[inv.StripeInvoiceID] = inv
	return nil
}

func (f *fakeStore) MarkBillingRun(_ context.Context, runID uuid.UUID, status cycle.BillingRunStatus, invoiceID string, totalCents int64) error {
	if f.errMarkRun != nil {
		return f.errMarkRun
	}
	f.markedRuns[runID] = markedRun{status: status, invoiceID: invoiceID, totalCents: totalCents}
	f.runStatus[runID] = status // persist terminal state so a re-run's reclaim gate sees it
	return nil
}

func (f *fakeStore) AccountsWithUsageEvents(_ context.Context, _, _ time.Time) ([]uuid.UUID, error) {
	if f.errUsageEvents != nil {
		return nil, f.errUsageEvents
	}
	return f.usageEventAccts, nil
}

func (f *fakeStore) AccountsWithUnbilledUsage(_ context.Context, _, _ time.Time) ([]uuid.UUID, error) {
	if f.errUnbilled != nil {
		return nil, f.errUnbilled
	}
	return f.unbilledAccounts, nil
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

// rawAggModel is rawAgg with the AI pricing-dimension model set (migration 018).
func rawAggModel(app, mod uuid.UUID, metric string, kind cycle.Kind, model, qty string) cycle.RawAggregate {
	return cycle.RawAggregate{AppID: app, ModuleID: mod, Metric: metric, Kind: kind, Model: model, BillableQuantity: qty}
}

// --- RollupPeriod: pricing + aggregation ----------------------------------

func TestRollupPeriod_CustomMetricNoMarkup(t *testing.T) {
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "orders.placed", usage.KindSum, "10")}
	store.prices[priceKey(mod, "orders.placed")] = 50_000 // $0.05/unit

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
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
	// As of PR #10a the platform-infra ingest (RecordInfraUsage) records these,
	// so they DO reach the rollup; the plane logic prices them at cost × 1.2.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "infra.compute.ms", usage.KindSum, "100")}
	store.prices[priceKey(mod, "infra.compute.ms")] = 1_000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)

	a := resp.Aggregates[0]
	require.Equal(t, 12, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 100_000, a.RawCostMicros) // 100 × 1_000
	require.EqualValues(t, 120_000, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_InfraEgressUnderSentinelPricesAt12Over10(t *testing.T) {
	// PR #10a foundation contract: an infra.egress.bytes event recorded by
	// RecordInfraUsage is stamped under the platform-infra SENTINEL module_id
	// (usage.PlatformInfraModuleID()); migration 017 seeds the matching
	// metric_definitions row under the SAME sentinel with the per-unit COGS, so
	// the rollup's price-lookup resolves a non-zero cost and the reserved-name
	// branch marks it up cost × 12/10. This proves an infra event prices at
	// 12/10 (NOT 10/10) end-to-end through the sentinel.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.egress.bytes", usage.KindSum, "1000")}
	store.prices[priceKey(sentinel, "infra.egress.bytes")] = 2 // seeded per-byte COGS (micros)

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)

	a := resp.Aggregates[0]
	require.Equal(t, sentinel, a.ModuleID)
	require.Equal(t, 12, a.MarkupNum) // reserved-name markup plane, NOT 10/10
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 2_000, a.RawCostMicros) // 1000 × 2
	require.EqualValues(t, 2_400, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_PlatformReservedPrefix(t *testing.T) {
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "platform.tokens", usage.KindSum, "5")}
	store.prices[priceKey(mod, "platform.tokens")] = 2_000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
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

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.EqualValues(t, 0, a.UnitPriceMicros)
	require.EqualValues(t, 0, a.RawCostMicros)
	require.EqualValues(t, 0, a.ChargedMicros)
}

func TestRollupPeriod_InfraMissingCatalogErrors(t *testing.T) {
	// A reserved infra metric with NO seeded price (migration 017 missing or
	// rolled back) MUST fail the cycle loudly — NOT silently price to 0 like an
	// unpriced custom metric. This guards the infra revenue-leak path: the
	// platform incurred the cloud COGS, so a zero charge is never acceptable.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.compute.ms", usage.KindSum, "100")}
	// deliberately register NO price for the infra metric

	_, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	requireCode(t, err, billing.CodeInternal)
}

func TestRollupPeriod_FractionalQuantityRoundHalfUp(t *testing.T) {
	// A time-weighted integral can be fractional (byte-hours). raw_cost =
	// round_half_up(quantity × unit_price). 2.5 × 3 = 7.5 → 8 (half-up).
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAgg(app, mod, "myapp.objects.byte_hours", usage.KindTimeWeighted, "2.5")}
	store.prices[priceKey(mod, "myapp.objects.byte_hours")] = 3

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
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

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
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

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
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

	_, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
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
	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
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
	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
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
	svc := cycle.NewService(store, nil)

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
	_, err := cycle.NewService(newFakeStore(), nil).RollupPeriod(context.Background(), uuid.Nil, periodStart, periodEnd)
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
			_, err := cycle.NewService(newFakeStore(), nil).RollupPeriod(context.Background(), uuid.New(), tc.start, tc.end)
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
			_, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
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

	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
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

	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
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

	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
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

	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
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

	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
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
	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
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

	resp, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
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
	svc := cycle.NewService(store, nil)

	_, err := svc.SettleDevelopers(context.Background(), acct, store.periodID)
	require.NoError(t, err)
	_, err = svc.SettleDevelopers(context.Background(), acct, store.periodID)
	require.NoError(t, err)
	require.Len(t, store.settlements, 1, "re-run upserts, never duplicates")
}

func TestSettleDevelopers_Validation(t *testing.T) {
	_, err := cycle.NewService(newFakeStore(), nil).SettleDevelopers(context.Background(), uuid.Nil, uuid.New())
	requireCode(t, err, billing.CodeInvalidInput)
	_, err = cycle.NewService(newFakeStore(), nil).SettleDevelopers(context.Background(), uuid.New(), uuid.Nil)
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
			_, err := cycle.NewService(store, nil).SettleDevelopers(context.Background(), uuid.New(), store.periodID)
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
	svc := cycle.NewService(store, nil)

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

// --- RollupPeriod: per-model AI token pricing (migration 018) --------------

// The roster model ids the producer (infra-metrics PR #2) stamps on AI events.
const (
	modelHaiku  = "anthropic.claude-haiku-4-5-20251001-v1:0"
	modelSonnet = "anthropic.claude-sonnet-4-6"
)

func TestRollupPeriod_AIInputTokensPerModelPrice(t *testing.T) {
	// An infra.ai.input.tokens event carrying a model is priced from the PER-MODEL
	// side-table (metric_model_prices), NOT the sentinel metric_definitions
	// fallback. Sonnet input = 3000 µ$/1k; the catalog fallback is the cheaper
	// Haiku rate (1000) — proving the per-model price is what resolves. Quantity
	// is in 1k-token units (design §3 rule 5). 2 (×1k) × 3000 × 12/10 = 7200.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAggModel(app, sentinel, "infra.ai.input.tokens", usage.KindSum, modelSonnet, "2")}
	store.modelPrices[modelPriceKey("infra.ai.input.tokens", modelSonnet)] = 3000
	store.prices[priceKey(sentinel, "infra.ai.input.tokens")] = 1000 // cheaper catalog fallback, must NOT win

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.EqualValues(t, 3000, a.UnitPriceMicros, "per-model price must win over the catalog fallback")
	require.Equal(t, 12, a.MarkupNum) // infra.* → 12/10 plane unchanged
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 6000, a.RawCostMicros) // 2 × 3000
	require.EqualValues(t, 7200, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_AIInputTokensDistinctPerModel(t *testing.T) {
	// Two models on the same metric resolve to DIFFERENT prices in one rollup —
	// the whole point of the side-table. Haiku in = 1000, Sonnet in = 3000.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{
		rawAggModel(app, sentinel, "infra.ai.input.tokens", usage.KindSum, modelHaiku, "1"),
		rawAggModel(app, sentinel, "infra.ai.input.tokens", usage.KindSum, modelSonnet, "1"),
	}
	store.modelPrices[modelPriceKey("infra.ai.input.tokens", modelHaiku)] = 1000
	store.modelPrices[modelPriceKey("infra.ai.input.tokens", modelSonnet)] = 3000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	byModel := map[string]cycle.MetricAggregate{}
	for _, a := range resp.Aggregates {
		byModel[a.Model] = a
	}
	require.EqualValues(t, 1000, byModel[modelHaiku].UnitPriceMicros)
	require.EqualValues(t, 3000, byModel[modelSonnet].UnitPriceMicros)
	require.EqualValues(t, 1200, byModel[modelHaiku].ChargedMicros)  // 1×1000×1.2
	require.EqualValues(t, 3600, byModel[modelSonnet].ChargedMicros) // 1×3000×1.2
}

func TestRollupPeriod_AITokensFallbackToDefinitionWhenNoModelPrice(t *testing.T) {
	// A model with NO per-model price row falls back to the catalog (sentinel
	// metric_definitions) fallback price — it does NOT zero-charge. Fallback =
	// 1000 µ$/1k. 3 × 1000 × 1.2 = 3600.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAggModel(app, sentinel, "infra.ai.input.tokens", usage.KindSum, "some-future-model", "3")}
	// no modelPrices row for "some-future-model"
	store.prices[priceKey(sentinel, "infra.ai.input.tokens")] = 1000 // catalog fallback

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.EqualValues(t, 1000, a.UnitPriceMicros, "missing per-model price falls back to the catalog row")
	require.EqualValues(t, 3600, a.ChargedMicros)
}

func TestRollupPeriod_InfraAINoModelUsesDefinition(t *testing.T) {
	// A model-less AI event (model == "") resolves straight from the catalog
	// fallback, never the per-model table. Even with a Sonnet per-model price
	// present, an empty-model row must use the catalog fallback (1000).
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAgg(app, sentinel, "infra.ai.input.tokens", usage.KindSum, "4")} // model ""
	store.modelPrices[modelPriceKey("infra.ai.input.tokens", modelSonnet)] = 3000
	store.prices[priceKey(sentinel, "infra.ai.input.tokens")] = 1000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.Equal(t, "", a.Model)
	require.EqualValues(t, 1000, a.UnitPriceMicros)
	require.EqualValues(t, 4800, a.ChargedMicros) // 4×1000×1.2
}

func TestRollupPeriod_AITokensMarkupIs12Over10(t *testing.T) {
	// AI token metrics are infra.* → they take the reserved 12/10 markup plane,
	// unchanged by the per-model price source.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAggModel(app, sentinel, "infra.ai.output.tokens", usage.KindSum, modelSonnet, "10")}
	store.modelPrices[modelPriceKey("infra.ai.output.tokens", modelSonnet)] = 15000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.Equal(t, 12, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 150_000, a.RawCostMicros) // 10 × 15000
	require.EqualValues(t, 180_000, a.ChargedMicros) // × 1.2
}

func TestRollupPeriod_CacheWriteVsCacheReadPriceDifference(t *testing.T) {
	// The cache-class split is the whole reason cache_write/cache_read are
	// separate metrics: write ≈ 1.25× input, read ≈ 0.1× input — pricing read as
	// input over-bills ~10×. Sonnet cache_write = 3750, cache_read = 300. They
	// resolve to distinct per-model prices in one rollup.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{
		rawAggModel(app, sentinel, "infra.ai.cache_write.tokens", usage.KindSum, modelSonnet, "2"),
		rawAggModel(app, sentinel, "infra.ai.cache_read.tokens", usage.KindSum, modelSonnet, "2"),
	}
	store.modelPrices[modelPriceKey("infra.ai.cache_write.tokens", modelSonnet)] = 3750
	store.modelPrices[modelPriceKey("infra.ai.cache_read.tokens", modelSonnet)] = 300

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	byMetric := map[string]cycle.MetricAggregate{}
	for _, a := range resp.Aggregates {
		byMetric[a.Metric] = a
	}
	require.EqualValues(t, 3750, byMetric["infra.ai.cache_write.tokens"].UnitPriceMicros)
	require.EqualValues(t, 300, byMetric["infra.ai.cache_read.tokens"].UnitPriceMicros)
	require.EqualValues(t, 9000, byMetric["infra.ai.cache_write.tokens"].ChargedMicros) // 2×3750×1.2
	require.EqualValues(t, 720, byMetric["infra.ai.cache_read.tokens"].ChargedMicros)   // 2×300×1.2
}

func TestRollupPeriod_AIRequestsZeroCharge(t *testing.T) {
	// infra.ai.requests is unpriced observability (price 0, no per-model rows):
	// it charges 0 regardless of the 12/10 plane. It still aggregates (the count
	// is retained for rate/abuse signal) but never bills.
	store := newFakeStore()
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	store.raws = []cycle.RawAggregate{rawAggModel(app, sentinel, "infra.ai.requests", usage.KindCount, modelSonnet, "5")}
	store.prices[priceKey(sentinel, "infra.ai.requests")] = 0 // seeded price 0 (observability)

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	a := resp.Aggregates[0]
	require.EqualValues(t, 0, a.UnitPriceMicros)
	require.EqualValues(t, 0, a.RawCostMicros)
	require.EqualValues(t, 0, a.ChargedMicros)
}
