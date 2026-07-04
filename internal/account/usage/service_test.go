package usage_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

func nan() float64 { return math.NaN() }
func inf() float64 { return math.Inf(1) }

// --- in-memory Store fake -------------------------------------------------

type fakeStore struct {
	defs                   map[string]usage.MetricDefinition // key: module/metric
	accounts               map[uuid.UUID]uuid.UUID           // owner userID → accountID
	events                 map[string]usage.UsageEvent       // event_id → event (idempotency)
	anchorDays             map[uuid.UUID]int                 // accountID → billing-period anchor day (0/absent → 1)
	periodRows             []usage.MetricUsageRaw
	historyRows            []usage.PeriodMetricUsageRaw
	versionRows            []usage.VersionUsageRaw
	appRows                []usage.AppMetricUsageRaw
	appBillRows            []usage.AppMetricUsageRaw
	appInfraBillRows       []usage.AppInfraUsage
	appModuleInfraBillRows []usage.AppModuleInfraUsage
	periodListRows         []usage.BillingPeriodRaw
	periodWindows          map[uuid.UUID]periodWindow // billing_periods id → window
	visibility             map[uuid.UUID]usage.Visibility
	invoiceRows            []usage.InvoiceMirrorRaw // unordered; ListInvoices applies the SQL contract

	// captured VersionBreakdown call args, so a test can assert the resolved
	// module filter reached the store unchanged.
	gotVersionModuleID uuid.UUID

	// captured AppUsage call args, so a test can assert account_id (payer) and
	// app_id reached the store unchanged.
	gotAppUsageAccountID uuid.UUID
	gotAppUsageAppID     uuid.UUID

	// captured AppBill call args (the full bill read gate).
	gotAppBillAccountID uuid.UUID
	gotAppBillAppID     uuid.UUID

	// captured AppInfraBill call args (the catalog-anchored infra breakdown).
	gotAppInfraBillAccountID uuid.UUID
	gotAppInfraBillAppID     uuid.UUID

	// captured AppModuleInfraBill call args (the per-module dual-price infra breakdown).
	gotAppModuleInfraBillAccountID uuid.UUID
	gotAppModuleInfraBillAppID     uuid.UUID
	appModuleInfraBillCalled       bool

	errLookup             error
	errAccount            error
	errInsert             error
	errPeriod             error
	errVisibility         error
	errUpsertDef          error
	errUpsertOverride     error
	errHistory            error
	errVersion            error
	errAppUsage           error
	errAppBill            error
	errAppInfraBill       error
	errAppModuleInfraBill error
	errPeriodList         error
	errPeriodWindow       error
	errAnchor             error
	errListInvoices       error

	// captured ListInvoices call args, so a test can assert the clamped
	// page+1 limit and the decoded cursor reached the store unchanged.
	gotInvoiceLimit  int32
	gotInvoiceCursor *usage.InvoiceCursor

	// captured window a read-path RPC resolved from the account's anchor, so a
	// test can assert the anchored [start, end) reached the store unchanged.
	gotPeriodStart time.Time
	gotPeriodEnd   time.Time
}

// periodWindow is a fake billing_periods window for BillingPeriodWindow lookups.
type periodWindow struct {
	start, end time.Time
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		defs:       map[string]usage.MetricDefinition{},
		accounts:   map[uuid.UUID]uuid.UUID{},
		events:     map[string]usage.UsageEvent{},
		anchorDays: map[uuid.UUID]int{},
		visibility: map[uuid.UUID]usage.Visibility{},
	}
}

// AccountAnchorDay returns the configured anchor day for an account, defaulting
// to 1 (the UTC calendar month) so tests that don't set one keep the pre-anchor
// window behavior.
func (f *fakeStore) AccountAnchorDay(_ context.Context, accountID uuid.UUID) (int, error) {
	if f.errAnchor != nil {
		return 0, f.errAnchor
	}
	if d, ok := f.anchorDays[accountID]; ok && d != 0 {
		return d, nil
	}
	return 1, nil
}

func defKey(moduleID uuid.UUID, metric string) string { return moduleID.String() + "/" + metric }

func (f *fakeStore) LookupMetricDefinition(_ context.Context, moduleID uuid.UUID, metric string) (usage.MetricDefinition, bool, error) {
	if f.errLookup != nil {
		return usage.MetricDefinition{}, false, f.errLookup
	}
	d, ok := f.defs[defKey(moduleID, metric)]
	return d, ok, nil
}

func (f *fakeStore) UpsertMetricDefinitions(_ context.Context, defs []usage.MetricDeclaration) error {
	if f.errUpsertDef != nil {
		return f.errUpsertDef // all-or-nothing: nothing is written on error
	}
	for _, def := range defs {
		f.defs[defKey(def.ModuleID, def.Metric)] = usage.MetricDefinition{
			Kind:            def.Kind,
			Unit:            def.Unit,
			UnitPriceMicros: def.UnitPriceMicros,
			Priced:          def.Priced,
			Active:          def.Active,
		}
	}
	return nil
}

// UpsertInfraPriceOverrides mirrors the real store: all-or-nothing, and each
// override inherits kind + unit from the SENTINEL catalog row (defKey(sentinel,
// metric)) — a missing sentinel row errors (the real store's INSERT ... SELECT
// affects 0 rows → error), never a silent write. The written row keys the REAL
// moduleID with the override price, so a test can assert f.defs[defKey(module,
// metric)] carries the override price + the sentinel's kind/unit.
func (f *fakeStore) UpsertInfraPriceOverrides(_ context.Context, moduleID uuid.UUID, overrides []usage.InfraPriceOverride) error {
	if f.errUpsertOverride != nil {
		return f.errUpsertOverride // all-or-nothing: nothing is written on error
	}
	sentinel := usage.PlatformInfraModuleID()
	// Validate the whole batch first so a mid-batch miss writes nothing.
	for _, o := range overrides {
		if _, ok := f.defs[defKey(sentinel, o.Metric)]; !ok {
			return errors.New("no sentinel catalog row for infra metric " + o.Metric)
		}
	}
	for _, o := range overrides {
		base := f.defs[defKey(sentinel, o.Metric)]
		f.defs[defKey(moduleID, o.Metric)] = usage.MetricDefinition{
			Kind:            base.Kind, // inherited from the sentinel row
			Unit:            base.Unit, // inherited from the sentinel row
			UnitPriceMicros: o.UnitPriceMicros,
			Priced:          true,
			Active:          true,
		}
	}
	return nil
}

func (f *fakeStore) InsertUsageEvent(_ context.Context, ev usage.UsageEvent) (bool, error) {
	if f.errInsert != nil {
		return false, f.errInsert
	}
	if _, exists := f.events[ev.EventID]; exists {
		return false, nil // ON CONFLICT DO NOTHING
	}
	f.events[ev.EventID] = ev
	return true, nil
}

func (f *fakeStore) AccountByOwner(_ context.Context, owner usage.Owner) (uuid.UUID, bool, error) {
	if f.errAccount != nil {
		return uuid.Nil, false, f.errAccount
	}
	if owner.OrgID != uuid.Nil {
		return uuid.Nil, false, nil // org path not yet provisioned
	}
	id, ok := f.accounts[owner.UserID]
	return id, ok, nil
}

func (f *fakeStore) CurrentPeriodUsage(_ context.Context, _ uuid.UUID, start, end time.Time) ([]usage.MetricUsageRaw, error) {
	f.gotPeriodStart, f.gotPeriodEnd = start, end
	if f.errPeriod != nil {
		return nil, f.errPeriod
	}
	return f.periodRows, nil
}

func (f *fakeStore) UpsertModuleVisibility(_ context.Context, moduleID uuid.UUID, vis usage.Visibility) error {
	if f.errVisibility != nil {
		return f.errVisibility
	}
	f.visibility[moduleID] = vis
	return nil
}

func (f *fakeStore) UsageHistory(_ context.Context, _ uuid.UUID, _, _ time.Time) ([]usage.PeriodMetricUsageRaw, error) {
	if f.errHistory != nil {
		return nil, f.errHistory
	}
	return f.historyRows, nil
}

func (f *fakeStore) VersionBreakdown(_ context.Context, _ uuid.UUID, _ time.Time, moduleID uuid.UUID) ([]usage.VersionUsageRaw, error) {
	f.gotVersionModuleID = moduleID
	if f.errVersion != nil {
		return nil, f.errVersion
	}
	return f.versionRows, nil
}

func (f *fakeStore) AppUsage(_ context.Context, accountID, appID uuid.UUID, _, _ time.Time) ([]usage.AppMetricUsageRaw, error) {
	f.gotAppUsageAccountID = accountID
	f.gotAppUsageAppID = appID
	if f.errAppUsage != nil {
		return nil, f.errAppUsage
	}
	return f.appRows, nil
}

func (f *fakeStore) AppBill(_ context.Context, accountID, appID uuid.UUID, _, _ time.Time) ([]usage.AppMetricUsageRaw, error) {
	f.gotAppBillAccountID = accountID
	f.gotAppBillAppID = appID
	if f.errAppBill != nil {
		return nil, f.errAppBill
	}
	return f.appBillRows, nil
}

func (f *fakeStore) AppInfraBill(_ context.Context, accountID, appID uuid.UUID, _, _ time.Time) ([]usage.AppInfraUsage, error) {
	f.gotAppInfraBillAccountID = accountID
	f.gotAppInfraBillAppID = appID
	if f.errAppInfraBill != nil {
		return nil, f.errAppInfraBill
	}
	return f.appInfraBillRows, nil
}

func (f *fakeStore) AppModuleInfraBill(_ context.Context, accountID, appID uuid.UUID, _, _ time.Time) ([]usage.AppModuleInfraUsage, error) {
	f.appModuleInfraBillCalled = true
	f.gotAppModuleInfraBillAccountID = accountID
	f.gotAppModuleInfraBillAppID = appID
	if f.errAppModuleInfraBill != nil {
		return nil, f.errAppModuleInfraBill
	}
	return f.appModuleInfraBillRows, nil
}

func (f *fakeStore) ListBillingPeriods(_ context.Context, _ uuid.UUID, _ time.Time) ([]usage.BillingPeriodRaw, error) {
	if f.errPeriodList != nil {
		return nil, f.errPeriodList
	}
	return f.periodListRows, nil
}

func (f *fakeStore) BillingPeriodWindow(_ context.Context, _, periodID uuid.UUID) (time.Time, time.Time, bool, error) {
	if f.errPeriodWindow != nil {
		return time.Time{}, time.Time{}, false, f.errPeriodWindow
	}
	w, ok := f.periodWindows[periodID]
	return w.start, w.end, ok, nil
}

// ListInvoices re-implements the ListInvoicesForAccount SQL contract in
// memory — drop drafts, keyset-filter strictly past the cursor, order
// (created_at, id) DESC, LIMIT — so service tests can walk real multi-page
// flows. The authoritative SQL is exercised by the integration tests.
func (f *fakeStore) ListInvoices(_ context.Context, _ uuid.UUID, limit int32, cursor *usage.InvoiceCursor) ([]usage.InvoiceMirrorRaw, error) {
	if f.errListInvoices != nil {
		return nil, f.errListInvoices
	}
	f.gotInvoiceLimit = limit
	f.gotInvoiceCursor = cursor

	rows := make([]usage.InvoiceMirrorRaw, 0, len(f.invoiceRows))
	for _, r := range f.invoiceRows {
		if r.Status == "draft" {
			continue
		}
		if cursor != nil {
			// Keep only rows strictly BEFORE the cursor tuple in DESC order,
			// i.e. (created_at, id) < (cursor.CreatedAt, cursor.ID). Postgres
			// compares uuids bytewise, matched here via bytes.Compare.
			if r.CreatedAt.After(cursor.CreatedAt) {
				continue
			}
			if r.CreatedAt.Equal(cursor.CreatedAt) && bytes.Compare(r.ID[:], cursor.ID[:]) >= 0 {
				continue
			}
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].CreatedAt.After(rows[j].CreatedAt)
		}
		return bytes.Compare(rows[i].ID[:], rows[j].ID[:]) > 0
	})
	if int32(len(rows)) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

// --- helpers --------------------------------------------------------------

func newService(store usage.Store) *usage.Service { return usage.NewService(store) }

func validRecord() usage.RecordUsageRequest {
	return usage.RecordUsageRequest{
		EventID:     "evt-1",
		AppID:       uuid.New(),
		ModuleID:    uuid.New(),
		OwnerUserID: uuid.New(),
		Metric:      "orders.placed",
		Value:       3,
		RecordedAt:  time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
	}
}

func requireCode(t *testing.T, err error, want billing.Code) {
	t.Helper()
	require.Error(t, err)
	var be *billing.Error
	require.True(t, errors.As(err, &be), "want *billing.Error, got %T", err)
	require.Equal(t, want, be.Code)
}

// --- RecordUsage ----------------------------------------------------------

// declare registers a metric in the fake catalog so RecordUsage accepts it
// (declaration-first: an undeclared metric is rejected).
func declare(store *fakeStore, req usage.RecordUsageRequest, kind usage.Kind) {
	store.defs[defKey(req.ModuleID, req.Metric)] = usage.MetricDefinition{
		Kind: kind, Active: true,
	}
}

func TestRecordUsage_FreshInsert(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	store.accounts[req.OwnerUserID] = uuid.New()

	resp, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Recorded)
	require.Len(t, store.events, 1)
}

func TestRecordUsage_IdempotentRetry(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	svc := newService(store)

	first, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, first.Recorded)

	// Same event_id → deduped, still success.
	second, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.False(t, second.Recorded)
	require.Len(t, store.events, 1)
}

func TestRecordUsage_SnapshotsDeclaredKindFromCatalog(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	req.Metric = "myapp.objects.bytes"
	store.defs[defKey(req.ModuleID, req.Metric)] = usage.MetricDefinition{
		Kind: usage.KindTimeWeighted, Active: true, UnitPriceMicros: 5, Priced: true,
	}

	_, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, usage.KindTimeWeighted, store.events[req.EventID].Kind)
}

func TestRecordUsage_RejectsUndeclaredMetric(t *testing.T) {
	// Declaration-first (design §1): a metric with no catalog row is
	// REJECTED, not recorded with a fallback kind.
	store := newFakeStore()
	req := validRecord() // no catalog row for this metric

	_, err := newService(store).RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.events, "undeclared metric must not be recorded")
}

func TestRecordUsage_RejectsRetiredMetric(t *testing.T) {
	// active=false means the metric is retired and no longer accepts events
	// (migration 006). RecordUsage rejects it rather than recording a fact
	// against a retired declaration.
	store := newFakeStore()
	req := validRecord()
	req.Metric = "myapp.objects.bytes"
	store.defs[defKey(req.ModuleID, req.Metric)] = usage.MetricDefinition{
		Kind: usage.KindTimeWeighted, Active: false, UnitPriceMicros: 5, Priced: true,
	}

	_, err := newService(store).RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.events, "retired metric must not be recorded")
}

func TestRecordUsage_LazyAccountWhenNoBillingAccount(t *testing.T) {
	store := newFakeStore()
	req := validRecord() // owner has no account row
	declare(store, req, usage.KindCount)

	_, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, store.events[req.EventID].AccountID, "lazy event records NULL account")
}

func TestRecordUsage_RejectsReservedPrefixes(t *testing.T) {
	for _, metric := range []string{"platform.tokens", "infra.egress.bytes", "infra.compute.ms"} {
		store := newFakeStore()
		req := validRecord()
		req.Metric = metric
		_, err := newService(store).RecordUsage(context.Background(), req)
		requireCode(t, err, billing.CodeInvalidInput)
		require.Empty(t, store.events, "reserved metric must not be recorded: %s", metric)
	}
}

func TestRecordUsage_RejectsNegativeAndNonFinite(t *testing.T) {
	for _, v := range []float64{-1, -0.0001} {
		req := validRecord()
		req.Value = v
		_, err := newService(newFakeStore()).RecordUsage(context.Background(), req)
		requireCode(t, err, billing.CodeInvalidInput)
	}
	// NaN / +Inf
	for _, v := range []float64{nan(), inf()} {
		req := validRecord()
		req.Value = v
		_, err := newService(newFakeStore()).RecordUsage(context.Background(), req)
		requireCode(t, err, billing.CodeInvalidInput)
	}
}

func TestRecordUsage_ValidatesRequiredFields(t *testing.T) {
	base := validRecord()
	cases := map[string]func(*usage.RecordUsageRequest){
		"no event_id": func(r *usage.RecordUsageRequest) { r.EventID = "" },
		"no app_id":   func(r *usage.RecordUsageRequest) { r.AppID = uuid.Nil },
		"no module":   func(r *usage.RecordUsageRequest) { r.ModuleID = uuid.Nil },
		"no metric":   func(r *usage.RecordUsageRequest) { r.Metric = "" },
		"both owners": func(r *usage.RecordUsageRequest) { r.OwnerOrgID = uuid.New() },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			req := base
			mutate(&req)
			_, err := newService(newFakeStore()).RecordUsage(context.Background(), req)
			requireCode(t, err, billing.CodeInvalidInput)
		})
	}
}

func TestRecordUsage_DefaultsRecordedAtToNow(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	req.RecordedAt = time.Time{}

	_, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.False(t, store.events[req.EventID].RecordedAt.IsZero())
}

func TestRecordUsage_CarriesModuleVersion(t *testing.T) {
	// The optional ModuleVersion field (migration 023, purely reporting) is
	// carried onto the usage_events.module_version column.
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	req.ModuleVersion = "3.2.1"

	_, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "3.2.1", store.events[req.EventID].ModuleVersion)
}

func TestRecordUsage_ModuleVersionEmptyWhenNotCarried(t *testing.T) {
	store := newFakeStore()
	req := validRecord() // no ModuleVersion set
	declare(store, req, usage.KindCount)

	_, err := newService(store).RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "", store.events[req.EventID].ModuleVersion)
}

func TestRecordUsage_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	store.errInsert = errors.New("boom")
	_, err := newService(store).RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInternal)
}

func TestRecordUsage_InternalOnLookupError(t *testing.T) {
	// A store failure resolving the catalog (LookupMetricDefinition) is an
	// INTERNAL error, distinct from the INVALID_INPUT "metric not declared"
	// no-row path. Exercises the billing.Internal branch at service.go:88.
	store := newFakeStore()
	req := validRecord()
	store.errLookup = errors.New("boom")
	_, err := newService(store).RecordUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInternal)
	require.Empty(t, store.events, "no event recorded when the catalog lookup fails")
}

// fakeBudgetEvaluator satisfies usage.BudgetEvaluator. err lets a test force
// a budget-eval failure to prove it does NOT fail the usage ingest; called
// records whether the hook ran.
type fakeBudgetEvaluator struct {
	err     error
	called  bool
	gotApp  uuid.UUID
	gotFrom time.Time
	gotTo   time.Time
}

func (f *fakeBudgetEvaluator) EvaluateAppBudget(_ context.Context, appID uuid.UUID, from, to time.Time) ([]int, error) {
	f.called = true
	f.gotApp = appID
	f.gotFrom = from
	f.gotTo = to
	return nil, f.err
}

func TestRecordUsage_BudgetEvalErrorDoesNotFailIngest(t *testing.T) {
	// Best-effort hook (design §10): a budget-evaluation error must NOT fail
	// the usage ingest — the event is already recorded.
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	eval := &fakeBudgetEvaluator{err: errors.New("budget boom")}

	svc := usage.NewService(store).WithBudgetEvaluator(eval)
	resp, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err, "budget error must not surface on the ingest path")
	require.True(t, resp.Recorded)
	require.True(t, eval.called, "the hook fires on a fresh insert")
	require.Equal(t, req.AppID, eval.gotApp)
	require.Len(t, store.events, 1)
}

func TestRecordUsage_BudgetEvalSkippedOnDedupedRetry(t *testing.T) {
	// A deduped retry (recorded=false) was already evaluated for its event_id;
	// the hook must be skipped so the same spend isn't re-walked.
	store := newFakeStore()
	req := validRecord()
	declare(store, req, usage.KindCount)
	eval := &fakeBudgetEvaluator{}
	svc := usage.NewService(store).WithBudgetEvaluator(eval)

	_, err := svc.RecordUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, eval.called)

	eval.called = false
	_, err = svc.RecordUsage(context.Background(), req) // same event_id → deduped
	require.NoError(t, err)
	require.False(t, eval.called, "hook is skipped on a deduped retry")
}

// --- GetUsageSummary ------------------------------------------------------

func TestGetUsageSummary_ChargesDeclaredPriceNoMarkup(t *testing.T) {
	// Declaration-first (design §1 / §4 Axis 1): a custom metric is charged
	// at quantity × the developer's declared price with NO blanket 1.2×, so
	// the customer charge equals the raw (quantity × unit_price) cost.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.periodRows = []usage.MetricUsageRaw{
		{Metric: "orders.placed", Kind: usage.KindCount, Quantity: 10, UnitPriceMicros: 100, RawCostMicros: 1000},
	}

	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 1)
	require.Equal(t, int64(1000), resp.Metrics[0].RawCostMicros)
	// No markup: charged == raw.
	require.Equal(t, int64(1000), resp.Metrics[0].ChargedMicros)
	require.Equal(t, int64(100), resp.Metrics[0].UnitPriceMicros)
}

func TestGetUsageSummary_PropagatesDisplayGroup(t *testing.T) {
	// §11 billing-display compaction: the catalog's display_group classification
	// (resolved at the store from metric_definitions.display_group) must travel
	// verbatim through GetUsageSummary so api-platform can proxy it and the
	// frontend can roll metrics up into ~7 group rows. billing-engine is the
	// AUTHORITATIVE classifier; the service never re-derives the group.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.periodRows = []usage.MetricUsageRaw{
		{Metric: "infra.ai.input.tokens", Kind: usage.KindSum, Quantity: 5, UnitPriceMicros: 1000, RawCostMicros: 5000, Group: "ai"},
		{Metric: "infra.egress.bytes", Kind: usage.KindSum, Quantity: 2, UnitPriceMicros: 1, RawCostMicros: 2, Group: "network"},
	}

	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 2)
	require.Equal(t, "ai", resp.Metrics[0].Group)
	require.Equal(t, "network", resp.Metrics[1].Group)
}

func TestGetUsageSummary_DefaultsGroupToOther(t *testing.T) {
	// A custom (Plane-2) metric, or any infra metric not yet mapped, carries
	// display_group 'other' — the store COALESCEs a missing/ungrouped catalog
	// row to "other" (mirroring the column's NOT NULL DEFAULT 'other'). The
	// service passes that through unchanged so the frontend always has a valid
	// group to bucket into.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.periodRows = []usage.MetricUsageRaw{
		{Metric: "orders.placed", Kind: usage.KindCount, Quantity: 10, UnitPriceMicros: 100, RawCostMicros: 1000, Group: "other"},
	}

	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 1)
	require.Equal(t, "other", resp.Metrics[0].Group)
}

func TestGetUsageSummary_PropagatesModuleIDAndVisibility(t *testing.T) {
	// A consumer previously had to hardcode a 30% platform-take assumption
	// because it couldn't see the real module_visibility value; GetUsageSummary
	// now carries both the emitting module_id and its visibility per metric.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()
	store.periodRows = []usage.MetricUsageRaw{
		{ModuleID: mod, Metric: "orders.placed", Kind: usage.KindCount, Quantity: 10, UnitPriceMicros: 100, RawCostMicros: 1000, Visibility: usage.VisibilityPublished},
	}

	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 1)
	require.Equal(t, mod, resp.Metrics[0].ModuleID)
	require.Equal(t, usage.VisibilityPublished, resp.Metrics[0].Visibility)
}

func TestGetUsageSummary_DefaultsVisibilityToPrivate(t *testing.T) {
	// A module with no visibility row yet defaults to 'private' (the higher
	// platform-take rate), matching the settlement default (design §7-B: never
	// under-collect on a lagging publish).
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.periodRows = []usage.MetricUsageRaw{
		{Metric: "orders.placed", Kind: usage.KindCount, Quantity: 10, UnitPriceMicros: 100, RawCostMicros: 1000, Visibility: usage.VisibilityPrivate},
	}

	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 1)
	require.Equal(t, usage.VisibilityPrivate, resp.Metrics[0].Visibility)
}

func TestGetUsageSummary_NoAccountReturnsEmpty(t *testing.T) {
	resp, err := newService(newFakeStore()).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: uuid.New()})
	require.NoError(t, err)
	require.Empty(t, resp.Metrics)
}

func TestGetUsageSummary_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
}

// TestGetUsageSummary_WindowsOnAccountAnchorDay proves the live-summary window is
// anchored to the account's card-binding day (ADR 0005), not the 1st, and that
// the exact anchored [start, end) reaches the store unchanged.
func TestGetUsageSummary_WindowsOnAccountAnchorDay(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	acct := uuid.New()
	store.accounts[owner] = acct
	store.anchorDays[acct] = 17 // bound a card on the 17th

	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	// Both boundaries fall on the 17th (17 ≤ 28, so no month ever clamps it).
	require.Equal(t, 17, resp.PeriodStart.Day(), "period starts on the anchor day, not the 1st")
	require.Equal(t, 17, resp.PeriodEnd.Day(), "period ends on the next anchor boundary")
	require.Equal(t, time.UTC, resp.PeriodStart.Location())
	// The resolved window is the one handed to the store (threaded unchanged).
	require.True(t, resp.PeriodStart.Equal(store.gotPeriodStart), "start threaded to store")
	require.True(t, resp.PeriodEnd.Equal(store.gotPeriodEnd), "end threaded to store")
}

// TestGetUsageSummary_DefaultsToCalendarMonthWhenUnactivated proves an account
// with no card-binding anchor (fake default) windows on the 1st — the pre-025
// calendar month — so un-activated accounts keep the historical behavior.
func TestGetUsageSummary_DefaultsToCalendarMonthWhenUnactivated(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New() // no anchorDays entry → default day 1
	resp, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Equal(t, 1, resp.PeriodStart.Day(), "un-activated account windows on the calendar month")
}

// TestGetUsageSummary_AnchorLookupErrorSurfaces proves an anchor-day lookup error
// fails the read loud (Internal) rather than silently mis-windowing.
func TestGetUsageSummary_AnchorLookupErrorSurfaces(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.errAnchor = errors.New("anchor db down")
	_, err := newService(store).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: owner})
	requireCode(t, err, billing.CodeInternal)
}

// --- GetUsageHistory -------------------------------------------------------

func TestGetUsageHistory_BucketsRowsIntoOrderedPeriods(t *testing.T) {
	// Multi-month data returns correctly ordered/bucketed: rows for two
	// different periods (already ordered period_start ASC, metric ASC by the
	// store contract) must split into two PeriodUsage entries, oldest first,
	// each carrying only its own period's metrics.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	jan := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	feb := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	mar := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	store.historyRows = []usage.PeriodMetricUsageRaw{
		{PeriodStart: jan, PeriodEnd: feb, Metric: "orders.placed", Kind: usage.KindCount, Quantity: 10, RawCostMicros: 1000, ChargedMicros: 1000},
		{PeriodStart: feb, PeriodEnd: mar, Metric: "orders.placed", Kind: usage.KindCount, Quantity: 20, RawCostMicros: 2000, ChargedMicros: 2000},
	}

	resp, err := newService(store).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{OwnerUserID: owner, Months: 6})
	require.NoError(t, err)
	require.Len(t, resp.Periods, 2)
	require.True(t, resp.Periods[0].PeriodStart.Equal(jan), "oldest period first")
	require.True(t, resp.Periods[1].PeriodStart.Equal(feb))
	require.Len(t, resp.Periods[0].Metrics, 1)
	require.EqualValues(t, 1000, resp.Periods[0].Metrics[0].ChargedMicros)
	require.EqualValues(t, 2000, resp.Periods[1].Metrics[0].ChargedMicros)
}

func TestGetUsageHistory_MultipleMetricsWithinOnePeriod(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	jan := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	feb := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	store.historyRows = []usage.PeriodMetricUsageRaw{
		{PeriodStart: jan, PeriodEnd: feb, Metric: "orders.placed", Kind: usage.KindCount, Quantity: 10, ChargedMicros: 1000},
		{PeriodStart: jan, PeriodEnd: feb, Metric: "storage.bytes", Kind: usage.KindTimeWeighted, Quantity: 5, ChargedMicros: 500},
	}

	resp, err := newService(store).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{OwnerUserID: owner, Months: 6})
	require.NoError(t, err)
	require.Len(t, resp.Periods, 1, "both rows share one period_start")
	require.Len(t, resp.Periods[0].Metrics, 2)
}

func TestGetUsageHistory_MissingMonthsDoNotError(t *testing.T) {
	// A month with no rolled-up usage (rollup hasn't run, or zero usage)
	// simply contributes no row — a gap in the returned Periods, never an
	// error.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	// historyRows stays empty: no usage_aggregates rows exist for the window.

	resp, err := newService(store).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{OwnerUserID: owner, Months: 6})
	require.NoError(t, err)
	require.Empty(t, resp.Periods)
}

func TestGetUsageHistory_NoAccountReturnsEmpty(t *testing.T) {
	resp, err := newService(newFakeStore()).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{OwnerUserID: uuid.New(), Months: 6})
	require.NoError(t, err)
	require.Empty(t, resp.Periods)
}

func TestGetUsageHistory_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{Months: 6})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetUsageHistory_RejectsNonPositiveMonths(t *testing.T) {
	for _, months := range []int{0, -1} {
		_, err := newService(newFakeStore()).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{OwnerUserID: uuid.New(), Months: months})
		requireCode(t, err, billing.CodeInvalidInput)
	}
}

func TestGetUsageHistory_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.errHistory = errors.New("boom")
	_, err := newService(store).GetUsageHistory(context.Background(), usage.GetUsageHistoryRequest{OwnerUserID: owner, Months: 6})
	requireCode(t, err, billing.CodeInternal)
}

// --- GetVersionBreakdown ---------------------------------------------------

func TestGetVersionBreakdown_GroupsByVersion(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.versionRows = []usage.VersionUsageRaw{
		{ModuleVersion: "1.0.0", BillableQuantity: 10, RawCostMicros: 1000, ChargedMicros: 1000},
		{ModuleVersion: "2.0.0", BillableQuantity: 5, RawCostMicros: 500, ChargedMicros: 500},
	}

	resp, err := newService(store).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Versions, 2)
	require.Equal(t, "1.0.0", resp.Versions[0].ModuleVersion)
	require.EqualValues(t, 1000, resp.Versions[0].ChargedMicros)
	require.Equal(t, "2.0.0", resp.Versions[1].ModuleVersion)
	require.EqualValues(t, 500, resp.Versions[1].ChargedMicros)
}

func TestGetVersionBreakdown_EmptyVersionRollsUpWithoutCrashing(t *testing.T) {
	// An event with an empty/missing version rolls up under '' (migration
	// 023's COALESCE(module_version, '')) rather than erroring.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.versionRows = []usage.VersionUsageRaw{
		{ModuleVersion: "", BillableQuantity: 10, RawCostMicros: 1000, ChargedMicros: 1000},
	}

	resp, err := newService(store).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Versions, 1)
	require.Equal(t, "", resp.Versions[0].ModuleVersion)
}

func TestGetVersionBreakdown_PassesThroughOptionalModuleFilter(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()

	_, err := newService(store).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{OwnerUserID: owner, ModuleID: mod})
	require.NoError(t, err)
	require.Equal(t, mod, store.gotVersionModuleID)
}

func TestGetVersionBreakdown_NoModuleFilterMeansAllModules(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()

	_, err := newService(store).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, store.gotVersionModuleID, "omitted module_id reaches the store as the zero UUID (no filter)")
}

func TestGetVersionBreakdown_NoAccountReturnsEmpty(t *testing.T) {
	resp, err := newService(newFakeStore()).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{OwnerUserID: uuid.New()})
	require.NoError(t, err)
	require.Empty(t, resp.Versions)
}

func TestGetVersionBreakdown_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetVersionBreakdown_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.errVersion = errors.New("boom")
	_, err := newService(store).GetVersionBreakdown(context.Background(), usage.GetVersionBreakdownRequest{OwnerUserID: owner})
	requireCode(t, err, billing.CodeInternal)
}

// --- GetAppUsageSummary ----------------------------------------------------

func TestGetAppUsageSummary_ReturnsPerModuleVersionLines(t *testing.T) {
	// The app bill carries one line per (module, metric, model, module_version)
	// so the UI can render per-version sub-lines (data exists — migration 023).
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()
	store.appRows = []usage.AppMetricUsageRaw{
		{ModuleID: mod, Metric: "orders.placed", Kind: usage.KindCount, ModuleVersion: "1.0.0", BillableQuantity: 4, UnitPriceMicros: 100, ChargedMicros: 400},
		{ModuleID: mod, Metric: "orders.placed", Kind: usage.KindCount, ModuleVersion: "2.0.0", BillableQuantity: 6, UnitPriceMicros: 100, ChargedMicros: 600},
	}

	resp, err := newService(store).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 2)
	require.Equal(t, mod, resp.Metrics[0].ModuleID)
	require.Equal(t, "1.0.0", resp.Metrics[0].ModuleVersion)
	require.EqualValues(t, 400, resp.Metrics[0].ChargedMicros)
	require.Equal(t, "2.0.0", resp.Metrics[1].ModuleVersion)
	require.EqualValues(t, 600, resp.Metrics[1].ChargedMicros)
}

func TestGetAppUsageSummary_ChargesDeclaredPriceNoMarkup(t *testing.T) {
	// The app owner pays the module's declared unit_price per unit with NO
	// customer markup by visibility — charged == unit_price × quantity, and the
	// response carries no visibility/markup fields at all.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.appRows = []usage.AppMetricUsageRaw{
		{Metric: "orders.placed", Kind: usage.KindCount, BillableQuantity: 10, UnitPriceMicros: 100, ChargedMicros: 1000},
	}

	resp, err := newService(store).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 1)
	require.EqualValues(t, 100, resp.Metrics[0].UnitPriceMicros)
	require.EqualValues(t, 1000, resp.Metrics[0].ChargedMicros)
}

func TestGetAppUsageSummary_GatesOnPayerAccountAndApp(t *testing.T) {
	// account_id (resolved from the owner principal) gates the payer; app_id
	// filters to the one app. Both must reach the store unchanged.
	store := newFakeStore()
	owner := uuid.New()
	acct := uuid.New()
	store.accounts[owner] = acct
	app := uuid.New()

	_, err := newService(store).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: owner, AppID: app})
	require.NoError(t, err)
	require.Equal(t, acct, store.gotAppUsageAccountID, "the payer account gates the query")
	require.Equal(t, app, store.gotAppUsageAppID, "the app_id filters to the one app")
}

func TestGetAppUsageSummary_EchoesAppIDAndWindow(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	app := uuid.New()

	resp, err := newService(store).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: owner, AppID: app})
	require.NoError(t, err)
	require.Equal(t, app, resp.AppID)
	require.False(t, resp.PeriodStart.IsZero())
	require.True(t, resp.PeriodEnd.After(resp.PeriodStart))
}

func TestGetAppUsageSummary_NoAccountReturnsEmpty(t *testing.T) {
	// No billing account yet → empty Metrics slice (not nil) + nil error, and
	// the requested app is still echoed.
	app := uuid.New()
	resp, err := newService(newFakeStore()).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: uuid.New(), AppID: app})
	require.NoError(t, err)
	require.Empty(t, resp.Metrics)
	require.Equal(t, app, resp.AppID)
}

func TestGetAppUsageSummary_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{AppID: uuid.New()})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetAppUsageSummary_RejectsBothOwners(t *testing.T) {
	_, err := newService(newFakeStore()).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{
		OwnerUserID: uuid.New(), OwnerOrgID: uuid.New(), AppID: uuid.New(),
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetAppUsageSummary_RequiresAppID(t *testing.T) {
	_, err := newService(newFakeStore()).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: uuid.New()})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetAppUsageSummary_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	store.errAppUsage = errors.New("boom")
	_, err := newService(store).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{OwnerUserID: owner, AppID: uuid.New()})
	requireCode(t, err, billing.CodeInternal)
}

// --- SetModuleVisibility --------------------------------------------------

func TestSetModuleVisibility_Upserts(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	_, err := newService(store).SetModuleVisibility(context.Background(), usage.SetModuleVisibilityRequest{
		ModuleID: mod, Visibility: usage.VisibilityPublished,
	})
	require.NoError(t, err)
	require.Equal(t, usage.VisibilityPublished, store.visibility[mod])
}

func TestSetModuleVisibility_RejectsBadVisibility(t *testing.T) {
	_, err := newService(newFakeStore()).SetModuleVisibility(context.Background(), usage.SetModuleVisibilityRequest{
		ModuleID: uuid.New(), Visibility: usage.Visibility("nonsense"),
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSetModuleVisibility_RequiresModuleID(t *testing.T) {
	_, err := newService(newFakeStore()).SetModuleVisibility(context.Background(), usage.SetModuleVisibilityRequest{
		Visibility: usage.VisibilityPrivate,
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

// --- SetMetricDefinitions -------------------------------------------------

func TestSetMetricDefinitions_SyncsCatalog(t *testing.T) {
	store := newFakeStore()
	mod := uuid.New()
	resp, err := newService(store).SetMetricDefinitions(context.Background(), usage.SetMetricDefinitionsRequest{
		ModuleID: mod,
		Metrics: []usage.MetricDef{
			{Metric: "orders.placed", Kind: usage.KindCount, Unit: "order", UnitPriceMicros: 50_000, Priced: true, Active: true},
			{Metric: "myapp.objects.bytes", Kind: usage.KindTimeWeighted, Unit: "byte", Active: true}, // unpriced
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, resp.Synced)

	got := store.defs[defKey(mod, "orders.placed")]
	require.Equal(t, usage.KindCount, got.Kind)
	require.True(t, got.Priced)
	require.Equal(t, int64(50_000), got.UnitPriceMicros)

	unpriced := store.defs[defKey(mod, "myapp.objects.bytes")]
	require.False(t, unpriced.Priced, "unpriced metric stays unpriced")
}

func TestSetMetricDefinitions_RejectsReservedPrefix(t *testing.T) {
	store := newFakeStore()
	_, err := newService(store).SetMetricDefinitions(context.Background(), usage.SetMetricDefinitionsRequest{
		ModuleID: uuid.New(),
		Metrics:  []usage.MetricDef{{Metric: "infra.egress.bytes", Kind: usage.KindCount, Active: true}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.defs, "reserved metric must not be synced")
}

func TestSetMetricDefinitions_RejectsBadKind(t *testing.T) {
	_, err := newService(newFakeStore()).SetMetricDefinitions(context.Background(), usage.SetMetricDefinitionsRequest{
		ModuleID: uuid.New(),
		Metrics:  []usage.MetricDef{{Metric: "orders.placed", Kind: usage.Kind("nonsense"), Active: true}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSetMetricDefinitions_RequiresModuleID(t *testing.T) {
	_, err := newService(newFakeStore()).SetMetricDefinitions(context.Background(), usage.SetMetricDefinitionsRequest{
		Metrics: []usage.MetricDef{{Metric: "orders.placed", Kind: usage.KindCount, Active: true}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSetMetricDefinitions_InternalOnStoreError(t *testing.T) {
	// A store failure on the batch upsert surfaces as INTERNAL (the catalog
	// sync is all-or-nothing; the transaction rolls back). Exercises the
	// billing.Internal branch around the UpsertMetricDefinitions call.
	store := newFakeStore()
	store.errUpsertDef = errors.New("boom")
	_, err := newService(store).SetMetricDefinitions(context.Background(), usage.SetMetricDefinitionsRequest{
		ModuleID: uuid.New(),
		Metrics:  []usage.MetricDef{{Metric: "orders.placed", Kind: usage.KindCount, Active: true}},
	})
	requireCode(t, err, billing.CodeInternal)
	require.Empty(t, store.defs, "all-or-nothing: nothing synced when the store errors")
}

// --- SetInfraPriceOverrides (decision 19 §4.3) ----------------------------

// seedSentinelInfra seeds a SENTINEL base infra catalog row into the fake so an
// override can inherit its kind/unit (the real store's INSERT ... SELECT source).
func seedSentinelInfra(store *fakeStore, metric string, kind usage.Kind, unit string, priceMicros int64) {
	store.defs[defKey(usage.PlatformInfraModuleID(), metric)] = usage.MetricDefinition{
		Kind: kind, Unit: unit, UnitPriceMicros: priceMicros, Priced: true, Active: true,
	}
}

func TestSetInfraPriceOverrides_WritesPriceOnlyRowInheritingKindUnit(t *testing.T) {
	store := newFakeStore()
	// The sentinel base row (as migration 017 seeds it): sum / millisecond / 1µ$.
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	mod := uuid.New()

	resp, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  mod,
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 5}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, resp.Synced)

	// The override row is keyed by the REAL module id (never the sentinel),
	// carries the override PRICE, and INHERITS kind + unit from the sentinel row.
	got := store.defs[defKey(mod, "infra.compute.walltime.ms")]
	require.Equal(t, int64(5), got.UnitPriceMicros, "override price is written")
	require.Equal(t, usage.KindSum, got.Kind, "kind inherited from sentinel")
	require.Equal(t, "millisecond", got.Unit, "unit inherited from sentinel")
	require.True(t, got.Active)

	// The sentinel base row is untouched (still the platform default price).
	base := store.defs[defKey(usage.PlatformInfraModuleID(), "infra.compute.walltime.ms")]
	require.Equal(t, int64(1), base.UnitPriceMicros, "sentinel base price unchanged")
}

func TestSetInfraPriceOverrides_ZeroPriceIsFullAbsorb(t *testing.T) {
	// ms.Price(0) → override 0 → full absorb. Zero is a VALID override price
	// (not "unpriced"), so it must persist as 0, not be rejected.
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	mod := uuid.New()

	resp, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  mod,
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 0}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, resp.Synced)
	require.Equal(t, int64(0), store.defs[defKey(mod, "infra.compute.walltime.ms")].UnitPriceMicros)
}

func TestSetInfraPriceOverrides_RejectsNonReservedMetric(t *testing.T) {
	// A custom (non-reserved) metric belongs on SetMetricDefinitions — this
	// RPC is the INVERSE gate and rejects it.
	store := newFakeStore()
	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  uuid.New(),
		Overrides: []usage.InfraPriceOverride{{Metric: "orders.placed", UnitPriceMicros: 5}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.defs, "non-reserved metric must not be written")
}

func TestSetInfraPriceOverrides_RejectsUnregisteredReservedMetric(t *testing.T) {
	// A reserved-prefixed name that is NOT a registered platform infra metric
	// has no platform-owned catalog row to inherit from → rejected.
	store := newFakeStore()
	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  uuid.New(),
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.not.a.real.metric", UnitPriceMicros: 5}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.defs)
}

func TestSetInfraPriceOverrides_RejectsSentinelModuleID(t *testing.T) {
	// The all-zero sentinel is the platform's BASE catalog, seeded by migration
	// and never re-priced through this RPC.
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  usage.PlatformInfraModuleID(),
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 0}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
	// The sentinel base row is unchanged (only the seeded row exists).
	require.Len(t, store.defs, 1)
	require.Equal(t, int64(1), store.defs[defKey(usage.PlatformInfraModuleID(), "infra.compute.walltime.ms")].UnitPriceMicros)
}

func TestSetInfraPriceOverrides_RequiresModuleID(t *testing.T) {
	_, err := newService(newFakeStore()).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 0}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSetInfraPriceOverrides_RejectsNegativePrice(t *testing.T) {
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  uuid.New(),
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: -1}},
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSetInfraPriceOverrides_AllOrNothingRejectsWholeBatch(t *testing.T) {
	// One invalid override in the batch rejects the whole request BEFORE the
	// store is touched, so no partial write lands.
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	mod := uuid.New()
	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID: mod,
		Overrides: []usage.InfraPriceOverride{
			{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 5}, // valid
			{Metric: "orders.placed", UnitPriceMicros: 5},             // invalid (non-reserved)
		},
	})
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.defs[defKey(mod, "infra.compute.walltime.ms")].Unit, "no override written when any in the batch is invalid")
}

func TestSetInfraPriceOverrides_EmptyOverridesNoOp(t *testing.T) {
	resp, err := newService(newFakeStore()).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID: uuid.New(),
	})
	require.NoError(t, err)
	require.Equal(t, 0, resp.Synced)
}

func TestSetInfraPriceOverrides_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	seedSentinelInfra(store, "infra.compute.walltime.ms", usage.KindSum, "millisecond", 1)
	store.errUpsertOverride = errors.New("boom")
	_, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  uuid.New(),
		Overrides: []usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 5}},
	})
	requireCode(t, err, billing.CodeInternal)
}

func TestSetInfraPriceOverrides_AcceptsAllRegisteredInfraMetrics(t *testing.T) {
	// Every metric RecordInfraUsage accepts is also overridable here (the two
	// gates share platformInfraKind), including the platform.* namespace door.
	metrics := []string{
		"infra.compute.walltime.ms", "infra.ai.input.tokens", "infra.ai.requests",
		"infra.request.count", "infra.mcp.tool_call.count", "infra.cron.count",
		"infra.event.count", "infra.event.bytes", "infra.egress.api.bytes",
		"infra.storage.put.count", "infra.storage.list.count", "infra.storage.gib_hours",
	}
	store := newFakeStore()
	for _, m := range metrics {
		seedSentinelInfra(store, m, usage.KindSum, "unit", 1)
	}
	mod := uuid.New()
	overrides := make([]usage.InfraPriceOverride, 0, len(metrics))
	for _, m := range metrics {
		overrides = append(overrides, usage.InfraPriceOverride{Metric: m, UnitPriceMicros: 0})
	}
	resp, err := newService(store).SetInfraPriceOverrides(context.Background(), usage.SetInfraPriceOverridesRequest{
		ModuleID:  mod,
		Overrides: overrides,
	})
	require.NoError(t, err)
	require.Equal(t, len(metrics), resp.Synced)
}
