package usage_test

import (
	"context"
	"errors"
	"math"
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
	defs       map[string]usage.MetricDefinition // key: module/metric
	accounts   map[uuid.UUID]uuid.UUID           // owner userID → accountID
	events     map[string]usage.UsageEvent       // event_id → event (idempotency)
	periodRows []usage.MetricUsageRaw
	visibility map[uuid.UUID]usage.Visibility

	errLookup     error
	errAccount    error
	errInsert     error
	errPeriod     error
	errVisibility error
	errUpsertDef  error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		defs:       map[string]usage.MetricDefinition{},
		accounts:   map[uuid.UUID]uuid.UUID{},
		events:     map[string]usage.UsageEvent{},
		visibility: map[uuid.UUID]usage.Visibility{},
	}
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

func (f *fakeStore) CurrentPeriodUsage(_ context.Context, _ uuid.UUID, _, _ time.Time) ([]usage.MetricUsageRaw, error) {
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

func TestGetUsageSummary_NoAccountReturnsEmpty(t *testing.T) {
	resp, err := newService(newFakeStore()).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{OwnerUserID: uuid.New()})
	require.NoError(t, err)
	require.Empty(t, resp.Metrics)
}

func TestGetUsageSummary_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).GetUsageSummary(context.Background(), usage.GetUsageSummaryRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
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
