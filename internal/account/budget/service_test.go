package budget_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/budget"
)

// --- in-memory Store fake -------------------------------------------------

type alertKey struct {
	budgetID    uuid.UUID
	periodStart time.Time
	percent     int
}

type fakeStore struct {
	budgets map[string]budget.Budget        // key: scope/scope_id
	spend   int64                           // current AppPeriodSpendMicros result
	alerts  map[alertKey]budget.BudgetAlert // recorded crossings (idempotency)

	errGet    error
	errUpsert error
	errSpend  error
	errInsert error
	errList   error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		budgets: map[string]budget.Budget{},
		alerts:  map[alertKey]budget.BudgetAlert{},
	}
}

func budgetKey(scope budget.Scope, scopeID uuid.UUID) string {
	return string(scope) + "/" + scopeID.String()
}

func (f *fakeStore) UpsertBudget(_ context.Context, b budget.Budget) (budget.Budget, error) {
	if f.errUpsert != nil {
		return budget.Budget{}, f.errUpsert
	}
	if b.ID == uuid.Nil {
		b.ID = uuid.New()
	}
	f.budgets[budgetKey(b.Scope, b.ScopeID)] = b
	return b, nil
}

func (f *fakeStore) GetBudget(_ context.Context, scope budget.Scope, scopeID uuid.UUID) (budget.Budget, bool, error) {
	if f.errGet != nil {
		return budget.Budget{}, false, f.errGet
	}
	b, ok := f.budgets[budgetKey(scope, scopeID)]
	return b, ok, nil
}

func (f *fakeStore) AppPeriodSpendMicros(_ context.Context, _ uuid.UUID, _, _ time.Time) (int64, error) {
	if f.errSpend != nil {
		return 0, f.errSpend
	}
	return f.spend, nil
}

func (f *fakeStore) InsertBudgetAlerts(_ context.Context, records []budget.AlertRecord) ([]int, error) {
	if f.errInsert != nil {
		return nil, f.errInsert // all-or-nothing: a failed batch records nothing
	}
	if len(records) == 0 {
		return nil, nil
	}
	var fired []int
	for _, a := range records {
		k := alertKey{budgetID: a.BudgetID, periodStart: a.PeriodStart, percent: a.Percent}
		if _, exists := f.alerts[k]; exists {
			continue // ON CONFLICT DO NOTHING
		}
		f.alerts[k] = budget.BudgetAlert{
			Percent:     a.Percent,
			SpendMicros: a.SpendMicros,
			LimitMicros: a.LimitMicros,
			PeriodStart: a.PeriodStart,
			FiredAt:     time.Now(),
		}
		fired = append(fired, a.Percent)
	}
	return fired, nil
}

func (f *fakeStore) ListBudgetAlerts(_ context.Context, budgetID uuid.UUID, periodStart time.Time) ([]budget.BudgetAlert, error) {
	if f.errList != nil {
		return nil, f.errList
	}
	var out []budget.BudgetAlert
	for k, v := range f.alerts {
		if k.budgetID == budgetID && k.periodStart.Equal(periodStart) {
			out = append(out, v)
		}
	}
	return out, nil
}

// --- helpers --------------------------------------------------------------

func newService(store budget.Store) *budget.Service { return budget.NewService(store) }

func requireCode(t *testing.T, err error, want billing.Code) {
	t.Helper()
	require.Error(t, err)
	var be *billing.Error
	require.True(t, errors.As(err, &be), "want *billing.Error, got %T", err)
	require.Equal(t, want, be.Code)
}

func period() (time.Time, time.Time) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	return start, start.AddDate(0, 1, 0)
}

// seedBudget installs an active app budget in the fake and returns it.
func seedBudget(store *fakeStore, appID uuid.UUID, limit int64, percents []int) budget.Budget {
	b := budget.Budget{
		ID:            uuid.New(),
		Scope:         budget.ScopeApp,
		ScopeID:       appID,
		LimitMicros:   limit,
		AlertPercents: percents,
		Active:        true,
	}
	store.budgets[budgetKey(budget.ScopeApp, appID)] = b
	return b
}

// --- SetBudget validation -------------------------------------------------

func TestSetBudget_DefaultsThresholds(t *testing.T) {
	store := newFakeStore()
	resp, err := newService(store).SetBudget(context.Background(), budget.SetBudgetRequest{
		Scope: budget.ScopeApp, ScopeID: uuid.New(), LimitMicros: 1_000_000, Active: true,
	})
	require.NoError(t, err)
	require.Equal(t, []int{80, 100}, resp.AlertPercents)
}

func TestSetBudget_DedupesAndSortsThresholds(t *testing.T) {
	store := newFakeStore()
	resp, err := newService(store).SetBudget(context.Background(), budget.SetBudgetRequest{
		Scope: budget.ScopeApp, ScopeID: uuid.New(), LimitMicros: 1_000_000,
		AlertPercents: []int{100, 50, 80, 50}, Active: true,
	})
	require.NoError(t, err)
	require.Equal(t, []int{50, 80, 100}, resp.AlertPercents)
}

func TestSetBudget_RejectsBadPercent(t *testing.T) {
	for _, bad := range [][]int{{0}, {101}, {-5}, {50, 200}} {
		store := newFakeStore()
		_, err := newService(store).SetBudget(context.Background(), budget.SetBudgetRequest{
			Scope: budget.ScopeApp, ScopeID: uuid.New(), LimitMicros: 100, AlertPercents: bad, Active: true,
		})
		requireCode(t, err, billing.CodeInvalidInput)
		require.Empty(t, store.budgets, "bad percent must not persist: %v", bad)
	}
}

func TestSetBudget_RejectsNegativeLimit(t *testing.T) {
	store := newFakeStore()
	_, err := newService(store).SetBudget(context.Background(), budget.SetBudgetRequest{
		Scope: budget.ScopeApp, ScopeID: uuid.New(), LimitMicros: -1, Active: true,
	})
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.budgets)
}

func TestSetBudget_RejectsOrgAndAccountScope(t *testing.T) {
	for _, scope := range []budget.Scope{budget.ScopeOrg, budget.ScopeAccount} {
		store := newFakeStore()
		_, err := newService(store).SetBudget(context.Background(), budget.SetBudgetRequest{
			Scope: scope, ScopeID: uuid.New(), LimitMicros: 100, Active: true,
		})
		requireCode(t, err, billing.CodeInvalidInput)
		require.Empty(t, store.budgets, "scope %s not yet supported", scope)
	}
}

func TestSetBudget_RejectsUnknownScope(t *testing.T) {
	_, err := newService(newFakeStore()).SetBudget(context.Background(), budget.SetBudgetRequest{
		Scope: budget.Scope("nonsense"), ScopeID: uuid.New(), LimitMicros: 100, Active: true,
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSetBudget_RequiresScopeID(t *testing.T) {
	_, err := newService(newFakeStore()).SetBudget(context.Background(), budget.SetBudgetRequest{
		Scope: budget.ScopeApp, LimitMicros: 100, Active: true,
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSetBudget_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	store.errUpsert = errors.New("boom")
	_, err := newService(store).SetBudget(context.Background(), budget.SetBudgetRequest{
		Scope: budget.ScopeApp, ScopeID: uuid.New(), LimitMicros: 100, Active: true,
	})
	requireCode(t, err, billing.CodeInternal)
}

// --- EvaluateAppBudget ----------------------------------------------------

func TestEvaluateAppBudget_CrossesEightyThenHundred_Idempotent(t *testing.T) {
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 1_000_000, []int{80, 100})
	svc := newService(store)
	start, end := period()

	// Spend at 80% of the cap → only the 80 threshold crosses.
	store.spend = 800_000
	fired, err := svc.EvaluateAppBudget(context.Background(), appID, start, end)
	require.NoError(t, err)
	require.Equal(t, []int{80}, fired)

	// Same spend again → idempotent, nothing new recorded.
	fired, err = svc.EvaluateAppBudget(context.Background(), appID, start, end)
	require.NoError(t, err)
	require.Empty(t, fired, "re-evaluating the same spend records nothing")
	require.Len(t, store.alerts, 1)

	// Spend climbs to the cap → only the 100 threshold is newly crossed (80
	// is already recorded and stays idempotent).
	store.spend = 1_000_000
	fired, err = svc.EvaluateAppBudget(context.Background(), appID, start, end)
	require.NoError(t, err)
	require.Equal(t, []int{100}, fired)
	require.Len(t, store.alerts, 2)
}

func TestEvaluateAppBudget_NotCrossed_NoAlert(t *testing.T) {
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 1_000_000, []int{80, 100})
	store.spend = 500_000 // 50% — below the lowest threshold
	start, end := period()

	fired, err := newService(store).EvaluateAppBudget(context.Background(), appID, start, end)
	require.NoError(t, err)
	require.Empty(t, fired)
	require.Empty(t, store.alerts)
}

func TestEvaluateAppBudget_MultipleThresholdsInOneJump(t *testing.T) {
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 1_000_000, []int{50, 80, 100})
	store.spend = 1_200_000 // 120% — every threshold crosses at once
	start, end := period()

	fired, err := newService(store).EvaluateAppBudget(context.Background(), appID, start, end)
	require.NoError(t, err)
	require.Equal(t, []int{50, 80, 100}, fired)
	require.Len(t, store.alerts, 3)
}

func TestEvaluateAppBudget_NoBudget_NoOp(t *testing.T) {
	store := newFakeStore()
	start, end := period()
	fired, err := newService(store).EvaluateAppBudget(context.Background(), uuid.New(), start, end)
	require.NoError(t, err)
	require.Empty(t, fired)
	require.Empty(t, store.alerts)
}

func TestEvaluateAppBudget_InactiveBudget_NoOp(t *testing.T) {
	store := newFakeStore()
	appID := uuid.New()
	b := seedBudget(store, appID, 1_000_000, []int{80})
	b.Active = false
	store.budgets[budgetKey(budget.ScopeApp, appID)] = b
	store.spend = 2_000_000 // would cross if active
	start, end := period()

	fired, err := newService(store).EvaluateAppBudget(context.Background(), appID, start, end)
	require.NoError(t, err)
	require.Empty(t, fired)
	require.Empty(t, store.alerts, "inactive budget is not evaluated")
}

func TestEvaluateAppBudget_ZeroLimit_AnySpendCrosses(t *testing.T) {
	// A zero-cap budget is fully consumed by any spend: every threshold is
	// at-or-over (integer math: spend×100 ≥ 0×p holds for spend ≥ 0).
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 0, []int{80, 100})
	store.spend = 1
	start, end := period()

	fired, err := newService(store).EvaluateAppBudget(context.Background(), appID, start, end)
	require.NoError(t, err)
	require.Equal(t, []int{80, 100}, fired)
}

func TestEvaluateAppBudget_PropagatesSpendError(t *testing.T) {
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 1_000_000, []int{80})
	store.errSpend = errors.New("boom")
	start, end := period()

	_, err := newService(store).EvaluateAppBudget(context.Background(), appID, start, end)
	require.Error(t, err) // raw error — the caller (usage ingest) logs it best-effort
}

func TestEvaluateAppBudget_LargeLimitNoOverflow(t *testing.T) {
	// The DB caps limit_micros at 1e15 ($1B). At the ceiling the threshold math
	// (limit×percent = 1e17) stays well under int64 max, and a spend just under
	// the 80% target must NOT cross while a spend at the target must. This is
	// the path the old spend×100 formulation got wrong for very large values.
	store := newFakeStore()
	appID := uuid.New()
	const limit = int64(1_000_000_000_000_000) // 1e15 micros = $1B (DB ceiling)
	seedBudget(store, appID, limit, []int{80, 100})
	start, end := period()

	store.spend = limit*80/100 - 1 // one micro under the 80% target
	fired, err := newService(store).EvaluateAppBudget(context.Background(), appID, start, end)
	require.NoError(t, err)
	require.Empty(t, fired, "just under 80%% must not cross")

	store.spend = limit * 80 / 100 // exactly at the 80% target
	fired, err = newService(store).EvaluateAppBudget(context.Background(), appID, start, end)
	require.NoError(t, err)
	require.Equal(t, []int{80}, fired, "at the 80%% target crosses 80 only")
}

func TestEvaluateAppBudget_PartialBatchFailureRecordsNothing(t *testing.T) {
	// The crossings insert as one all-or-nothing batch: if the store errors,
	// NO alert is recorded (the transaction rolls back), and the error
	// propagates for the caller to log best-effort. The next ingest retries
	// the whole batch atomically.
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 1_000_000, []int{50, 80, 100})
	store.spend = 1_200_000 // would cross all three
	store.errInsert = errors.New("tx boom")
	start, end := period()

	_, err := newService(store).EvaluateAppBudget(context.Background(), appID, start, end)
	require.Error(t, err)
	require.Empty(t, store.alerts, "a failed batch records nothing (all-or-nothing)")
}

// --- GetBudgetStatus ------------------------------------------------------

func TestGetBudgetStatus_ReportsSpendAndCrossings(t *testing.T) {
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 1_000_000, []int{80, 100})
	store.spend = 850_000

	resp, err := newService(store).GetBudgetStatus(context.Background(), budget.GetBudgetStatusRequest{
		Scope: budget.ScopeApp, ScopeID: appID,
	})
	require.NoError(t, err)
	require.True(t, resp.Exists)
	require.Equal(t, int64(850_000), resp.SpendMicros)
	require.Equal(t, int64(1_000_000), resp.LimitMicros)
	require.Equal(t, 85, resp.PercentUsed)
	require.Equal(t, []int{80}, resp.Crossed)
}

func TestGetBudgetStatus_NoBudget_ExistsFalse(t *testing.T) {
	resp, err := newService(newFakeStore()).GetBudgetStatus(context.Background(), budget.GetBudgetStatusRequest{
		Scope: budget.ScopeApp, ScopeID: uuid.New(),
	})
	require.NoError(t, err)
	require.False(t, resp.Exists)
	require.Empty(t, resp.Crossed)
}

func TestGetBudgetStatus_ZeroLimit_PercentMatchesCrossed(t *testing.T) {
	// A 0-cap budget with positive spend is fully consumed: PercentUsed must
	// report 100 (not 0) so the display agrees with Crossed, which treats a
	// 0-cap as having crossed every threshold.
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 0, []int{80, 100})
	store.spend = 1

	resp, err := newService(store).GetBudgetStatus(context.Background(), budget.GetBudgetStatusRequest{
		Scope: budget.ScopeApp, ScopeID: appID,
	})
	require.NoError(t, err)
	require.Equal(t, 100, resp.PercentUsed, "0-cap with spend reads 100%%, matching Crossed")
	require.Equal(t, []int{80, 100}, resp.Crossed)
}

func TestGetBudgetStatus_RejectsNonAppScope(t *testing.T) {
	_, err := newService(newFakeStore()).GetBudgetStatus(context.Background(), budget.GetBudgetStatusRequest{
		Scope: budget.ScopeOrg, ScopeID: uuid.New(),
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetBudgetStatus_WithNow_DrivesDefaultPeriod(t *testing.T) {
	// WithNow pins the clock so GetBudgetStatus reports the period window for a
	// chosen month boundary deterministically (the default-period escape hatch
	// for tests). Mid-month and last-second-of-month both resolve to the same
	// first-of-month start / first-of-next-month end.
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 1_000_000, []int{80})

	at := time.Date(2026, 2, 28, 23, 59, 59, 0, time.UTC) // last second of Feb
	svc := newService(store).WithNow(func() time.Time { return at })

	resp, err := svc.GetBudgetStatus(context.Background(), budget.GetBudgetStatusRequest{
		Scope: budget.ScopeApp, ScopeID: appID,
	})
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), resp.PeriodStart)
	require.Equal(t, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), resp.PeriodEnd)
}

// --- GetBudgetAlerts ------------------------------------------------------

func TestGetBudgetAlerts_ReturnsRecordedCrossings(t *testing.T) {
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 1_000_000, []int{80, 100})
	store.spend = 1_000_000
	start, end := period()
	svc := newService(store)

	_, err := svc.EvaluateAppBudget(context.Background(), appID, start, end)
	require.NoError(t, err)

	resp, err := svc.GetBudgetAlerts(context.Background(), budget.GetBudgetAlertsRequest{
		Scope: budget.ScopeApp, ScopeID: appID, PeriodStart: start,
	})
	require.NoError(t, err)
	require.Len(t, resp.Alerts, 2)
}

func TestGetBudgetAlerts_NoBudget_Empty(t *testing.T) {
	resp, err := newService(newFakeStore()).GetBudgetAlerts(context.Background(), budget.GetBudgetAlertsRequest{
		Scope: budget.ScopeApp, ScopeID: uuid.New(),
	})
	require.NoError(t, err)
	require.Empty(t, resp.Alerts)
}
