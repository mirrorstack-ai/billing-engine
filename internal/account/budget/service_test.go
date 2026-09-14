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
	budgets   map[string]budget.Budget        // key: scope/scope_id
	spend     int64                           // current AppPeriodSpendMicros result
	alerts    map[alertKey]budget.BudgetAlert // recorded crossings (idempotency)
	anchorDay int                             // AppAnchorDay result (0 → 1, calendar month)

	// captured window AppPeriodSpendMicros was called with, so a test can assert
	// the anchored [start, end) reached the store unchanged.
	gotSpendStart    time.Time
	gotSpendEnd      time.Time
	gotSpendCategory budget.Category
	gotSpendTemplate string
	gotSpendAccount  uuid.UUID

	spendBy      map[string]int64        // "category/template" → spend (overrides spend)
	accountSpend int64                   // AccountPeriodAISpendMicros result
	orgAccounts  map[uuid.UUID]uuid.UUID // org id → its billing account

	// exposure pool (085)
	accountAllSpend int64                                // AccountPeriodSpendMicros (every metric)
	appPayers       map[uuid.UUID]uuid.UUID              // app → payer account
	signals         map[uuid.UUID]budget.ExposureSignals // account → curve inputs ("" mode = not paas)
	rampCfg         budget.RiskRampConfig

	errGet    error
	errUpsert error
	errSpend  error
	errInsert error
	errList   error
	errAnchor error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		budgets:     map[string]budget.Budget{},
		alerts:      map[alertKey]budget.BudgetAlert{},
		spendBy:     map[string]int64{},
		orgAccounts: map[uuid.UUID]uuid.UUID{},
		appPayers:   map[uuid.UUID]uuid.UUID{},
		signals:     map[uuid.UUID]budget.ExposureSignals{},
		rampCfg:     budget.RiskRampConfig{NoCardMicros: 5_000_000, CardBaseMicros: 10_000_000, CeilingMicros: 200_000_000, Exponent: 0.5, DelinquentFloor: true, LatePenaltyK: 2},
	}
}

func budgetKey(scope budget.Scope, scopeID uuid.UUID, cat budget.Category, tpl string) string {
	if cat == "" {
		cat = budget.CategoryAll
	}
	return string(scope) + "/" + scopeID.String() + "/" + string(cat) + "/" + tpl
}

func (f *fakeStore) UpsertBudget(_ context.Context, b budget.Budget) (budget.Budget, error) {
	if f.errUpsert != nil {
		return budget.Budget{}, f.errUpsert
	}
	// ON CONFLICT DO UPDATE keeps the row's id — the alert idempotency key
	// (budget_id, period, percent) must survive a re-upsert, as it does in SQL.
	k := budgetKey(b.Scope, b.ScopeID, b.Category, b.TemplateKey)
	if existing, ok := f.budgets[k]; ok {
		b.ID = existing.ID
	} else if b.ID == uuid.Nil {
		b.ID = uuid.New()
	}
	f.budgets[k] = b
	return b, nil
}

func (f *fakeStore) GetBudget(_ context.Context, scope budget.Scope, scopeID uuid.UUID, cat budget.Category, tpl string) (budget.Budget, bool, error) {
	if f.errGet != nil {
		return budget.Budget{}, false, f.errGet
	}
	b, ok := f.budgets[budgetKey(scope, scopeID, cat, tpl)]
	return b, ok, nil
}

// AppPeriodSpendMicros answers f.spend, or a per-(category, template) figure
// from f.spendBy when set — so a precedence test can give the template's own
// events a different total from the app's AI-wide total.
func (f *fakeStore) AppPeriodSpendMicros(_ context.Context, _ uuid.UUID, cat budget.Category, tpl string, start, end time.Time) (int64, error) {
	f.gotSpendStart, f.gotSpendEnd = start, end
	f.gotSpendCategory, f.gotSpendTemplate = cat, tpl
	if f.errSpend != nil {
		return 0, f.errSpend
	}
	if v, ok := f.spendBy[string(cat)+"/"+tpl]; ok {
		return v, nil
	}
	return f.spend, nil
}

func (f *fakeStore) AccountPeriodAISpendMicros(_ context.Context, accountID uuid.UUID, start, end time.Time) (int64, error) {
	f.gotSpendStart, f.gotSpendEnd = start, end
	f.gotSpendAccount = accountID
	if f.errSpend != nil {
		return 0, f.errSpend
	}
	return f.accountSpend, nil
}

func (f *fakeStore) OrgAccountID(_ context.Context, orgID uuid.UUID) (uuid.UUID, bool, error) {
	id, ok := f.orgAccounts[orgID]
	return id, ok, nil
}

func (f *fakeStore) AccountPeriodSpendMicros(_ context.Context, accountID uuid.UUID, start, end time.Time) (int64, error) {
	if f.errSpend != nil {
		return 0, f.errSpend
	}
	return f.accountAllSpend, nil
}

func (f *fakeStore) AppPayerAccountID(_ context.Context, appID uuid.UUID) (uuid.UUID, bool, error) {
	id, ok := f.appPayers[appID]
	return id, ok, nil
}

func (f *fakeStore) ExposureSignals(_ context.Context, accountID uuid.UUID) (budget.ExposureSignals, error) {
	return f.signals[accountID], nil
}

func (f *fakeStore) RiskRampConfig(_ context.Context) (budget.RiskRampConfig, error) {
	return f.rampCfg, nil
}

func (f *fakeStore) SetAIEnforcementPaused(_ context.Context, paused bool, reason, actorID string) (bool, error) {
	f.rampCfg.EnforcementPaused = paused
	if paused {
		f.rampCfg.PausedReason, f.rampCfg.PausedBy, f.rampCfg.PausedAt = reason, actorID, time.Now()
	} else {
		f.rampCfg.PausedReason, f.rampCfg.PausedBy, f.rampCfg.PausedAt = "", "", time.Time{}
	}
	return paused, nil
}

func (f *fakeStore) AccountAnchorDay(_ context.Context, _ uuid.UUID) (int, error) {
	if f.anchorDay != 0 {
		return f.anchorDay, nil
	}
	return 1, nil
}

// AppAnchorDay returns the configured app anchor day, defaulting to 1 (the UTC
// calendar month) so tests that don't set one keep the pre-anchor window.
func (f *fakeStore) AppAnchorDay(_ context.Context, _ uuid.UUID) (int, error) {
	if f.errAnchor != nil {
		return 0, f.errAnchor
	}
	if f.anchorDay != 0 {
		return f.anchorDay, nil
	}
	return 1, nil
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
		Category:      budget.CategoryAll,
		LimitMicros:   limit,
		AlertPercents: percents,
		Active:        true,
	}
	store.budgets[budgetKey(budget.ScopeApp, appID, budget.CategoryAll, "")] = b
	return b
}

// seedAIBudget installs an active AI cap (app-wide when tpl == "", else the
// template's own) with a hard cap, and returns it.
func seedAIBudget(store *fakeStore, scope budget.Scope, id uuid.UUID, tpl string, limit int64, hard, overage bool) budget.Budget {
	b := budget.Budget{
		ID:            uuid.New(),
		Scope:         scope,
		ScopeID:       id,
		Category:      budget.CategoryAI,
		TemplateKey:   tpl,
		LimitMicros:   limit,
		AlertPercents: []int{80, 100},
		Active:        true,
		HardCap:       hard,
		AllowOverage:  overage,
	}
	store.budgets[budgetKey(scope, id, budget.CategoryAI, tpl)] = b
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
	fired, err := svc.EvaluateAppBudget(context.Background(), appID, "", start, end)
	require.NoError(t, err)
	require.Equal(t, []int{80}, fired)

	// Same spend again → idempotent, nothing new recorded.
	fired, err = svc.EvaluateAppBudget(context.Background(), appID, "", start, end)
	require.NoError(t, err)
	require.Empty(t, fired, "re-evaluating the same spend records nothing")
	require.Len(t, store.alerts, 1)

	// Spend climbs to the cap → only the 100 threshold is newly crossed (80
	// is already recorded and stays idempotent).
	store.spend = 1_000_000
	fired, err = svc.EvaluateAppBudget(context.Background(), appID, "", start, end)
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

	fired, err := newService(store).EvaluateAppBudget(context.Background(), appID, "", start, end)
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

	fired, err := newService(store).EvaluateAppBudget(context.Background(), appID, "", start, end)
	require.NoError(t, err)
	require.Equal(t, []int{50, 80, 100}, fired)
	require.Len(t, store.alerts, 3)
}

func TestEvaluateAppBudget_NoBudget_NoOp(t *testing.T) {
	store := newFakeStore()
	start, end := period()
	fired, err := newService(store).EvaluateAppBudget(context.Background(), uuid.New(), "", start, end)
	require.NoError(t, err)
	require.Empty(t, fired)
	require.Empty(t, store.alerts)
}

func TestEvaluateAppBudget_InactiveBudget_NoOp(t *testing.T) {
	store := newFakeStore()
	appID := uuid.New()
	b := seedBudget(store, appID, 1_000_000, []int{80})
	b.Active = false
	store.budgets[budgetKey(budget.ScopeApp, appID, budget.CategoryAll, "")] = b
	store.spend = 2_000_000 // would cross if active
	start, end := period()

	fired, err := newService(store).EvaluateAppBudget(context.Background(), appID, "", start, end)
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

	fired, err := newService(store).EvaluateAppBudget(context.Background(), appID, "", start, end)
	require.NoError(t, err)
	require.Equal(t, []int{80, 100}, fired)
}

func TestEvaluateAppBudget_PropagatesSpendError(t *testing.T) {
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 1_000_000, []int{80})
	store.errSpend = errors.New("boom")
	start, end := period()

	_, err := newService(store).EvaluateAppBudget(context.Background(), appID, "", start, end)
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
	fired, err := newService(store).EvaluateAppBudget(context.Background(), appID, "", start, end)
	require.NoError(t, err)
	require.Empty(t, fired, "just under 80%% must not cross")

	store.spend = limit * 80 / 100 // exactly at the 80% target
	fired, err = newService(store).EvaluateAppBudget(context.Background(), appID, "", start, end)
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

	_, err := newService(store).EvaluateAppBudget(context.Background(), appID, "", start, end)
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

// TestGetBudgetStatus_WindowsOnAppAnchorDay proves the budget status window is
// anchored to the app payer's card-binding day (ADR 0005), not the 1st, and that
// the anchored [start, end) reaches the spend query unchanged.
func TestGetBudgetStatus_WindowsOnAppAnchorDay(t *testing.T) {
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 1_000_000, []int{80})
	store.anchorDay = 17 // the app's payer bound a card on the 17th

	at := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	svc := newService(store).WithNow(func() time.Time { return at })

	resp, err := svc.GetBudgetStatus(context.Background(), budget.GetBudgetStatusRequest{
		Scope: budget.ScopeApp, ScopeID: appID,
	})
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC), resp.PeriodStart)
	require.Equal(t, time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC), resp.PeriodEnd)
	// The same anchored window reached AppPeriodSpendMicros (threaded unchanged).
	require.Equal(t, resp.PeriodStart, store.gotSpendStart)
	require.Equal(t, resp.PeriodEnd, store.gotSpendEnd)
}

// TestGetBudgetStatus_AnchorLookupErrorSurfaces proves an app-anchor lookup error
// fails the read loud (Internal) rather than silently mis-windowing the budget.
func TestGetBudgetStatus_AnchorLookupErrorSurfaces(t *testing.T) {
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 1_000_000, []int{80})
	store.errAnchor = errors.New("anchor db down")
	_, err := newService(store).GetBudgetStatus(context.Background(), budget.GetBudgetStatusRequest{
		Scope: budget.ScopeApp, ScopeID: appID,
	})
	requireCode(t, err, billing.CodeInternal)
}

// --- GetBudgetAlerts ------------------------------------------------------

func TestGetBudgetAlerts_ReturnsRecordedCrossings(t *testing.T) {
	store := newFakeStore()
	appID := uuid.New()
	seedBudget(store, appID, 1_000_000, []int{80, 100})
	store.spend = 1_000_000
	start, end := period()
	svc := newService(store)

	_, err := svc.EvaluateAppBudget(context.Background(), appID, "", start, end)
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

// --- AI caps: precedence, exhaustion, scopes (migration 084, T101) ----------

func TestSetBudget_AICapsValidateScopeAndTemplate(t *testing.T) {
	store := newFakeStore()
	svc := newService(store)
	app, org, acct := uuid.New(), uuid.New(), uuid.New()

	// A template cap on an app's AI budget: stored with its key, hard cap and overage flag.
	resp, err := svc.SetBudget(context.Background(), budget.SetBudgetRequest{
		Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI, TemplateKey: "member-help",
		LimitMicros: 5_000_000, Active: true, HardCap: true,
	})
	require.NoError(t, err)
	require.Equal(t, budget.CategoryAI, resp.Category)
	require.Equal(t, "member-help", resp.TemplateKey)
	require.True(t, resp.HardCap)
	require.False(t, resp.AllowOverage)

	// Org and account scopes take 'ai' only; 'all' stays app-only.
	_, err = svc.SetBudget(context.Background(), budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI, LimitMicros: 1, Active: true, HardCap: true, AllowOverage: true})
	require.NoError(t, err)
	_, err = svc.SetBudget(context.Background(), budget.SetBudgetRequest{Scope: budget.ScopeAccount, ScopeID: acct, Category: budget.CategoryAI, LimitMicros: 1, Active: true})
	require.NoError(t, err)
	_, err = svc.SetBudget(context.Background(), budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: org, LimitMicros: 1, Active: true})
	requireCode(t, err, billing.CodeInvalidInput)

	// A template key is an app AI thing with the module's key shape.
	_, err = svc.SetBudget(context.Background(), budget.SetBudgetRequest{Scope: budget.ScopeApp, ScopeID: app, TemplateKey: "member-help", LimitMicros: 1, Active: true})
	requireCode(t, err, billing.CodeInvalidInput)
	_, err = svc.SetBudget(context.Background(), budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI, TemplateKey: "member-help", LimitMicros: 1, Active: true})
	requireCode(t, err, billing.CodeInvalidInput)
	_, err = svc.SetBudget(context.Background(), budget.SetBudgetRequest{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI, TemplateKey: "Member Help", LimitMicros: 1, Active: true})
	requireCode(t, err, billing.CodeInvalidInput)
	_, err = svc.SetBudget(context.Background(), budget.SetBudgetRequest{Scope: budget.ScopeApp, ScopeID: app, Category: "tokens", LimitMicros: 1, Active: true})
	requireCode(t, err, billing.CodeInvalidInput)
}

// TestGetBudgetStatus_TemplateAndAppWideCompose pins the composition ONE
// place computes (billing-engine#221 review): the template's row narrows the
// app's AI-wide cap and never shadows it — exhausted if EITHER is, the
// exhausted row (template first) else the more specific row reported.
func TestGetBudgetStatus_TemplateAndAppWideCompose(t *testing.T) {
	store := newFakeStore()
	svc := newService(store)
	app := uuid.New()
	ctx := context.Background()
	read := func(tpl string) *budget.GetBudgetStatusResponse {
		st, err := svc.GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI, TemplateKey: tpl})
		require.NoError(t, err)
		return st
	}

	// Nothing configured: exists=false, decided_by none.
	st := read("member-help")
	require.False(t, st.Exists)
	require.Equal(t, budget.DecidedByNone, st.DecidedBy)

	// App-wide AI row only: a template read is governed by it and sums the APP's AI spend.
	seedAIBudget(store, budget.ScopeApp, app, "", 10_000_000, true, false)
	store.spendBy["ai/"] = 9_000_000
	store.spendBy["ai/member-help"] = 2_000_000
	st = read("member-help")
	require.True(t, st.Exists)
	require.Equal(t, budget.DecidedByApp, st.DecidedBy)
	require.EqualValues(t, 9_000_000, st.SpendMicros)
	require.False(t, st.Exhausted)
	require.EqualValues(t, 1_000_000, st.RemainingMicros)

	// The template's own row (roomy, not spent): reported as the specific row,
	// but the app-wide row still counts.
	seedAIBudget(store, budget.ScopeApp, app, "member-help", 5_000_000, true, false)
	st = read("member-help")
	require.Equal(t, budget.DecidedByTemplate, st.DecidedBy)
	require.Equal(t, "member-help", st.TemplateKey)
	require.EqualValues(t, 2_000_000, st.SpendMicros, "the template row reports its own events")
	require.False(t, st.Exhausted)

	// 🔴 The escape 94 named: the app-wide cap fills up while the template is
	// under its own limit — the verdict must still be exhausted, labelled by
	// the row that bit.
	store.spendBy["ai/"] = 10_000_000
	st = read("member-help")
	require.True(t, st.Exhausted, "a template row must not shadow an exhausted app-wide cap")
	require.Equal(t, budget.DecidedByApp, st.DecidedBy)
	require.EqualValues(t, 10_000_000, st.SpendMicros)

	// Both spent: the template wins the label.
	store.spendBy["ai/member-help"] = 5_000_000
	st = read("member-help")
	require.True(t, st.Exhausted)
	require.Equal(t, budget.DecidedByTemplate, st.DecidedBy)

	// Template spent, app-wide fine: exhausted by the template.
	store.spendBy["ai/"] = 6_000_000
	st = read("member-help")
	require.True(t, st.Exhausted)
	require.Equal(t, budget.DecidedByTemplate, st.DecidedBy)
	require.Zero(t, st.RemainingMicros)

	// A template with overage allowed does not lift the app-wide cap.
	store.budgets[budgetKey(budget.ScopeApp, app, budget.CategoryAI, "member-help")] = func() budget.Budget {
		b := store.budgets[budgetKey(budget.ScopeApp, app, budget.CategoryAI, "member-help")]
		b.AllowOverage = true
		return b
	}()
	store.spendBy["ai/"] = 10_000_000
	st = read("member-help")
	require.True(t, st.Exhausted)
	require.Equal(t, budget.DecidedByApp, st.DecidedBy)

	// Another template with no row of its own composes with the app-wide row alone.
	store.spendBy["ai/"] = 1_000_000
	st = read("faq")
	require.Equal(t, budget.DecidedByApp, st.DecidedBy)
	require.False(t, st.Exhausted)
}

// TestGetBudgetStatus_ExhaustedRules: only an ACTIVE, HARD cap without the
// customer's overage opt-out is ever exhausted; alert-only rows report the
// crossing but never refuse.
func TestGetBudgetStatus_ExhaustedRules(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name          string
		hard, overage bool
		active        bool
		spend         int64
		want          bool
	}{
		{"hard cap at limit", true, false, true, 1_000_000, true},
		{"hard cap over limit", true, false, true, 1_500_000, true},
		{"hard cap under limit", true, false, true, 999_999, false},
		{"alert-only at limit", false, false, true, 1_000_000, false},
		{"overage allowed at limit", true, true, true, 1_000_000, false},
		{"inactive hard cap at limit", true, false, false, 1_000_000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			app := uuid.New()
			b := seedAIBudget(store, budget.ScopeApp, app, "", 1_000_000, tc.hard, tc.overage)
			b.Active = tc.active
			store.budgets[budgetKey(budget.ScopeApp, app, budget.CategoryAI, "")] = b
			store.spend = tc.spend
			st, err := newService(store).GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI})
			require.NoError(t, err)
			require.Equal(t, tc.want, st.Exhausted)
			require.Equal(t, tc.hard, st.HardCap)
			require.Equal(t, tc.overage, st.AllowOverage)
		})
	}
}

// TestGetBudgetStatus_OrgAndAccountScopesSumTheAccount: scenario 2 — an
// account row sums that account's AI events; an org row resolves the org's
// own billing account first; a lazy org (no account) has spent nothing.
func TestGetBudgetStatus_OrgAndAccountScopesSumTheAccount(t *testing.T) {
	store := newFakeStore()
	svc := newService(store)
	ctx := context.Background()
	acct, org, orgAcct := uuid.New(), uuid.New(), uuid.New()
	store.accountSpend = 3_000_000

	seedAIBudget(store, budget.ScopeAccount, acct, "", 3_000_000, true, false)
	st, err := svc.GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeAccount, ScopeID: acct, Category: budget.CategoryAI})
	require.NoError(t, err)
	require.Equal(t, budget.DecidedByAccount, st.DecidedBy)
	require.Equal(t, acct, store.gotSpendAccount)
	require.True(t, st.Exhausted)

	seedAIBudget(store, budget.ScopeOrg, org, "", 10_000_000, true, false)
	st, err = svc.GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI})
	require.NoError(t, err)
	require.Equal(t, budget.DecidedByOrg, st.DecidedBy)
	require.Zero(t, st.SpendMicros, "a lazy org with no account has spent nothing")
	require.False(t, st.Exhausted)

	store.orgAccounts[org] = orgAcct
	st, err = svc.GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI})
	require.NoError(t, err)
	require.Equal(t, orgAcct, store.gotSpendAccount, "an org row sums the ORG's own account")
	require.EqualValues(t, 3_000_000, st.SpendMicros)

	// The wrong category on an org scope is refused, never silently read as 'all'.
	_, err = svc.GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeOrg, ScopeID: org})
	requireCode(t, err, billing.CodeInvalidInput)
}

// TestEvaluateAppBudget_WalksAllAIAndTemplateRows: one AI event re-evaluates
// the app's 'all' row, its AI-wide row and the stamped template's row, each
// against its own spend, and records each row's crossings once.
func TestEvaluateAppBudget_WalksAllAIAndTemplateRows(t *testing.T) {
	store := newFakeStore()
	svc := newService(store)
	app := uuid.New()
	start, end := period()
	seedBudget(store, app, 100_000_000, []int{80, 100})
	seedAIBudget(store, budget.ScopeApp, app, "", 10_000_000, true, false)
	seedAIBudget(store, budget.ScopeApp, app, "member-help", 2_000_000, true, false)
	store.spendBy["all/"] = 50_000_000          // 50% of 'all': nothing
	store.spendBy["ai/"] = 8_000_000            // 80% of the AI-wide row
	store.spendBy["ai/member-help"] = 2_000_000 // 100% of the template row

	fired, err := svc.EvaluateAppBudget(context.Background(), app, "member-help", start, end)
	require.NoError(t, err)
	require.ElementsMatch(t, []int{80, 80, 100}, fired, "AI-wide crossed 80; the template crossed 80 and 100")

	fired, err = svc.EvaluateAppBudget(context.Background(), app, "member-help", start, end)
	require.NoError(t, err)
	require.Empty(t, fired, "idempotent per row+period+percent")

	// A module event (no template) walks only the 'all' and AI-wide rows.
	store.spendBy["ai/faq"] = 5_000_000
	fired, err = svc.EvaluateAppBudget(context.Background(), app, "", start, end)
	require.NoError(t, err)
	require.Empty(t, fired)
}

func TestEvaluateAccountBudget_AccountThenOwningOrg(t *testing.T) {
	store := newFakeStore()
	svc := newService(store)
	acct, org := uuid.New(), uuid.New()
	start, end := period()
	store.accountSpend = 4_000_000
	seedAIBudget(store, budget.ScopeAccount, acct, "", 5_000_000, true, false) // 80%
	seedAIBudget(store, budget.ScopeOrg, org, "", 4_000_000, true, false)      // 100%

	fired, err := svc.EvaluateAccountBudget(context.Background(), acct, org, start, end)
	require.NoError(t, err)
	require.ElementsMatch(t, []int{80, 80, 100}, fired)

	fired, err = svc.EvaluateAccountBudget(context.Background(), acct, uuid.Nil, start, end)
	require.NoError(t, err)
	require.Empty(t, fired, "no org, and the account's crossings are already recorded")
}

// --- PaaS risk-exposure pool (migration 085, PR-B) ------------------------

// TestExposureLimitMicros_OwnerCurve pins the owner's curve: $5 without a
// card; with a card $10 × √(1+k) for k paid invoices, flattening; capped at
// the ceiling; whole micros rounded half up.
func TestExposureLimitMicros_OwnerCurve(t *testing.T) {
	cfg := budget.RiskRampConfig{NoCardMicros: 5_000_000, CardBaseMicros: 10_000_000, CeilingMicros: 200_000_000, Exponent: 0.5}
	for _, tc := range []struct {
		card bool
		paid int
		want int64
	}{
		{false, 0, 5_000_000},
		{false, 50, 5_000_000},
		{true, 0, 10_000_000},
		{true, 1, 14_142_136},
		{true, 2, 17_320_508},
		{true, 3, 20_000_000},
		{true, 8, 30_000_000},
		{true, 15, 40_000_000},
		{true, 24, 50_000_000},
		{true, 399, 200_000_000},
		{true, 10_000, 200_000_000},
		{true, -3, 10_000_000},
	} {
		require.EqualValues(t, tc.want, budget.ExposureLimitMicros(cfg, budget.ExposureSignals{HasUsableCard: tc.card, PaidInvoices: tc.paid}), "card=%v paid=%d", tc.card, tc.paid)
	}
	// A misconfigured floor above the ceiling is still bounded by the ceiling.
	require.EqualValues(t, 1, budget.ExposureLimitMicros(budget.RiskRampConfig{NoCardMicros: 9, CardBaseMicros: 9, CeilingMicros: 1}, budget.ExposureSignals{}))

	// The SHAPE is config: the owner's candidate steeper curves, and linear.
	card := budget.ExposureSignals{HasUsableCard: true, PaidInvoices: 3} // k=3 → (1+k)=4
	require.EqualValues(t, 40_000_000, budget.ExposureLimitMicros(budget.RiskRampConfig{CardBaseMicros: 20_000_000, CeilingMicros: 200_000_000, Exponent: 0.5}, card), "$20×√4")
	require.EqualValues(t, 32_490_096, budget.ExposureLimitMicros(budget.RiskRampConfig{CardBaseMicros: 10_000_000, CeilingMicros: 200_000_000, Exponent: 0.85}, card), "$10×4^0.85")
	require.EqualValues(t, 42_426_407, budget.ExposureLimitMicros(budget.RiskRampConfig{CardBaseMicros: 15_000_000, CeilingMicros: 200_000_000, Exponent: 0.75}, card), "$15×4^0.75")
	require.EqualValues(t, 40_000_000, budget.ExposureLimitMicros(budget.RiskRampConfig{CardBaseMicros: 10_000_000, CeilingMicros: 200_000_000, Exponent: 1}, card), "linear")
	require.EqualValues(t, 20_000_000, budget.ExposureLimitMicros(budget.RiskRampConfig{CardBaseMicros: 10_000_000, CeilingMicros: 200_000_000, Exponent: 0}, card), "an unset exponent falls back to √")
}

// TestGetBudgetStatus_ExposurePoolComposesWithTheCaps: on a PaaS account the
// pool rides every AI read, exhausts on the account's WHOLE usage, is named
// only when no customer cap bit, and is never lifted by allow_overage; a
// credits account or an unattributed app has no pool.
func TestGetBudgetStatus_ExposurePoolComposesWithTheCaps(t *testing.T) {
	store := newFakeStore()
	svc := newService(store)
	ctx := context.Background()
	app, acct := uuid.New(), uuid.New()
	store.appPayers[app] = acct
	store.signals[acct] = budget.ExposureSignals{BillingMode: "standard", HasUsableCard: true, PaidInvoices: 3} // $20
	read := func() *budget.GetBudgetStatusResponse {
		st, err := svc.GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI, TemplateKey: "member-help"})
		require.NoError(t, err)
		return st
	}

	// No customer cap, pool under: exists=false, not exhausted, pool reported.
	store.accountAllSpend = 12_000_000
	st := read()
	require.False(t, st.Exists)
	require.False(t, st.Exhausted)
	require.NotNil(t, st.Pool)
	require.Equal(t, "paas", st.Pool.Mode)
	require.Equal(t, budget.PoolSourceExposureLimit, st.Pool.Source)
	require.EqualValues(t, 20_000_000, st.Pool.LimitMicros)
	require.EqualValues(t, 8_000_000, st.Pool.RemainingMicros)
	require.True(t, st.Pool.HasUsableCard)
	require.Equal(t, 3, st.Pool.PaidInvoices)
	// The system row exists now, with the curve as its limit.
	sys, found, err := store.GetBudget(ctx, budget.ScopeAccount, acct, budget.CategoryExposure, "")
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 20_000_000, sys.LimitMicros)
	require.True(t, sys.HardCap)

	// No customer cap, pool full: exhausted by the pool.
	store.accountAllSpend = 20_000_000
	st = read()
	require.False(t, st.Exists)
	require.True(t, st.Exhausted)
	require.Equal(t, budget.DecidedByExposureLimit, st.DecidedBy)

	// A customer cap that is fine + a full pool: still exhausted, by the pool.
	seedAIBudget(store, budget.ScopeApp, app, "", 100_000_000, true, false)
	store.spendBy["ai/"] = 1_000_000
	st = read()
	require.True(t, st.Exists)
	require.True(t, st.Exhausted)
	require.Equal(t, budget.DecidedByExposureLimit, st.DecidedBy)
	require.EqualValues(t, 100_000_000, st.LimitMicros, "the cap row's own figures are still reported")

	// allow_overage on the customer's cap never lifts the pool.
	store.budgets[budgetKey(budget.ScopeApp, app, budget.CategoryAI, "")] = func() budget.Budget {
		b := store.budgets[budgetKey(budget.ScopeApp, app, budget.CategoryAI, "")]
		b.AllowOverage = true
		return b
	}()
	st = read()
	require.True(t, st.Exhausted)
	require.Equal(t, budget.DecidedByExposureLimit, st.DecidedBy)

	// A customer cap that bites keeps ITS label even with the pool full.
	store.spendBy["ai/"] = 100_000_000
	store.budgets[budgetKey(budget.ScopeApp, app, budget.CategoryAI, "")] = func() budget.Budget {
		b := store.budgets[budgetKey(budget.ScopeApp, app, budget.CategoryAI, "")]
		b.AllowOverage = false
		return b
	}()
	st = read()
	require.True(t, st.Exhausted)
	require.Equal(t, budget.DecidedByApp, st.DecidedBy)
	require.True(t, st.Pool.Exhausted)

	// Credits mode: no pool, the caps alone decide.
	store.signals[acct] = budget.ExposureSignals{BillingMode: "credits", HasUsableCard: true}
	store.spendBy["ai/"] = 0
	st = read()
	require.False(t, st.Exhausted)
	require.Equal(t, budget.PoolSourceNone, st.Pool.Source)
	require.Equal(t, "credits", st.Pool.Mode)

	// An unattributed app (no payer yet): no pool either.
	delete(store.appPayers, app)
	st = read()
	require.Equal(t, budget.PoolSourceNone, st.Pool.Source)
	require.Equal(t, "none", st.Pool.Mode)

	// The system category is never accepted from a caller.
	_, err = svc.SetBudget(ctx, budget.SetBudgetRequest{Scope: budget.ScopeAccount, ScopeID: acct, Category: budget.CategoryExposure, LimitMicros: 1, Active: true})
	requireCode(t, err, billing.CodeInvalidInput)
	_, err = svc.GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeAccount, ScopeID: acct, Category: budget.CategoryExposure})
	requireCode(t, err, billing.CodeInvalidInput)
}

// TestEvaluateAccountBudget_RecordsPoolCrossings: an AI event on a PaaS
// account records the pool's 80% and 100% crossings on the system row — the
// pre-cliff warning — once per period, alongside the account's own AI cap.
func TestEvaluateAccountBudget_RecordsPoolCrossings(t *testing.T) {
	store := newFakeStore()
	svc := newService(store)
	acct := uuid.New()
	start, end := period()
	store.signals[acct] = budget.ExposureSignals{BillingMode: "standard", HasUsableCard: false} // $5
	store.accountAllSpend = 4_000_000                                                           // 80%

	fired, err := svc.EvaluateAccountBudget(context.Background(), acct, uuid.Nil, start, end)
	require.NoError(t, err)
	require.Equal(t, []int{80}, fired)

	store.accountAllSpend = 5_000_000 // 100%
	fired, err = svc.EvaluateAccountBudget(context.Background(), acct, uuid.Nil, start, end)
	require.NoError(t, err)
	require.Equal(t, []int{100}, fired, "80 was already recorded; only the cliff is new")

	fired, err = svc.EvaluateAccountBudget(context.Background(), acct, uuid.Nil, start, end)
	require.NoError(t, err)
	require.Empty(t, fired)

	// The recorded crossings are readable on the system row.
	sys, found, err := store.GetBudget(context.Background(), budget.ScopeAccount, acct, budget.CategoryExposure, "")
	require.NoError(t, err)
	require.True(t, found)
	alerts, err := store.ListBudgetAlerts(context.Background(), sys.ID, start)
	require.NoError(t, err)
	require.Len(t, alerts, 2)

	// A credits account records nothing for the pool.
	other := uuid.New()
	store.signals[other] = budget.ExposureSignals{BillingMode: "credits"}
	fired, err = svc.EvaluateAccountBudget(context.Background(), other, uuid.Nil, start, end)
	require.NoError(t, err)
	require.Empty(t, fired)
}

// TestGetBudgetStatus_KillSwitchAllowsEveryVerdict: the incident switch
// (085) is read per verdict — flipping it through the admin RPC allows the
// next read with decided_by "paused" while the figures still show what would
// have refused; flipping it back restores the refusal, no deploy involved.
func TestGetBudgetStatus_KillSwitchAllowsEveryVerdict(t *testing.T) {
	store := newFakeStore()
	svc := newService(store)
	ctx := context.Background()
	app, acct := uuid.New(), uuid.New()
	store.appPayers[app] = acct
	store.signals[acct] = budget.ExposureSignals{BillingMode: "standard"} // $5 pool
	store.accountAllSpend = 5_000_000                                     // pool full
	seedAIBudget(store, budget.ScopeApp, app, "", 1_000_000, true, false)
	store.spendBy["ai/"] = 1_000_000 // cap full too
	read := func() *budget.GetBudgetStatusResponse {
		st, err := svc.GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI})
		require.NoError(t, err)
		return st
	}

	st := read()
	require.True(t, st.Exhausted)
	require.Equal(t, budget.DecidedByApp, st.DecidedBy)

	// A pause without a reason and an actor is refused: the row must answer
	// "why was enforcement off, and who did it".
	_, err := svc.SetAIEnforcementPaused(ctx, budget.SetAIEnforcementPausedRequest{Paused: true})
	requireCode(t, err, billing.CodeInvalidInput)
	resp, err := svc.SetAIEnforcementPaused(ctx, budget.SetAIEnforcementPausedRequest{Paused: true, Reason: "incident: gate misfiring", ActorID: "ops:owner"})
	require.NoError(t, err)
	require.True(t, resp.Paused)
	require.Equal(t, "incident: gate misfiring", resp.PausedReason)
	require.Equal(t, "ops:owner", resp.PausedBy)
	require.False(t, resp.PausedAt.IsZero())
	require.EqualValues(t, 5_000_000, resp.NoCardMicros)

	st = read()
	require.False(t, st.Exhausted, "paused: every verdict is allowed")
	require.Equal(t, budget.DecidedByPaused, st.DecidedBy)
	require.True(t, st.Pool.Exhausted, "the pool's own figure still says what would have refused")
	require.EqualValues(t, 1_000_000, st.SpendMicros)

	got, err := svc.GetAIEnforcement(ctx)
	require.NoError(t, err)
	require.True(t, got.Paused)

	resp, err = svc.SetAIEnforcementPaused(ctx, budget.SetAIEnforcementPausedRequest{Paused: false})
	require.NoError(t, err)
	require.False(t, resp.Paused)
	require.Empty(t, resp.PausedReason, "resume clears the provenance")
	require.True(t, resp.PausedAt.IsZero())
	st = read()
	require.True(t, st.Exhausted, "unpaused: the refusal is back")
	require.Equal(t, budget.DecidedByApp, st.DecidedBy)

	// A verdict that was not exhausted is untouched by the switch.
	store.spendBy["ai/"] = 0
	store.accountAllSpend = 0
	_, err = svc.SetAIEnforcementPaused(ctx, budget.SetAIEnforcementPausedRequest{Paused: true, Reason: "drill", ActorID: "ops:owner"})
	require.NoError(t, err)
	st = read()
	require.False(t, st.Exhausted)
	require.Equal(t, budget.DecidedByApp, st.DecidedBy, "paused labels only a verdict it changed")
}

// TestExposureLimitMicros_DelinquencyRuleB pins rule (b): a delinquent
// account is floored at the no-card figure regardless of card or history;
// once settled, each late invoice costs late_penalty_k paid invoices of
// trust; a huge penalty is rule (a) (any late payment resets k); the floor
// can be switched off by config.
func TestExposureLimitMicros_DelinquencyRuleB(t *testing.T) {
	cfg := budget.RiskRampConfig{NoCardMicros: 5_000_000, CardBaseMicros: 10_000_000, CeilingMicros: 200_000_000, Exponent: 0.5, DelinquentFloor: true, LatePenaltyK: 2}

	// Delinquent NOW: floored at $5 regardless of card and k.
	require.EqualValues(t, 5_000_000, budget.ExposureLimitMicros(cfg, budget.ExposureSignals{HasUsableCard: true, PaidInvoices: 24, DelinquentNow: true}), "a delinquent account with a card and 24 paid invoices is floored")
	require.EqualValues(t, 5_000_000, budget.ExposureLimitMicros(cfg, budget.ExposureSignals{HasUsableCard: false, PaidInvoices: 24, DelinquentNow: true}))

	// Settled, with memory: 8 paid, 1 late → k_eff 6 → $10×√7.
	require.EqualValues(t, 26_457_513, budget.ExposureLimitMicros(cfg, budget.ExposureSignals{HasUsableCard: true, PaidInvoices: 8, LateCount: 1}))
	require.Equal(t, 6, budget.EffectivePaid(cfg, budget.ExposureSignals{PaidInvoices: 8, LateCount: 1}))
	// 3 paid, 2 late → k_eff floors at 0 → $10.
	require.EqualValues(t, 10_000_000, budget.ExposureLimitMicros(cfg, budget.ExposureSignals{HasUsableCard: true, PaidInvoices: 3, LateCount: 2}))
	require.Zero(t, budget.EffectivePaid(cfg, budget.ExposureSignals{PaidInvoices: 3, LateCount: 2}))
	// No late history: unchanged curve.
	require.EqualValues(t, 30_000_000, budget.ExposureLimitMicros(cfg, budget.ExposureSignals{HasUsableCard: true, PaidInvoices: 8}))

	// Rule (a) through the same column: a huge penalty resets k on any late payment.
	reset := cfg
	reset.LatePenaltyK = 1_000_000
	require.EqualValues(t, 10_000_000, budget.ExposureLimitMicros(reset, budget.ExposureSignals{HasUsableCard: true, PaidInvoices: 24, LateCount: 1}))
	require.Zero(t, budget.EffectivePaid(reset, budget.ExposureSignals{PaidInvoices: 24, LateCount: 1}))

	// Penalty 0: late history is forgotten.
	none := cfg
	none.LatePenaltyK = 0
	require.EqualValues(t, 30_000_000, budget.ExposureLimitMicros(none, budget.ExposureSignals{HasUsableCard: true, PaidInvoices: 8, LateCount: 5}))

	// Floor switched off: a delinquent account keeps the curve (memory still applies).
	noFloor := cfg
	noFloor.DelinquentFloor = false
	require.EqualValues(t, 26_457_513, budget.ExposureLimitMicros(noFloor, budget.ExposureSignals{HasUsableCard: true, PaidInvoices: 8, LateCount: 1, DelinquentNow: true}))
}

// TestGetBudgetStatus_DelinquentAccountIsFlooredOnTheWire: the pool a
// delinquent PaaS account reports is the $5 floor, and its inputs are echoed
// so the console can say why.
func TestGetBudgetStatus_DelinquentAccountIsFlooredOnTheWire(t *testing.T) {
	store := newFakeStore()
	svc := newService(store)
	acct := uuid.New()
	store.signals[acct] = budget.ExposureSignals{BillingMode: "standard", HasUsableCard: true, PaidInvoices: 24, DelinquentNow: true, LateCount: 1}
	store.accountAllSpend = 4_500_000
	st, err := svc.GetBudgetStatus(context.Background(), budget.GetBudgetStatusRequest{Scope: budget.ScopeAccount, ScopeID: acct, Category: budget.CategoryAI})
	require.NoError(t, err)
	require.EqualValues(t, 5_000_000, st.Pool.LimitMicros)
	require.True(t, st.Pool.DelinquentNow)
	require.Equal(t, 1, st.Pool.LateCount)
	require.Equal(t, 22, st.Pool.EffectivePaid)
	require.False(t, st.Pool.Exhausted)
	store.accountAllSpend = 5_000_000
	st, err = svc.GetBudgetStatus(context.Background(), budget.GetBudgetStatusRequest{Scope: budget.ScopeAccount, ScopeID: acct, Category: budget.CategoryAI})
	require.NoError(t, err)
	require.True(t, st.Exhausted)
	require.Equal(t, budget.DecidedByExposureLimit, st.DecidedBy)
}
