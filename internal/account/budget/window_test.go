package budget_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/budget"
)

// Spend windows (migration 097, billing-engine#378). Every test drives the
// service clock through WithNow and the fake's event timeline, so the UTC day
// boundary is exercised on purpose and never by the machine's clock or zone.

func utc(y int, m time.Month, d, h, min int) time.Time {
	return time.Date(y, m, d, h, min, 0, 0, time.UTC)
}

// clock is a settable service clock.
type clock struct{ now time.Time }

func (c *clock) fn() func() time.Time { return func() time.Time { return c.now } }

// dayOrg wires an org whose AI spend is billed to its own account and returns
// (org, account).
func dayOrg(store *fakeStore) (uuid.UUID, uuid.UUID) {
	org, acct := uuid.New(), uuid.New()
	store.orgAccounts[org] = acct
	return org, acct
}

// TestSetBudget_WindowIsAClosedListDefaultingToPeriod: an omitted window
// stores "period" (every pre-097 caller keeps its meaning), "day" and
// "period" are stored and echoed, and anything else — including a near miss —
// is INVALID_INPUT with nothing written.
func TestSetBudget_WindowIsAClosedListDefaultingToPeriod(t *testing.T) {
	store := newFakeStore()
	svc := newService(store)
	ctx := context.Background()
	org, _ := dayOrg(store)
	key := budgetKey(budget.ScopeOrg, org, budget.CategoryAI, "")
	req := budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI, LimitMicros: 3_000_000, Active: true, HardCap: true}

	resp, err := svc.SetBudget(ctx, req)
	require.NoError(t, err)
	require.Equal(t, budget.WindowPeriod, resp.Window, "no window = the period, as before 097")
	require.Equal(t, budget.WindowPeriod, store.budgets[key].Window)

	req.Window = budget.WindowDay
	resp, err = svc.SetBudget(ctx, req)
	require.NoError(t, err)
	require.Equal(t, budget.WindowDay, resp.Window)
	require.Equal(t, budget.WindowDay, store.budgets[key].Window)

	for _, bad := range []budget.Window{"week", "Day", "DAY", "daily", " day", "month"} {
		req.Window = bad
		_, err = svc.SetBudget(ctx, req)
		requireCode(t, err, billing.CodeInvalidInput)
		require.Equal(t, budget.WindowDay, store.budgets[key].Window, "a refused window %q writes nothing", bad)
	}

	req.Window = budget.WindowPeriod
	resp, err = svc.SetBudget(ctx, req)
	require.NoError(t, err)
	require.Equal(t, budget.WindowPeriod, resp.Window)

	// A re-set replaces every field, the window included: omitting it after a
	// day row stores the period again, exactly as an omitted hard_cap stores false.
	req.Window = budget.WindowDay
	_, err = svc.SetBudget(ctx, req)
	require.NoError(t, err)
	req.Window = ""
	resp, err = svc.SetBudget(ctx, req)
	require.NoError(t, err)
	require.Equal(t, budget.WindowPeriod, resp.Window)
}

// TestGetBudgetStatus_DayBudgetRefusesUntilTheNextUTCDay is row TK28's case:
// an org AI budget of US$3 a day, hard, allow_overage=false. Yesterday's $5
// never counts; the day refuses once today's spend reaches $3, stays refused
// to the last nanosecond of the UTC day — whatever zone the clock reads in —
// and answers again at 00:00 UTC. The 50/80/100 marks read the same day spend.
func TestGetBudgetStatus_DayBudgetRefusesUntilTheNextUTCDay(t *testing.T) {
	store := newFakeStore()
	c := &clock{now: utc(2026, 11, 25, 10, 0)}
	svc := newService(store).WithNow(c.fn())
	ctx := context.Background()
	org, _ := dayOrg(store)
	store.events = []spendEvent{
		{at: utc(2026, 11, 24, 23, 30), micros: 5_000_000}, // yesterday: the period sees it, the day never does
		{at: utc(2026, 11, 25, 1, 0), micros: 1_500_000},
	}
	_, err := svc.SetBudget(ctx, budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI,
		LimitMicros: 3_000_000, AlertPercents: []int{50, 80, 100}, Active: true, HardCap: true, Window: budget.WindowDay})
	require.NoError(t, err)
	status := func() *budget.GetBudgetStatusResponse {
		t.Helper()
		st, err := svc.GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI})
		require.NoError(t, err)
		return st
	}

	st := status()
	require.Equal(t, budget.WindowDay, st.Window)
	require.Equal(t, utc(2026, 11, 25, 0, 0), st.PeriodStart)
	require.Equal(t, utc(2026, 11, 26, 0, 0), st.PeriodEnd)
	require.EqualValues(t, 1_500_000, st.SpendMicros, "today only: yesterday's $5 is outside the day")
	require.Equal(t, 50, st.PercentUsed)
	require.Equal(t, []int{50}, st.Crossed)
	require.False(t, st.Exhausted)
	require.EqualValues(t, 1_500_000, st.RemainingMicros)

	store.events = append(store.events, spendEvent{at: utc(2026, 11, 25, 9, 59), micros: 1_500_000})
	st = status()
	require.EqualValues(t, 3_000_000, st.SpendMicros)
	require.Equal(t, []int{50, 80, 100}, st.Crossed)
	require.True(t, st.Exhausted, "the day's spend reached the limit: refuse")
	require.Equal(t, budget.DecidedByOrg, st.DecidedBy)
	require.Zero(t, st.RemainingMicros)

	c.now = time.Date(2026, 11, 25, 23, 59, 59, 999_999_999, time.UTC)
	require.True(t, status().Exhausted, "still the same UTC day")

	// 07:30 in UTC+8 on the 26th is 23:30 UTC on the 25th: the day is UTC's,
	// never the clock's zone.
	c.now = time.Date(2026, 11, 26, 7, 30, 0, 0, time.FixedZone("UTC+8", 8*3600))
	st = status()
	require.True(t, st.Exhausted)
	require.Equal(t, utc(2026, 11, 25, 0, 0), st.PeriodStart)

	c.now = utc(2026, 11, 26, 0, 0)
	st = status()
	require.False(t, st.Exhausted, "00:00 UTC: a new day, the cap answers again")
	require.Zero(t, st.SpendMicros)
	require.Empty(t, st.Crossed)
	require.Equal(t, utc(2026, 11, 26, 0, 0), st.PeriodStart)
	require.Equal(t, utc(2026, 11, 27, 0, 0), st.PeriodEnd)
}

// TestGetBudgetStatus_PeriodBudgetUnchanged is the control: the same timeline
// and clock with no window in the request measure the whole anchored period,
// so yesterday's $5 counts and the cap stays exhausted across the UTC day.
func TestGetBudgetStatus_PeriodBudgetUnchanged(t *testing.T) {
	store := newFakeStore()
	c := &clock{now: utc(2026, 11, 25, 10, 0)}
	svc := newService(store).WithNow(c.fn())
	ctx := context.Background()
	org, _ := dayOrg(store)
	store.events = []spendEvent{
		{at: utc(2026, 11, 24, 23, 30), micros: 5_000_000},
		{at: utc(2026, 11, 25, 1, 0), micros: 1_500_000},
	}
	_, err := svc.SetBudget(ctx, budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI,
		LimitMicros: 3_000_000, AlertPercents: []int{50, 80, 100}, Active: true, HardCap: true})
	require.NoError(t, err)

	for _, now := range []time.Time{utc(2026, 11, 25, 10, 0), utc(2026, 11, 26, 0, 0)} {
		c.now = now
		st, err := svc.GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI})
		require.NoError(t, err)
		require.Equal(t, budget.WindowPeriod, st.Window)
		require.Equal(t, utc(2026, 11, 1, 0, 0), st.PeriodStart, "the anchored period (anchor day 1 = the calendar month)")
		require.Equal(t, utc(2026, 12, 1, 0, 0), st.PeriodEnd)
		require.EqualValues(t, 6_500_000, st.SpendMicros)
		require.Equal(t, []int{50, 80, 100}, st.Crossed)
		require.True(t, st.Exhausted, "a period cap does not reset at 00:00 UTC")
	}
}

// TestGetBudgetStatus_TemplateDayRowComposesWithAPeriodAppRow: each row is
// measured over its OWN window. A template's $2/day cap refuses today and
// answers tomorrow; the app's $10 AI-wide period cap beneath it still sums the
// whole period and bites once that is spent, whatever the day.
func TestGetBudgetStatus_TemplateDayRowComposesWithAPeriodAppRow(t *testing.T) {
	store := newFakeStore()
	c := &clock{now: utc(2026, 11, 25, 12, 0)}
	svc := newService(store).WithNow(c.fn())
	ctx := context.Background()
	app := uuid.New()
	store.events = []spendEvent{{at: utc(2026, 11, 25, 8, 0), micros: 2_000_000}}
	for _, r := range []budget.SetBudgetRequest{
		{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI, TemplateKey: "member-help", LimitMicros: 2_000_000, Active: true, HardCap: true, Window: budget.WindowDay},
		{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI, LimitMicros: 10_000_000, Active: true, HardCap: true},
	} {
		_, err := svc.SetBudget(ctx, r)
		require.NoError(t, err)
	}
	read := func() *budget.GetBudgetStatusResponse {
		t.Helper()
		st, err := svc.GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI, TemplateKey: "member-help"})
		require.NoError(t, err)
		return st
	}

	st := read()
	require.True(t, st.Exhausted)
	require.Equal(t, budget.DecidedByTemplate, st.DecidedBy)
	require.Equal(t, budget.WindowDay, st.Window)
	require.Equal(t, utc(2026, 11, 25, 0, 0), st.PeriodStart)

	c.now = utc(2026, 11, 26, 0, 0)
	st = read()
	require.False(t, st.Exhausted, "the template's day is over")
	require.Equal(t, budget.DecidedByTemplate, st.DecidedBy, "the most specific row is reported when none bit")
	require.Zero(t, st.SpendMicros)

	store.events = append(store.events, spendEvent{at: utc(2026, 11, 20, 8, 0), micros: 8_000_000}) // earlier this period
	st = read()
	require.True(t, st.Exhausted, "the app's period cap ($10) is spent: a day row never shadows it")
	require.Equal(t, budget.DecidedByApp, st.DecidedBy)
	require.Equal(t, budget.WindowPeriod, st.Window)
	require.Equal(t, utc(2026, 11, 1, 0, 0), st.PeriodStart)
	require.EqualValues(t, 10_000_000, st.SpendMicros)
}

// TestEvaluateAccountBudget_DayRowRecordsEachUTCDay: the ingest path records a
// day row's 50/80/100 crossings under the day's own start — once per day, and
// again the next day — while a period row on the same evaluation keeps the
// period key. GetBudgetAlerts defaults to the row's current window.
func TestEvaluateAccountBudget_DayRowRecordsEachUTCDay(t *testing.T) {
	store := newFakeStore()
	c := &clock{now: utc(2026, 11, 25, 10, 0)}
	svc := newService(store).WithNow(c.fn())
	ctx := context.Background()
	org, acct := dayOrg(store)
	pStart, pEnd := utc(2026, 11, 1, 0, 0), utc(2026, 12, 1, 0, 0)
	store.events = []spendEvent{
		{at: utc(2026, 11, 24, 12, 0), micros: 20_000_000},
		{at: utc(2026, 11, 25, 1, 0), micros: 3_000_000},
	}
	_, err := svc.SetBudget(ctx, budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI,
		LimitMicros: 3_000_000, AlertPercents: []int{50, 80, 100}, Active: true, HardCap: true, Window: budget.WindowDay})
	require.NoError(t, err)
	_, err = svc.SetBudget(ctx, budget.SetBudgetRequest{Scope: budget.ScopeAccount, ScopeID: acct, Category: budget.CategoryAI,
		LimitMicros: 100_000_000, AlertPercents: []int{20, 50}, Active: true})
	require.NoError(t, err)
	orgRow := store.budgets[budgetKey(budget.ScopeOrg, org, budget.CategoryAI, "")]
	acctRow := store.budgets[budgetKey(budget.ScopeAccount, acct, budget.CategoryAI, "")]

	fired, err := svc.EvaluateAccountBudget(ctx, acct, org, pStart, pEnd)
	require.NoError(t, err)
	require.ElementsMatch(t, []int{20, 50, 80, 100}, fired, "account period row: $23 of $100 = 20; org day row: $3 of $3 = 50, 80, 100")
	day25 := utc(2026, 11, 25, 0, 0)
	for _, p := range []int{50, 80, 100} {
		a, ok := store.alerts[alertKey{budgetID: orgRow.ID, periodStart: day25, percent: p}]
		require.True(t, ok, "the day row's %d%% is keyed on the day's start", p)
		require.EqualValues(t, 3_000_000, a.SpendMicros)
	}
	_, ok := store.alerts[alertKey{budgetID: acctRow.ID, periodStart: pStart, percent: 20}]
	require.True(t, ok, "the period row keeps the period key")

	fired, err = svc.EvaluateAccountBudget(ctx, acct, org, pStart, pEnd)
	require.NoError(t, err)
	require.Empty(t, fired, "once per day")

	c.now = utc(2026, 11, 26, 9, 0)
	store.events = append(store.events, spendEvent{at: utc(2026, 11, 26, 8, 0), micros: 1_500_000})
	fired, err = svc.EvaluateAccountBudget(ctx, acct, org, pStart, pEnd)
	require.NoError(t, err)
	require.Equal(t, []int{50}, fired, "a new UTC day: the day row's 50% fires again on $1.5")

	got, err := svc.GetBudgetAlerts(ctx, budget.GetBudgetAlertsRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI})
	require.NoError(t, err)
	require.Len(t, got.Alerts, 1, "the default window is today")
	require.Equal(t, 50, got.Alerts[0].Percent)
	got, err = svc.GetBudgetAlerts(ctx, budget.GetBudgetAlertsRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI, PeriodStart: day25})
	require.NoError(t, err)
	require.Len(t, got.Alerts, 3, "yesterday's crossings stay readable under yesterday's start")
}

// TestEvaluateAppAndSandbox_DayRowsReadTheDay: the app hook and the sandbox
// hook read a day row's spend over the UTC day and key it there too.
func TestEvaluateAppAndSandbox_DayRowsReadTheDay(t *testing.T) {
	store := newFakeStore()
	svc := newService(store).WithNow(func() time.Time { return utc(2026, 11, 25, 10, 0) })
	ctx := context.Background()
	app := uuid.New()
	org, acct := dayOrg(store)
	pStart, pEnd := utc(2026, 11, 1, 0, 0), utc(2026, 12, 1, 0, 0)
	day := utc(2026, 11, 25, 0, 0)
	store.events = []spendEvent{
		{at: utc(2026, 11, 3, 0, 0), micros: 9_000_000},
		{at: utc(2026, 11, 25, 2, 0), micros: 1_000_000},
	}
	_, err := svc.SetBudget(ctx, budget.SetBudgetRequest{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI,
		LimitMicros: 2_000_000, AlertPercents: []int{50, 100}, Active: true, HardCap: true, Window: budget.WindowDay})
	require.NoError(t, err)
	fired, err := svc.EvaluateAppBudget(ctx, app, "", pStart, pEnd)
	require.NoError(t, err)
	require.Equal(t, []int{50}, fired, "$1 of $2 today; the $9 on the 3rd is another day")
	require.Equal(t, day, store.gotSpendStart)
	require.Equal(t, day.AddDate(0, 0, 1), store.gotSpendEnd)
	appRow := store.budgets[budgetKey(budget.ScopeApp, app, budget.CategoryAI, "")]
	_, ok := store.alerts[alertKey{budgetID: appRow.ID, periodStart: day, percent: 50}]
	require.True(t, ok)

	_, err = svc.SetBudget(ctx, budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategorySandbox,
		LimitMicros: 1_000_000, AlertPercents: []int{100}, Active: true, HardCap: true, Window: budget.WindowDay})
	require.NoError(t, err)
	fired, err = svc.EvaluateSandboxBudget(ctx, acct, org, pStart, pEnd)
	require.NoError(t, err)
	require.Equal(t, []int{100}, fired)
	sbRow := store.budgets[budgetKey(budget.ScopeOrg, org, budget.CategorySandbox, "")]
	_, ok = store.alerts[alertKey{budgetID: sbRow.ID, periodStart: day, percent: 100}]
	require.True(t, ok)
}
