//go:build integration

package budget_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/budget"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// TestPgxStore_SpendWindow pins migration 097 against a real Postgres: a row
// written the pre-097 way (no spend_window) reads as a period row, the CHECK
// refuses a window outside the closed list, the store round-trips 'day' and
// writes 'period' for a Go row without one, and the TK28 shape — an org AI cap
// on a UTC day — refuses on today's real spend and answers again at 00:00 UTC.
func TestPgxStore_SpendWindow(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := budget.NewStore(pool)
	ctx := context.Background()
	org, other := uuid.New(), uuid.New()

	// A binary built before 097 names no spend_window on its INSERT.
	_, err := pool.Exec(ctx, `INSERT INTO ms_billing.budgets (scope, scope_id, category, limit_micros) VALUES ('org', $1, 'ai', 1)`, other.String())
	require.NoError(t, err)
	got, found, err := store.GetBudget(ctx, budget.ScopeOrg, other, budget.CategoryAI, "")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, budget.WindowPeriod, got.Window, "an existing or pre-097 row keeps the period")
	_, err = pool.Exec(ctx, `UPDATE ms_billing.budgets SET spend_window = 'week' WHERE scope_id = $1`, other.String())
	require.Error(t, err, "the CHECK holds the closed list")

	// A Go row without a window (the system exposure row's shape) is stored as period.
	saved, err := store.UpsertBudget(ctx, budget.Budget{Scope: budget.ScopeOrg, ScopeID: other, Category: budget.CategoryAI, LimitMicros: 2, AlertPercents: []int{100}, Active: true})
	require.NoError(t, err)
	require.Equal(t, budget.WindowPeriod, saved.Window)

	acct := seedAccountRow(t, pool, "org", org)
	app := uuid.New()
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	svc := budget.NewService(store).WithNow(func() time.Time { return now })
	_, err = svc.SetBudget(ctx, budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI,
		LimitMicros: 30_000, AlertPercents: []int{50, 80, 100}, Active: true, HardCap: true, Window: budget.WindowDay})
	require.NoError(t, err)
	got, found, err = store.GetBudget(ctx, budget.ScopeOrg, org, budget.CategoryAI, "")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, budget.WindowDay, got.Window)

	// Haiku input at 1,000 µ$ per 1k: yesterday 50,000 µ$, today 30,000 µ$.
	seedAIEvent(t, pool, acct, app, "infra.ai.input.tokens", 50, haiku, "", time.Date(2026, 11, 24, 23, 30, 0, 0, time.UTC), false)
	seedAIEvent(t, pool, acct, app, "infra.ai.input.tokens", 30, haiku, "", time.Date(2026, 11, 25, 1, 0, 0, 0, time.UTC), false)
	req := budget.GetBudgetStatusRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI}

	st, err := svc.GetBudgetStatus(ctx, req)
	require.NoError(t, err)
	require.Equal(t, budget.WindowDay, st.Window)
	require.Equal(t, time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC), st.PeriodStart.UTC())
	require.EqualValues(t, 30_000, st.SpendMicros, "today's event only")
	require.Equal(t, []int{50, 80, 100}, st.Crossed)
	require.True(t, st.Exhausted)
	require.Equal(t, budget.DecidedByOrg, st.DecidedBy)

	now = time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC)
	st, err = svc.GetBudgetStatus(ctx, req)
	require.NoError(t, err)
	require.Zero(t, st.SpendMicros)
	require.False(t, st.Exhausted, "a new UTC day answers again")

	// Control: the same org's cap re-set without a window measures the period.
	_, err = svc.SetBudget(ctx, budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategoryAI,
		LimitMicros: 30_000, AlertPercents: []int{50, 80, 100}, Active: true, HardCap: true})
	require.NoError(t, err)
	st, err = svc.GetBudgetStatus(ctx, req)
	require.NoError(t, err)
	require.Equal(t, budget.WindowPeriod, st.Window)
	require.EqualValues(t, 80_000, st.SpendMicros, "the period sums both days")
	require.True(t, st.Exhausted)
}
