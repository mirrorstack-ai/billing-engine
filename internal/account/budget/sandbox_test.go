package budget_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/budget"
)

func seedSandboxBudget(store *fakeStore, scope budget.Scope, id uuid.UUID, limit int64) {
	store.budgets[budgetKey(scope, id, budget.CategorySandbox, "")] = budget.Budget{
		ID: uuid.New(), Scope: scope, ScopeID: id, Category: budget.CategorySandbox,
		LimitMicros: limit, AlertPercents: []int{80, 100}, Active: true, HardCap: true,
	}
}

func TestSetBudget_Sandbox_OrgAndAccountOnly(t *testing.T) {
	ctx, store := context.Background(), newFakeStore()
	for _, scope := range []budget.Scope{budget.ScopeOrg, budget.ScopeAccount} {
		got, err := newService(store).SetBudget(ctx, budget.SetBudgetRequest{Scope: scope, ScopeID: uuid.New(),
			Category: budget.CategorySandbox, LimitMicros: 25_000_000, Active: true, HardCap: true})
		require.NoError(t, err, "scope %s", scope)
		require.Equal(t, budget.CategorySandbox, got.Category)
	}
	for _, req := range []budget.SetBudgetRequest{ // never per app, never per template
		{Scope: budget.ScopeApp, ScopeID: uuid.New(), Category: budget.CategorySandbox, LimitMicros: 1},
		{Scope: budget.ScopeOrg, ScopeID: uuid.New(), Category: budget.CategorySandbox, TemplateKey: "faq", LimitMicros: 1},
	} {
		_, err := newService(store).SetBudget(ctx, req)
		requireCode(t, err, billing.CodeInvalidInput)
	}
}

// Hard cap exhausts at the limit and not below; an org without a row is
// unaffected by another org's row; the account's ordinary AI spend never moves it.
func TestGetBudgetStatus_Sandbox_HardCapAndIsolation(t *testing.T) {
	ctx, store := context.Background(), newFakeStore()
	orgA, acctA, orgB, acctB := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	store.orgAccounts[orgA], store.orgAccounts[orgB] = acctA, acctB
	store.accountSpend = 50_000_000 // ordinary AI spend, huge
	store.sandboxSpend = map[uuid.UUID]int64{acctA: 3_999_999}
	seedSandboxBudget(store, budget.ScopeOrg, orgA, 4_000_000)
	read := func(org uuid.UUID) *budget.GetBudgetStatusResponse {
		st, err := newService(store).GetBudgetStatus(ctx, budget.GetBudgetStatusRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: budget.CategorySandbox})
		require.NoError(t, err)
		return st
	}

	st := read(orgA)
	require.EqualValues(t, 3_999_999, st.SpendMicros)
	require.False(t, st.Exhausted, "below the cap (control)")
	require.Nil(t, st.Pool, "the trial is not PaaS exposure")
	store.sandboxSpend[acctA] = 4_000_000
	require.True(t, read(orgA).Exhausted)
	require.False(t, read(orgB).Exists)
	require.False(t, read(orgB).Exhausted)
}

func TestEvaluateSandboxBudget_RecordsCrossingsOnAccountThenOrgRow(t *testing.T) {
	ctx, store := context.Background(), newFakeStore()
	acct, org := uuid.New(), uuid.New()
	start, end := period()
	store.sandboxSpend = map[uuid.UUID]int64{acct: 4_000_000}
	seedSandboxBudget(store, budget.ScopeAccount, acct, 5_000_000) // 80%
	seedSandboxBudget(store, budget.ScopeOrg, org, 4_000_000)      // 100%

	fired, err := newService(store).EvaluateSandboxBudget(ctx, acct, org, start, end)
	require.NoError(t, err)
	require.ElementsMatch(t, []int{80, 80, 100}, fired)
	fired, err = newService(store).EvaluateSandboxBudget(ctx, acct, org, start, end)
	require.NoError(t, err)
	require.Empty(t, fired, "each crossing is recorded once")
}
