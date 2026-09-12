package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// Per-app plans on the charge legs (core-v2#1412, billing-engine#202 PR-2):
// every leg that bills an app's base bills that app's OWN plan base.

func setPlan(store *fakeStore, appID uuid.UUID, plan usage.Plan) {
	app := store.apps[appID]
	app.Plan = plan
	store.apps[appID] = app
}

func TestRunBillingCycle_AdvanceBaseIsEachAppsPlanBase(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_plan_base"
	store.activation[chargeAccount] = time.Date(2026, 1, 31, 9, 0, 0, 0, time.UTC)
	for _, plan := range []usage.Plan{usage.PlanFree, usage.PlanPro, usage.PlanBusiness} {
		setPlan(store, seedApp(store, chargeAccount, 0, false), plan)
	}

	svc, _ := chargeSvcProposing(store, newFakeStripe())
	resp, err := svc.RunBillingCycle(context.Background(), chargeAccount,
		time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC), time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC), 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, resp.Status)
	require.EqualValues(t, 70_000_000, resp.AdvanceBaseMicros, "Free $0 + Pro $20 + Business $50, not 3 x one price")

}

func TestChargeCreationProration_ChargesTheAppsPlanBase(t *testing.T) {
	// Anchor day 11, created Jul 17: 25 of 31 days remain. Pro 20e6 x 25/31 =
	// 16_129_032 micros → 1613 cents; Business 50e6 x 25/31 → 4032 cents.
	for _, tc := range []struct {
		plan      usage.Plan
		wantCents int64
	}{
		{usage.PlanPro, 1613},
		{usage.PlanBusiness, 4032},
	} {
		t.Run(string(tc.plan), func(t *testing.T) {
			store := newFakeStore()
			user, acct := registeredAccount(store)
			store.activation[acct] = time.Date(2026, 5, 11, 9, 0, 0, 0, time.UTC)
			createdAt := time.Date(2026, 7, 17, 12, 34, 0, 0, time.UTC)
			sc := newFakeStripe()
			sc.invoiceAmountDue = tc.wantCents
			p := &capturingProposer{}
			svc := cycle.NewService(store, sc).
				WithNow(func() time.Time { return usage.GraceExpiry(createdAt).Add(time.Hour) }).
				WithIntentProposer(p)
			appID := uuid.New()
			registerMirror(t, svc, user, appID, createdAt, 0)
			setPlan(store, appID, tc.plan)

			resp, err := svc.ChargeCreationProration(context.Background(), appID)
			require.NoError(t, err)
			require.Equal(t, cycle.ProrationStatusProposed, resp.Status)
			require.Equal(t, tc.wantCents, sealedProrationCents(t, p), "the creation charge prorates the app's own plan base")
		})
	}
}

func TestChargeCreationProration_FreeAppSealsNothing(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	store.activation[acct] = time.Date(2026, 5, 11, 9, 0, 0, 0, time.UTC)
	createdAt := time.Date(2026, 7, 17, 12, 34, 0, 0, time.UTC)
	p := &capturingProposer{}
	svc := cycle.NewService(store, newFakeStripe()).
		WithNow(func() time.Time { return usage.GraceExpiry(createdAt).Add(time.Hour) }).
		WithIntentProposer(p)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, createdAt, 0)
	setPlan(store, appID, usage.PlanFree)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	if resp != nil {
		require.NotEqual(t, cycle.ProrationStatusProposed, resp.Status)
	}
	require.Empty(t, p.charges, "a Free app has no base, so its creation charge seals nothing")
}

func TestChargeCreationProration_CreditModeDrawsTheAppsPlanBase(t *testing.T) {
	// Anchor day 4 (registeredAccount), created Jun 19: 15 of 30 days remain.
	for _, tc := range []struct {
		plan usage.Plan
		want int64
	}{
		{usage.PlanPro, 10_000_000},
		{usage.PlanBusiness, 25_000_000},
	} {
		t.Run(string(tc.plan), func(t *testing.T) {
			store := newFakeStore()
			user, acct := registeredAccount(store)
			store.walletMode = cycle.CreditBillingModeCredits
			seedWalletSource(store, "grant", 100_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
			svc := appsSvc(store, newFakeStripe()).WithCreditWallet(true)
			created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
			appID := uuid.New()
			registerMirror(t, svc, user, appID, created, 0)
			setPlan(store, appID, tc.plan)

			resp, err := svc.ChargeCreationProration(context.Background(), appID)
			require.NoError(t, err)
			require.Equal(t, cycle.ProrationStatusWalletCharged, resp.Status)
			require.EqualValues(t, tc.want, store.creationDrawn[appID], "the wallet draws the app's own prorated plan base")
			snap, ok := store.baseSnapshots[snapKey{appID, billingPeriodStart(store, acct, created)}]
			require.True(t, ok)
			require.EqualValues(t, tc.want, snap.snap.BaseMicros, "the display snapshot freezes what was drawn")
		})
	}
}

// seedPlannedApps seeds one live app per plan and returns each app's plan base.
func seedPlannedApps(store *fakeStore) map[uuid.UUID]int64 {
	want := map[uuid.UUID]int64{}
	for plan, base := range map[usage.Plan]int64{usage.PlanFree: 0, usage.PlanPro: 20_000_000, usage.PlanBusiness: 50_000_000} {
		id := seedApp(store, chargeAccount, 0, false)
		setPlan(store, id, plan)
		want[id] = base
	}
	return want
}

func requireAdvanceSnapshots(t *testing.T, store *fakeStore, want map[uuid.UUID]int64) {
	t.Helper()
	for id, base := range want {
		snap, ok := store.baseSnapshots[snapKey{id, periodEnd}]
		require.True(t, ok, "every live app gets the new period's display snapshot")
		require.Equal(t, base, snap.snap.BaseMicros, "the display snapshot freezes the app's own plan base")
	}
}

func TestRunBillingCycle_CreditsModeSnapshotsEachAppsPlanBase(t *testing.T) {
	store := newFakeStore()
	store.walletMode = cycle.CreditBillingModeCredits
	store.chargedTotal = 1_000_000
	seedWalletSource(store, "grant", 100_000_000, time.Time{}, timeUTC(2026, 1, 1, 0))
	want := seedPlannedApps(store)

	resp, err := chargeSvc(store, newFakeStripe()).WithCreditWallet(true).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 70_000_000, resp.AdvanceBaseMicros)
	requireAdvanceSnapshots(t, store, want)
}

func TestRunBillingCycle_RecoveredInvoiceSnapshotsEachAppsPlanBase(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_plan_recovered"
	want := seedPlannedApps(store)
	sc := newFakeStripe()
	runID := seedFrozenRun(t, store, sc, 7_100)
	sc.setFindByRef("run:"+runID.String(), billingstripe.Invoice{
		ID: "in_plan_recovered", Status: "paid", AmountDue: 7_100, AmountPaid: 7_100, Currency: "usd",
	})

	svc, _ := chargeSvcProposing(store, sc)
	resp, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	requireAdvanceSnapshots(t, store, want)
}
