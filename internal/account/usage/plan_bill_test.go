package usage_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// Per-app plans on the bill reads (core-v2#1412, billing-engine#202 PR-1): each
// app is priced from its OWN plan, and the bill says which plan and what it
// includes.

func TestGetAppBill_BaseFeeFollowsTheAppsPlan(t *testing.T) {
	for _, tc := range []struct {
		plan usage.Plan
		want int64
	}{
		{usage.PlanFree, 0},
		{usage.PlanPro, 20_000_000},
		{usage.PlanBusiness, 50_000_000},
	} {
		t.Run(string(tc.plan), func(t *testing.T) {
			store := newFakeStore()
			owner := uuid.New()
			store.accounts[owner] = uuid.New()
			appID := uuid.New()
			store.appMirrors[appID] = usage.AppMirrorInfo{ModuleCount: 1, CreatedAt: time.Now().Add(-48 * time.Hour), Plan: tc.plan}

			resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: appID})
			require.NoError(t, err)
			require.Equal(t, tc.want, resp.BaseFeeMicros, "the base fee is the app's own plan's")
			require.Equal(t, usage.TermsFor(tc.plan), resp.Plan, "the bill names the plan and what it includes")
		})
	}
}

func TestGetAppBill_UnmirroredAppIsOnTheDefaultPlan(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)
	require.Equal(t, usage.TermsFor(usage.DefaultPlan), resp.Plan)
	require.Equal(t, usage.BaseFeeMicros, resp.BaseFeeMicros)
}

func TestGetAccountBill_EachAppCarriesItsOwnPlan(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	business, free := uuid.New(), uuid.New()
	created := time.Now().Add(-48 * time.Hour)
	store.appMirrors[business] = usage.AppMirrorInfo{ModuleCount: 1, CreatedAt: created, Plan: usage.PlanBusiness}
	store.appMirrors[free] = usage.AppMirrorInfo{ModuleCount: 1, CreatedAt: created, Plan: usage.PlanFree}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{OwnerUserID: owner})
	require.NoError(t, err)
	byApp := map[uuid.UUID]usage.AccountAppBill{}
	for _, app := range resp.Apps {
		byApp[app.AppID] = app
	}
	require.Equal(t, usage.PlanBusiness, byApp[business].Plan)
	require.Equal(t, usage.PlanFree, byApp[free].Plan)
	require.Equal(t, int64(50_000_000), byApp[business].BaseFeeMicros)
	require.Zero(t, byApp[free].BaseFeeMicros)
	require.Equal(t, int64(50_000_000), byApp[business].ProjectedBaseFeeMicros)
	require.Zero(t, byApp[free].ProjectedBaseFeeMicros)
	require.Equal(t, int64(50_000_000), resp.ProjectedBaseFeeTotalMicros,
		"the projected base is the sum of each app's own plan base, not apps x one price")
}

func TestGetAccountBill_UnresolvedCreationChargeIsPricedAtTheAppsPlan(t *testing.T) {
	// Period Jul 11 - Aug 11 (31 days); created Jul 25, so 17 days remain and
	// the 3-day grace does not straddle into the next period.
	for _, tc := range []struct {
		plan usage.Plan
		base int64
	}{
		{usage.PlanFree, 0},
		{usage.PlanPro, 20_000_000},
		{usage.PlanBusiness, 50_000_000},
	} {
		t.Run(string(tc.plan), func(t *testing.T) {
			store := newFakeStore()
			owner, accountID := uuid.New(), uuid.New()
			store.accounts[owner] = accountID
			store.anchorDays[accountID] = 11
			createdAt := time.Date(2026, 7, 25, 11, 0, 0, 0, time.UTC)
			appID := seqUUID(1)
			store.appMirrors[appID] = usage.AppMirrorInfo{CreatedAt: createdAt, Plan: tc.plan}
			store.unresolvedOneTimeCharges = []usage.UnresolvedOneTimeChargeRaw{{
				Kind:           usage.UnresolvedOneTimeChargeCreationBase,
				ChargeID:       appID,
				AppID:          appID,
				ChargeAt:       createdAt,
				GraceExpiresAt: usage.GraceExpiry(createdAt),
				ActivatedAt:    time.Date(2026, 6, 11, 9, 0, 0, 0, time.UTC),
			}}
			store.activatedRecurring = &usage.RecurringFeeCounts{}

			bill, err := newService(store).WithNow(func() time.Time { return createdAt }).
				GetAccountBill(context.Background(), usage.GetAccountBillRequest{OwnerUserID: owner})
			require.NoError(t, err)
			pending := bill.ProjectedTotalMicros - bill.ProjectedBaseFeeTotalMicros
			require.InDelta(t, float64(tc.base)*17/31, float64(pending), 1,
				"the pending creation charge prorates the app's own plan base")
		})
	}
}

func TestListNewCreationCharges_PendingPreviewIsTheAppsPlanBase(t *testing.T) {
	// Period Jul 11 - Aug 11 (31 days); created Jul 18, so 24 days remain.
	for _, tc := range []struct {
		plan usage.Plan
		base int64
	}{
		{usage.PlanFree, 0},
		{usage.PlanPro, 20_000_000},
		{usage.PlanBusiness, 50_000_000},
	} {
		t.Run(string(tc.plan), func(t *testing.T) {
			store := newFakeStore()
			owner, accountID := uuid.New(), uuid.New()
			store.accounts[owner] = accountID
			store.anchorDays[accountID] = 11
			createdAt := time.Date(2026, 7, 18, 9, 0, 0, 0, time.UTC)
			store.appMirrors[uuid.New()] = usage.AppMirrorInfo{CreatedAt: createdAt, Name: "Plan App", Plan: tc.plan}

			resp, err := newService(store).WithNow(func() time.Time { return createdAt.Add(3 * time.Hour) }).
				ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{OwnerUserID: owner})
			require.NoError(t, err)
			require.Len(t, resp.Charges, 1)
			require.InDelta(t, float64(tc.base)*24/31, float64(resp.Charges[0].BaseFeeMicros), 1,
				"the pending preview prorates the app's own plan base")
		})
	}
}

func TestGetAccountBill_FrozenAttemptDeductsTheAppsOwnPlanUnit(t *testing.T) {
	// A frozen creation attempt whose straddled full period is exactly the
	// projected recurring period deducts ONE full unit of the app's plan base:
	// $50 for Business, not the pre-plan $20.
	activatedAt := time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	projectedStart := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	const partial = int64(1_290_323)
	store := newFakeStore()
	owner, accountID, appID := uuid.New(), uuid.New(), uuid.New()
	store.accounts[owner] = accountID
	store.anchorDays[accountID] = activatedAt.Day()
	store.appMirrors[appID] = usage.AppMirrorInfo{CreatedAt: time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC), Plan: usage.PlanBusiness}
	store.unresolvedOneTimeCharges = []usage.UnresolvedOneTimeChargeRaw{{
		Kind:                      usage.UnresolvedOneTimeChargeCreationBase,
		ChargeID:                  appID,
		AppID:                     appID,
		ChargeAt:                  time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
		GraceExpiresAt:            time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC),
		ActivatedAt:               activatedAt,
		CountsTowardRecurring:     true,
		Frozen:                    true,
		FrozenAmountMicros:        partial + 50_000_000,
		FrozenSnapshotPeriodStart: time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC),
		FrozenSnapshotPeriodEnd:   projectedStart,
		FrozenSnapshotBaseMicros:  partial,
		FrozenHasStraddle:         true,
		FrozenStraddlePeriodStart: projectedStart,
		FrozenStraddlePeriodEnd:   time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC),
		FrozenStraddleBaseMicros:  50_000_000,
	}}

	bill, err := newService(store).WithNow(func() time.Time { return time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC) }).
		GetAccountBill(context.Background(), usage.GetAccountBillRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Equal(t, partial, bill.ProjectedTotalMicros-bill.ProjectedBaseFeeTotalMicros,
		"the straddled Business period is already in the recurring forecast, so exactly one $50 unit is deducted")
}
