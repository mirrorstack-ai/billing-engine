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
