package cycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// GetAppPlan / SetAppPlan (core-v2#1412, billing-engine#202 PR-1).

func requirePlanErrCode(t *testing.T, err error, want billing.Code) {
	t.Helper()
	var be *billing.Error
	require.True(t, errors.As(err, &be), "err = %v, want a *billing.Error", err)
	require.Equal(t, want, be.Code)
}

func registeredPlanApp(t *testing.T) (*fakeStore, *cycle.Service, uuid.UUID) {
	t.Helper()
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc := appsSvc(store, newFakeStripe())
	appID := uuid.New()
	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, ModuleCount: 1,
		CreatedAt: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	return store, svc, appID
}

// TestSetAppPlan_OnlyProUntilTheChargeLegsArePlanAware pins the deliberate
// refusal: free and business are refused until the charge legs bill them.
func TestSetAppPlan_OnlyProUntilTheChargeLegsArePlanAware(t *testing.T) {
	store, svc, appID := registeredPlanApp(t)

	resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	require.Equal(t, usage.TermsFor(usage.PlanPro), resp.Terms)
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan)

	for _, plan := range []string{"free", "business"} {
		_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: plan})
		requirePlanErrCode(t, err, billing.CodePlanNotAvailable)
		require.Equal(t, usage.PlanPro, store.apps[appID].Plan, "a refused change moves nothing")
	}
}

func TestSetAppPlan_RejectsUnknownPlansAndAbsentApps(t *testing.T) {
	store, svc, appID := registeredPlanApp(t)

	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "hobby"})
	requirePlanErrCode(t, err, billing.CodeInvalidInput)
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodeInvalidInput)
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: uuid.New(), Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodeNotFound)

	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodeNotFound)

	store.errSetPlan = errors.New("db down")
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: uuid.New(), Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodeInternal)
}

func TestGetAppPlan_ReadsTheMirrorElseTheDefaultPlan(t *testing.T) {
	store, svc, appID := registeredPlanApp(t)

	resp, err := svc.GetAppPlan(context.Background(), cycle.GetAppPlanRequest{AppID: uuid.New()})
	require.NoError(t, err)
	require.Equal(t, usage.TermsFor(usage.DefaultPlan), resp.Terms, "an unmirrored app is on the default plan")

	app := store.apps[appID]
	app.Plan = usage.PlanBusiness // as a later, plan-aware change would write it
	store.apps[appID] = app
	resp, err = svc.GetAppPlan(context.Background(), cycle.GetAppPlanRequest{AppID: appID})
	require.NoError(t, err)
	require.Equal(t, usage.TermsFor(usage.PlanBusiness), resp.Terms)

	_, err = svc.GetAppPlan(context.Background(), cycle.GetAppPlanRequest{})
	requirePlanErrCode(t, err, billing.CodeInvalidInput)
}
