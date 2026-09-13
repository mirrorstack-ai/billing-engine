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

// GetAppPlan / SetAppPlan (core-v2#1412, billing-engine#202 PR-1 and PR-2b).
// The plan-change scenarios themselves live in plan_change_test.go; this file
// keeps the request-shape and refusal pins.

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

// TestSetAppPlan_BusinessStillRefused pins the one deliberate refusal left:
// business waits for its per-app module allowance (PR-2c). Free is accepted
// now (plan_change_test.go), and the current plan is a no-op.
func TestSetAppPlan_BusinessStillRefused(t *testing.T) {
	store, svc, appID := registeredPlanApp(t)

	resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	require.Equal(t, usage.TermsFor(usage.PlanPro), resp.Terms)
	require.Nil(t, resp.Change, "asking for the current plan changes nothing")
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan)

	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "business"})
	requirePlanErrCode(t, err, billing.CodePlanNotAvailable)
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan, "a refused change moves nothing")
	require.Empty(t, store.planChanges, "a refused change writes no ledger row")
}

func TestSetAppPlan_RejectsUnknownPlansAndAbsentApps(t *testing.T) {
	_, svc, appID := registeredPlanApp(t)

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

	// A store failure while opening the change surfaces as INTERNAL, and the
	// plan does not move.
	store2, svc2, app2 := registeredPlanApp(t)
	store2.errOpenPlanChange = errors.New("db down")
	_, err = svc2.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: app2, Plan: "free"})
	requirePlanErrCode(t, err, billing.CodeInternal)
	require.Equal(t, usage.PlanPro, store2.apps[app2].Plan)
}

func TestGetAppPlan_ReadsTheMirrorElseTheDefaultPlan(t *testing.T) {
	store, svc, appID := registeredPlanApp(t)

	resp, err := svc.GetAppPlan(context.Background(), cycle.GetAppPlanRequest{AppID: uuid.New()})
	require.NoError(t, err)
	require.Equal(t, usage.TermsFor(usage.DefaultPlan), resp.Terms, "an unmirrored app is on the default plan")
	require.False(t, resp.UsageAllowanceAccrues, "nothing has been bought for an unmirrored app")

	app := store.apps[appID]
	app.Plan = usage.PlanBusiness // as a later, plan-aware change would write it
	store.apps[appID] = app
	resp, err = svc.GetAppPlan(context.Background(), cycle.GetAppPlanRequest{AppID: appID})
	require.NoError(t, err)
	require.Equal(t, usage.TermsFor(usage.PlanBusiness), resp.Terms)
	require.Nil(t, resp.PendingChange)
	require.Equal(t, usage.GraceExpiry(app.CreatedAt), resp.CreationGraceEndsAt)
	// appsNow (Jul 1) is before this app's Sep 1 creation — the fake clock is
	// deliberately odd here — so the allowance is not in force yet.
	require.False(t, resp.UsageAllowanceAccrues)

	_, err = svc.GetAppPlan(context.Background(), cycle.GetAppPlanRequest{})
	requirePlanErrCode(t, err, billing.CodeInvalidInput)
}
