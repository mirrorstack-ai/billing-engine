package cycle_test

// QuoteChange (billing-engine#255): a read-only preview of plan steps. Every
// test here cross-checks the RPC's numbers against the SAME pricing functions
// the invoice code uses (usage.TermsFor, usage.ModuleBlockMicros,
// usage.ProratedBaseMicros) rather than a second hardcoded expectation, so a
// drift between the preview and the real charge fails here first.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// quoteNow matches appsNow's anchor-4 fixture so quote tests can reuse
// registeredAccount verbatim: current anchored period [2026-06-04, 2026-07-04).
var quoteNow = appsNow

func quoteSvc(store *fakeStore) *cycle.Service {
	return cycle.NewService(store, newFakeStripe()).WithNow(func() time.Time { return quoteNow })
}

func quotePeriodWindow(activatedAt time.Time) (time.Time, time.Time) {
	return billingperiod.AnchoredPeriodWindow(quoteNow.UTC(), billingperiod.AnchorDay(activatedAt))
}

// --- create_app ---------------------------------------------------------

func TestQuoteChange_CreateApp_PerPlan(t *testing.T) {
	for _, plan := range []usage.Plan{usage.PlanFree, usage.PlanPro} {
		t.Run(string(plan), func(t *testing.T) {
			store := newFakeStore()
			user, _ := registeredAccount(store)

			resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
				OwnerUserID: user,
				Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepCreateApp, Plan: string(plan)}},
			})
			require.NoError(t, err)
			require.Len(t, resp.Steps, 1)
			step := resp.Steps[0]

			wantBase := usage.TermsFor(plan).BaseFeeMicros
			require.Equal(t, cycle.QuoteStandingOK, step.Standing)
			require.Equal(t, int64(0), step.OneTimeMicros, "creation grace: nothing now")
			require.Equal(t, wantBase, step.RecurringMicros)
			require.Equal(t, wantBase, step.GraceAmountMicros, "after grace, the base fee becomes due")
			require.Equal(t, usage.GraceDays, step.GraceDays)
			require.Equal(t, usage.IncludedModules, step.ModulesIncludedTotal)
		})
	}
}

func TestQuoteChange_CreateApp_BusinessRefused(t *testing.T) {
	// Mirrors RegisterApp's exact refusal (apps.go): Business's per-app module
	// allowance is not billed yet, so a preview must not price it either.
	store := newFakeStore()
	user, _ := registeredAccount(store)

	_, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepCreateApp, Plan: "business"}},
	})
	requireCode(t, err, billing.CodePlanNotAvailable)
}

// --- install_modules ------------------------------------------------------

func TestQuoteChange_InstallModules_Table(t *testing.T) {
	for _, existing := range []int{0, 3, 5} {
		for _, n := range []int{1, 5, 6, 10, 11} {
			name := fmt.Sprintf("existing=%d/n=%d", existing, n)
			t.Run(name, func(t *testing.T) {
				store := newFakeStore()
				user, acct := registeredAccount(store)
				store.liveModuleCountByAccount[acct] = existing
				appID := registeredApp(store, acct, usage.PlanFree)

				resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
					OwnerUserID: user,
					Steps: []cycle.QuoteChangeStep{
						{Kind: cycle.QuoteStepInstallModules, AppID: appID, ModuleCount: n},
					},
				})
				require.NoError(t, err)
				require.Len(t, resp.Steps, 1)
				step := resp.Steps[0]

				wantDelta := usage.ModuleBlockMicros(int64(existing+n-usage.IncludedModules)) -
					usage.ModuleBlockMicros(int64(existing-usage.IncludedModules))
				wantIncludedUsed := min(existing+n, usage.IncludedModules)

				require.Equal(t, cycle.QuoteStandingOK, step.Standing)
				require.Equal(t, int64(0), step.OneTimeMicros, "install grace: nothing now")
				require.Equal(t, wantDelta, step.RecurringMicros, "existing=%d n=%d", existing, n)
				require.Equal(t, wantDelta, step.GraceAmountMicros)
				require.Equal(t, usage.GraceDays, step.GraceDays)
				require.Equal(t, wantIncludedUsed, step.ModulesIncludedUsed)
				require.Equal(t, usage.IncludedModules, step.ModulesIncludedTotal)
			})
		}
	}
}

func TestQuoteChange_InstallModules_CumulativeAcrossSteps(t *testing.T) {
	// Steps 1c/1e price IN ORDER: modules an earlier step adds count against
	// the pool for every later step, in the SAME request.
	store := newFakeStore()
	user, acct := registeredAccount(store)
	store.liveModuleCountByAccount[acct] = 3
	appID := registeredApp(store, acct, usage.PlanFree)

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps: []cycle.QuoteChangeStep{
			{Kind: cycle.QuoteStepInstallModules, AppID: appID, ModuleCount: 2}, // 3 -> 5, still fully included
			{Kind: cycle.QuoteStepInstallModules, AppID: appID, ModuleCount: 6}, // 5 -> 11, one full block over
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Steps, 2)

	require.Equal(t, int64(0), resp.Steps[0].RecurringMicros, "3->5 stays within the included pool")
	require.Equal(t, usage.IncludedModules, resp.Steps[0].ModulesIncludedUsed)

	wantSecondDelta := usage.ModuleBlockMicros(11-usage.IncludedModules) - usage.ModuleBlockMicros(5-usage.IncludedModules)
	require.Equal(t, wantSecondDelta, resp.Steps[1].RecurringMicros)
}

// --- set_plan ---------------------------------------------------------

func registeredApp(store *fakeStore, acct uuid.UUID, plan usage.Plan) uuid.UUID {
	appID := uuid.New()
	store.apps[appID] = cycle.AppMirror{
		AppID: appID, AccountID: acct, Plan: plan, CreatedPlan: plan,
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	return appID
}

func TestQuoteChange_SetPlan_Upgrade(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	appID := registeredApp(store, acct, usage.PlanFree)

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepSetPlan, AppID: appID, Plan: "pro"}},
	})
	require.NoError(t, err)
	require.Len(t, resp.Steps, 1)
	step := resp.Steps[0]

	periodStart, periodEnd := quotePeriodWindow(store.activation[acct])
	delta := usage.TermsFor(usage.PlanPro).BaseFeeMicros - usage.TermsFor(usage.PlanFree).BaseFeeMicros
	wantOneTime := usage.ProratedBaseMicros(delta, quoteNow.UTC(), periodStart, periodEnd)

	require.Equal(t, cycle.QuoteStandingOK, step.Standing)
	require.Equal(t, wantOneTime, step.OneTimeMicros)
	require.Equal(t, usage.TermsFor(usage.PlanPro).BaseFeeMicros, step.RecurringMicros)
	require.Positive(t, step.OneTimeMicros, "mid-period upgrade must charge something now")
}

func TestQuoteChange_SetPlan_Downgrade(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	appID := registeredApp(store, acct, usage.PlanPro)

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepSetPlan, AppID: appID, Plan: "free"}},
	})
	require.NoError(t, err)
	require.Len(t, resp.Steps, 1)
	step := resp.Steps[0]

	require.Equal(t, cycle.QuoteStandingOK, step.Standing)
	require.Equal(t, int64(0), step.OneTimeMicros, "a downgrade waits for the period boundary: nothing now")
	require.Equal(t, usage.TermsFor(usage.PlanFree).BaseFeeMicros, step.RecurringMicros)
}

func TestQuoteChange_SetPlan_ByStepRef(t *testing.T) {
	// An app this SAME request creates (create_app), referenced by index
	// rather than a real app id — the plan confirm page's exact shape.
	store := newFakeStore()
	user, _ := registeredAccount(store)

	ref := 0
	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps: []cycle.QuoteChangeStep{
			{Kind: cycle.QuoteStepCreateApp, Plan: "free"},
			{Kind: cycle.QuoteStepSetPlan, StepRef: &ref, Plan: "pro"},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Steps, 2)
	require.Positive(t, resp.Steps[1].OneTimeMicros, "upgrade from the step-created app's Free plan")
}

// --- standing -------------------------------------------------------------

func TestQuoteChange_Standing_FundedOwnerIsOK(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepCreateApp, Plan: "pro"}},
	})
	require.NoError(t, err)
	require.Equal(t, cycle.QuoteStandingOK, resp.Steps[0].Standing)
}

func TestQuoteChange_Standing_UnfundedUserIsFundingRequired(t *testing.T) {
	store := newFakeStore()
	user := uuid.New() // no account at all

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepCreateApp, Plan: "pro"}},
	})
	require.NoError(t, err)
	require.Equal(t, cycle.QuoteStandingFundingRequired, resp.Steps[0].Standing)
	// Funding status does not block pricing: the amount is still the real one.
	require.Equal(t, usage.TermsFor(usage.PlanPro).BaseFeeMicros, resp.Steps[0].RecurringMicros)
}

func TestQuoteChange_Standing_UnfundedOrgIsFundingRequired(t *testing.T) {
	store := newFakeStore()
	org := uuid.New() // never designated

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerOrgID: org,
		Steps:      []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepCreateApp, Plan: "pro"}},
	})
	require.NoError(t, err)
	require.Equal(t, cycle.QuoteStandingFundingRequired, resp.Steps[0].Standing)
}

func TestQuoteChange_Standing_SponsorFundedOrgResolvesThroughSponsorHop(t *testing.T) {
	store := newFakeStore()
	org, orgAcct, sponsorAcct := uuid.New(), uuid.New(), uuid.New()
	store.accountsByOrg[org] = orgAcct
	store.orgDesignations[org] = cycle.OrgDesignation{OrgID: org, Funding: cycle.OrgFundingSponsor, SponsorAccountID: sponsorAcct}
	store.activation[orgAcct] = time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	store.cardCount[orgAcct] = 0 // the org account itself never holds cards
	store.cardCount[sponsorAcct] = 1

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerOrgID: org,
		Steps:      []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepCreateApp, Plan: "pro"}},
	})
	require.NoError(t, err)
	require.Equal(t, cycle.QuoteStandingOK, resp.Steps[0].Standing, "funded through the sponsor's card, not the org account's")

	store.cardCount[sponsorAcct] = 0
	resp, err = quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerOrgID: org,
		Steps:      []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepCreateApp, Plan: "pro"}},
	})
	require.NoError(t, err)
	require.Equal(t, cycle.QuoteStandingFundingRequired, resp.Steps[0].Standing, "sponsor's card gone")
}

func TestQuoteChange_Standing_StoreErrorIsUnknownWithNoAmounts(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.errActivation = errors.New("db unreachable")

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps: []cycle.QuoteChangeStep{
			{Kind: cycle.QuoteStepCreateApp, Plan: "pro"},
			{Kind: cycle.QuoteStepInstallModules, AppID: uuid.New(), ModuleCount: 3},
		},
	})
	require.NoError(t, err, "a backend read failure degrades the response, it is not an RPC error")
	require.Len(t, resp.Steps, 2)
	for _, step := range resp.Steps {
		require.Equal(t, cycle.QuoteStandingUnknown, step.Standing)
		require.Zero(t, step.OneTimeMicros)
		require.Zero(t, step.RecurringMicros)
		require.Zero(t, step.GraceAmountMicros)
	}
}

func TestQuoteChange_Standing_LiveModuleCountErrorIsUnknown(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	appID := registeredApp(store, acct, usage.PlanFree)
	store.errLiveModuleCountForAccount = errors.New("db unreachable")

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepInstallModules, AppID: appID, ModuleCount: 3}},
	})
	require.NoError(t, err)
	require.Equal(t, cycle.QuoteStandingUnknown, resp.Steps[0].Standing)
	require.Zero(t, resp.Steps[0].RecurringMicros)
}

// --- app ownership ------------------------------------------------------
//
// An app_id in install_modules / set_plan is caller-supplied. The quote must
// price it only when it is THIS owner's live app: otherwise set_plan's
// prorated upgrade amount (new base fee minus the app's current plan's) would
// read another tenant's plan back, and a typo would get a confident price from
// a call the real SetAppPlan / SyncAppModules would refuse.

func requireAppNotOwned(t *testing.T, resp *cycle.QuoteChangeResponse, err error) {
	t.Helper()
	require.Nil(t, resp, "a refused quote must not carry any priced step")
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeNotFound, be.Code)
	require.Equal(t, "app not registered or deleted", be.Message,
		"one message for unregistered, deleted and foreign: no oracle for another tenant's app")
}

func TestQuoteChange_SetPlan_RefusesAppOfAnotherAccount(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	_, otherAcct := registeredAccount(store) // a second tenant, funded
	foreign := registeredApp(store, otherAcct, usage.PlanPro)

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepSetPlan, AppID: foreign, Plan: "pro"}},
	})
	requireAppNotOwned(t, resp, err)
}

func TestQuoteChange_SetPlan_RefusesUnregisteredApp(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepSetPlan, AppID: uuid.New(), Plan: "pro"}},
	})
	requireAppNotOwned(t, resp, err)
}

func TestQuoteChange_SetPlan_RefusesDeletedApp(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	appID := registeredApp(store, acct, usage.PlanFree)
	deleted := store.apps[appID]
	deleted.Deleted = true
	store.apps[appID] = deleted

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepSetPlan, AppID: appID, Plan: "pro"}},
	})
	requireAppNotOwned(t, resp, err)
}

func TestQuoteChange_InstallModules_RefusesForeignAndUnregisteredApp(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	_, otherAcct := registeredAccount(store)
	foreign := registeredApp(store, otherAcct, usage.PlanFree)

	for name, appID := range map[string]uuid.UUID{"foreign": foreign, "unregistered": uuid.New()} {
		t.Run(name, func(t *testing.T) {
			resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
				OwnerUserID: user,
				Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepInstallModules, AppID: appID, ModuleCount: 2}},
			})
			requireAppNotOwned(t, resp, err)
		})
	}
}

func TestQuoteChange_AppStepRefusesWhenOwnerHasNoAccount(t *testing.T) {
	// An unfunded owner (no accounts row) owns no app: even an app that exists
	// is not provably theirs, so it is refused rather than priced.
	store := newFakeStore()
	_, otherAcct := registeredAccount(store)
	existing := registeredApp(store, otherAcct, usage.PlanPro)

	// A roster row with no account at all must not match an owner with no
	// account either (nil == nil is not ownership).
	orphan := registeredApp(store, uuid.Nil, usage.PlanPro)

	for name, appID := range map[string]uuid.UUID{"existing": existing, "no-account-row": orphan} {
		t.Run(name, func(t *testing.T) {
			resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
				OwnerUserID: uuid.New(),
				Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepSetPlan, AppID: appID, Plan: "pro"}},
			})
			requireAppNotOwned(t, resp, err)
		})
	}
}

func TestQuoteChange_AppOwnership_OrgAndUserPrincipalsAreSeparate(t *testing.T) {
	store := newFakeStore()
	org, orgAcct := uuid.New(), uuid.New()
	store.accountsByOrg[org] = orgAcct
	store.orgDesignations[org] = cycle.OrgDesignation{OrgID: org, Funding: cycle.OrgFundingOrg}
	store.activation[orgAcct] = time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	store.cardCount[orgAcct] = 1

	orgApp := registeredApp(store, orgAcct, usage.PlanFree)
	owned := store.apps[orgApp]
	owned.OwnerOrgID = org
	store.apps[orgApp] = owned

	otherOrg := uuid.New()
	otherOrgAcct := uuid.New()
	store.accountsByOrg[otherOrg] = otherOrgAcct
	store.orgDesignations[otherOrg] = cycle.OrgDesignation{OrgID: otherOrg, Funding: cycle.OrgFundingOrg}
	store.activation[otherOrgAcct] = time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	store.cardCount[otherOrgAcct] = 1

	svc := quoteSvc(store)
	step := []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepSetPlan, AppID: orgApp, Plan: "pro"}}

	// The owning org prices its own app.
	resp, err := svc.QuoteChange(context.Background(), cycle.QuoteChangeRequest{OwnerOrgID: org, Steps: step})
	require.NoError(t, err)
	require.Positive(t, resp.Steps[0].OneTimeMicros)

	// Another org does not.
	resp, err = svc.QuoteChange(context.Background(), cycle.QuoteChangeRequest{OwnerOrgID: otherOrg, Steps: step})
	requireAppNotOwned(t, resp, err)

	// A user whose personal account happens to bill the org's app (the row's
	// account matches, its principal does not) does not either.
	user, _ := registeredAccount(store)
	billedToUser := store.apps[orgApp]
	billedToUser.AccountID = store.accountsByUser[user]
	store.apps[orgApp] = billedToUser
	resp, err = svc.QuoteChange(context.Background(), cycle.QuoteChangeRequest{OwnerUserID: user, Steps: step})
	requireAppNotOwned(t, resp, err)
}

func TestQuoteChange_AppLookupErrorIsUnknownWithNoAmounts(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	appID := registeredApp(store, acct, usage.PlanFree)
	store.errAppMirror = errors.New("db unreachable")

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepSetPlan, AppID: appID, Plan: "pro"}},
	})
	require.NoError(t, err)
	require.Equal(t, cycle.QuoteStandingUnknown, resp.Steps[0].Standing)
	require.Zero(t, resp.Steps[0].OneTimeMicros)
	require.Zero(t, resp.Steps[0].RecurringMicros)
}

// --- step cap -----------------------------------------------------------

func TestQuoteChange_StepCountIsCapped(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	steps := func(n int) []cycle.QuoteChangeStep {
		out := make([]cycle.QuoteChangeStep, n)
		for i := range out {
			out[i] = cycle.QuoteChangeStep{Kind: cycle.QuoteStepCreateApp, Plan: "free"}
		}
		return out
	}

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{OwnerUserID: user, Steps: steps(10)})
	require.NoError(t, err)
	require.Len(t, resp.Steps, 10, "ten steps is the largest accepted request")

	resp, err = quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{OwnerUserID: user, Steps: steps(11)})
	require.Nil(t, resp)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInvalidInput, be.Code)
}

// --- no writes --------------------------------------------------------

// readOnlyStore is the ENTIRE Store surface QuoteChange may touch. It embeds
// the Store interface itself (nil), so any method not defined here — every
// existing mutator and any write a later change adds — panics on a nil
// interface instead of silently running against the fake. The reads below are
// the explicit allowlist; each records its name so the test can also assert
// the call set is not wider than intended. Nothing here can get-or-create an
// accounts row.
type readOnlyStore struct {
	cycle.Store
	fake  *fakeStore
	calls map[string]int
}

func (r *readOnlyStore) record(name string) { r.calls[name]++ }

func (r *readOnlyStore) AccountIDByUser(ctx context.Context, userID uuid.UUID) (uuid.UUID, bool, error) {
	r.record("AccountIDByUser")
	return r.fake.AccountIDByUser(ctx, userID)
}
func (r *readOnlyStore) AccountActivation(ctx context.Context, accountID uuid.UUID) (time.Time, bool, error) {
	r.record("AccountActivation")
	return r.fake.AccountActivation(ctx, accountID)
}
func (r *readOnlyStore) ResolveOrgFundedAccount(ctx context.Context, orgID uuid.UUID) (uuid.UUID, bool, error) {
	r.record("ResolveOrgFundedAccount")
	return r.fake.ResolveOrgFundedAccount(ctx, orgID)
}
func (r *readOnlyStore) ChargeFundingAccount(ctx context.Context, accountID uuid.UUID) (uuid.UUID, error) {
	r.record("ChargeFundingAccount")
	return r.fake.ChargeFundingAccount(ctx, accountID)
}
func (r *readOnlyStore) UsableNonFraudCardCount(ctx context.Context, accountID uuid.UUID) (int, error) {
	r.record("UsableNonFraudCardCount")
	return r.fake.UsableNonFraudCardCount(ctx, accountID)
}
func (r *readOnlyStore) LiveModuleTimerCountForAccount(ctx context.Context, accountID uuid.UUID) (int, error) {
	r.record("LiveModuleTimerCountForAccount")
	return r.fake.LiveModuleTimerCountForAccount(ctx, accountID)
}
func (r *readOnlyStore) AppMirror(ctx context.Context, appID uuid.UUID) (cycle.AppMirror, bool, error) {
	r.record("AppMirror")
	return r.fake.AppMirror(ctx, appID)
}

func TestQuoteChange_MakesNoWrite(t *testing.T) {
	base := newFakeStore()
	user, acct := registeredAccount(base)
	base.liveModuleCountByAccount[acct] = 3
	appID := registeredApp(base, acct, usage.PlanFree)
	store := &readOnlyStore{fake: base, calls: map[string]int{}}

	svc := cycle.NewService(store, newFakeStripe()).WithNow(func() time.Time { return quoteNow })
	ref := 0
	var resp *cycle.QuoteChangeResponse
	var err error
	require.NotPanics(t, func() {
		resp, err = svc.QuoteChange(context.Background(), cycle.QuoteChangeRequest{
			OwnerUserID: user,
			Steps: []cycle.QuoteChangeStep{
				{Kind: cycle.QuoteStepCreateApp, Plan: "pro"},
				{Kind: cycle.QuoteStepInstallModules, StepRef: &ref, ModuleCount: 4},
				{Kind: cycle.QuoteStepSetPlan, AppID: appID, Plan: "pro"},
			},
		})
	}, "QuoteChange called a Store method outside the read allowlist (readOnlyStore)")
	require.NoError(t, err)
	require.Len(t, resp.Steps, 3)
	require.Equal(t, 1, store.calls["AppMirror"], "one app read per distinct app_id")
	require.Len(t, base.accountsByUser, 1, "no new accounts row was created")
}
