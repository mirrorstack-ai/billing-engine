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

				resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
					OwnerUserID: user,
					Steps: []cycle.QuoteChangeStep{
						{Kind: cycle.QuoteStepInstallModules, AppID: uuid.New(), ModuleCount: n},
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
	appID := uuid.New()

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
	user, _ := registeredAccount(store)
	store.errLiveModuleCountForAccount = errors.New("db unreachable")

	resp, err := quoteSvc(store).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps:       []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepInstallModules, AppID: uuid.New(), ModuleCount: 3}},
	})
	require.NoError(t, err)
	require.Equal(t, cycle.QuoteStandingUnknown, resp.Steps[0].Standing)
	require.Zero(t, resp.Steps[0].RecurringMicros)
}

// --- no writes --------------------------------------------------------

// noWriteStore wraps fakeStore and fails the test if QuoteChange calls any
// method that would mutate state or get-or-create an accounts row — the
// acceptance bar for a read-only RPC.
type noWriteStore struct {
	*fakeStore
	t      *testing.T
	writes int
}

func (w *noWriteStore) fail(method string) {
	w.t.Helper()
	w.writes++
	w.t.Errorf("QuoteChange must not call %s (it is read-only)", method)
}

func (w *noWriteStore) EnsureUserAccount(ctx context.Context, userID uuid.UUID) (uuid.UUID, error) {
	w.fail("EnsureUserAccount")
	return uuid.Nil, nil
}
func (w *noWriteStore) EnsureOrgAccount(ctx context.Context, orgID uuid.UUID) (uuid.UUID, error) {
	w.fail("EnsureOrgAccount")
	return uuid.Nil, nil
}
func (w *noWriteStore) InsertAppMirror(ctx context.Context, appID, accountID, ownerOrgID uuid.UUID, moduleCount, memberCount int, createdAt time.Time, name string, plan usage.Plan) error {
	w.fail("InsertAppMirror")
	return nil
}
func (w *noWriteStore) InsertFreeAppMirror(ctx context.Context, appID, accountID, ownerOrgID uuid.UUID, moduleCount, memberCount int, createdAt time.Time, name string) (bool, error) {
	w.fail("InsertFreeAppMirror")
	return false, nil
}
func (w *noWriteStore) SetAppModuleCount(ctx context.Context, appID uuid.UUID, moduleCount int) error {
	w.fail("SetAppModuleCount")
	return nil
}
func (w *noWriteStore) SetAppPlan(ctx context.Context, appID uuid.UUID, plan usage.Plan) (bool, error) {
	w.fail("SetAppPlan")
	return false, nil
}
func (w *noWriteStore) OpenPlanChange(ctx context.Context, p cycle.OpenPlanChangeParams) (cycle.PlanChange, cycle.OpenPlanChangeOutcome, error) {
	w.fail("OpenPlanChange")
	return cycle.PlanChange{}, cycle.PlanChangeAppStale, nil
}
func (w *noWriteStore) MarkAppDeleted(ctx context.Context, appID uuid.UUID) error {
	w.fail("MarkAppDeleted")
	return nil
}
func (w *noWriteStore) ReconcileModuleTimersToTarget(ctx context.Context, appID uuid.UUID, installedAt, graceExpiresAt, removedAt time.Time) error {
	w.fail("ReconcileModuleTimersToTarget")
	return nil
}
func (w *noWriteStore) InsertModuleOverageTimers(ctx context.Context, accountID, appID uuid.UUID, installedAt, graceExpiresAt time.Time, n int) error {
	w.fail("InsertModuleOverageTimers")
	return nil
}

func TestQuoteChange_MakesNoWrite(t *testing.T) {
	base := newFakeStore()
	user, acct := registeredAccount(base)
	base.liveModuleCountByAccount[acct] = 3
	appID := registeredApp(base, acct, usage.PlanFree)
	store := &noWriteStore{fakeStore: base, t: t}

	svc := cycle.NewService(store, newFakeStripe()).WithNow(func() time.Time { return quoteNow })
	ref := 0
	resp, err := svc.QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerUserID: user,
		Steps: []cycle.QuoteChangeStep{
			{Kind: cycle.QuoteStepCreateApp, Plan: "pro"},
			{Kind: cycle.QuoteStepInstallModules, StepRef: &ref, ModuleCount: 4},
			{Kind: cycle.QuoteStepSetPlan, AppID: appID, Plan: "pro"},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Steps, 3)
	require.Zero(t, store.writes, "QuoteChange must never write")
	require.Len(t, base.accountsByUser, 1, "no new accounts row was created")
}
