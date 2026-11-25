package cycle_test

// TB-FUND (billing-engine#348): `platform_funded` fills ONE gap, an org with no
// resolved funding. These tests pin that the path RAN (the audit line and the
// unbilled roster row, never a bare "nothing was charged") and that every
// control — an org outside PORG, a funded org, a user owner, an org with an
// account but no card — behaves exactly as before.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/platformfunded"
	"github.com/mirrorstack-ai/billing-engine/internal/account/platformfunded/pftest"
)

func porgSet(t *testing.T, orgs ...uuid.UUID) platformfunded.Set {
	t.Helper()
	ids := make([]string, len(orgs))
	for i, o := range orgs {
		ids[i] = o.String()
	}
	set, err := platformfunded.Parse(strings.Join(ids, ","))
	require.NoError(t, err)
	return set
}

func requireNoProviderCall(t *testing.T, sc *fakeStripe) {
	t.Helper()
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, sc.finalizeCalls)
}

func TestRegisterApp_PlatformFundedOrgRegistersUnbilledAndIsAudited(t *testing.T) {
	for _, plan := range []string{"free", "pro"} { // both insert paths: the capped Free insert and the plain one
		t.Run(plan, func(t *testing.T) {
			uses := pftest.Capture(t)
			store := newFakeStore()
			sc := newFakeStripe()
			org, appID := uuid.New(), uuid.New() // the org has no designation, no account, no card
			svc := appsSvc(store, sc).WithPlatformOrgs(porgSet(t, org))

			resp, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
				OwnerOrgID: org, AppID: appID, Plan: plan, ModuleCount: 2,
			})
			require.NoError(t, err)

			// The path RAN: one audit line, at the create gate, for this org.
			require.Equal(t, []pftest.Use{{Gate: platformfunded.GateRegisterApp, OrgID: org.String()}}, uses())
			// ...and it left the UNBILLED shape: no account on the row, the org
			// stamped as owner, and no account opened for it.
			require.Equal(t, uuid.Nil, resp.AccountID)
			require.Equal(t, uuid.Nil, store.apps[appID].AccountID)
			require.Equal(t, org, store.appOwnerOrg[appID])
			require.Empty(t, store.accountsByOrg, "no billing account is created for a platform-funded org")
			// No charge, no invoice, no provider call of any kind.
			requireNoProviderCall(t, sc)
		})
	}
}

func TestRegisterApp_OrgOutsidePORGIsStillRefused(t *testing.T) {
	uses := pftest.Capture(t)
	store := newFakeStore()
	sc := newFakeStripe()
	porg, outsider, appID := uuid.New(), uuid.New(), uuid.New()
	svc := appsSvc(store, sc).WithPlatformOrgs(porgSet(t, porg))

	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerOrgID: outsider, AppID: appID})

	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodePaymentRequired, be.Code)
	require.NotContains(t, store.apps, appID, "a refused create writes no roster row")
	require.Empty(t, uses(), "the attribute was not used for an org outside PORG")
}

func TestRegisterApp_PlatformFundedDoesNotOverrideRealFunding(t *testing.T) {
	// A PORG org that DID bind a card (sandbox mode, through the add-card page)
	// is a normal funded org: billed on its account, the attribute not used.
	uses := pftest.Capture(t)
	store := newFakeStore()
	org, acct, appID := uuid.New(), uuid.New(), uuid.New()
	store.accountsByOrg[org] = acct
	store.orgDesignations[org] = cycle.OrgDesignation{OrgID: org, Funding: cycle.OrgFundingOrg}
	store.activation[acct] = time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	store.cardCount[acct] = 1

	resp, err := appsSvc(store, newFakeStripe()).WithPlatformOrgs(porgSet(t, org)).
		RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerOrgID: org, AppID: appID})
	require.NoError(t, err)
	require.Equal(t, acct, resp.AccountID)
	require.Equal(t, acct, store.apps[appID].AccountID)
	require.Empty(t, uses(), "real funding resolved first; platform_funded was never consulted")
}

func TestRegisterApp_PlatformFundedDoesNotRescueAnAccountWithNoCard(t *testing.T) {
	// An activated org account whose card is gone stays refused, as before.
	uses := pftest.Capture(t)
	store := newFakeStore()
	org, acct := uuid.New(), uuid.New()
	store.accountsByOrg[org] = acct
	store.orgDesignations[org] = cycle.OrgDesignation{OrgID: org, Funding: cycle.OrgFundingOrg}
	store.activation[acct] = time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	store.cardCount[acct] = 0

	_, err := appsSvc(store, newFakeStripe()).WithPlatformOrgs(porgSet(t, org)).
		RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerOrgID: org, AppID: uuid.New()})
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodePaymentRequired, be.Code)
	require.Empty(t, uses())
}

func TestRegisterApp_PlatformFundedNeverAppliesToAUserOwner(t *testing.T) {
	// A user owner is never looked up in the ORG-id set, even on an equal id.
	uses := pftest.Capture(t)
	store := newFakeStore()
	user := uuid.New()
	_, err := appsSvc(store, newFakeStripe()).WithPlatformOrgs(porgSet(t, user)).
		RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: uuid.New()})
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodePaymentRequired, be.Code)
	require.Empty(t, uses())
}

func TestRegisterApp_PlatformFundedRetryIsIdempotentAndAuditedOncePerCreate(t *testing.T) {
	uses := pftest.Capture(t)
	store := newFakeStore()
	org, appID := uuid.New(), uuid.New()
	svc := appsSvc(store, newFakeStripe()).WithPlatformOrgs(porgSet(t, org))
	req := cycle.RegisterAppRequest{OwnerOrgID: org, AppID: appID}
	for i := 0; i < 2; i++ {
		_, err := svc.RegisterApp(context.Background(), req)
		require.NoError(t, err)
	}
	require.Len(t, uses(), 1, "a retry of a mirrored app never re-enters the funding gate")
	require.Len(t, store.apps, 1)
}

func TestQuoteChange_PlatformFundedOrgIsOKAndOwnsItsUnbilledApps(t *testing.T) {
	uses := pftest.Capture(t)
	store := newFakeStore()
	sc := newFakeStripe()
	org, appID := uuid.New(), uuid.New()
	svc := appsSvc(store, sc).WithPlatformOrgs(porgSet(t, org))
	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerOrgID: org, AppID: appID})
	require.NoError(t, err)

	resp, err := svc.QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerOrgID: org,
		Steps: []cycle.QuoteChangeStep{
			{Kind: cycle.QuoteStepCreateApp, Plan: "pro"},
			{Kind: cycle.QuoteStepInstallModules, AppID: appID, ModuleCount: 1},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Steps, 2)
	for i, step := range resp.Steps {
		// "ok", never a new standing value: api-platform's money gate reads
		// only ok and funding_required as an answer.
		require.Equal(t, cycle.QuoteStandingOK, step.Standing, "step %d", i)
	}
	require.Contains(t, uses(), pftest.Use{Gate: platformfunded.GateQuoteChange, OrgID: org.String()})
	requireNoProviderCall(t, sc)
}

func TestQuoteChange_PlatformFundedOrgDoesNotOwnForeignOrBilledApps(t *testing.T) {
	store := newFakeStore()
	org, otherOrg, billedAcct := uuid.New(), uuid.New(), uuid.New()
	foreign, billed := uuid.New(), uuid.New()
	store.apps[foreign] = cycle.AppMirror{AppID: foreign, OwnerOrgID: otherOrg} // another org's unbilled app
	store.apps[billed] = cycle.AppMirror{AppID: billed, AccountID: billedAcct, OwnerOrgID: org}
	svc := quoteSvc(store).WithPlatformOrgs(porgSet(t, org))

	for name, appID := range map[string]uuid.UUID{"another org's unbilled app": foreign, "an app billed to an account": billed} {
		_, err := svc.QuoteChange(context.Background(), cycle.QuoteChangeRequest{
			OwnerOrgID: org,
			Steps:      []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepInstallModules, AppID: appID, ModuleCount: 1}},
		})
		var be *billing.Error
		require.ErrorAs(t, err, &be, name)
		require.Equal(t, billing.CodeNotFound, be.Code, name)
	}
}

func TestQuoteChange_OrgOutsidePORGStaysFundingRequired(t *testing.T) {
	uses := pftest.Capture(t)
	store := newFakeStore()
	porg, outsider := uuid.New(), uuid.New()
	resp, err := quoteSvc(store).WithPlatformOrgs(porgSet(t, porg)).QuoteChange(context.Background(), cycle.QuoteChangeRequest{
		OwnerOrgID: outsider,
		Steps:      []cycle.QuoteChangeStep{{Kind: cycle.QuoteStepCreateApp, Plan: "pro"}},
	})
	require.NoError(t, err)
	require.Equal(t, cycle.QuoteStandingFundingRequired, resp.Steps[0].Standing)
	require.Empty(t, uses())
}
