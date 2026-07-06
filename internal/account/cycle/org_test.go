package cycle_test

// SetOrgDesignation / GetOrgDesignation / RevokeSponsorship / RepointOrgUsage
// (org-billing W0 substrate, design D1). Reuses the in-memory fakeStore
// (service_test.go) — no new harness. The org RPCs never touch Stripe, so the
// services here wire a nil client.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

// Fixed clock: 2026-07-06 12:00 UTC. A sponsor designation activates the org
// account at this instant (anchor day 6), so the account's current anchored
// window is [2026-07-06, 2026-08-06) and the repoint sweep clamps to Jul 6.
var orgNow = time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)

func orgSvc(store *fakeStore) *cycle.Service {
	return cycle.NewService(store, nil).WithNow(func() time.Time { return orgNow })
}

// sponsorUser seeds an acting user with an existing billing account + usable
// default PM — the valid-sponsor baseline tests weaken. Returns (user, account).
func sponsorUser(store *fakeStore) (uuid.UUID, uuid.UUID) {
	user, acct := uuid.New(), uuid.New()
	store.accountsByUser[user] = acct
	store.hasPMByAccount[acct] = true
	return user, acct
}

// seedOrgApp seeds an UNBILLED org roster row (Nil account, owner_org_id
// stamped) — the pre-designation shape RegisterApp writes for an unfunded org.
func seedOrgApp(store *fakeStore, orgID uuid.UUID, moduleCount int) uuid.UUID {
	id := uuid.New()
	store.apps[id] = cycle.AppMirror{
		AppID: id, AccountID: uuid.Nil, ModuleCount: moduleCount,
		CreatedAt: orgNow.AddDate(0, 0, -30),
	}
	store.appOwnerOrg[id] = orgID
	return id
}

// --- SetOrgDesignation: sponsor funding -------------------------------------

func TestSetOrgDesignation_SponsorActivatesAndRunsAttachSweep(t *testing.T) {
	store := newFakeStore()
	user, sponsorAcct := sponsorUser(store)
	org := uuid.New()
	app1 := seedOrgApp(store, org, 3)
	app2 := seedOrgApp(store, org, 0)
	store.orgBacklog[org] = 12_345_000
	store.orgNullEvents[org] = 7

	resp, err := orgSvc(store).SetOrgDesignation(context.Background(), cycle.SetOrgDesignationRequest{
		OrgID: org, ActingUserID: user, Funding: "sponsor",
	})
	require.NoError(t, err)
	acct := store.accountsByOrg[org]
	require.NotEqual(t, uuid.Nil, acct, "the org account row is ensured")
	require.Equal(t, acct, resp.AccountID)
	require.Equal(t, "sponsor", resp.Funding)
	require.True(t, resp.Activated)
	require.EqualValues(t, 12_345_000, resp.DisclosedBacklogMicros)

	// Designation row: the sponsor pair is the ACTING user + their account,
	// with the disclosure recorded at write time.
	d := store.orgDesignations[org]
	require.Equal(t, cycle.OrgFundingSponsor, d.Funding)
	require.Equal(t, sponsorAcct, d.SponsorAccountID)
	require.Equal(t, user, d.SponsorUserID)
	require.EqualValues(t, 12_345_000, d.DisclosedBacklogMicros)
	require.Equal(t, user, d.UpdatedBy)

	// Sponsor funding activates immediately: anchor = designation instant.
	require.Equal(t, orgNow, store.activation[acct])

	// Attach sweep ran inline: both roster rows attached...
	require.EqualValues(t, 2, resp.AttachedApps)
	require.Equal(t, acct, store.apps[app1].AccountID)
	require.Equal(t, acct, store.apps[app2].AccountID)

	// ...the NULL-account events folded into the account's current open window
	// (anchor day 6 → clamped to Jul 6 00:00, decision 1)...
	require.EqualValues(t, 7, resp.RepointedEvents)
	require.Equal(t, []repointCall{{org, acct, time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC)}}, store.repointCalls)

	// ...and each live app's timers synthesized fresh, anchored NOW
	// (prospective grace — never retroactive).
	require.Equal(t, 3, liveTimerCount(store, app1))
	require.Zero(t, liveTimerCount(store, app2))
	for _, tm := range store.timers {
		require.Equal(t, orgNow, tm.installedAt)
		require.Equal(t, orgNow.AddDate(0, 0, 3), tm.graceExpiresAt)
	}
}

func TestSetOrgDesignation_SponsorRequiresAccountAndUsablePM(t *testing.T) {
	// A sponsor without an existing account, or with an account but no usable
	// default PM, is rejected — a card-less sponsor would activate the org
	// into a state where every charge run skips no_pm. Nothing is written.
	t.Run("no account", func(t *testing.T) {
		store := newFakeStore()
		_, err := orgSvc(store).SetOrgDesignation(context.Background(), cycle.SetOrgDesignationRequest{
			OrgID: uuid.New(), ActingUserID: uuid.New(), Funding: "sponsor",
		})
		requireCode(t, err, billing.CodeInvalidInput)
		require.Empty(t, store.orgDesignations)
	})
	t.Run("no usable PM", func(t *testing.T) {
		store := newFakeStore()
		user, acct := sponsorUser(store)
		store.hasPMByAccount[acct] = false
		_, err := orgSvc(store).SetOrgDesignation(context.Background(), cycle.SetOrgDesignationRequest{
			OrgID: uuid.New(), ActingUserID: user, Funding: "sponsor",
		})
		requireCode(t, err, billing.CodeInvalidInput)
		require.Empty(t, store.orgDesignations)
		require.Empty(t, store.activation)
	})
}

func TestSetOrgDesignation_Validation(t *testing.T) {
	svc := orgSvc(newFakeStore())
	for _, tc := range []struct {
		name string
		req  cycle.SetOrgDesignationRequest
	}{
		{"nil org", cycle.SetOrgDesignationRequest{ActingUserID: uuid.New(), Funding: "sponsor"}},
		{"nil acting user", cycle.SetOrgDesignationRequest{OrgID: uuid.New(), Funding: "sponsor"}},
		{"bad funding", cycle.SetOrgDesignationRequest{OrgID: uuid.New(), ActingUserID: uuid.New(), Funding: "wallet"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.SetOrgDesignation(context.Background(), tc.req)
			requireCode(t, err, billing.CodeInvalidInput)
		})
	}
}

// --- SetOrgDesignation: org funding ------------------------------------------

func TestSetOrgDesignation_OrgFundingDefersSweepToCardBind(t *testing.T) {
	// funding='org': the designation records but the account is NOT activated
	// here (the card-bind webhook stamps it), so no attach sweep runs — "the
	// pointer never flips to an unfunded account". Once the account IS
	// activated, a re-designation sweeps inline.
	store := newFakeStore()
	org := uuid.New()
	app := seedOrgApp(store, org, 2)
	svc := orgSvc(store)
	req := cycle.SetOrgDesignationRequest{OrgID: org, ActingUserID: uuid.New(), Funding: "org"}

	resp, err := svc.SetOrgDesignation(context.Background(), req)
	require.NoError(t, err)
	require.False(t, resp.Activated)
	require.Zero(t, resp.AttachedApps)
	require.Zero(t, resp.RepointedEvents)
	acct := store.accountsByOrg[org]
	require.Equal(t, acct, resp.AccountID)
	_, activated := store.activation[acct]
	require.False(t, activated, "activation is the card-bind webhook's job")
	require.Equal(t, uuid.Nil, store.apps[app].AccountID, "no sweep before funding")
	require.Empty(t, store.repointCalls)
	require.Zero(t, liveTimerCount(store, app))
	d := store.orgDesignations[org]
	require.Equal(t, cycle.OrgFundingOrg, d.Funding)
	require.Equal(t, uuid.Nil, d.SponsorAccountID)
	require.Equal(t, uuid.Nil, d.SponsorUserID)

	// Card bound (activation stamped); a funding switch back to 'org' finds
	// resolution already live and sweeps inline.
	store.activation[acct] = orgNow.AddDate(0, 0, -1)
	resp, err = svc.SetOrgDesignation(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Activated)
	require.EqualValues(t, 1, resp.AttachedApps)
	require.Equal(t, acct, store.apps[app].AccountID)
	require.Equal(t, 2, liveTimerCount(store, app))
}

// --- GetOrgDesignation --------------------------------------------------------

func TestGetOrgDesignation_NotFoundStillEchoesBacklog(t *testing.T) {
	// A never-designated org reports Found=false but STILL carries the live
	// backlog estimate — the number the designation UI must disclose.
	store := newFakeStore()
	org := uuid.New()
	store.orgBacklog[org] = 555

	resp, err := orgSvc(store).GetOrgDesignation(context.Background(), cycle.GetOrgDesignationRequest{OrgID: org})
	require.NoError(t, err)
	require.False(t, resp.Found)
	require.EqualValues(t, 555, resp.PendingBacklogMicros)
	require.Equal(t, uuid.Nil, resp.AccountID)
	require.False(t, resp.Activated)

	_, err = orgSvc(store).GetOrgDesignation(context.Background(), cycle.GetOrgDesignationRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetOrgDesignation_FoundReportsFundingState(t *testing.T) {
	store := newFakeStore()
	user, _ := sponsorUser(store)
	org := uuid.New()
	svc := orgSvc(store)
	_, err := svc.SetOrgDesignation(context.Background(), cycle.SetOrgDesignationRequest{
		OrgID: org, ActingUserID: user, Funding: "sponsor",
	})
	require.NoError(t, err)
	store.orgBacklog[org] = 999 // live estimate at read time

	resp, err := svc.GetOrgDesignation(context.Background(), cycle.GetOrgDesignationRequest{OrgID: org})
	require.NoError(t, err)
	require.True(t, resp.Found)
	require.Equal(t, "sponsor", resp.Funding)
	require.Equal(t, user, resp.SponsorUserID)
	require.Equal(t, store.accountsByOrg[org], resp.AccountID)
	require.True(t, resp.Activated)
	require.EqualValues(t, 999, resp.PendingBacklogMicros)
}

// --- RevokeSponsorship ---------------------------------------------------------

func TestRevokeSponsorship_IdempotentWhenAbsent(t *testing.T) {
	resp, err := orgSvc(newFakeStore()).RevokeSponsorship(context.Background(), cycle.RevokeSponsorshipRequest{
		OrgID: uuid.New(), ActingUserID: uuid.New(),
	})
	require.NoError(t, err)
	require.False(t, resp.Revoked)
}

func TestRevokeSponsorship_OnlyTheCurrentSponsorOfASponsorDesignation(t *testing.T) {
	store := newFakeStore()
	user, _ := sponsorUser(store)
	org := uuid.New()
	svc := orgSvc(store)
	_, err := svc.SetOrgDesignation(context.Background(), cycle.SetOrgDesignationRequest{
		OrgID: org, ActingUserID: user, Funding: "sponsor",
	})
	require.NoError(t, err)

	// A non-sponsor caller (org admin, whoever) cannot revoke — the self-revoke
	// is authorized by BEING the sponsor, and the designation survives.
	_, err = svc.RevokeSponsorship(context.Background(), cycle.RevokeSponsorshipRequest{
		OrgID: org, ActingUserID: uuid.New(),
	})
	requireCode(t, err, billing.CodeInvalidInput)
	require.Contains(t, store.orgDesignations, org)

	// The sponsor revokes: the designation row is gone (the org drops to
	// unbilled until re-designation).
	resp, err := svc.RevokeSponsorship(context.Background(), cycle.RevokeSponsorshipRequest{
		OrgID: org, ActingUserID: user,
	})
	require.NoError(t, err)
	require.True(t, resp.Revoked)
	require.NotContains(t, store.orgDesignations, org)
}

func TestRevokeSponsorship_RejectsOrgFundedDesignation(t *testing.T) {
	// funding='org' has no sponsorship to revoke — the org PM family is
	// managed through the card flows, not this endpoint.
	store := newFakeStore()
	org, user := uuid.New(), uuid.New()
	svc := orgSvc(store)
	_, err := svc.SetOrgDesignation(context.Background(), cycle.SetOrgDesignationRequest{
		OrgID: org, ActingUserID: user, Funding: "org",
	})
	require.NoError(t, err)

	_, err = svc.RevokeSponsorship(context.Background(), cycle.RevokeSponsorshipRequest{
		OrgID: org, ActingUserID: user,
	})
	requireCode(t, err, billing.CodeInvalidInput)
	require.Contains(t, store.orgDesignations, org)
}

// --- RepointOrgUsage -----------------------------------------------------------

func TestRepointOrgUsage_UnfundedReportsNotFundedWithoutError(t *testing.T) {
	// No funded designation → Funded=false, nothing swept, and NOT an error —
	// callers may fire optimistically.
	store := newFakeStore()
	org := uuid.New()
	app := seedOrgApp(store, org, 2)

	resp, err := orgSvc(store).RepointOrgUsage(context.Background(), cycle.RepointOrgUsageRequest{OrgID: org})
	require.NoError(t, err)
	require.False(t, resp.Funded)
	require.Equal(t, uuid.Nil, store.apps[app].AccountID)
	require.Empty(t, store.repointCalls)

	_, err = orgSvc(store).RepointOrgUsage(context.Background(), cycle.RepointOrgUsageRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestRepointOrgUsage_FundedRunsAttachSweep(t *testing.T) {
	// The card-bind completion path: a funding='org' designation whose account
	// has since activated sweeps — roster attached, events repointed into the
	// account's current open window, timers synthesized per live app.
	store := newFakeStore()
	org := uuid.New()
	app := seedOrgApp(store, org, 2)
	store.orgNullEvents[org] = 4
	svc := orgSvc(store)
	_, err := svc.SetOrgDesignation(context.Background(), cycle.SetOrgDesignationRequest{
		OrgID: org, ActingUserID: uuid.New(), Funding: "org",
	})
	require.NoError(t, err)
	acct := store.accountsByOrg[org]
	store.activation[acct] = orgNow.AddDate(0, 0, -2) // card bound Jul 4 → anchor day 4

	resp, err := svc.RepointOrgUsage(context.Background(), cycle.RepointOrgUsageRequest{OrgID: org})
	require.NoError(t, err)
	require.True(t, resp.Funded)
	require.Equal(t, acct, resp.AccountID)
	require.EqualValues(t, 1, resp.AttachedApps)
	require.EqualValues(t, 4, resp.RepointedEvents)
	require.Equal(t, acct, store.apps[app].AccountID)
	require.Equal(t, []repointCall{{org, acct, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)}}, store.repointCalls)
	require.Equal(t, 2, liveTimerCount(store, app))

	// Re-fire (crashed-sweep recovery): idempotent — nothing new attaches or
	// repoints, the timer set stays reconciled.
	resp, err = svc.RepointOrgUsage(context.Background(), cycle.RepointOrgUsageRequest{OrgID: org})
	require.NoError(t, err)
	require.True(t, resp.Funded)
	require.Zero(t, resp.AttachedApps)
	require.Zero(t, resp.RepointedEvents)
	require.Equal(t, 2, liveTimerCount(store, app))
}
