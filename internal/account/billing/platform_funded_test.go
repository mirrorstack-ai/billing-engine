package billing_test

// TB-FUND (billing-engine#348), the serving-standing leg: without it a
// platform-funded org's unbilled apps would be BLOCKED at the edge on the card
// gate. The audit line proves the path ran; the controls prove nothing else moved.

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/eligibility"
	"github.com/mirrorstack-ai/billing-engine/internal/account/platformfunded"
	"github.com/mirrorstack-ai/billing-engine/internal/account/platformfunded/pftest"
)

func porgStatusSvc(t *testing.T, store *fakeStore, orgs ...uuid.UUID) *billing.Service {
	t.Helper()
	ids := make([]string, len(orgs))
	for i, o := range orgs {
		ids[i] = o.String()
	}
	set, err := platformfunded.Parse(strings.Join(ids, ","))
	require.NoError(t, err)
	return billing.NewService(store, &fakeStripe{}, "").WithPlatformOrgs(set)
}

func TestGetServiceStatus_PlatformFundedOrgWithNoFundingIsNotBlockedAndIsAudited(t *testing.T) {
	uses := pftest.Capture(t)
	store := newFakeStore()
	org := uuid.New() // no designation, no account, no card

	resp, err := porgStatusSvc(t, store, org).GetServiceStatus(context.Background(),
		billing.GetServiceStatusRequest{OrgID: org})

	require.NoError(t, err)
	require.False(t, resp.Blocked)
	require.Equal(t, string(eligibility.ReasonEligible), resp.Reason)
	require.Equal(t, []pftest.Use{{Gate: platformfunded.GateServiceState, OrgID: org.String()}}, uses())
}

func TestGetServiceStatus_OrgOutsidePORGStaysBlockedOnTheCardGate(t *testing.T) {
	uses := pftest.Capture(t)
	store := newFakeStore()
	porg, outsider := uuid.New(), uuid.New()

	resp, err := porgStatusSvc(t, store, porg).GetServiceStatus(context.Background(),
		billing.GetServiceStatusRequest{OrgID: outsider})

	require.NoError(t, err)
	require.True(t, resp.Blocked)
	require.Equal(t, string(eligibility.ReasonNoUsableCard), resp.Reason)
	require.Empty(t, uses())
}

func TestGetServiceStatus_PlatformFundedDoesNotOverrideAResolvedAccountsStanding(t *testing.T) {
	// The attribute fills "no resolved funding" only: a PORG org whose account
	// resolves is judged on its own signals, here a card gone.
	uses := pftest.Capture(t)
	store := newFakeStore()
	org, acct := uuid.New(), uuid.New()
	store.accountsByOrg[org] = fakeAccount{id: acct}
	store.fundedOrgs[org] = true
	store.serviceSignals[acct] = billing.ServiceSignals{UsableCardCount: 0}

	resp, err := porgStatusSvc(t, store, org).GetServiceStatus(context.Background(),
		billing.GetServiceStatusRequest{OrgID: org})

	require.NoError(t, err)
	require.True(t, resp.Blocked)
	require.Equal(t, string(eligibility.ReasonNoUsableCard), resp.Reason)
	require.Empty(t, uses())
}

func TestGetServiceStatus_PlatformFundedNeverAppliesToAUser(t *testing.T) {
	uses := pftest.Capture(t)
	store := newFakeStore()
	user := uuid.New()

	resp, err := porgStatusSvc(t, store, user).GetServiceStatus(context.Background(),
		billing.GetServiceStatusRequest{UserID: user})

	require.NoError(t, err)
	require.True(t, resp.Blocked)
	require.Equal(t, string(eligibility.ReasonNoUsableCard), resp.Reason)
	require.Empty(t, uses())
}
