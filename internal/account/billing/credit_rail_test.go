package billing_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// A customer org linked to its distributor (ms_billing.org_distributors,
// migration 053) must never pay MirrorStack's own Stripe merchant —
// core-v2#231 decision 8 says that org pays the distributor's own store.
// Before this change, StartCreditPurchase never looked at the distributor
// link at all, so a linked org sealed a credit-purchase obligation against
// MirrorStack's own rail exactly like an unlinked one. This test fails on
// that code: it proposes a charge intent and returns rail "stripe" instead of
// CREDIT_RAIL_UNAVAILABLE.
func TestStartCreditPurchase_RefusesOrgLinkedToDistributor(t *testing.T) {
	store := newFakeStore()
	customerOrgID, accountID, distributorOrgID := uuid.New(), uuid.New(), uuid.New()
	store.accountsByOrg[customerOrgID] = fakeAccount{id: accountID}
	store.stripeCustomerOf[accountID] = "cus_linked_customer"
	store.orgDistributors = map[uuid.UUID]uuid.UUID{customerOrgID: distributorOrgID}
	stripeFake := &fakeStripe{}
	svc, proposals := creditPurchaseSvcProposing(store, stripeFake)
	svc = svc.WithCreditWallet(true)

	resp, err := svc.StartCreditPurchase(context.Background(), billing.StartCreditPurchaseRequest{
		OwnerOrgID:     customerOrgID,
		AmountMicros:   billing.MinCreditPurchaseMicros,
		IdempotencyKey: "distributor-linked-purchase",
	})

	requireBillingErrorCode(t, err, billing.CodeCreditRailUnavailable)
	require.Nil(t, resp)
	require.Zero(t, store.creditPurchaseCreates, "no ledger row may be created for a linked org")
	require.Empty(t, proposals.charges, "no charge intent may be proposed for a linked org")
	require.Empty(t, stripeFake.creditDraftCalls)
	require.Empty(t, stripeFake.creditItemCalls)
	require.Empty(t, stripeFake.creditFinalizeCalls)
	require.Empty(t, stripeFake.creditFindCalls)
	require.Empty(t, stripeFake.creditGetCalls)
	require.Empty(t, stripeFake.createdCustomers)
}

// An org with no distributor row, and a personal account, must keep the
// existing Stripe path byte for byte.
func TestStartCreditPurchase_UnlinkedOrgAndPersonalAccountKeepStripe(t *testing.T) {
	t.Run("org with no distributor link", func(t *testing.T) {
		store := newFakeStore()
		customerOrgID, accountID := uuid.New(), uuid.New()
		store.accountsByOrg[customerOrgID] = fakeAccount{id: accountID}
		store.stripeCustomerOf[accountID] = "cus_unlinked_customer"
		svc, _ := creditPurchaseSvcProposing(store, &fakeStripe{})
		svc = svc.WithCreditWallet(true)

		resp, err := svc.StartCreditPurchase(context.Background(), billing.StartCreditPurchaseRequest{
			OwnerOrgID:     customerOrgID,
			AmountMicros:   billing.MinCreditPurchaseMicros,
			IdempotencyKey: "unlinked-org-purchase",
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, "stripe", resp.Rail)
	})

	t.Run("personal account", func(t *testing.T) {
		store := newFakeStore()
		userID, accountID := uuid.New(), uuid.New()
		store.accountsByUser[userID] = fakeAccount{id: accountID}
		store.stripeCustomerOf[accountID] = "cus_personal"
		svc, _ := creditPurchaseSvcProposing(store, &fakeStripe{})
		svc = svc.WithCreditWallet(true)

		resp, err := svc.StartCreditPurchase(context.Background(), billing.StartCreditPurchaseRequest{
			OwnerUserID:    userID,
			AmountMicros:   billing.MinCreditPurchaseMicros,
			IdempotencyKey: "personal-purchase",
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, "stripe", resp.Rail)
	})
}

// A customer org linked to a distributor cannot enable auto top-up; disabling
// an existing config still works.
func TestSetAutoTopUp_RefusesEnableForOrgLinkedToDistributor(t *testing.T) {
	store := newFakeStore()
	customerOrgID, accountID, distributorOrgID := uuid.New(), uuid.New(), uuid.New()
	store.accountsByOrg[customerOrgID] = fakeAccount{id: accountID}
	store.stripeCustomerOf[accountID] = "cus_linked_customer"
	store.orgDistributors = map[uuid.UUID]uuid.UUID{customerOrgID: distributorOrgID}
	svc, _ := creditPurchaseSvcProposing(store, &fakeStripe{})
	svc = svc.WithCreditWallet(true)

	_, err := svc.SetAutoTopUp(context.Background(), billing.SetAutoTopUpRequest{
		OwnerOrgID:      customerOrgID,
		Enabled:         true,
		ThresholdMicros: 0,
		AmountMicros:    billing.MinCreditPurchaseMicros,
		PaymentMethodID: uuid.NewString(),
	})
	requireBillingErrorCode(t, err, billing.CodeCreditRailUnavailable)
	require.Zero(t, store.creditAutoTopUpWrites, "no config write may happen for a refused enable")

	_, err = svc.SetAutoTopUp(context.Background(), billing.SetAutoTopUpRequest{
		OwnerOrgID: customerOrgID,
		Enabled:    false,
	})
	require.NoError(t, err, "disabling auto top-up for a linked org must still work")
}
