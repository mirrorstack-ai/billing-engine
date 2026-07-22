package billing_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

func TestGetCreditStanding_FoldsEligibilityAndCreditLimit(t *testing.T) {
	tests := []struct {
		name            string
		signals         billing.ServiceSignals
		balanceMicros   int64
		wantBlocked     bool
		wantBlockReason string
	}{
		{
			name:            "otherwise eligible exhausted wallet is out of credits",
			signals:         billing.ServiceSignals{UsableCardCount: 1, FirstChargeStatus: "paid"},
			balanceMicros:   -5_000_000,
			wantBlocked:     true,
			wantBlockReason: "out_of_credits",
		},
		{
			name:            "base eligibility reason keeps priority",
			signals:         billing.ServiceSignals{FirstChargeStatus: "paid"},
			balanceMicros:   -5_000_000,
			wantBlocked:     true,
			wantBlockReason: "card_gate",
		},
		{
			name:          "wallet above negative limit stays eligible",
			signals:       billing.ServiceSignals{UsableCardCount: 1, FirstChargeStatus: "paid"},
			balanceMicros: -4_999_999,
			wantBlocked:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			userID, accountID := uuid.New(), uuid.New()
			store.accountsByUser[userID] = fakeAccount{id: accountID}
			store.serviceSignals[accountID] = tc.signals
			store.creditStanding[accountID] = billing.CreditStandingRow{
				BillingMode:       billing.BillingModeCredits,
				BalanceMicros:     tc.balanceMicros,
				CreditLimitMicros: 5_000_000,
			}
			svc := billing.NewService(store, &fakeStripe{}, "").WithCreditWallet(true)

			resp, err := svc.GetCreditStanding(context.Background(), billing.GetCreditStandingRequest{
				OwnerUserID: userID,
			})

			require.NoError(t, err)
			require.Equal(t, billing.BillingModeCredits, resp.BillingMode)
			require.Equal(t, tc.balanceMicros, resp.BalanceMicros)
			require.Equal(t, int64(5_000_000), resp.CreditLimitMicros)
			require.Equal(t, tc.wantBlocked, resp.Blocked)
			require.Equal(t, tc.wantBlockReason, resp.BlockReason)
		})
	}
}

func TestStartCreditPurchase_EnforcesInclusiveBounds(t *testing.T) {
	tests := []struct {
		name         string
		amountMicros int64
		wantValid    bool
	}{
		{name: "below minimum", amountMicros: billing.MinCreditPurchaseMicros - 1},
		{name: "minimum", amountMicros: billing.MinCreditPurchaseMicros, wantValid: true},
		{name: "maximum", amountMicros: billing.MaxCreditPurchaseMicros, wantValid: true},
		{name: "above maximum", amountMicros: billing.MaxCreditPurchaseMicros + 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			userID, accountID := uuid.New(), uuid.New()
			store.accountsByUser[userID] = fakeAccount{id: accountID}
			store.stripeCustomerOf[accountID] = "cus_credit"
			stripeFake := &fakeStripe{}
			svc := billing.NewService(store, stripeFake, "").WithCreditWallet(true)

			resp, err := svc.StartCreditPurchase(context.Background(), billing.StartCreditPurchaseRequest{
				OwnerUserID:    userID,
				AmountMicros:   tc.amountMicros,
				IdempotencyKey: "bounds-" + tc.name,
			})

			if !tc.wantValid {
				requireBillingErrorCode(t, err, billing.CodeInvalidInput)
				require.Nil(t, resp)
				require.Zero(t, store.creditPurchaseCreates)
				require.Empty(t, stripeFake.creditDraftCalls)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, "stripe", resp.Rail)
			require.Equal(t, 1, store.creditPurchaseCreates)
			require.Len(t, stripeFake.creditDraftCalls, 1)
			require.Len(t, stripeFake.creditItemCalls, 1)
			require.Equal(t, tc.amountMicros/10_000, stripeFake.creditItemCalls[0].amountCents)
			require.Len(t, stripeFake.creditFinalizeCalls, 1)
		})
	}
}

func TestStartCreditPurchase_SameKeyRunsOneStripeInvoiceFlow(t *testing.T) {
	store := newFakeStore()
	userID, accountID := uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	store.stripeCustomerOf[accountID] = "cus_credit"
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake, "").WithCreditWallet(true)
	req := billing.StartCreditPurchaseRequest{
		OwnerUserID:    userID,
		AmountMicros:   12_340_000,
		IdempotencyKey: "credit-purchase-retry",
	}

	first, err := svc.StartCreditPurchase(context.Background(), req)
	require.NoError(t, err)
	second, err := svc.StartCreditPurchase(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, first.PurchaseID, second.PurchaseID)
	require.Equal(t, first.Stripe, second.Stripe)
	require.Equal(t, 1, store.creditPurchaseCreates)
	require.Equal(t, 2, store.creditIdempotencyReads)
	require.Len(t, stripeFake.creditFindCalls, 1)
	require.Len(t, stripeFake.creditDraftCalls, 1)
	require.Len(t, stripeFake.creditItemCalls, 1)
	require.Len(t, stripeFake.creditFinalizeCalls, 1)
	require.Len(t, stripeFake.creditGetCalls, 1)
}

func TestGrantCredits_InvalidDistributorRelationshipRejectedBeforeInsert(t *testing.T) {
	store := newFakeStore()
	customerOrgID, customerAccountID := uuid.New(), uuid.New()
	store.accountsByOrg[customerOrgID] = fakeAccount{id: customerAccountID}
	// Even a colliding key must not be inspected until the actor has proved it
	// manages the requested customer.
	store.creditLedgerByKey["grant-invalid-relationship"] = billing.CreditLedgerRecord{
		ID:             uuid.New(),
		AccountID:      customerAccountID,
		AmountMicros:   1_000_000,
		Type:           "grant",
		Status:         "settled",
		Actor:          "distributor",
		IdempotencyKey: "grant-invalid-relationship",
	}
	svc := billing.NewService(store, &fakeStripe{}, "").WithCreditWallet(true)

	resp, err := svc.GrantCredits(context.Background(), billing.GrantCreditsRequest{
		DistributorOrgID: uuid.New(),
		CustomerOrgID:    customerOrgID,
		AmountMicros:     1_000_000,
		Actor:            "distributor",
		IdempotencyKey:   "grant-invalid-relationship",
	})

	requireBillingErrorCode(t, err, billing.CodeInvalidInput)
	require.Nil(t, resp)
	require.Equal(t, 1, store.distributorRelationReads)
	require.Zero(t, store.creditIdempotencyReads)
	require.Zero(t, store.creditGrantInserts)
}

func TestCreditRPCs_FlagOffReturnUnavailableBeforeStoreAccess(t *testing.T) {
	ownerUserID := uuid.New()
	ownerOrgID := uuid.New()
	distributorOrgID := uuid.New()
	// A nil store makes the zero-call guarantee executable: any store access
	// before the fail-closed guard would panic instead of returning this error.
	svc := billing.NewService(nil, nil, "").WithCreditWallet(false)

	tests := []struct {
		name string
		call func(t *testing.T) error
	}{
		{
			name: "GetCreditStanding",
			call: func(t *testing.T) error {
				resp, err := svc.GetCreditStanding(context.Background(), billing.GetCreditStandingRequest{OwnerUserID: ownerUserID})
				require.Nil(t, resp)
				return err
			},
		},
		{
			name: "ListCreditLedger",
			call: func(t *testing.T) error {
				resp, err := svc.ListCreditLedger(context.Background(), billing.ListCreditLedgerRequest{OwnerUserID: ownerUserID})
				require.Nil(t, resp)
				return err
			},
		},
		{
			name: "StartCreditPurchase",
			call: func(t *testing.T) error {
				resp, err := svc.StartCreditPurchase(context.Background(), billing.StartCreditPurchaseRequest{
					OwnerUserID: ownerUserID, AmountMicros: billing.MinCreditPurchaseMicros, IdempotencyKey: "flag-off",
				})
				require.Nil(t, resp)
				return err
			},
		},
		{
			name: "FinishCreditPurchase",
			call: func(t *testing.T) error {
				resp, err := svc.FinishCreditPurchase(context.Background(), billing.FinishCreditPurchaseRequest{
					OwnerUserID: ownerUserID, PurchaseID: uuid.New().String(),
				})
				require.Nil(t, resp)
				return err
			},
		},
		{
			name: "SetAutoTopUp",
			call: func(t *testing.T) error {
				resp, err := svc.SetAutoTopUp(context.Background(), billing.SetAutoTopUpRequest{OwnerUserID: ownerUserID})
				require.Nil(t, resp)
				return err
			},
		},
		{
			name: "SetCustomerBillingMode",
			call: func(t *testing.T) error {
				resp, err := svc.SetCustomerBillingMode(context.Background(), billing.SetCustomerBillingModeRequest{
					OwnerUserID: ownerUserID, BillingMode: billing.BillingModeStandard,
				})
				require.Nil(t, resp)
				return err
			},
		},
		{
			name: "ListDistributorCustomers",
			call: func(t *testing.T) error {
				resp, err := svc.ListDistributorCustomers(context.Background(), billing.ListDistributorCustomersRequest{
					DistributorOrgID: distributorOrgID,
				})
				require.Nil(t, resp)
				return err
			},
		},
		{
			name: "GrantCredits",
			call: func(t *testing.T) error {
				resp, err := svc.GrantCredits(context.Background(), billing.GrantCreditsRequest{
					CustomerOrgID: ownerOrgID, AmountMicros: 1, Actor: "system", IdempotencyKey: "flag-off",
				})
				require.Nil(t, resp)
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(t)
			requireBillingErrorCode(t, err, billing.CodeUnavailable)
			var billingErr *billing.Error
			require.ErrorAs(t, err, &billingErr)
			require.Equal(t, "credit wallet is not enabled", billingErr.Message)
		})
	}
}

func requireBillingErrorCode(t *testing.T, err error, code billing.Code) {
	t.Helper()
	var billingErr *billing.Error
	require.ErrorAs(t, err, &billingErr)
	require.Equal(t, code, billingErr.Code)
}
