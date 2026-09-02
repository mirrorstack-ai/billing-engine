package billing_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditrecovery"
)

type creditAccessFixture struct {
	store                *fakeStore
	stripe               *fakeStripe
	ownerUserID          uuid.UUID
	ownerAccountID       uuid.UUID
	customerOrgID        uuid.UUID
	customerAccountID    uuid.UUID
	distributorOrgID     uuid.UUID
	distributorAccountID uuid.UUID
	purchaseID           uuid.UUID
}

func newCreditAccessFixture() creditAccessFixture {
	fixture := creditAccessFixture{
		store:                newFakeStore(),
		stripe:               &fakeStripe{},
		ownerUserID:          uuid.New(),
		ownerAccountID:       uuid.New(),
		customerOrgID:        uuid.New(),
		customerAccountID:    uuid.New(),
		distributorOrgID:     uuid.New(),
		distributorAccountID: uuid.New(),
		purchaseID:           uuid.New(),
	}
	fixture.store.accountsByUser[fixture.ownerUserID] = fakeAccount{id: fixture.ownerAccountID}
	fixture.store.accountsByOrg[fixture.customerOrgID] = fakeAccount{id: fixture.customerAccountID}
	fixture.store.accountsByOrg[fixture.distributorOrgID] = fakeAccount{id: fixture.distributorAccountID}
	fixture.store.distributorCustomers[distributorCustomerKey{
		distributorOrgID: fixture.distributorOrgID,
		customerOrgID:    fixture.customerOrgID,
	}] = fixture.customerAccountID
	fixture.store.stripeCustomerOf[fixture.ownerAccountID] = "cus_credit_access"
	fixture.store.creditStanding[fixture.ownerAccountID] = billing.CreditStandingRow{
		BillingMode:       billing.BillingModeCredits,
		BalanceMicros:     2 * billing.MinCreditPurchaseMicros,
		CreditLimitMicros: billing.DefaultCreditsLimitMicros,
	}
	fixture.store.creditStanding[fixture.customerAccountID] = billing.CreditStandingRow{
		BillingMode:       billing.BillingModeCredits,
		BalanceMicros:     billing.MinCreditPurchaseMicros,
		CreditLimitMicros: billing.DefaultCreditsLimitMicros,
	}
	fixture.store.creditPurchases[fixture.purchaseID] = billing.CreditPurchaseRow{
		ID:                 fixture.purchaseID,
		AccountID:          fixture.ownerAccountID,
		AmountMicros:       billing.MinCreditPurchaseMicros,
		Type:               "purchase",
		Status:             "settled",
		BalanceAfterMicros: 2 * billing.MinCreditPurchaseMicros,
		Actor:              "self",
		IdempotencyKey:     "credit-access-finished",
	}
	return fixture
}

func (s *fakeStore) creditWalletCallCount() int {
	return s.creditStandingReads +
		s.creditLedgerListReads +
		s.creditIdempotencyReads +
		s.creditPurchaseCreates +
		s.creditPurchaseReads +
		s.creditPurchaseAttaches +
		s.creditPurchaseFinalizes +
		s.creditAutoTopUpWrites +
		s.creditGateSnapshotReads +
		s.creditBillingModeWrites +
		s.distributorStateReads +
		s.creditGrantInserts
}

type creditAccessRPCCase struct {
	name              string
	expectedAccountID func(creditAccessFixture) uuid.UUID
	relationshipRead  bool
	call              func(*testing.T, *billing.Service, creditAccessFixture) error
}

func creditAccessRPCCases() []creditAccessRPCCase {
	return []creditAccessRPCCase{
		{
			name:              "GetCreditStanding",
			expectedAccountID: func(f creditAccessFixture) uuid.UUID { return f.ownerAccountID },
			call: func(t *testing.T, svc *billing.Service, f creditAccessFixture) error {
				resp, err := svc.GetCreditStanding(context.Background(), billing.GetCreditStandingRequest{
					OwnerUserID: f.ownerUserID,
				})
				if err == nil {
					require.NotNil(t, resp)
				}
				return err
			},
		},
		{
			name:              "ListCreditLedger",
			expectedAccountID: func(f creditAccessFixture) uuid.UUID { return f.ownerAccountID },
			call: func(t *testing.T, svc *billing.Service, f creditAccessFixture) error {
				resp, err := svc.ListCreditLedger(context.Background(), billing.ListCreditLedgerRequest{
					OwnerUserID: f.ownerUserID,
				})
				if err == nil {
					require.NotNil(t, resp)
				}
				return err
			},
		},
		{
			name:              "StartCreditPurchase",
			expectedAccountID: func(f creditAccessFixture) uuid.UUID { return f.ownerAccountID },
			call: func(t *testing.T, svc *billing.Service, f creditAccessFixture) error {
				resp, err := svc.StartCreditPurchase(context.Background(), billing.StartCreditPurchaseRequest{
					OwnerUserID:    f.ownerUserID,
					AmountMicros:   billing.MinCreditPurchaseMicros,
					IdempotencyKey: "credit-access-start",
				})
				if err == nil {
					require.NotNil(t, resp)
				}
				return err
			},
		},
		{
			name:              "SetAutoTopUp",
			expectedAccountID: func(f creditAccessFixture) uuid.UUID { return f.ownerAccountID },
			call: func(t *testing.T, svc *billing.Service, f creditAccessFixture) error {
				resp, err := svc.SetAutoTopUp(context.Background(), billing.SetAutoTopUpRequest{
					OwnerUserID: f.ownerUserID,
				})
				if err == nil {
					require.NotNil(t, resp)
				}
				return err
			},
		},
		{
			name:              "SetCustomerBillingMode",
			expectedAccountID: func(f creditAccessFixture) uuid.UUID { return f.customerAccountID },
			relationshipRead:  true,
			call: func(t *testing.T, svc *billing.Service, f creditAccessFixture) error {
				resp, err := svc.SetCustomerBillingMode(context.Background(), billing.SetCustomerBillingModeRequest{
					OwnerOrgID:       f.customerOrgID,
					DistributorOrgID: f.distributorOrgID,
					BillingMode:      billing.BillingModeStandard,
				})
				if err == nil {
					require.NotNil(t, resp)
				}
				return err
			},
		},
		{
			name:              "ListDistributorCustomers",
			expectedAccountID: func(f creditAccessFixture) uuid.UUID { return f.distributorAccountID },
			call: func(t *testing.T, svc *billing.Service, f creditAccessFixture) error {
				resp, err := svc.ListDistributorCustomers(context.Background(), billing.ListDistributorCustomersRequest{
					DistributorOrgID: f.distributorOrgID,
				})
				if err == nil {
					require.NotNil(t, resp)
				}
				return err
			},
		},
		{
			name:              "GrantCredits",
			expectedAccountID: func(f creditAccessFixture) uuid.UUID { return f.customerAccountID },
			relationshipRead:  true,
			call: func(t *testing.T, svc *billing.Service, f creditAccessFixture) error {
				resp, err := svc.GrantCredits(context.Background(), billing.GrantCreditsRequest{
					DistributorOrgID: f.distributorOrgID,
					CustomerOrgID:    f.customerOrgID,
					AmountMicros:     1_000_000,
					Actor:            "distributor",
					IdempotencyKey:   "credit-access-grant",
				})
				if err == nil {
					require.NotNil(t, resp)
				}
				return err
			},
		},
	}
}

func TestFinishCreditPurchase_BypassesCurrentRolloutSelectionForAuthorizedAttempt(t *testing.T) {
	fixture := newCreditAccessFixture()
	var accessed []uuid.UUID
	svc := newCreditPurchaseTestService(fixture.store, fixture.stripe).
		WithCreditWallet(true).
		WithCreditAccess(func(accountID uuid.UUID) bool {
			accessed = append(accessed, accountID)
			return false
		})

	response, err := svc.FinishCreditPurchase(
		context.Background(),
		billing.FinishCreditPurchaseRequest{
			OwnerUserID: fixture.ownerUserID,
			PurchaseID:  fixture.purchaseID.String(),
		},
	)

	require.NoError(t, err)
	require.Equal(t, "settled", response.Status)
	require.Empty(t, accessed, "recovery must not consult mutable rollout selection")
	require.Positive(t, fixture.store.creditPurchaseReads)
}

func TestFinishCreditPurchaseCapabilityFalseStopsBeforeWalletStore(t *testing.T) {
	fixture := newCreditAccessFixture()
	probeCalls := 0
	svc := newCreditPurchaseTestService(fixture.store, fixture.stripe).
		WithCreditRecoveryCapability(creditrecovery.NewRuntimeCapability(
			func(context.Context) (bool, error) {
				probeCalls++
				return false, nil
			},
		))

	response, err := svc.FinishCreditPurchase(
		context.Background(),
		billing.FinishCreditPurchaseRequest{
			OwnerUserID: fixture.ownerUserID,
			PurchaseID:  fixture.purchaseID.String(),
		},
	)

	require.Nil(t, response)
	requireBillingErrorCode(t, err, billing.CodeUnavailable)
	require.Equal(t, 1, probeCalls)
	require.Zero(t, fixture.store.creditPurchaseReads)
	require.Zero(t, fixture.store.creditStandingReads)
	require.Zero(t, fixture.store.creditWalletCallCount(),
		"catalog capability failure must stop before every wallet Store call")
}

func TestCreditRPCs_AccountAccessDeniedBeforeWalletStoreAccess(t *testing.T) {
	for _, tc := range creditAccessRPCCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newCreditAccessFixture()
			var accessed []uuid.UUID
			svc := newCreditPurchaseTestService(fixture.store, fixture.stripe).
				WithCreditWallet(true).
				WithCreditAccess(func(accountID uuid.UUID) bool {
					accessed = append(accessed, accountID)
					return false
				})

			err := tc.call(t, svc, fixture)

			requireBillingErrorCode(t, err, billing.CodeUnavailable)
			require.Equal(t, []uuid.UUID{tc.expectedAccountID(fixture)}, accessed,
				"the account selector must run only after legacy ownership resolution")
			require.Zero(t, fixture.store.creditWalletCallCount(),
				"an excluded account must not name any wallet table through the Store")
			if tc.relationshipRead {
				require.Equal(t, 1, fixture.store.distributorRelationReads,
					"the established distributor relationship must resolve before rollout selection")
			}
		})
	}
}

func TestCreditRPCs_AccountAccessSelectedRunsExistingWalletPath(t *testing.T) {
	for _, tc := range creditAccessRPCCases() {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newCreditAccessFixture()
			var accessed []uuid.UUID
			svc := newCreditPurchaseTestService(fixture.store, fixture.stripe).
				WithCreditWallet(true).
				WithCreditAccess(func(accountID uuid.UUID) bool {
					accessed = append(accessed, accountID)
					return true
				})

			err := tc.call(t, svc, fixture)

			require.NoError(t, err)
			require.Equal(t, []uuid.UUID{tc.expectedAccountID(fixture)}, accessed)
			require.Positive(t, fixture.store.creditWalletCallCount(),
				"a selected account must preserve the existing wallet implementation")
			if tc.relationshipRead {
				require.Equal(t, 1, fixture.store.distributorRelationReads)
			}
		})
	}
}
