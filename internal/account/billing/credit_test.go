package billing_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit/rollout"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

type countingCreditObserver struct {
	accounts               []uuid.UUID
	settlementObservations []bool
}

func (o *countingCreditObserver) ObserveAccount(ctx context.Context, accountID uuid.UUID) error {
	o.accounts = append(o.accounts, accountID)
	o.settlementObservations = append(
		o.settlementObservations,
		creditledger.IsSettlementObservation(ctx),
	)
	return nil
}

func TestGetCreditStanding_FoldsSingleServiceStatusGate(t *testing.T) {
	tests := []struct {
		name            string
		signals         billing.ServiceSignals
		balanceMicros   int64
		creditLimit     int64
		gateBlocked     bool
		wantBlocked     bool
		wantBlockReason string
	}{
		{
			name:            "otherwise eligible authoritative shortfall is out of credits",
			signals:         billing.ServiceSignals{UsableCardCount: 1, FirstChargeStatus: "paid"},
			balanceMicros:   0,
			gateBlocked:     true,
			wantBlocked:     true,
			wantBlockReason: "out_of_credits",
		},
		{
			name:            "base eligibility reason keeps priority",
			signals:         billing.ServiceSignals{FirstChargeStatus: "paid"},
			balanceMicros:   0,
			gateBlocked:     true,
			wantBlocked:     true,
			wantBlockReason: "card_gate",
		},
		{
			name:          "zero-limit equality stays eligible when authoritative gate allows",
			signals:       billing.ServiceSignals{UsableCardCount: 1, FirstChargeStatus: "paid"},
			balanceMicros: 0,
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
				CreditLimitMicros: tc.creditLimit,
			}
			gate := &fixedCreditGate{blocked: tc.gateBlocked}
			svc := billing.NewService(store, &fakeStripe{}, "").
				WithCreditWallet(true).
				WithCreditCoordinator(gate, nil)

			resp, err := svc.GetCreditStanding(context.Background(), billing.GetCreditStandingRequest{
				OwnerUserID: userID,
			})

			require.NoError(t, err)
			require.Equal(t, billing.BillingModeCredits, resp.BillingMode)
			require.Equal(t, tc.balanceMicros, resp.BalanceMicros)
			require.Equal(t, tc.creditLimit, resp.CreditLimitMicros)
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
			svc, proposals := creditPurchaseSvcProposing(store, stripeFake)
			svc = svc.WithCreditWallet(true)

			resp, err := svc.StartCreditPurchase(context.Background(), billing.StartCreditPurchaseRequest{
				OwnerUserID:    userID,
				AmountMicros:   tc.amountMicros,
				IdempotencyKey: "bounds-" + tc.name,
			})

			if !tc.wantValid {
				requireBillingErrorCode(t, err, billing.CodeInvalidInput)
				require.Nil(t, resp)
				require.Zero(t, store.creditPurchaseCreates)
				require.Empty(t, proposals.charges,
					"an out-of-bounds amount was sealed as an obligation; the bound has to "+
						"hold before the document exists, not after")
				require.Empty(t, stripeFake.creditDraftCalls)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, "stripe", resp.Rail)
			require.Equal(t, billing.PurchaseStartProposed, resp.Status)
			require.Nil(t, resp.Stripe, "a proposed purchase has no invoice to pay against")
			require.Equal(t, 1, store.creditPurchaseCreates)

			// The three calls this assertion used to count are deleted. Their
			// absence is now the assertion.
			require.Empty(t, stripeFake.creditDraftCalls)
			require.Empty(t, stripeFake.creditItemCalls)
			require.Empty(t, stripeFake.creditFinalizeCalls)

			// 🔴 THE BOUNDARY AMOUNT, ON THE NEW ARTIFACT. It used to be read
			// off the Stripe invoice item; it is now read off the sealed
			// intent, and it is the same integer either way. A boundary that
			// drifted here would change what the customer pays at exactly the
			// amounts nobody re-checks by hand.
			sealed := proposals.onlySealed(t)
			require.Equal(t, intent.KindCreditPurchase, sealed.Kind())
			// Seal normalizes the case of the leg's "usd" (chargeintent.go:420);
			// the denomination itself is what must not drift.
			require.Equal(t, "USD", sealed.Currency())
			require.EqualValues(t, tc.amountMicros, sealed.TotalMicros(),
				"the boundary amount changed in the cutover")
			require.EqualValues(t, tc.amountMicros, sealed.ProviderRemainderMicros(),
				"§6: buying credit draws nothing from the wallet it tops up, so the "+
					"whole obligation is the provider's to collect")

			// The old assertion was on cents, because that is what actually
			// reached Stripe. Both bounds must still be a whole number of
			// cents, or the single rounding the adapter performs silently
			// changes the boundary.
			require.Zero(t, sealed.ProviderRemainderMicros()%10_000,
				"the boundary no longer lands on a whole cent")
			require.Equal(t, tc.amountMicros/10_000, sealed.ProviderRemainderMicros()/10_000)

			require.Equal(t,
				"intent:"+sealed.Digest(),
				store.creditPurchaseProposedRefs[uuid.MustParse(resp.PurchaseID)],
				"the durable row cannot be walked to the intent that now owns this money")
		})
	}
}

// The same test's subject on the new rail: ONE key, ONE obligation.
//
// It used to say "one Stripe invoice flow" because a second flow was a second
// invoice and a second card charge. The intent rail's version of that double
// charge is a second SEALED INTENT for one purchase — two documents, each
// collectable — so that is what the replay must not produce.
func TestStartCreditPurchase_SameKeySealsExactlyOneIntent(t *testing.T) {
	store := newFakeStore()
	userID, accountID := uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	store.stripeCustomerOf[accountID] = "cus_credit"
	stripeFake := &fakeStripe{}
	svc, proposals := creditPurchaseSvcProposing(store, stripeFake)
	svc = svc.WithCreditWallet(true)
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
	require.Equal(t, billing.PurchaseStartProposed, first.Status)
	require.Equal(t, first.Status, second.Status,
		"the replay reported a different shape than the call it is replaying")
	require.Nil(t, first.Stripe)
	require.Nil(t, second.Stripe)
	require.Equal(t, 1, store.creditPurchaseCreates)
	require.Equal(t, 2, store.creditIdempotencyReads)

	// 🔴 One key, one sealed obligation, for the exact amount asked for.
	sealed := proposals.onlySealed(t)
	require.EqualValues(t, req.AmountMicros, sealed.TotalMicros())
	require.EqualValues(t, req.AmountMicros, sealed.ProviderRemainderMicros())
	require.Equal(t, 1, store.creditPurchaseProposes,
		"the replay re-marked a row the first call had already moved")
	require.Equal(t,
		"intent:"+sealed.Digest(),
		store.creditPurchaseProposedRefs[uuid.MustParse(first.PurchaseID)],
		"the replay repointed the purchase at a different intent")

	// The four Stripe calls this test used to count are the flow that was
	// deleted. Neither call may reach the provider at all — not even to read:
	// a proposed purchase has no resource there to be authoritative about.
	require.Empty(t, stripeFake.creditFindCalls)
	require.Empty(t, stripeFake.creditDraftCalls)
	require.Empty(t, stripeFake.creditItemCalls)
	require.Empty(t, stripeFake.creditFinalizeCalls)
	require.Empty(t, stripeFake.creditGetCalls)
}

func TestCreditMutationObserversRunOnlyForFirstDurableTransition(t *testing.T) {
	// 🔴 Starting a purchase no longer produces a durable SETTLEMENT at all.
	//
	// The legacy start collected: finalize returned "paid", the wallet gained
	// the credit, and the observer fired once for that transition. The intent
	// rail seals a document and collects nothing, so the honest assertion on
	// this leg is the other side of the same rule — an observation must not
	// fire for a balance that did not move, on the first call or the replay.
	//
	// The "exactly once for the first durable transition" rule itself is still
	// pinned by the three sibling subtests below, which all still settle.
	t.Run("start purchase proposes and settles nothing", func(t *testing.T) {
		store := newFakeStore()
		userID, accountID := uuid.New(), uuid.New()
		store.accountsByUser[userID] = fakeAccount{id: accountID}
		store.stripeCustomerOf[accountID] = "cus_credit"
		store.creditStanding[accountID] = billing.CreditStandingRow{
			BillingMode:   billing.BillingModeCredits,
			BalanceMicros: 7_000_000,
		}
		stripeFake := &fakeStripe{}
		observer := &countingCreditObserver{}
		svc, proposals := creditPurchaseSvcProposing(store, stripeFake)
		svc = svc.WithCreditWallet(true).
			WithCreditCoordinator(nil, observer)
		req := billing.StartCreditPurchaseRequest{
			OwnerUserID: userID, AmountMicros: 12_340_000,
			IdempotencyKey: "settled-start-observer",
		}

		_, err := svc.StartCreditPurchase(context.Background(), req)
		require.NoError(t, err)
		_, err = svc.StartCreditPurchase(context.Background(), req)
		require.NoError(t, err)

		require.Empty(t, observer.accounts,
			"a proposal observed a settlement; nothing was collected, so the "+
				"projection would be told a balance changed when it did not")
		require.Empty(t, observer.settlementObservations)
		require.EqualValues(t, 7_000_000, store.creditStanding[accountID].BalanceMicros,
			"a proposed purchase credited the wallet before anyone paid for it")

		// The zero above is not vacuous: the purchase really was taken, once,
		// for the full amount, and the replay added nothing.
		require.Len(t, proposals.sealed, 1)
		require.EqualValues(t, req.AmountMicros, proposals.sealed[0].TotalMicros())
		require.Equal(t, 1, store.creditPurchaseCreates)
		require.Equal(t, 1, store.creditPurchaseProposes)
		require.Empty(t, stripeFake.creditFinalizeCalls)
	})

	t.Run("finish purchase settled replay", func(t *testing.T) {
		store := newFakeStore()
		userID, accountID, purchaseID := uuid.New(), uuid.New(), uuid.New()
		store.accountsByUser[userID] = fakeAccount{id: accountID}
		store.stripeCustomerOf[accountID] = "cus_original_sponsor"
		store.creditPurchases[purchaseID] = billing.CreditPurchaseRow{
			ID: purchaseID, AccountID: accountID,
			AmountMicros: billing.MinCreditPurchaseMicros,
			Type:         "purchase", Status: "pending", Actor: "self",
			IdempotencyKey:  "settled-finish-observer",
			StripeInvoiceID: "in_paid_credit",
		}
		stripeFake := &fakeStripe{}
		stripeFake.seedExactCreditPurchaseInvoice(
			store.creditPurchases[purchaseID],
			"cus_original_sponsor",
			"paid",
		)
		observer := &countingCreditObserver{}
		svc := newCreditPurchaseTestService(store, stripeFake).
			WithCreditWallet(true).
			WithCreditCoordinator(nil, observer)
		req := billing.FinishCreditPurchaseRequest{
			OwnerUserID: userID, PurchaseID: purchaseID.String(),
		}

		_, err := svc.FinishCreditPurchase(context.Background(), req)
		require.NoError(t, err)
		_, err = svc.FinishCreditPurchase(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{accountID}, observer.accounts)
		require.Equal(t, []bool{true}, observer.settlementObservations)
	})

	t.Run("grant replay", func(t *testing.T) {
		store := newFakeStore()
		customerOrgID, accountID := uuid.New(), uuid.New()
		store.accountsByOrg[customerOrgID] = fakeAccount{id: accountID}
		observer := &countingCreditObserver{}
		svc := billing.NewService(store, &fakeStripe{}, "").
			WithCreditWallet(true).
			WithCreditCoordinator(nil, observer)
		req := billing.GrantCreditsRequest{
			CustomerOrgID: customerOrgID, AmountMicros: 1_000_000,
			Actor: "system", IdempotencyKey: "grant-observer",
		}

		_, err := svc.GrantCredits(context.Background(), req)
		require.NoError(t, err)
		_, err = svc.GrantCredits(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{accountID}, observer.accounts)
	})

	t.Run("billing mode no-op", func(t *testing.T) {
		store := newFakeStore()
		userID, accountID := uuid.New(), uuid.New()
		store.accountsByUser[userID] = fakeAccount{id: accountID}
		store.creditStanding[accountID] = billing.CreditStandingRow{
			BillingMode: billing.BillingModeStandard,
		}
		observer := &countingCreditObserver{}
		svc := billing.NewService(store, &fakeStripe{}, "").
			WithCreditWallet(true).
			WithCreditCoordinator(nil, observer)
		req := billing.SetCustomerBillingModeRequest{
			OwnerUserID: userID, BillingMode: billing.BillingModeCredits,
		}

		_, err := svc.SetCustomerBillingMode(context.Background(), req)
		require.NoError(t, err)
		_, err = svc.SetCustomerBillingMode(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{accountID}, observer.accounts)
	})
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

func TestFinishCreditPurchase_RolloutOffStillRecoversAuthorizedPurchase(t *testing.T) {
	store := newFakeStore()
	userID, accountID, purchaseID := uuid.New(), uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	store.creditPurchases[purchaseID] = billing.CreditPurchaseRow{
		ID: purchaseID, AccountID: accountID,
		AmountMicros:   billing.MinCreditPurchaseMicros,
		Type:           "purchase",
		Status:         "settled",
		IdempotencyKey: "rollout-off-finish",
	}
	svc := newCreditPurchaseTestService(store, &fakeStripe{}).
		WithCreditWallet(false)

	response, err := svc.FinishCreditPurchase(
		context.Background(),
		billing.FinishCreditPurchaseRequest{
			OwnerUserID: userID,
			PurchaseID:  purchaseID.String(),
		},
	)

	require.NoError(t, err)
	require.Equal(t, "settled", response.Status)
}

func TestFinishCreditPurchase_AttachedSponsorInvoiceSurvivesFundingDrift(t *testing.T) {
	store := newFakeStore()
	orgID, accountID := uuid.New(), uuid.New()
	newSponsorAccountID, purchaseID := uuid.New(), uuid.New()
	store.accountsByOrg[orgID] = fakeAccount{id: accountID}
	store.fundingOf[accountID] = newSponsorAccountID
	store.stripeCustomerOf[newSponsorAccountID] = "cus_new_sponsor"
	store.creditPurchases[purchaseID] = billing.CreditPurchaseRow{
		ID: purchaseID, AccountID: accountID,
		AmountMicros:            billing.MinCreditPurchaseMicros,
		Type:                    "purchase",
		Status:                  "failed",
		IdempotencyKey:          "sponsor-drift-recovery",
		StripeInvoiceID:         "in_old_sponsor_paid",
		BalanceAfterMicros:      billing.MinCreditPurchaseMicros,
		ChargeFundingAccountID:  accountID,
		ChargeFundingGeneration: uuid.New(),
		StripeCustomerID:        "cus_original_sponsor",
	}
	stripeFake := &fakeStripe{}
	stripeFake.seedExactCreditPurchaseInvoice(
		store.creditPurchases[purchaseID],
		"cus_original_sponsor",
		"paid",
	)
	svc := newCreditPurchaseTestService(store, stripeFake).
		WithCreditWallet(false)

	response, err := svc.FinishCreditPurchase(
		context.Background(),
		billing.FinishCreditPurchaseRequest{
			OwnerOrgID: orgID,
			PurchaseID: purchaseID.String(),
		},
	)

	require.NoError(t, err)
	require.Equal(t, "settled", response.Status)
	require.Zero(t, store.chargeFundingReads,
		"an attached invoice freezes payer identity; recovery must not consult the current sponsor")
}

func TestFinishCreditPurchase_ExcludedSettlementDoesNotEnterRolloutGraph(t *testing.T) {
	store := newFakeStore()
	userID, accountID, purchaseID := uuid.New(), uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	store.creditPurchases[purchaseID] = billing.CreditPurchaseRow{
		ID: purchaseID, AccountID: accountID,
		AmountMicros:            billing.MinCreditPurchaseMicros,
		Type:                    "purchase",
		Status:                  "failed",
		IdempotencyKey:          "excluded-paid-recovery",
		StripeInvoiceID:         "in_excluded_paid",
		ChargeFundingAccountID:  accountID,
		ChargeFundingGeneration: uuid.New(),
		StripeCustomerID:        "cus_excluded_original",
	}
	stripeFake := &fakeStripe{}
	stripeFake.seedExactCreditPurchaseInvoice(
		store.creditPurchases[purchaseID],
		"cus_excluded_original",
		"paid",
	)
	enforceObserver := &countingCreditObserver{}
	excludedPolicy := rollout.Parse(rollout.Config{
		MasterEnabled:   true,
		SchemaReady:     true,
		Component:       rollout.ComponentAPI,
		Mode:            string(rollout.ModeEnforce),
		BasisPoints:     "0",
		AllowlistSHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		CoreManifestSHA: "1111111111111111111111111111111111111111",
		BillingSHA:      "2222222222222222222222222222222222222222",
	})
	controller := rollout.NewController(excludedPolicy, nil)
	svc := newCreditPurchaseTestService(store, stripeFake).
		WithCreditWallet(false).
		WithCreditAccess(func(uuid.UUID) bool { return false }).
		WithCreditCoordinator(
			nil,
			rollout.NewSettlementObserver(controller, enforceObserver),
		)

	response, err := svc.FinishCreditPurchase(
		context.Background(),
		billing.FinishCreditPurchaseRequest{
			OwnerUserID: userID,
			PurchaseID:  purchaseID.String(),
		},
	)

	require.NoError(t, err)
	require.Equal(t, "settled", response.Status)
	require.Empty(t, enforceObserver.accounts,
		"excluded recovery commits durable money without entering the enforce graph")
}

func requireBillingErrorCode(t *testing.T, err error, code billing.Code) {
	t.Helper()
	var billingErr *billing.Error
	require.ErrorAs(t, err, &billingErr)
	require.Equal(t, code, billingErr.Code)
}
