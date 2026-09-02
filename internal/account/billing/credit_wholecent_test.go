package billing_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// A credit purchase is charged in CENTS (a card cannot take less) but credited
// in MICROS. Settlement checks the rounded cents were paid and then credits the
// raw micros, so any amount that is not a whole cent grants up to 4,999 micros
// nobody paid for — repeatable per purchase, and bounded only by how many
// purchases the caller makes. The bounds check is the only other validation and
// does not constrain the sub-cent digits at all.
//
// The amount below is not invented: it was published, with its yield, in a
// planning document in this PUBLIC repository.
func TestStartCreditPurchase_RejectsSubCentAmounts(t *testing.T) {
	tests := []struct {
		name         string
		amountMicros int64
		wantValid    bool
	}{
		{name: "the published amount", amountMicros: 5_004_999},
		{name: "one micro above the minimum", amountMicros: billing.MinCreditPurchaseMicros + 1},
		{name: "one micro below the maximum", amountMicros: billing.MaxCreditPurchaseMicros - 1},
		{name: "half a cent", amountMicros: 5_000_000 + 5_000},
		{name: "the minimum, a whole cent", amountMicros: billing.MinCreditPurchaseMicros, wantValid: true},
		{name: "a whole cent above the minimum", amountMicros: 5_010_000, wantValid: true},
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
				IdempotencyKey: "wholecent-" + tc.name,
			})

			if !tc.wantValid {
				requireBillingErrorCode(t, err, billing.CodeInvalidInput)
				require.Nil(t, resp)
				// The obligation must not exist. A sub-cent amount that is
				// sealed and only rejected later is a document the executor
				// would still be entitled to collect.
				require.Zero(t, store.creditPurchaseCreates)
				require.Empty(t, proposals.charges)
				require.Empty(t, stripeFake.creditDraftCalls)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp,
				"a whole-cent amount inside the bounds must still be accepted; "+
					"a guard that rejects everything is not a fix")
		})
	}
}

// The same amount reaches settlement through the auto top-up config, where it
// is charged on every top-up rather than once.
func TestSetAutoTopUp_RejectsSubCentAmounts(t *testing.T) {
	store := newFakeStore()
	userID, accountID := uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	store.stripeCustomerOf[accountID] = "cus_credit"
	svc, _ := creditPurchaseSvcProposing(store, &fakeStripe{})
	svc = svc.WithCreditWallet(true)

	_, err := svc.SetAutoTopUp(context.Background(), billing.SetAutoTopUpRequest{
		OwnerUserID:     userID,
		Enabled:         true,
		ThresholdMicros: 1_000_000,
		AmountMicros:    5_004_999,
		PaymentMethodID: uuid.NewString(),
	})
	requireBillingErrorCode(t, err, billing.CodeInvalidInput)
}
