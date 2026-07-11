package billing_test

// ListUnpaidInvoices / PayInvoice + GetServiceStatus's unpaid gate and org
// resolution (funding-gates wave — docs-temp/billing-funding-gates/design.md).
// Reuses the in-memory fakeStore / fakeStripe from service_test.go.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// requireBillingCode asserts err is a *billing.Error carrying the code.
func requireBillingCode(t *testing.T, err error, code billing.Code) {
	t.Helper()
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, code, be.Code)
}

// --- GetServiceStatus: unpaid gate (C3) --------------------------------------

func TestGetServiceStatus_UnpaidInvoiceBoundaries(t *testing.T) {
	// 0 and 1 unpaid pass; 2 blocks (>= 2 rule). The count is the store's
	// unpaid predicate (open/uncollectible, amount_due > 0) — the SQL is
	// pinned by the integration suite; here the rule wiring is.
	for _, tc := range []struct {
		unpaid      int
		wantBlocked bool
	}{
		{0, false},
		{1, false},
		{2, true},
	} {
		store := newFakeStore()
		userID, accountID := uuid.New(), uuid.New()
		store.accountsByUser[userID] = fakeAccount{id: accountID}
		store.serviceSignals[accountID] = billing.ServiceSignals{UsableCardCount: 1, FirstChargeStatus: "paid"}
		store.unpaidCount[accountID] = tc.unpaid
		svc := billing.NewService(store, &fakeStripe{}, "")

		resp, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{UserID: userID})
		require.NoError(t, err)
		require.Equal(t, tc.wantBlocked, resp.Blocked, "unpaid=%d", tc.unpaid)
		if tc.wantBlocked {
			require.Equal(t, "UNPAID_INVOICES", resp.Reason)
		}
	}
}

// --- GetServiceStatus: org owner (C3) -----------------------------------------

func TestGetServiceStatus_OwnerValidation(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")
	// Neither owner.
	_, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{})
	requireBillingCode(t, err, billing.CodeInvalidInput)
	// Both owners.
	_, err = svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{UserID: uuid.New(), OrgID: uuid.New()})
	requireBillingCode(t, err, billing.CodeInvalidInput)
}

func TestGetServiceStatus_UnfundedOrg_Blocked(t *testing.T) {
	// An org without a resolvable funding designation has no standing →
	// blocked on the card gate (same posture as a user with no account).
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

	resp, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{OrgID: uuid.New()})
	require.NoError(t, err)
	require.True(t, resp.Blocked)
	require.Equal(t, "NO_USABLE_CARD", resp.Reason)
}

func TestGetServiceStatus_FundedOrg_Eligible(t *testing.T) {
	store := newFakeStore()
	orgID, accountID := uuid.New(), uuid.New()
	store.accountsByOrg[orgID] = fakeAccount{id: accountID}
	store.fundedOrgs[orgID] = true
	store.serviceSignals[accountID] = billing.ServiceSignals{UsableCardCount: 1, FirstChargeStatus: "paid"}
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{OrgID: orgID})
	require.NoError(t, err)
	require.False(t, resp.Blocked)
}

func TestGetServiceStatus_SponsorFundedOrg_CardSignalHopsToSponsor(t *testing.T) {
	// A sponsor-funded org account owns no cards; the card signal must read
	// the FUNDING account. Invoice-derived signals stay on the org account.
	store := newFakeStore()
	orgID, orgAcct, sponsorAcct := uuid.New(), uuid.New(), uuid.New()
	store.accountsByOrg[orgID] = fakeAccount{id: orgAcct}
	store.fundedOrgs[orgID] = true
	store.fundingOf[orgAcct] = sponsorAcct
	store.serviceSignals[orgAcct] = billing.ServiceSignals{UsableCardCount: 0, FirstChargeStatus: "paid"}
	store.serviceSignals[sponsorAcct] = billing.ServiceSignals{UsableCardCount: 1, FirstChargeStatus: "paid"}
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{OrgID: orgID})
	require.NoError(t, err)
	require.False(t, resp.Blocked, "the sponsor's card satisfies the org's card gate")

	// The ORG's own unpaid invoices still block, regardless of sponsor cards.
	store.unpaidCount[orgAcct] = 2
	resp, err = svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{OrgID: orgID})
	require.NoError(t, err)
	require.True(t, resp.Blocked)
	require.Equal(t, "UNPAID_INVOICES", resp.Reason)
}

// --- ListUnpaidInvoices (C4) ---------------------------------------------------

func TestListUnpaidInvoices_OwnerValidation(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")
	_, err := svc.ListUnpaidInvoices(context.Background(), billing.ListUnpaidInvoicesRequest{})
	requireBillingCode(t, err, billing.CodeInvalidInput)
	_, err = svc.ListUnpaidInvoices(context.Background(), billing.ListUnpaidInvoicesRequest{OwnerUserID: uuid.New(), OwnerOrgID: uuid.New()})
	requireBillingCode(t, err, billing.CodeInvalidInput)
}

func TestListUnpaidInvoices_NoAccount_EmptyPage(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

	resp, err := svc.ListUnpaidInvoices(context.Background(), billing.ListUnpaidInvoicesRequest{OwnerUserID: uuid.New()})
	require.NoError(t, err)
	require.Empty(t, resp.Invoices)
	require.Zero(t, resp.Count)
	require.Zero(t, resp.TotalMicros)
}

func TestListUnpaidInvoices_ReturnsRowsCountAndTotal(t *testing.T) {
	store := newFakeStore()
	userID, accountID := uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	inv1, inv2 := uuid.New(), uuid.New()
	store.unpaidInvoices[accountID] = []billing.UnpaidInvoiceRow{
		{ID: inv1, Number: "813C-0001", AmountDueMicros: 20_000_000, CreatedAt: time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)},
		{ID: inv2, Number: "", AmountDueMicros: 3_500_000, CreatedAt: time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)},
	}
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.ListUnpaidInvoices(context.Background(), billing.ListUnpaidInvoicesRequest{OwnerUserID: userID})
	require.NoError(t, err)
	require.Equal(t, 2, resp.Count)
	require.EqualValues(t, 23_500_000, resp.TotalMicros)
	require.Equal(t, inv1.String(), resp.Invoices[0].InvoiceID, "oldest first")
	require.Equal(t, "813C-0001", resp.Invoices[0].Number)
	require.Equal(t, "", resp.Invoices[1].Number, "unenriched number stays present-but-empty")
}

func TestListUnpaidInvoices_OrgResolvesThroughDesignation(t *testing.T) {
	store := newFakeStore()
	orgID, accountID := uuid.New(), uuid.New()
	store.accountsByOrg[orgID] = fakeAccount{id: accountID}
	store.unpaidInvoices[accountID] = []billing.UnpaidInvoiceRow{
		{ID: uuid.New(), AmountDueMicros: 1_000_000, CreatedAt: time.Now().UTC()},
	}
	svc := billing.NewService(store, &fakeStripe{}, "")

	// Not funded yet → the lazy empty page (no billable account resolved).
	resp, err := svc.ListUnpaidInvoices(context.Background(), billing.ListUnpaidInvoicesRequest{OwnerOrgID: orgID})
	require.NoError(t, err)
	require.Zero(t, resp.Count)

	store.fundedOrgs[orgID] = true
	resp, err = svc.ListUnpaidInvoices(context.Background(), billing.ListUnpaidInvoicesRequest{OwnerOrgID: orgID})
	require.NoError(t, err)
	require.Equal(t, 1, resp.Count)
}

// --- PayInvoice (C5) -----------------------------------------------------------

// paySetup seeds a funded user account owning one unpaid invoice and returns
// (store, userID, invoiceID). Tests weaken from this payable baseline.
func paySetup(status string) (*fakeStore, uuid.UUID, uuid.UUID) {
	store := newFakeStore()
	userID, accountID, invoiceID := uuid.New(), uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_pay"}
	store.hasUsableDefPM[accountID] = true
	store.payTargets[invoiceID] = fakePayTarget{
		accountID: accountID,
		target:    billing.InvoicePayTarget{StripeInvoiceID: "in_123", Status: status},
	}
	return store, userID, invoiceID
}

func TestPayInvoice_Validation(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")
	_, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{InvoiceID: uuid.New()})
	requireBillingCode(t, err, billing.CodeInvalidInput)
	_, err = svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: uuid.New()})
	requireBillingCode(t, err, billing.CodeInvalidInput)
}

func TestPayInvoice_HappyPath_PaysWithDeterministicIdemKey(t *testing.T) {
	store, userID, invoiceID := paySetup("open")
	sc := &fakeStripe{}
	svc := billing.NewService(store, sc, "")

	resp, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	require.NoError(t, err)
	require.Equal(t, "paid", resp.Status)
	require.Equal(t, []string{"in_123|payinv-" + invoiceID.String()}, sc.paidInvoices,
		"pays the Stripe invoice under the deterministic payinv-<mirror uuid> idempotency key")
}

func TestPayInvoice_UncollectibleIsPayable(t *testing.T) {
	// 'uncollectible' means Stripe gave up retrying, not that the debt is
	// gone — the manual Pay action is exactly the recovery for it.
	store, userID, invoiceID := paySetup("uncollectible")
	sc := &fakeStripe{}
	svc := billing.NewService(store, sc, "")

	resp, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	require.NoError(t, err)
	require.Equal(t, "paid", resp.Status)
	require.Len(t, sc.paidInvoices, 1)
}

func TestPayInvoice_PendingWhenStripeReportsUnsettled(t *testing.T) {
	store, userID, invoiceID := paySetup("open")
	sc := &fakeStripe{payStatusToReturn: "open"} // async PM: pay accepted, not settled
	svc := billing.NewService(store, sc, "")

	resp, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	require.NoError(t, err)
	require.Equal(t, "pending", resp.Status)
}

func TestPayInvoice_ForeignOrUnknownInvoice_NotFound(t *testing.T) {
	store, userID, _ := paySetup("open")
	// A second user's account, so the owner resolves but owns nothing.
	stranger, strangerAcct := uuid.New(), uuid.New()
	store.accountsByUser[stranger] = fakeAccount{id: strangerAcct}
	store.hasUsableDefPM[strangerAcct] = true
	sc := &fakeStripe{}
	svc := billing.NewService(store, sc, "")

	// Unknown id.
	_, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: uuid.New()})
	requireBillingCode(t, err, billing.CodeNotFound)

	// Someone else's invoice — indistinguishable from unknown.
	var foreignInvoice uuid.UUID
	for id := range store.payTargets {
		foreignInvoice = id
	}
	_, err = svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: stranger, InvoiceID: foreignInvoice})
	requireBillingCode(t, err, billing.CodeNotFound)

	// No billing account at all.
	_, err = svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: uuid.New(), InvoiceID: foreignInvoice})
	requireBillingCode(t, err, billing.CodeNotFound)

	require.Empty(t, sc.paidInvoices, "no Stripe call on any ownership miss")
}

func TestPayInvoice_NoUsableCard_PaymentRequired(t *testing.T) {
	store, userID, invoiceID := paySetup("open")
	for k := range store.hasUsableDefPM {
		store.hasUsableDefPM[k] = false
	}
	sc := &fakeStripe{}
	svc := billing.NewService(store, sc, "")

	_, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	requireBillingCode(t, err, billing.CodePaymentRequired)
	require.Empty(t, sc.paidInvoices)
}

func TestPayInvoice_AlreadyPaid_ShortCircuitsWithoutStripe(t *testing.T) {
	// The retry-after-success path: the mirror already settled 'paid' (via the
	// webhook) → answer "paid" idempotently, never re-hit Stripe.
	store, userID, invoiceID := paySetup("paid")
	sc := &fakeStripe{}
	svc := billing.NewService(store, sc, "")

	resp, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	require.NoError(t, err)
	require.Equal(t, "paid", resp.Status)
	require.Empty(t, sc.paidInvoices)
}

func TestPayInvoice_NonPayableStates_InvalidInput(t *testing.T) {
	for _, status := range []string{"void", "draft"} {
		store, userID, invoiceID := paySetup(status)
		sc := &fakeStripe{}
		svc := billing.NewService(store, sc, "")

		_, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
		requireBillingCode(t, err, billing.CodeInvalidInput)
		require.Empty(t, sc.paidInvoices)
	}
}

func TestPayInvoice_StripeError_Surfaced(t *testing.T) {
	store, userID, invoiceID := paySetup("open")
	sc := &fakeStripe{errPayInvoice: errors.New("card_declined")}
	svc := billing.NewService(store, sc, "")

	_, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	requireBillingCode(t, err, billing.CodeStripeError)
}

func TestPayInvoice_SponsorFundedOrg_CardGateHopsToSponsor(t *testing.T) {
	// The org's invoice is paid with the SPONSOR's default card: the card gate
	// must check the funding account (the invoice's Stripe customer lives
	// there — same hop as the charge legs).
	store := newFakeStore()
	orgID, orgAcct, sponsorAcct, invoiceID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	store.accountsByOrg[orgID] = fakeAccount{id: orgAcct}
	store.fundedOrgs[orgID] = true
	store.fundingOf[orgAcct] = sponsorAcct
	store.hasUsableDefPM[orgAcct] = false // org account owns no cards
	store.hasUsableDefPM[sponsorAcct] = true
	store.payTargets[invoiceID] = fakePayTarget{
		accountID: orgAcct,
		target:    billing.InvoicePayTarget{StripeInvoiceID: "in_org", Status: "open"},
	}
	sc := &fakeStripe{}
	svc := billing.NewService(store, sc, "")

	resp, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerOrgID: orgID, InvoiceID: invoiceID})
	require.NoError(t, err)
	require.Equal(t, "paid", resp.Status)
	require.Len(t, sc.paidInvoices, 1)
}
