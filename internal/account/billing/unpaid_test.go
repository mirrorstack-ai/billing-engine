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

// fakeProposer is the intent seam PayInvoice proposes through. It records
// every call, so a test can assert not only what was proposed but that nothing
// was — the assertion that matters most now that no other path can collect.
type fakeProposer struct {
	sourceFor    map[string]string // stripe invoice id → the intent that raised it
	digest       string            // digest ProposeReceivable seals as
	errSource    error
	errPropose   error
	sourceCalls  []string
	proposeCalls []proposeCall
}

type proposeCall struct {
	sourceDigest    string
	accountID       string
	remainderMicros int64
}

func (f *fakeProposer) SourceIntentFor(_ context.Context, providerReference string) (string, bool, error) {
	f.sourceCalls = append(f.sourceCalls, providerReference)
	if f.errSource != nil {
		return "", false, f.errSource
	}
	digest, ok := f.sourceFor[providerReference]
	return digest, ok, nil
}

func (f *fakeProposer) ProposeReceivable(
	_ context.Context, sourceDigest, accountID string, remainderMicros int64,
) (string, error) {
	f.proposeCalls = append(f.proposeCalls, proposeCall{sourceDigest, accountID, remainderMicros})
	if f.errPropose != nil {
		return "", f.errPropose
	}
	return f.digest, nil
}

// railRaised reports paySetup's invoice as one the intent rail itself raised —
// the only case this leg can answer, since collect_receivable links to a
// SOURCE intent.
func railRaised() *fakeProposer {
	return &fakeProposer{
		sourceFor: map[string]string{"in_123": "int_source"},
		digest:    "int_receivable",
	}
}

// paySetup seeds a funded user account owning one unpaid invoice, plus the
// Stripe fake, and returns (store, sc, userID, invoiceID). The Stripe fake is
// carried purely so every test can assert it was never called: after the
// cutover this leg holds no provider write port. Tests weaken from this
// payable baseline.
func paySetup(status string) (*fakeStore, *fakeStripe, uuid.UUID, uuid.UUID) {
	store := newFakeStore()
	userID, accountID, invoiceID := uuid.New(), uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_pay"}
	store.stripeCustomerOf[accountID] = "cus_pay"
	store.hasUsableDefPM[accountID] = true
	store.payTargets[invoiceID] = fakePayTarget{
		accountID: accountID,
		target: billing.InvoicePayTarget{
			StripeInvoiceID:        "in_123",
			Status:                 status,
			ChargeFundingAccountID: accountID,
			AmountDueMicros:        7_250_000,
		},
	}
	return store, &fakeStripe{getInvoiceCustomer: "cus_pay"}, userID, invoiceID
}

// requireNoProviderCall asserts the leg moved no money and read nothing from
// the provider. It is the standing assertion of the cutover: PayInvoice's
// Invoices.Pay — the one collecting call in the tree with no idempotency key —
// is deleted, along with the customer and invoice reads that chose a card for
// it.
func requireNoProviderCall(t *testing.T, sc *fakeStripe) {
	t.Helper()
	require.Empty(t, sc.paidInvoices, "the legacy collector is deleted; nothing here may charge")
}

func TestPayInvoice_Validation(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")
	_, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{InvoiceID: uuid.New()})
	requireBillingCode(t, err, billing.CodeInvalidInput)
	_, err = svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: uuid.New()})
	requireBillingCode(t, err, billing.CodeInvalidInput)
}

func TestPayInvoice_ProposesReceivableForRailRaisedInvoice(t *testing.T) {
	// The whole leg: an invoice the rail raised is answered by SEALING what is
	// still owed on it, not by collecting. "proposed" is neither "paid" nor
	// "pending" precisely because no payment was attempted.
	store, sc, userID, invoiceID := paySetup("open")
	p := railRaised()
	svc := billing.NewService(store, sc, "").WithReceivableProposer(p)

	resp, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	require.NoError(t, err)
	require.Equal(t, "proposed", resp.Status)
	require.Equal(t, "int_receivable", resp.IntentDigest)

	// The source is looked up by the invoice's PROVIDER reference, and the
	// remainder proposed is the mirror's amount_due verbatim — this leg must
	// not re-derive an amount.
	require.Equal(t, []string{"in_123"}, p.sourceCalls)
	require.Equal(t, []proposeCall{{
		sourceDigest:    "int_source",
		accountID:       store.accountsByUser[userID].id.String(),
		remainderMicros: 7_250_000,
	}}, p.proposeCalls)
	requireNoProviderCall(t, sc)
}

func TestPayInvoice_UncollectibleIsStillProposable(t *testing.T) {
	// 'uncollectible' means Stripe gave up retrying, not that the debt is
	// gone. The state gate that admits it survives the cutover; what it now
	// admits the row to is the proposal.
	store, sc, userID, invoiceID := paySetup("uncollectible")
	p := railRaised()
	svc := billing.NewService(store, sc, "").WithReceivableProposer(p)

	resp, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	require.NoError(t, err)
	require.Equal(t, "proposed", resp.Status)
	require.Len(t, p.proposeCalls, 1)
	requireNoProviderCall(t, sc)
}

func TestPayInvoice_InvoiceWithNoSourceIntent_RefusedNotCollected(t *testing.T) {
	// 🔴 The pre-rail backlog. collect_receivable is CollectRemainderOf(source),
	// so an invoice the rail never raised has nothing to link to — and this leg
	// keeps NO provider path for it. Such an invoice is at rest, not in flight:
	// finalized, open, amount_due > 0 and mirrored, so nothing about it is
	// unprovable and refusing strands no charge. Deleting the unkeyed
	// Invoices.Pay is the point of the cutover; carrying it for these rows
	// would carry it forever.
	store, sc, userID, invoiceID := paySetup("open")
	p := &fakeProposer{digest: "int_receivable"} // knows of no source for in_123
	svc := billing.NewService(store, sc, "").WithReceivableProposer(p)

	_, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	requireBillingCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, p.proposeCalls, "an empty source would fabricate the link §6 requires")
	requireNoProviderCall(t, sc)
}

func TestPayInvoice_WithoutProposer_FailsClosed(t *testing.T) {
	// The proposer is no longer optional: there is no second branch to fall
	// through to. An unarmed service must say so rather than nil-panic at the
	// seal — and must not collect, because it no longer can.
	store, sc, userID, invoiceID := paySetup("open")
	svc := billing.NewService(store, sc, "")

	_, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	requireBillingCode(t, err, billing.CodeInternal)
	requireNoProviderCall(t, sc)
}

func TestPayInvoice_SourceLookupFails_Internal(t *testing.T) {
	// A source lookup that ERRORS is not a missing source: answering "no
	// source" on a read failure would refuse an invoice the rail did raise.
	store, sc, userID, invoiceID := paySetup("open")
	p := railRaised()
	p.errSource = errors.New("intent store unavailable")
	svc := billing.NewService(store, sc, "").WithReceivableProposer(p)

	_, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	requireBillingCode(t, err, billing.CodeInternal)
	require.Empty(t, p.proposeCalls)
	requireNoProviderCall(t, sc)
}

func TestPayInvoice_ProposalFails_Internal(t *testing.T) {
	store, sc, userID, invoiceID := paySetup("open")
	p := railRaised()
	p.errPropose = errors.New("seal rejected")
	svc := billing.NewService(store, sc, "").WithReceivableProposer(p)

	_, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	requireBillingCode(t, err, billing.CodeInternal)
	requireNoProviderCall(t, sc)
}

func TestPayInvoice_ForeignOrUnknownInvoice_NotFound(t *testing.T) {
	store, sc, userID, _ := paySetup("open")
	// A second user's account, so the owner resolves but owns nothing.
	stranger, strangerAcct := uuid.New(), uuid.New()
	store.accountsByUser[stranger] = fakeAccount{id: strangerAcct}
	store.hasUsableDefPM[strangerAcct] = true
	p := railRaised()
	svc := billing.NewService(store, sc, "").WithReceivableProposer(p)

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

	require.Empty(t, p.sourceCalls, "the ownership gate runs before the seam")
	requireNoProviderCall(t, sc)
}

func TestPayInvoice_AlreadyPaid_ShortCircuitsWithoutProposerOrStripe(t *testing.T) {
	// The retry-after-success path: the mirror already settled 'paid' (via the
	// webhook) → answer "paid" idempotently. There is nothing left to collect,
	// so this echo must not consult the seam — and must keep working on a
	// service that was never armed, which is why the nil-proposer guard sits
	// after this branch rather than at the top of the RPC.
	store, sc, userID, invoiceID := paySetup("paid")
	p := railRaised()
	svc := billing.NewService(store, sc, "").WithReceivableProposer(p)

	resp, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	require.NoError(t, err)
	require.Equal(t, "paid", resp.Status)
	require.Empty(t, resp.IntentDigest, "nothing was sealed")
	require.Empty(t, p.sourceCalls)
	requireNoProviderCall(t, sc)

	unarmed := billing.NewService(store, sc, "")
	resp, err = unarmed.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
	require.NoError(t, err)
	require.Equal(t, "paid", resp.Status)
}

func TestPayInvoice_NonPayableStates_InvalidInput(t *testing.T) {
	for _, status := range []string{"void", "draft"} {
		store, sc, userID, invoiceID := paySetup(status)
		p := railRaised()
		svc := billing.NewService(store, sc, "").WithReceivableProposer(p)

		_, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerUserID: userID, InvoiceID: invoiceID})
		requireBillingCode(t, err, billing.CodeInvalidInput)
		require.Empty(t, p.sourceCalls, "a void or draft row is refused before the seam")
		requireNoProviderCall(t, sc)
	}
}

func TestPayInvoice_SponsorFundedOrg_ProposesAgainstTheOrgAccount(t *testing.T) {
	// The org resolution survives: an org principal still resolves through its
	// funding designation to the ORG's account, and that account is the one the
	// receivable is proposed against.
	//
	// 🔴 Note what this pins: the payer the proposer receives is the account
	// that OWNS the invoice, not the sponsor that funds it. The sponsor hop the
	// deleted card gate performed is NOT carried onto the intent path — a known
	// gap of the rail, recorded here rather than quietly re-added by this leg.
	store := newFakeStore()
	orgID, orgAcct, sponsorAcct, invoiceID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	store.accountsByOrg[orgID] = fakeAccount{id: orgAcct}
	store.fundedOrgs[orgID] = true
	store.fundingOf[orgAcct] = sponsorAcct
	store.hasUsableDefPM[orgAcct] = false // org account owns no cards
	store.hasUsableDefPM[sponsorAcct] = true
	store.stripeCustomerOf[sponsorAcct] = "cus_sponsor"
	store.payTargets[invoiceID] = fakePayTarget{
		accountID: orgAcct,
		target: billing.InvoicePayTarget{
			StripeInvoiceID:        "in_org",
			Status:                 "open",
			ChargeFundingAccountID: sponsorAcct,
			AmountDueMicros:        1_000_000,
		},
	}
	sc := &fakeStripe{getInvoiceCustomer: "cus_sponsor"}
	p := &fakeProposer{
		sourceFor: map[string]string{"in_org": "int_org_source"},
		digest:    "int_org_receivable",
	}
	svc := billing.NewService(store, sc, "").WithReceivableProposer(p)

	resp, err := svc.PayInvoice(context.Background(), billing.PayInvoiceRequest{OwnerOrgID: orgID, InvoiceID: invoiceID})
	require.NoError(t, err)
	require.Equal(t, "proposed", resp.Status)
	require.Equal(t, []proposeCall{{
		sourceDigest:    "int_org_source",
		accountID:       orgAcct.String(),
		remainderMicros: 1_000_000,
	}}, p.proposeCalls)
	requireNoProviderCall(t, sc)
}
