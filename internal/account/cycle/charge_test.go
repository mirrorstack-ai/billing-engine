package cycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// --- fake Stripe client ---------------------------------------------------
//
// Mocks the billingstripe.Client interface with recorded side-effects + injected
// errors. NEVER calls real Stripe. The charge-relevant methods (CreateInvoiceItem,
// CreateInvoice) record their args; the card-management methods are no-ops (the
// charge cycle never calls them).

type fakeStripe struct {
	// recorded calls
	itemCalls     []itemCall
	invoiceCalls  []invoiceCall // draft creations (one per invoice, C2 flow)
	finalizeCalls []finalizeCall

	// returns
	invoiceID         string
	invoiceStatus     string
	invoiceAmountDue  int64
	invoiceAmountPaid int64

	// injected errors
	errItem       error
	errDraft      error
	errInvoice    error // injected on FinalizeInvoice — the money-moving step
	errFindByRef  error
	errListItems  error
	errGetInvoice error

	// Stripe resource truth for combined-proration recovery.
	invoicesByID   map[string]billingstripe.Invoice
	itemsByInvoice map[string][]billingstripe.InvoiceItem
	itemsByIdem    map[string]billingstripe.InvoiceItem

	// crash-recovery lookup (FindInvoiceByRef): invoices "found" on Stripe
	// KEYED BY REF (wave 2 critic finding — an unkeyed fake could not detect a
	// cross-leg charge-ref mixup), and the refs queried. Tests seed via
	// setFindByRef.
	findByRefByRef map[string]billingstripe.Invoice
	findByRefCalls []string
	// findByRefCustIDs records the customer each lookup searched under — the
	// recovery legs must resolve the SAME funding hop as the fresh-charge path.
	findByRefCustIDs []string
	// onCreateInvoice, when set, runs INSIDE FinalizeInvoice right before it
	// returns success — modeling a concurrent account mutation (e.g. a
	// threshold edit) that lands while the real Stripe HTTP call is in
	// flight, i.e. strictly AFTER any pre-charge store read the caller
	// already did and strictly BEFORE any post-charge store read the caller
	// does once this call returns. Used by the finding-#2 regression test.
	onCreateInvoice func()
}

type itemCall struct {
	custID    string
	invoiceID string
	amountCfg int64
	currency  string
	desc      string
	period    billingstripe.LinePeriod
	idemKey   string
}

type invoiceCall struct {
	custID  string
	ref     string
	idemKey string
}

type finalizeCall struct {
	invoiceID string
	idemKey   string
}

func newFakeStripe() *fakeStripe {
	return &fakeStripe{
		invoiceID: "in_test_" + uuid.NewString(),
		// Finalize settles asynchronously, so a healthy finalize returns "open".
		invoiceStatus:    "open",
		invoiceAmountDue: 0, // overridden per test where the charged amount matters
		invoicesByID:     map[string]billingstripe.Invoice{},
		itemsByInvoice:   map[string][]billingstripe.InvoiceItem{},
		itemsByIdem:      map[string]billingstripe.InvoiceItem{},
	}
}

func (f *fakeStripe) RetrieveCharge(_ context.Context, _ string) (billingstripe.ChargeCardRef, error) {
	return billingstripe.ChargeCardRef{}, nil // unused by the charge cycle
}

func (f *fakeStripe) CreateDraftInvoice(_ context.Context, custID, ref, idemKey string) (billingstripe.Invoice, error) {
	f.invoiceCalls = append(f.invoiceCalls, invoiceCall{custID, ref, idemKey})
	if f.errDraft != nil {
		return billingstripe.Invoice{}, f.errDraft
	}
	if existing, ok := f.invoicesByID[f.invoiceID]; ok {
		return existing, nil
	}
	inv := billingstripe.Invoice{
		ID:         f.invoiceID,
		CustomerID: custID,
		Status:     "draft",
		Currency:   "usd",
	}
	f.invoicesByID[f.invoiceID] = inv
	return inv, nil
}

func (f *fakeStripe) CreateCreditPurchaseInvoice(
	ctx context.Context,
	customerID string,
	_ string,
	ledgerID string,
	idemKey string,
) (billingstripe.Invoice, error) {
	return f.CreateDraftInvoice(ctx, customerID, "credit-purchase:"+ledgerID, idemKey)
}

func (f *fakeStripe) CreateInvoiceItem(_ context.Context, custID, invoiceID string, amountCents int64, currency, desc string, period billingstripe.LinePeriod, idemKey string) (billingstripe.InvoiceItem, error) {
	f.itemCalls = append(f.itemCalls, itemCall{
		custID: custID, invoiceID: invoiceID, amountCfg: amountCents,
		currency: currency, desc: desc, period: period, idemKey: idemKey,
	})
	if f.errItem != nil {
		return billingstripe.InvoiceItem{}, f.errItem
	}
	return billingstripe.InvoiceItem{ID: "ii_test_" + uuid.NewString()}, nil
}

func (f *fakeStripe) CreateCombinedProrationInvoiceItem(
	ctx context.Context,
	custID string,
	invoiceID string,
	amountCents int64,
	currency string,
	desc string,
	period billingstripe.LinePeriod,
	idemKey string,
	identity billingstripe.CombinedProrationItemIdentity,
) (billingstripe.InvoiceItem, error) {
	if existing, ok := f.itemsByIdem[idemKey]; ok {
		return existing, nil
	}
	item, err := f.CreateInvoiceItem(ctx, custID, invoiceID, amountCents, currency, desc, period, idemKey)
	if err != nil {
		return billingstripe.InvoiceItem{}, err
	}
	item.AmountCents = amountCents
	item.Currency = currency
	item.Description = desc
	item.Period = period
	item.CombinedProrationAppID = identity.AppID
	if identity.TimerID == "" {
		item.CombinedProrationComponent = billingstripe.CombinedProrationComponentAppBase
	} else {
		item.CombinedProrationComponent = billingstripe.CombinedProrationComponentModuleOverage
		item.CombinedProrationTimerID = identity.TimerID
	}
	f.itemsByIdem[idemKey] = item
	f.itemsByInvoice[invoiceID] = append(f.itemsByInvoice[invoiceID], item)
	return item, nil
}

func (f *fakeStripe) ListInvoiceItems(_ context.Context, invoiceID string) ([]billingstripe.InvoiceItem, error) {
	if f.errListItems != nil {
		return nil, f.errListItems
	}
	return append([]billingstripe.InvoiceItem(nil), f.itemsByInvoice[invoiceID]...), nil
}

// setFindByRef seeds the invoice the recovery lookup finds under EXACTLY the
// given ref — a lookup with any other ref misses, so a leg reconciling against
// another leg's charge identity fails its test.
func (f *fakeStripe) setFindByRef(ref string, inv billingstripe.Invoice) {
	if f.findByRefByRef == nil {
		f.findByRefByRef = map[string]billingstripe.Invoice{}
	}
	f.findByRefByRef[ref] = inv
	if f.invoicesByID == nil {
		f.invoicesByID = map[string]billingstripe.Invoice{}
	}
	f.invoicesByID[inv.ID] = inv
}

func (f *fakeStripe) FindInvoiceByRef(_ context.Context, custID, ref string) (billingstripe.Invoice, bool, error) {
	f.findByRefCalls = append(f.findByRefCalls, ref)
	f.findByRefCustIDs = append(f.findByRefCustIDs, custID)
	if f.errFindByRef != nil {
		return billingstripe.Invoice{}, false, f.errFindByRef
	}
	if inv, ok := f.findByRefByRef[ref]; ok {
		return inv, true, nil
	}
	return billingstripe.Invoice{}, false, nil
}

func (f *fakeStripe) FinalizeInvoice(_ context.Context, invoiceID, idemKey string) (billingstripe.Invoice, error) {
	f.finalizeCalls = append(f.finalizeCalls, finalizeCall{invoiceID, idemKey})
	if f.errInvoice != nil {
		return billingstripe.Invoice{}, f.errInvoice
	}
	if f.onCreateInvoice != nil {
		f.onCreateInvoice()
	}
	amountDue := f.invoiceAmountDue
	if amountDue == 0 {
		for _, item := range f.itemsByInvoice[invoiceID] {
			amountDue += item.AmountCents
		}
	}
	inv := billingstripe.Invoice{
		ID:         invoiceID,
		Status:     f.invoiceStatus,
		AmountDue:  amountDue,
		AmountPaid: f.invoiceAmountPaid,
		Currency:   "usd",
	}
	if previous, ok := f.invoicesByID[invoiceID]; ok {
		inv.CustomerID = previous.CustomerID
	}
	f.invoicesByID[invoiceID] = inv
	return inv, nil
}

// Card-management methods: never called by the charge cycle. Present only to
// satisfy the billingstripe.Client interface; each panics if hit, proving the
// charge path never touches the card surface.
func (f *fakeStripe) CreateCustomer(context.Context, string, string) (*stripego.Customer, error) {
	panic("CreateCustomer must not be called by the charge cycle")
}
func (f *fakeStripe) UpdateCustomerEmail(context.Context, string, string) error {
	panic("UpdateCustomerEmail must not be called by the charge cycle")
}
func (f *fakeStripe) CreateCheckoutSession(context.Context, string, string) (*stripego.CheckoutSession, error) {
	panic("CreateCheckoutSession must not be called by the charge cycle")
}
func (f *fakeStripe) DetachPaymentMethod(context.Context, string) error {
	panic("DetachPaymentMethod must not be called by the charge cycle")
}
func (f *fakeStripe) SetDefaultPaymentMethod(context.Context, string, string) error {
	panic("SetDefaultPaymentMethod must not be called by the charge cycle")
}
func (f *fakeStripe) GetCustomer(context.Context, string) (*stripego.Customer, error) {
	return &stripego.Customer{}, nil
}
func (f *fakeStripe) GetInvoice(_ context.Context, invoiceID string) (billingstripe.Invoice, error) {
	if f.errGetInvoice != nil {
		return billingstripe.Invoice{}, f.errGetInvoice
	}
	inv, ok := f.invoicesByID[invoiceID]
	if !ok {
		return billingstripe.Invoice{}, errors.New("invoice not found")
	}
	if inv.Status == "draft" {
		inv.AmountDue = 0
		for _, item := range f.itemsByInvoice[invoiceID] {
			inv.AmountDue += item.AmountCents
		}
		f.invoicesByID[invoiceID] = inv
	}
	return inv, nil
}
func (f *fakeStripe) PayInvoice(context.Context, string) (billingstripe.Invoice, error) {
	panic("PayInvoice must not be called by the charge cycle")
}

// Compile-time check: fakeStripe satisfies the full Client interface.
var _ billingstripe.Client = (*fakeStripe)(nil)
var _ billingstripe.CombinedProrationClient = (*fakeStripe)(nil)

// --- helpers --------------------------------------------------------------

var chargeAccount = uuid.New()

// chargeSvc builds the boundary service the way it can now be built: WITH an
// intent proposer.
//
// It is not a convenience. The boundary leg's Stripe collector is deleted, so
// a service without a proposer has no way to finish a boundary at all — a
// fixture that omitted one would not be testing a leaner leg, it would be
// testing a broken one. Every caller of this helper therefore exercises the
// proposing path; the ones that care what was proposed use chargeSvcProposing.
func chargeSvc(store *fakeStore, sc billingstripe.Client) *cycle.Service {
	svc, _ := chargeSvcProposing(store, sc)
	return svc
}

// chargeSvcProposing is chargeSvc for tests that assert on the intents the
// boundary sealed instead of on a Stripe call that no longer happens.
func chargeSvcProposing(store *fakeStore, sc billingstripe.Client) (*cycle.Service, *capturingProposer) {
	p := &capturingProposer{}
	return cycle.NewService(store, sc).WithIntentProposer(p), p
}

// proposedMicros is the total the boundary sealed across every intent in the
// group — the post-cutover reading of "what this boundary collects", and the
// figure that must equal what the legacy path used to send to Stripe.
func proposedMicros(t *testing.T, p *capturingProposer) int64 {
	t.Helper()
	require.Len(t, p.groups, 1, "the boundary did not propose exactly one group")
	var total int64
	for _, c := range p.groups[0] {
		total += c.TotalMicros()
	}
	return total
}

// proposedRemainderMicros is what the sealed group asks a provider for: the
// gross minus the wallet credit already allocated to it. It is the intent-path
// reading of the legacy path's "charge only the remainder".
func proposedRemainderMicros(t *testing.T, p *capturingProposer) int64 {
	t.Helper()
	require.Len(t, p.groups, 1, "the boundary did not propose exactly one group")
	var remainder int64
	for _, c := range p.groups[0] {
		remainder += c.TotalMicros() - c.WalletAllocationMicros
	}
	return remainder
}

// seedFrozenRun creates the boundary run row for the standard window and
// stamps a durable FROZEN charge on it — the state a crashed attempt of the
// now-deleted collector left behind.
//
// It exists because no code in this tree can produce that state any more: the
// draft→item→finalize flow that used to freeze and then die mid-charge is
// gone, so the recovery path it left behind has to be reached by seeding the
// row a crashed predecessor would have written. The first pass runs with no
// usable PM purely to mint the run row (it skips, and a skip never freezes).
func seedFrozenRun(t *testing.T, store *fakeStore, sc *fakeStripe, cents int64) uuid.UUID {
	t.Helper()
	hadPM := store.hasPM
	store.hasPM = false
	_, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Len(t, store.insertedRuns, 1, "the seeding pass must create exactly one run row")
	require.Empty(t, store.frozenCharges, "a skipped run must not freeze")
	var runID uuid.UUID
	for _, id := range store.insertedRuns {
		runID = id
	}
	store.frozenCharges[runID] = cycle.FrozenBoundaryCharge{
		Cents:                   cents,
		ChargeFundingAccountID:  chargeAccount,
		ChargeFundingGeneration: uuid.New(),
	}
	store.hasPM = hadPM
	return runID
}

// frozenClaim returns the run's durable arming claim — the row the freeze
// writes before anything else may happen. It survives the collector's deletion
// because it is what pins the funding instrument and refuses a boundary with no
// usable PM, and it is now the only place this leg's micros→cents conversion is
// observable.
func frozenClaim(t *testing.T, store *fakeStore) cycle.FrozenBoundaryCharge {
	t.Helper()
	require.Len(t, store.frozenCharges, 1, "the boundary did not arm exactly one claim")
	for _, c := range store.frozenCharges {
		return c
	}
	return cycle.FrozenBoundaryCharge{}
}

func requireLinePeriod(t *testing.T, got billingstripe.LinePeriod, wantStart, wantEnd time.Time) {
	t.Helper()
	require.True(t, got.Start.Equal(wantStart), "line period start = %s, want %s", got.Start, wantStart)
	require.True(t, got.End.Equal(wantEnd), "line period end = %s, want %s", got.End, wantEnd)
}

// --- RunBillingCycle: happy path ------------------------------------------

func TestRunBillingCycle_ChargesArrears(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_234_500 // micros → 123.45 cents → round-half-up 123 cents
	store.hasPM = true
	store.stripeCustomer = "cus_test_1"
	sc := newFakeStripe()
	sc.invoiceAmountDue = 123

	svc, p := chargeSvcProposing(store, sc)
	resp, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.True(t, resp.FirstRun)
	require.Equal(t, cycle.RunStatusProposed, resp.Status)
	require.EqualValues(t, 1_234_500, resp.ArrearsMicros)

	// The SAME arrears, sealed instead of collected. Micros, not cents: the
	// single rounding moved to the provider boundary with the collection.
	require.EqualValues(t, 1_234_500, proposedMicros(t, p))
	require.Equal(t, chargeAccount.String(), p.groups[0][0].AccountID,
		"the intent must be attributed to the account the run belongs to")

	// 🔴 The assertion the deletion exists for: nothing reached Stripe.
	require.Empty(t, sc.invoiceCalls, "a boundary created a Stripe draft")
	require.Empty(t, sc.itemCalls, "a boundary created a Stripe invoice item")
	require.Empty(t, sc.finalizeCalls, "a boundary finalized a Stripe invoice")
	require.Empty(t, resp.StripeInvoiceID)
	require.Zero(t, resp.ChargedCents, "a proposed run reported cents as charged")
	require.Empty(t, store.invoices, "a proposed boundary mirrored an invoice that does not exist")

	require.Len(t, store.markedRuns, 1)
	for _, m := range store.markedRuns {
		require.Equal(t, cycle.RunStatusProposed, m.status)
		require.Zero(t, m.totalCents)
		require.Empty(t, m.invoiceID)
	}
}

// The advance half of a boundary covers the NEW period, so the window sealed
// on the intent has to run to that period's own anchored end — including the
// short-month clamp. The legacy path expressed this as the Stripe line's
// coverage; the intent expresses it as the window a collection may happen in.
func TestRunBillingCycle_AdvanceIntentRunsToTheNextAnchoredBoundary(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_coverage_base"
	// Anchor 31 demonstrates that the line ends at the actual next anchored
	// boundary, including independent short-month clamping.
	store.activation[chargeAccount] = time.Date(2026, 1, 31, 9, 0, 0, 0, time.UTC)
	seedApp(store, chargeAccount, 0, false)
	sc := newFakeStripe()
	closedStart := time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC)
	closedEnd := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)

	svc, p := chargeSvcProposing(store, sc)
	resp, err := svc.RunBillingCycle(context.Background(), chargeAccount, closedStart, closedEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, resp.Status)
	require.EqualValues(t, usage.BaseFeeMicros, resp.AdvanceBaseMicros)
	require.Empty(t, sc.itemCalls)

	// Anchor day 31 clamped into a 30-day month and back out again: the next
	// anchored boundary after 2026-06-30 is 2026-07-31, not 2026-07-30.
	newPeriodEnd := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	require.Len(t, p.groups, 1)
	for _, c := range p.groups[0] {
		require.True(t, c.ExecuteNotBefore.Equal(closedEnd),
			"execution may not begin before the boundary the charge is derived at")
		require.True(t, c.ExecuteNotAfter.Equal(newPeriodEnd),
			"execute-not-after = %s, want the next anchored boundary %s", c.ExecuteNotAfter, newPeriodEnd)
	}
}

// --- org-billing D1: the funding hop (resolveChargeableCustomer) --------------

func TestRunBillingCycle_SponsorFundingHopGatesOnTheSponsorsPM(t *testing.T) {
	// An org account whose designation names a sponsor gates on the SPONSOR's
	// default PM + Stripe customer, while everything else (the run row, the
	// intent's attribution) stays keyed to the ORG account. The org account
	// itself has NO usable PM and NO customer, so a leg resolving the org
	// account directly would have skipped instead of proposing — which is what
	// TestRunBillingCycle_SponsorRevokedDegradesToNoPMSkip pins from the
	// other side.
	store := newFakeStore()
	org, orgAcct, sponsorAcct := uuid.New(), uuid.New(), uuid.New()
	store.accountsByOrg[org] = orgAcct
	store.orgDesignations[org] = cycle.OrgDesignation{
		OrgID: org, Funding: cycle.OrgFundingSponsor, SponsorAccountID: sponsorAcct,
	}
	store.hasPMByAccount[orgAcct] = false
	store.hasPMByAccount[sponsorAcct] = true
	store.stripeCustomerByAccount[sponsorAcct] = "cus_sponsor"
	store.chargedTotal = 1_000_000
	sc := newFakeStripe()

	svc, p := chargeSvcProposing(store, sc)
	resp, err := svc.RunBillingCycle(context.Background(), orgAcct, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, resp.Status,
		"the sponsor's PM is what let this boundary past the gate")
	require.Empty(t, sc.itemCalls)

	// Attribution never moves: the run row and the intent stay on the ORG
	// account. The proposer — not this leg — resolves the funder's owner.
	require.Equal(t, orgAcct.String(), p.groups[0][0].AccountID)
	_, ok := store.insertedRuns[runKey(orgAcct, periodStart, periodEnd)]
	require.True(t, ok)
}

func TestRunBillingCycle_SponsorRevokedDegradesToNoPMSkip(t *testing.T) {
	// The same org account with its designation revoked funds ITSELF (identity
	// hop) — and it has no PM, so the run degrades to the ordinary transient
	// skipped_no_pm, never an error and never a charge on the ex-sponsor.
	store := newFakeStore()
	org, orgAcct, sponsorAcct := uuid.New(), uuid.New(), uuid.New()
	store.accountsByOrg[org] = orgAcct // no designation row (revoked)
	store.hasPMByAccount[orgAcct] = false
	store.hasPMByAccount[sponsorAcct] = true
	store.stripeCustomerByAccount[sponsorAcct] = "cus_sponsor"
	store.chargedTotal = 1_000_000
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), orgAcct, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedNoPM, resp.Status)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.invoiceCalls)
}

// --- FINDING 3: a reclaimed boundary run reuses its FROZEN charge amount, never
// a freshly-recomputed live total, so the stable Stripe idem key never conflicts -

// The inverse D7 race is equally important: a concurrent daemon can finish a
// zero-charge path after this process read "unfrozen" but before it installs a
// non-zero marker. Once that terminal mark wins, the stale process must fail
// before Stripe rather than resurrecting an already-complete run as a charge.
func TestRunBillingCycle_LostFreezeRaceToTerminalZeroRefusesStripe(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_terminal_zero_race"

	sc := newFakeStripe()
	store.onFreezeCharge = func(runID uuid.UUID) {
		store.runStatus[runID] = cycle.RunStatusInvoiced
	}

	resp, err := chargeSvc(store, sc).RunBillingCycle(
		context.Background(),
		chargeAccount,
		periodStart,
		periodEnd,
		0,
	)

	require.Nil(t, resp)
	require.ErrorContains(t, err, "no frozen charge immediately after freezing")
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, sc.finalizeCalls)
}

// 🔴 THE ONE PATH THAT MAY STILL REACH THE PROVIDER, AND WHY IT MUST.
//
// A crashed attempt of the deleted collector can have left a FINALIZED invoice
// at the provider: the customer may already have been debited. That run's only
// remaining job is to mirror it and go terminal. Abandoning it — proposing the
// same boundary onto the intent rail instead — would strand a charge nothing in
// this tree recorded, and then collect it a second time.
//
// Note what finishing it costs: nothing. The invoice already exists, so this
// path performs a READ and a mirror. It writes nothing at the provider.
func TestRunBillingCycle_RecoveredInvoiceFinishesDespitePrepaidAndPMGates(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_h8b"
	sc := newFakeStripe()

	runID := seedFrozenRun(t, store, sc, 100)
	sc.setFindByRef("run:"+runID.String(), billingstripe.Invoice{
		ID: "in_crashed_boundary", Status: "paid", AmountDue: 100, AmountPaid: 100, Currency: "usd",
	})

	// The account tightened to prepaid (possibly triggered by the crashed
	// attempt's own invoice) AND its default PM is gone. Neither may strand the
	// charge that already exists.
	store.collection.Mode = cycle.BillingModePrepaid
	store.hasPM = false

	svc, p := chargeSvcProposing(store, sc)
	resp, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status,
		"a run whose charge exists at the provider finishes; it is never re-gated into a skip over moved money")
	require.Empty(t, p.groups,
		"the boundary sealed a SECOND collection beside the one already at the provider")
	require.Len(t, store.invoices, 1, "the crashed attempt's charge was not mirrored")
	require.Empty(t, sc.itemCalls, "finishing an existing invoice must write nothing at the provider")
	require.Empty(t, sc.finalizeCalls)
}

// A LATE reclaim — past the provider's ~24h idempotency-key window — adopts the
// invoice found under the run's ms_charge_ref and creates nothing new.
func TestRunBillingCycle_LateReclaimAdoptsFoundInvoiceWithoutNewObjects(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_h5"
	sc := newFakeStripe()

	runID := seedFrozenRun(t, store, sc, 100)
	sc.setFindByRef("run:"+runID.String(), billingstripe.Invoice{
		ID: "in_prior_boundary", Status: "paid", AmountDue: 100, AmountPaid: 100, Currency: "usd",
	})

	svc, p := chargeSvcProposing(store, sc)
	resp, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 100, resp.ChargedCents, "the frozen amount the crashed attempt committed to")
	require.Empty(t, sc.invoiceCalls, "no draft on a recovered reclaim")
	require.Empty(t, sc.finalizeCalls, "no second finalize — the money moved once")
	require.Empty(t, p.groups, "a recovered charge must not also be proposed")
	_, mirrored := store.invoices["in_prior_boundary"]
	require.True(t, mirrored, "the crashed attempt's invoice is mirrored")
}

// 🔴 AN INERT DRAFT IS NOT A CHARGE, SO IT IS PROPOSED — NOT FINALIZED.
//
// The deleted collector created its draft with AutoAdvance(false)
// (shared/stripe/client.go inertDraftInvoiceParams), so a draft left behind by
// a crash never collects on its own. Finalizing it here would have been a FRESH
// off-session debit wearing a recovered invoice's clothes — exactly the collect
// this cutover removes — which is why the gates above already refuse to treat a
// draft as "money may have moved".
//
// Without this test the leg could quietly keep collecting through the recovery
// door: every other recovery assertion passes whether or not a draft is
// finalized.
func TestRunBillingCycle_RecoveredInertDraftIsProposedNotFinalized(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_inert_draft"
	sc := newFakeStripe()

	runID := seedFrozenRun(t, store, sc, 100)
	sc.setFindByRef("run:"+runID.String(), billingstripe.Invoice{
		ID: "in_inert_draft", Status: "draft", AmountDue: 0, Currency: "usd",
	})

	svc, p := chargeSvcProposing(store, sc)
	resp, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, resp.Status,
		"an inert draft moved no money, so the boundary is sealed like any other")
	require.EqualValues(t, 1_000_000, proposedMicros(t, p))
	require.Empty(t, sc.itemCalls, "the draft was completed — that is a fresh debit, not a recovery")
	require.Empty(t, sc.finalizeCalls, "the draft was FINALIZED — the collector is not deleted")
	require.Empty(t, store.invoices)
}

// A VOID invoice under the run's own charge ref is refused loudly. Adopting it
// would mark the run invoiced against an invoice that collects nothing —
// forgiving the whole boundary silently — and proposing beside it would seal a
// second document for a charge somebody deliberately canceled. Ops resolves it.
func TestRunBillingCycle_RecoveredVoidInvoiceIsRefused(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_void"
	sc := newFakeStripe()

	runID := seedFrozenRun(t, store, sc, 100)
	sc.setFindByRef("run:"+runID.String(), billingstripe.Invoice{
		ID: "in_voided", Status: "void", AmountDue: 100, Currency: "usd",
	})

	svc, p := chargeSvcProposing(store, sc)
	_, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "VOID")
	require.Empty(t, p.groups, "a canceled charge must not be re-sealed as an intent")
	require.Empty(t, store.invoices, "a void invoice must not be mirrored as a collection")
	require.Equal(t, cycle.RunStatusFailed, store.markedRuns[runID].status,
		"the run must be auditable as failed, not left looking untouched")
}

func TestRunBillingCycle_CentsRoundHalfUp(t *testing.T) {
	// 5_000 micros = 0.5 cents → round-half-up → 1 cent, on the arming claim.
	// The intent itself carries MICROS — the collection's single rounding moved
	// to the provider boundary with the collection — so the claim is where this
	// leg's own conversion stays observable.
	store := newFakeStore()
	store.chargedTotal = 5_000
	store.hasPM = true
	store.stripeCustomer = "cus_x"
	sc := newFakeStripe()

	svc, p := chargeSvcProposing(store, sc)
	_, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.EqualValues(t, 1, frozenClaim(t, store).Cents)
	require.EqualValues(t, 5_000, proposedMicros(t, p), "the sealed amount is not rounded")
}

func TestRunBillingCycle_CentsLargeValueNoWrap(t *testing.T) {
	// cents = round_half_up(micros / 10_000), and cents ≤ micros, so a value that
	// fit as int64 micros always fits as int64 cents — the conversion never wraps
	// at the top. Pin a large valid total and assert the rounded value is computed
	// exactly (no silent overflow / negative wrap).
	store := newFakeStore()
	const big = int64(1)<<62 - 1 // large but valid micros
	store.chargedTotal = big
	store.hasPM = true
	store.stripeCustomer = "cus_y"
	sc := newFakeStripe()

	_, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	// round_half_up(big / 10_000) computed independently with integer math.
	wantCents := (big + 5_000) / 10_000
	claim := frozenClaim(t, store)
	require.EqualValues(t, wantCents, claim.Cents)
	require.Greater(t, claim.Cents, int64(0))
}

// --- RunBillingCycle: allowance netting -----------------------------------

func TestRunBillingCycle_AllowanceNetsArrears(t *testing.T) {
	// arrears = max(0, usage − allowance). usage 1_000_000, allowance 400_000 →
	// 600_000 micros → 60 cents.
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_a"
	sc := newFakeStripe()

	svc, p := chargeSvcProposing(store, sc)
	resp, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 400_000)
	require.NoError(t, err)
	require.EqualValues(t, 600_000, resp.ArrearsMicros)
	require.EqualValues(t, 600_000, proposedMicros(t, p), "the allowance nets the SEALED amount too")
	require.Empty(t, sc.invoiceCalls)
}

func TestRunBillingCycle_AllowanceExceedsUsageNoCharge(t *testing.T) {
	// allowance > usage → arrears clamps to 0 → NO Stripe call, NO Customer
	// touched, run marked invoiced.
	store := newFakeStore()
	store.chargedTotal = 100_000
	store.hasPM = true
	store.stripeCustomer = "cus_b"
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 500_000)
	require.NoError(t, err)
	require.True(t, resp.FirstRun)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 0, resp.ArrearsMicros)
	require.EqualValues(t, 0, resp.ChargedCents)
	require.Empty(t, resp.StripeInvoiceID)

	require.Empty(t, sc.itemCalls, "zero arrears must not call Stripe")
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, store.invoices)
	require.Len(t, store.markedRuns, 1)
	for _, m := range store.markedRuns {
		require.Equal(t, cycle.RunStatusInvoiced, m.status)
		require.EqualValues(t, 0, m.totalCents)
	}
}

func TestRunBillingCycle_EmptyPeriodNoStripeCustomer(t *testing.T) {
	// Zero usage → arrears 0 → run done, NO Stripe Customer auto-created (the
	// fake panics if any card method is hit; here we assert no charge methods
	// were called and HasUsableDefaultPM / AccountStripeCustomer were never
	// needed — but the gate is the zero-arrears short-circuit).
	store := newFakeStore()
	store.chargedTotal = 0
	store.hasPM = false       // even with no PM, zero arrears wins first
	store.stripeCustomer = "" // no customer
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.invoiceCalls)
}

// --- RunBillingCycle: no-PM gate ------------------------------------------

func TestRunBillingCycle_SkippedNoPM(t *testing.T) {
	// Positive arrears + no usable PM → skipped_no_pm, NO charge, usage RETAINED
	// (the fake's chargedTotal is untouched), run marked skipped.
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = false
	store.stripeCustomer = "cus_c"
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.True(t, resp.FirstRun)
	require.Equal(t, cycle.RunStatusSkippedNoPM, resp.Status)
	require.EqualValues(t, 1_000_000, resp.ArrearsMicros)
	require.EqualValues(t, 0, resp.ChargedCents)

	require.Empty(t, sc.itemCalls, "no PM must not call Stripe")
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, store.invoices)
	require.Len(t, store.markedRuns, 1)
	for _, m := range store.markedRuns {
		require.Equal(t, cycle.RunStatusSkippedNoPM, m.status)
	}

	// Usage is RETAINED: the source total is unchanged (the cycle never deletes
	// usage_aggregates), so a re-attempt next cycle still sees it.
	require.EqualValues(t, 1_000_000, store.chargedTotal)
}

// --- RunBillingCycle: idempotency -----------------------------------------

func TestRunBillingCycle_SkippedNoPMReattemptsNextCycle(t *testing.T) {
	// A skipped_no_pm run is RECLAIMED on the next cycle: when the account adds a
	// PM, the re-run charges the RETAINED usage on the SAME run row (no new row,
	// no double-charge protection bypassed). FirstRun stays true (an attempt
	// happened); the run flips skipped_no_pm → invoiced.
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = false // cycle 1: no PM
	store.stripeCustomer = "cus_reclaim"
	sc := newFakeStripe()
	svc, p := chargeSvcProposing(store, sc)

	first, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedNoPM, first.Status)
	require.Empty(t, p.groups, "no PM must not seal an intent either")
	require.Len(t, store.insertedRuns, 1)

	// Cycle 2: the account now has a usable PM. The skipped run is reclaimed.
	store.hasPM = true
	second, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.True(t, second.FirstRun, "a skipped run is reclaimed for a fresh attempt")
	require.Equal(t, cycle.RunStatusProposed, second.Status)
	require.EqualValues(t, 1_000_000, proposedMicros(t, p), "the RETAINED usage is what gets sealed on retry")
	require.Len(t, store.insertedRuns, 1, "reclaim reuses the same run row")
}

func TestRunBillingCycle_UnfinishedRunReattemptsNextCycle(t *testing.T) {
	// A boundary that could not be sealed is RECLAIMED next cycle on the SAME
	// run row. Nothing was attempted at a provider, so the first attempt leaves
	// no terminal mark at all — that is the property the retry depends on.
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_fail_then_ok"
	sc := newFakeStripe()
	svc, p := chargeSvcProposing(store, sc)
	p.err = errors.New("proposal refused")

	_, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	requireCode(t, err, billing.CodeInternal)
	require.Len(t, store.insertedRuns, 1)
	require.Empty(t, store.markedRuns, "an unfinished boundary must stay pending, not go terminal")

	// The transient cause clears: the run is reclaimed and now seals.
	p.err = nil
	second, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.True(t, second.FirstRun, "an unfinished run is reclaimed for a fresh attempt")
	require.Equal(t, cycle.RunStatusProposed, second.Status)
	require.Len(t, store.insertedRuns, 1, "reclaim reuses the same run row")
}

func TestRunBillingCycle_InvoicedBlocksReattempt(t *testing.T) {
	// A terminal-success (invoiced) run is NEVER reclaimed: a re-run is a no-op.
	//
	// 'invoiced' can now only be reached by ADOPTING a crashed attempt's
	// invoice, so that is how this sets it up. The gate itself is unchanged:
	// InsertBillingRun's ON CONFLICT reclaims every status BUT this one.
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_done"
	sc := newFakeStripe()

	runID := seedFrozenRun(t, store, sc, 100)
	sc.setFindByRef("run:"+runID.String(), billingstripe.Invoice{
		ID: "in_adopted", Status: "paid", AmountDue: 100, AmountPaid: 100, Currency: "usd",
	})

	svc, p := chargeSvcProposing(store, sc)
	first, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, first.Status)

	second, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.False(t, second.FirstRun, "an invoiced run blocks any re-attempt")
	require.Empty(t, second.Status)
	require.Empty(t, p.groups, "a settled boundary was proposed again")
	require.Len(t, store.invoices, 1, "no second collection of any kind")
}

// --- RunBillingCycle: charge failure --------------------------------------

func TestRunBillingCycle_UsablePMButNoCustomerIsAnomaly(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "" // anomaly: PM but no Customer
	sc := newFakeStripe()

	_, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	requireCode(t, err, billing.CodeInternal)
	require.Empty(t, sc.itemCalls, "anomaly must not call Stripe")
}

// --- RunBillingCycle: validation + error propagation ----------------------

func TestRunBillingCycle_Validation(t *testing.T) {
	sc := newFakeStripe()
	_, err := chargeSvc(newFakeStore(), sc).RunBillingCycle(context.Background(), uuid.Nil, periodStart, periodEnd, 0)
	requireCode(t, err, billing.CodeInvalidInput)
	_, err = chargeSvc(newFakeStore(), sc).RunBillingCycle(context.Background(), chargeAccount, periodEnd, periodStart, 0)
	requireCode(t, err, billing.CodeInvalidInput)
	_, err = chargeSvc(newFakeStore(), sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, -1)
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestRunBillingCycle_NilStripeRejected(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	_, err := cycle.NewService(store, nil).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	requireCode(t, err, billing.CodeInternal)
}

func TestRunBillingCycle_PropagatesStoreErrors(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name  string
		setup func(*fakeStore)
	}{
		{"insert run", func(f *fakeStore) { f.errInsertRun = boom }},
		{"total", func(f *fakeStore) { f.errTotal = boom }},
		{"pm", func(f *fakeStore) { f.chargedTotal = 1_000_000; f.errPM = boom }},
		{"customer", func(f *fakeStore) { f.chargedTotal = 1_000_000; f.hasPM = true; f.errCustomer = boom }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			store.stripeCustomer = "cus_z"
			tc.setup(store)
			sc := newFakeStripe()
			_, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
			requireCode(t, err, billing.CodeInternal)
		})
	}
}

// --- large auto-collect disclosure flag (migration 034) -------------------

// --- regression: finding #2 (threshold resolved at different points relative
// to the charge in RunBillingCycle vs. RegisterApp) -------------------------
//
// Both tests below charge $150 while a concurrent threshold edit ($100
// default → $200 override) lands DURING the Stripe CreateInvoice HTTP call —
// i.e. strictly after any pre-charge store read and strictly before any
// post-charge store read. Both call sites must resolve the SAME way (the
// edit that landed mid-charge is picked up), matching the "resolved at charge
// time" contract identically on both legs.
//
// FAILS without the fix: RunBillingCycle read `acct` (and its
// AutoCollectThresholdMicros) at the TOP of the function — before the risk
// gate, the PM check, and both Stripe HTTP calls — so it never observes the
// edit and still uses the stale $100 default, flagging the $150 charge as
// large. RegisterApp, by contrast, already re-resolves the threshold AFTER
// its Stripe call succeeds, so it picks up the new $200 override and does
// NOT flag the same $150 charge. That asymmetry — same race, different
// outcome depending on which leg charged — is exactly what this test
// forbids.

// DELETED with the path it tested: TestChargeCreationProration_
// ConcurrentThresholdEditMidChargeResolvesPostCharge asserted that the
// creation-proration leg resolved its large-auto-collect disclosure threshold
// AFTER its Stripe call succeeded, and read the answer off the mirrored
// invoice. That leg no longer creates, finalizes, or mirrors an invoice: it
// seals an intent, and the disclosure travels with the intent's notice policy
// for whatever holds the write port to apply. There is no post-charge instant
// on this leg to resolve a threshold at any more.
//
// The boundary leg had a paired version of this test, which the comment block
// above still introduces; whoever drops that leg owns it and the block.
