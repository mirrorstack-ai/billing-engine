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
	errItem    error
	errDraft   error
	errInvoice error // injected on FinalizeInvoice — the money-moving step
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
		invoiceID:        "in_test_" + uuid.NewString(),
		invoiceStatus:    "paid",
		invoiceAmountDue: 0, // overridden per test where the charged amount matters
	}
}

func (f *fakeStripe) CreateDraftInvoice(_ context.Context, custID, ref, idemKey string) (billingstripe.Invoice, error) {
	f.invoiceCalls = append(f.invoiceCalls, invoiceCall{custID, ref, idemKey})
	if f.errDraft != nil {
		return billingstripe.Invoice{}, f.errDraft
	}
	return billingstripe.Invoice{ID: f.invoiceID, Status: "draft", Currency: "usd"}, nil
}

func (f *fakeStripe) CreateInvoiceItem(_ context.Context, custID, invoiceID string, amountCents int64, currency, desc, idemKey string) (billingstripe.InvoiceItem, error) {
	f.itemCalls = append(f.itemCalls, itemCall{custID, invoiceID, amountCents, currency, desc, idemKey})
	if f.errItem != nil {
		return billingstripe.InvoiceItem{}, f.errItem
	}
	return billingstripe.InvoiceItem{ID: "ii_test_" + uuid.NewString()}, nil
}

func (f *fakeStripe) FinalizeInvoice(_ context.Context, invoiceID, idemKey string) (billingstripe.Invoice, error) {
	f.finalizeCalls = append(f.finalizeCalls, finalizeCall{invoiceID, idemKey})
	if f.errInvoice != nil {
		return billingstripe.Invoice{}, f.errInvoice
	}
	if f.onCreateInvoice != nil {
		f.onCreateInvoice()
	}
	return billingstripe.Invoice{
		ID:         invoiceID,
		Status:     f.invoiceStatus,
		AmountDue:  f.invoiceAmountDue,
		AmountPaid: f.invoiceAmountPaid,
		Currency:   "usd",
	}, nil
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

// Compile-time check: fakeStripe satisfies the full Client interface.
var _ billingstripe.Client = (*fakeStripe)(nil)

// --- helpers --------------------------------------------------------------

var chargeAccount = uuid.New()

func chargeSvc(store *fakeStore, sc billingstripe.Client) *cycle.Service {
	return cycle.NewService(store, sc)
}

// --- RunBillingCycle: happy path ------------------------------------------

func TestRunBillingCycle_ChargesArrears(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_234_500 // micros → 123.45 cents → round-half-up 123 cents
	store.hasPM = true
	store.stripeCustomer = "cus_test_1"
	sc := newFakeStripe()
	sc.invoiceAmountDue = 123

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.True(t, resp.FirstRun)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 1_234_500, resp.ArrearsMicros)
	require.EqualValues(t, 123, resp.ChargedCents) // round_half_up(1_234_500 / 10_000) = round(123.45) = 123
	require.NotEmpty(t, resp.StripeInvoiceID)

	// Stripe was called once each.
	require.Len(t, sc.itemCalls, 1)
	require.Len(t, sc.invoiceCalls, 1)
	require.Equal(t, "cus_test_1", sc.itemCalls[0].custID)
	require.EqualValues(t, 123, sc.itemCalls[0].amountCfg)
	require.Equal(t, "usd", sc.itemCalls[0].currency)
	require.Len(t, sc.finalizeCalls, 1, "the draft is finalized (auto_advance) — the money-moving step")

	// Invoice mirrored + run marked invoiced.
	require.Len(t, store.invoices, 1)
	mirror := store.invoices[resp.StripeInvoiceID]
	require.Equal(t, chargeAccount, mirror.AccountID)
	require.EqualValues(t, 123, mirror.AmountDueCents)
	require.Equal(t, "usd", mirror.Currency)

	require.Len(t, store.markedRuns, 1)
	for _, m := range store.markedRuns {
		require.Equal(t, cycle.RunStatusInvoiced, m.status)
		require.EqualValues(t, 123, m.totalCents)
		require.NotEmpty(t, m.invoiceID)
	}
}

// --- FINDING 3: a reclaimed boundary run reuses its FROZEN charge amount, never
// a freshly-recomputed live total, so the stable Stripe idem key never conflicts -

func TestRunBillingCycle_ReclaimReusesFrozenBoundaryChargeAmount(t *testing.T) {
	// Reproduces the exact failure scenario. Account X's boundary run computes
	// arrears $1 + advance base $40 (2 apps) + advance overage $6 (2 ongoing
	// over-modules) = $47 (4700¢), calls Stripe under ii-<run>/inv-<run> (the money
	// moves), but crashes before MarkBillingRun commits — the run stays 'pending'.
	// Before the retry a customer uninstalls one over-module, so a LIVE recompute
	// would now yield only $44 (4400¢). InsertBillingRun RECLAIMS the SAME run id
	// (same idem keys). Pre-fix, the retry re-sent those keys with the recomputed
	// $44 — a mismatched body under a used idem key — which Stripe rejects,
	// permanently stalling the run. Fixed: the retry REUSES the frozen $47 under the
	// same keys and completes.
	store := newFakeStore()
	store.chargedTotal = 1_000_000 // $1 usage arrears
	store.hasPM = true
	store.stripeCustomer = "cus_f3"

	// 2 live apps created before the new period → $40 advance base.
	seedApp(store, chargeAccount, 0, false)
	app2 := seedApp(store, chargeAccount, 0, false)
	// 5 included (ranks 0-4) + 2 ongoing over-modules already charged in a prior
	// period (ranks 5-6) → overCount 2 → $6 advance overage.
	seedIncluded(store, chargeAccount, app2, timeUTC(2026, 5, 1, 0), 5)
	o1 := seedTimer(store, chargeAccount, app2, timeUTC(2026, 5, 10, 0))
	o2 := seedTimer(store, chargeAccount, app2, timeUTC(2026, 5, 11, 0))
	for _, id := range []uuid.UUID{o1, o2} {
		store.timers[id].graceResolved = true
		store.timers[id].graceCharged = true // charged in a prior period → ongoing
	}

	sc := newFakeStripe()

	// FIRST attempt: Stripe charges $47, but MarkBillingRun fails (Lambda timed out
	// before commit) → the run stays 'pending', the frozen $47 is durable.
	store.errMarkRun = errors.New("lambda timeout before MarkBillingRun commit")
	_, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.Error(t, err, "the mark failed → the run is left pending, resumable")
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 4700, sc.itemCalls[0].amountCfg, "$1 + $40 + 2×$3 = $47")
	firstIdem := sc.itemCalls[0].idemKey

	// Between attempts: a customer uninstalls one over-module → a LIVE recompute
	// would now yield overCount 1 → $44, NOT $47.
	store.timers[o2].removed = true
	store.timers[o2].removedAt = timeUTC(2026, 6, 15, 0)
	store.errMarkRun = nil // the retry's mark succeeds

	// RETRY: reclaims the SAME run id (same idem keys). It must charge the FROZEN
	// $47, never the recomputed $44 — otherwise Stripe rejects the reused key.
	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.True(t, resp.FirstRun, "a reclaimed non-terminal run is a fresh charge attempt")
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.Len(t, sc.itemCalls, 2)
	require.Equal(t, firstIdem, sc.itemCalls[1].idemKey,
		"the reclaim reuses the same run id → the same Stripe idem key")
	require.EqualValues(t, 4700, sc.itemCalls[1].amountCfg,
		"so the amount under that key must be the frozen $47, not the recomputed $44")
	require.EqualValues(t, 4700, resp.ChargedCents)
	for _, m := range store.markedRuns {
		require.Equal(t, cycle.RunStatusInvoiced, m.status)
		require.EqualValues(t, 4700, m.totalCents)
	}
}

func TestRunBillingCycle_CentsRoundHalfUp(t *testing.T) {
	// 5_000 micros = 0.5 cents → round-half-up → 1 cent.
	store := newFakeStore()
	store.chargedTotal = 5_000
	store.hasPM = true
	store.stripeCustomer = "cus_x"
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.ChargedCents)
	require.EqualValues(t, 1, sc.itemCalls[0].amountCfg)
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

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	// round_half_up(big / 10_000) computed independently with integer math.
	wantCents := (big + 5_000) / 10_000
	require.EqualValues(t, wantCents, resp.ChargedCents)
	require.Greater(t, resp.ChargedCents, int64(0))
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

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 400_000)
	require.NoError(t, err)
	require.EqualValues(t, 600_000, resp.ArrearsMicros)
	require.EqualValues(t, 60, resp.ChargedCents)
	require.Len(t, sc.invoiceCalls, 1)
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

func TestRunBillingCycle_IdempotentReRunNoSecondCharge(t *testing.T) {
	// Re-running the SAME period: the second InsertBillingRun hits the gate
	// (firstTime=false) → FirstRun=false, NO second Stripe charge.
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_d"
	sc := newFakeStripe()
	svc := chargeSvc(store, sc)

	first, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.True(t, first.FirstRun)
	require.Equal(t, cycle.RunStatusInvoiced, first.Status)
	require.Len(t, sc.invoiceCalls, 1)

	second, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.False(t, second.FirstRun, "re-run hits the idempotency gate")
	require.Empty(t, second.Status)

	// Stripe was NOT called a second time.
	require.Len(t, sc.itemCalls, 1, "no second invoice item")
	require.Len(t, sc.invoiceCalls, 1, "no second invoice / no double charge")
}

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
	svc := chargeSvc(store, sc)

	first, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedNoPM, first.Status)
	require.Empty(t, sc.invoiceCalls, "no charge while PM missing")
	require.Len(t, store.insertedRuns, 1)

	// Cycle 2: the account now has a usable PM. The skipped run is reclaimed.
	store.hasPM = true
	second, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.True(t, second.FirstRun, "a skipped run is reclaimed for a fresh attempt")
	require.Equal(t, cycle.RunStatusInvoiced, second.Status)
	require.Len(t, sc.invoiceCalls, 1, "the retained usage is charged on retry")
	require.Len(t, store.insertedRuns, 1, "reclaim reuses the same run row")
}

func TestRunBillingCycle_FailedReattemptsNextCycle(t *testing.T) {
	// A failed charge is RECLAIMED next cycle: the transient failure (e.g. a
	// declined card later fixed) re-attempts on the same run row.
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_fail_then_ok"
	sc := newFakeStripe()
	sc.errInvoice = errors.New("card_declined")
	svc := chargeSvc(store, sc)

	_, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	requireCode(t, err, billing.CodeStripeError)
	require.Len(t, store.insertedRuns, 1)

	// Card fixed: the failed run is reclaimed and now succeeds.
	sc.errInvoice = nil
	second, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.True(t, second.FirstRun, "a failed run is reclaimed for a fresh attempt")
	require.Equal(t, cycle.RunStatusInvoiced, second.Status)
	require.Len(t, store.insertedRuns, 1, "reclaim reuses the same run row")
}

func TestRunBillingCycle_InvoicedBlocksReattempt(t *testing.T) {
	// A terminal-success (invoiced) run is NEVER reclaimed: a re-run is a no-op.
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_done"
	sc := newFakeStripe()
	svc := chargeSvc(store, sc)

	first, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, first.Status)

	second, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.False(t, second.FirstRun, "an invoiced run blocks any re-attempt")
	require.Len(t, sc.invoiceCalls, 1, "no second charge")
}

func TestRunBillingCycle_DeterministicIdemKeys(t *testing.T) {
	// The per-run Stripe Idempotency-Keys are ii-<run> and inv-<run>.
	store := newFakeStore()
	store.chargedTotal = 500_000
	store.hasPM = true
	store.stripeCustomer = "cus_e"
	sc := newFakeStripe()

	_, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Len(t, store.insertedRuns, 1)
	var runID uuid.UUID
	for _, id := range store.insertedRuns {
		runID = id
	}
	require.Equal(t, "ii-"+runID.String(), sc.itemCalls[0].idemKey)
	require.Equal(t, "inv-"+runID.String(), sc.invoiceCalls[0].idemKey)
}

// --- RunBillingCycle: charge failure --------------------------------------

func TestRunBillingCycle_ChargeFailureMarksFailed(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_f"
	sc := newFakeStripe()
	sc.errInvoice = errors.New("card_declined")

	_, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	requireCode(t, err, billing.CodeStripeError)

	require.Len(t, store.markedRuns, 1)
	for _, m := range store.markedRuns {
		require.Equal(t, cycle.RunStatusFailed, m.status)
	}
	require.Empty(t, store.invoices, "no mirror on a failed charge")
}

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

func TestRunBillingCycle_LargeChargeFlagsMirror(t *testing.T) {
	// A charge above the default $100 threshold (nil override) freezes
	// is_large_auto_collect=true on the mirror. $150 arrears → flagged.
	store := newFakeStore()
	store.chargedTotal = 150_000_000 // $150 in micros, over the $100 default
	store.hasPM = true
	store.stripeCustomer = "cus_large"
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.True(t, store.invoices[resp.StripeInvoiceID].IsLargeAutoCollect,
		"$150 > $100 default threshold → disclosed as large")
}

func TestRunBillingCycle_SmallChargeDoesNotFlagMirror(t *testing.T) {
	// A charge below the default threshold leaves the flag false (the historic /
	// non-disclosed default).
	store := newFakeStore()
	store.chargedTotal = 50_000_000 // $50 in micros, under the $100 default
	store.hasPM = true
	store.stripeCustomer = "cus_small"
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.False(t, store.invoices[resp.StripeInvoiceID].IsLargeAutoCollect,
		"$50 < $100 default threshold → not disclosed")
}

func TestRunBillingCycle_PerAccountThresholdOverrideRespected(t *testing.T) {
	// A $200 per-account override governs over the $100 default: a $150 charge is
	// under the CUSTOM threshold and so is NOT flagged, proving the flag is
	// resolved against the account's own threshold at charge time.
	store := newFakeStore()
	override := int64(200_000_000) // $200 override
	store.collection.AutoCollectThresholdMicros = &override
	store.chargedTotal = 150_000_000 // $150, over default but under the override
	store.hasPM = true
	store.stripeCustomer = "cus_override"
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.False(t, store.invoices[resp.StripeInvoiceID].IsLargeAutoCollect,
		"$150 < $200 per-account override → not disclosed despite exceeding the default")
}

// TestRunBillingCycle_SubCentAboveThresholdChargesExactThresholdNotFlagged is
// the end-to-end regression for finding #1 (collection.IsLargeAutoCollect
// compared raw pre-rounding micros against the threshold instead of the SAME
// post-rounding cents Stripe actually charges).
//
// FAILS without the fix: arrears = $100.00 + 100 micros ($100.0001) is
// strictly ABOVE the raw $100,000,000-micro default threshold, so the old
// `chargedMicros > threshold` comparison flagged the mirror row "large" even
// though the money that actually hit the card — the SAME
// centsFromMicros(arrears) conversion this test asserts on the fake Stripe
// call — is EXACTLY $100.00 (round-half-up rounds 100_000_100/10_000 =
// 10000.01 DOWN to 10000 cents), identical to what a charge of exactly the
// threshold itself would produce. Proves the concrete dollar amount, not just
// "no error": the Stripe invoice item is created for precisely 10000 cents
// ($100.00), and the mirror is NOT flagged, matching the "exactly at
// threshold is not large" contract.
func TestRunBillingCycle_SubCentAboveThresholdChargesExactThresholdNotFlagged(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 100_000_100 // $100.00 + 100 micros ($0.0001) — inside the half-cent gap
	store.hasPM = true
	store.stripeCustomer = "cus_subcent"
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)

	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 10000, sc.itemCalls[0].amountCfg,
		"Stripe is charged exactly 10000 cents ($100.00), not $100.01 — the same amount charging exactly the threshold would produce")
	require.EqualValues(t, 10000, resp.ChargedCents)

	require.False(t, store.invoices[resp.StripeInvoiceID].IsLargeAutoCollect,
		"a charge that rounds down to EXACTLY the $100 threshold must not be disclosed as large")
}

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

func TestRunBillingCycle_ConcurrentThresholdEditMidChargeResolvesPostCharge(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 150_000_000 // $150: over the $100 default, under a $200 override
	store.hasPM = true
	store.stripeCustomer = "cus_race_boundary"
	sc := newFakeStripe()
	sc.onCreateInvoice = func() {
		// The concurrent edit: an operator (or the account owner) raises the
		// disclosure threshold to $200 WHILE this charge's Stripe call is in
		// flight.
		override := int64(200_000_000)
		store.collection.AutoCollectThresholdMicros = &override
	}

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 15000, resp.ChargedCents, "still charges $150 — the edit only affects disclosure, never the amount")
	require.False(t, store.invoices[resp.StripeInvoiceID].IsLargeAutoCollect,
		"the threshold is resolved AFTER the Stripe charge succeeds, so the mid-charge $200 edit governs — $150 is not flagged")
}

func TestChargeCreationProration_ConcurrentThresholdEditMidChargeResolvesPostCharge(t *testing.T) {
	// The creation-proration leg (the grace-sweep charge, proration.go) resolves
	// its large-charge disclosure threshold at the SAME point relative to the
	// actual charge as the boundary leg above: immediately AFTER the Stripe call
	// succeeds, never from a pre-charge snapshot. Under the unified model this
	// leg bills the FLAT per-app base only ($20 — module overage is no longer
	// folded in), so the straddle here is a $20 charge between a $10 pre-charge
	// threshold and a $30 mid-charge override.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	// Pre-charge threshold $10: were it resolved BEFORE the charge, the $20 base
	// would flag "large" ($20 > $10).
	stale := int64(10_000_000)
	store.collection.AutoCollectThresholdMicros = &stale
	sc := newFakeStripe()
	sc.onCreateInvoice = func() {
		// The concurrent edit lands WHILE the Stripe call is in flight, raising
		// the threshold to $30 — above the $20 base.
		override := int64(30_000_000)
		store.collection.AutoCollectThresholdMicros = &override
	}
	svc := appsSvc(store, sc)
	appID := uuid.New()
	// CreatedAt on the anchored period boundary (day 4, matching registeredAccount's
	// May-4 activation) → the FULL flat $20 base, no proration dampening.
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), 3)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusCharged, resp.Status)
	require.EqualValues(t, 2000, resp.ProrationCents, "flat $20 base, charged in full")
	require.NotEmpty(t, resp.ProrationInvoiceID)

	require.False(t, store.invoices[resp.ProrationInvoiceID].IsLargeAutoCollect,
		"the threshold is resolved AFTER the Stripe charge succeeds on this leg too, so the mid-charge $30 edit governs — $20 is not flagged")
}
