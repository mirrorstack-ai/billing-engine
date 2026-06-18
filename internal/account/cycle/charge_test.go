package cycle_test

import (
	"context"
	"errors"
	"testing"

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
	itemCalls    []itemCall
	invoiceCalls []invoiceCall

	// returns
	invoiceID         string
	invoiceStatus     string
	invoiceAmountDue  int64
	invoiceAmountPaid int64

	// injected errors
	errItem    error
	errInvoice error
}

type itemCall struct {
	custID    string
	amountCfg int64
	currency  string
	desc      string
	idemKey   string
}

type invoiceCall struct {
	custID      string
	autoAdvance bool
	idemKey     string
}

func newFakeStripe() *fakeStripe {
	return &fakeStripe{
		invoiceID:        "in_test_" + uuid.NewString(),
		invoiceStatus:    "paid",
		invoiceAmountDue: 0, // overridden per test where the charged amount matters
	}
}

func (f *fakeStripe) CreateInvoiceItem(_ context.Context, custID string, amountCents int64, currency, desc, idemKey string) (billingstripe.InvoiceItem, error) {
	f.itemCalls = append(f.itemCalls, itemCall{custID, amountCents, currency, desc, idemKey})
	if f.errItem != nil {
		return billingstripe.InvoiceItem{}, f.errItem
	}
	return billingstripe.InvoiceItem{ID: "ii_test_" + uuid.NewString()}, nil
}

func (f *fakeStripe) CreateInvoice(_ context.Context, custID string, autoAdvance bool, idemKey string) (billingstripe.Invoice, error) {
	f.invoiceCalls = append(f.invoiceCalls, invoiceCall{custID, autoAdvance, idemKey})
	if f.errInvoice != nil {
		return billingstripe.Invoice{}, f.errInvoice
	}
	return billingstripe.Invoice{
		ID:         f.invoiceID,
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
	require.True(t, sc.invoiceCalls[0].autoAdvance)

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
