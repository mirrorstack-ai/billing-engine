package cycle_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

// Risk-graded collection gate (PR #9). These tests exercise RunBillingCycle's
// gate: prepaid mode skips, spend ceiling caps, credit-limit / delinquency
// tighten + persist, all RETAINING usage and NEVER calling Stripe on a skip.

// --- prepaid mode: skip, retain, no Stripe --------------------------------

func TestRunBillingCycle_PrepaidModeSkipsRetainsUsage(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_prepaid"
	store.collection = cycle.AccountCollection{Mode: cycle.BillingModePrepaid, CreditLimitMicros: 25_000_000}
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.True(t, resp.FirstRun)
	require.Equal(t, cycle.RunStatusSkippedPrepaid, resp.Status)

	// No Stripe call, no invoice mirror, run marked skipped_prepaid.
	require.Empty(t, sc.itemCalls, "prepaid mode must not call Stripe")
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, store.invoices)
	require.Len(t, store.markedRuns, 1)
	for _, m := range store.markedRuns {
		require.Equal(t, cycle.RunStatusSkippedPrepaid, m.status)
		require.EqualValues(t, 0, m.totalCents)
	}
	// Usage RETAINED: the source total is untouched.
	require.EqualValues(t, 1_000_000, store.chargedTotal)
}

func TestRunBillingCycle_PrepaidModeShortCircuitsBeforeAggregateRead(t *testing.T) {
	// Prepaid mode is the fast path: it skips before PeriodChargedTotal, so even
	// an injected aggregate-read error never fires.
	store := newFakeStore()
	store.collection = cycle.AccountCollection{Mode: cycle.BillingModePrepaid}
	store.errTotal = errors.New("must not be read")
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedPrepaid, resp.Status)
}

// --- spend ceiling: hard bill-shock cap -----------------------------------

func TestRunBillingCycle_SpendCeilingExceededNotCharged(t *testing.T) {
	// Arrears above the customer-set ceiling → NOT auto-charged (bill-shock
	// guard); run skipped_prepaid, usage retained, no Stripe.
	store := newFakeStore()
	store.chargedTotal = 2_000_000 // 200 cents
	store.hasPM = true
	store.stripeCustomer = "cus_ceiling"
	store.collection = cycle.AccountCollection{
		Mode:               cycle.BillingModeArrears,
		CreditLimitMicros:  1_000_000_000,
		HasSpendCeiling:    true,
		SpendCeilingMicros: 1_000_000, // ceiling 100 cents < 200 cents arrears
	}
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	// skipped_ceiling (NOT skipped_prepaid): a per-cycle cap skip, mode unchanged.
	require.Equal(t, cycle.RunStatusSkippedCeiling, resp.Status)
	require.Empty(t, sc.invoiceCalls, "above ceiling must not auto-charge")
	require.EqualValues(t, 2_000_000, store.chargedTotal, "usage retained")
	require.Nil(t, store.updatedCollection, "ceiling breach does not change the mode")
	for _, m := range store.markedRuns {
		require.Equal(t, cycle.RunStatusSkippedCeiling, m.status)
	}
}

func TestRunBillingCycle_SpendCeilingAppliedToNettedArrears(t *testing.T) {
	// The ceiling is checked against the NETTED arrears (after allowance), so an
	// allowance that drops usage below the ceiling lets the charge proceed.
	store := newFakeStore()
	store.chargedTotal = 2_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_ceiling_ok"
	store.collection = cycle.AccountCollection{
		Mode:               cycle.BillingModeArrears,
		CreditLimitMicros:  1_000_000_000,
		HasSpendCeiling:    true,
		SpendCeilingMicros: 1_500_000,
	}
	sc := newFakeStripe()

	// allowance 1_000_000 → netted arrears 1_000_000 <= ceiling 1_500_000 → charge.
	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 1_000_000)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.Len(t, sc.invoiceCalls, 1)
}

// --- credit limit exceeded: tighten + persist + retain --------------------

func TestRunBillingCycle_OverCreditLimitTightensToPrepaid(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_overlimit"
	store.collection = cycle.AccountCollection{
		Mode:              cycle.BillingModeArrears,
		CreditLimitMicros: 500_000, // arrears 1_000_000 >= limit → over
	}
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedPrepaid, resp.Status)
	require.Empty(t, sc.invoiceCalls, "over-limit must not charge")

	// The tighten is PERSISTED: usage_billing_mode flipped to prepaid.
	require.NotNil(t, store.updatedCollection)
	require.Equal(t, cycle.BillingModePrepaid, store.updatedCollection.Mode)
	require.EqualValues(t, 1_000_000, store.chargedTotal, "usage retained")
}

// --- delinquency signal: tighten + persist --------------------------------

func TestRunBillingCycle_DelinquencyTightensToPrepaid(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 100_000 // small, well within the limit
	store.hasPM = true
	store.stripeCustomer = "cus_delinquent"
	store.collection = cycle.AccountCollection{Mode: cycle.BillingModeArrears, CreditLimitMicros: 1_000_000_000}
	store.unpaidInvoice = true // #7 delinquency signal
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedPrepaid, resp.Status)
	require.Empty(t, sc.invoiceCalls, "delinquent account must not off-session charge")

	require.NotNil(t, store.updatedCollection)
	require.Equal(t, cycle.BillingModePrepaid, store.updatedCollection.Mode)
}

// --- within limits, clean: charges as today -------------------------------

func TestRunBillingCycle_ArrearsWithinLimitsCharges(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_ok"
	store.collection = cycle.AccountCollection{Mode: cycle.BillingModeArrears, CreditLimitMicros: 1_000_000_000}
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.Len(t, sc.invoiceCalls, 1)
	require.Nil(t, store.updatedCollection, "no transition when within limits + clean")
}

// --- idempotency: a tightened account on re-run does not double-charge -----

func TestRunBillingCycle_TightenedThenReRunNoDoubleCharge(t *testing.T) {
	// Cycle 1 tightens to prepaid (over limit) and marks skipped_prepaid. A
	// re-run reads the now-prepaid mode and stays skipped — never charges. The
	// run row is RECLAIMED (skipped_prepaid is non-terminal for reclaim), but the
	// prepaid gate skips it again, so Stripe is never called.
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_reidem"
	store.collection = cycle.AccountCollection{Mode: cycle.BillingModeArrears, CreditLimitMicros: 500_000}
	sc := newFakeStripe()
	svc := chargeSvc(store, sc)

	first, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedPrepaid, first.Status)
	require.Equal(t, cycle.BillingModePrepaid, store.collection.Mode, "transition persisted")

	second, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedPrepaid, second.Status)
	require.Empty(t, sc.invoiceCalls, "a tightened account never charges on re-run")
	require.Len(t, store.insertedRuns, 1, "same run row reclaimed, no duplicate")
}

// --- error propagation for the new store reads ----------------------------

func TestRunBillingCycle_PropagatesCollectionStoreErrors(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name  string
		setup func(*fakeStore)
	}{
		{"collection load", func(f *fakeStore) { f.errCollection = boom }},
		{"delinquency lookup", func(f *fakeStore) {
			f.chargedTotal = 1_000_000
			f.errUnpaid = boom
		}},
		{"persist transition", func(f *fakeStore) {
			f.chargedTotal = 1_000_000
			f.collection = cycle.AccountCollection{Mode: cycle.BillingModeArrears, CreditLimitMicros: 1}
			f.errUpdateColl = boom
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			store.hasPM = true
			store.stripeCustomer = "cus_err"
			tc.setup(store)
			sc := newFakeStripe()
			_, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
			requireCode(t, err, billing.CodeInternal)
		})
	}
}
