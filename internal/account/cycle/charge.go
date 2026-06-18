package cycle

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// RunBillingCycle charges one account for one closed billing period — the
// USAGE (arrears) leg of the cycle (design §4 Axis 4). It is the charge spine:
//
//  1. InsertBillingRun — the FIRST idempotency layer. If a run for this exact
//     (account, period) window already exists, this call is a no-op (FirstRun=
//     false): the work was already done by the original run, so we NEVER
//     re-read arrears or re-charge. A re-fire / partial-failure resume lands
//     here and safely skips.
//  2. arrears = max(0, PeriodChargedTotal − allowanceMicros). The
//     allowance-netting MATH is implemented here with allowanceMicros as an
//     INPUT; v1 callers pass 0. (TODO: a dedicated subscription/tier PR sources
//     the allowance from the account's tier `included_allowance` + adds the
//     ADVANCE leg — seats × seat_price + apps × $20. Those need tier pricing +
//     per-account seat/app counts that do NOT exist in billing yet.)
//  3. arrears == 0 → MarkBillingRun('invoiced') with NO Stripe call. We NEVER
//     auto-create a Stripe Customer with nothing to charge (design §4 Axis 4).
//  4. no usable default PM → MarkBillingRun('skipped_no_pm'). The usage is
//     RETAINED (usage_aggregates untouched); the next cycle re-attempts. NOT a
//     failure, NOT lost usage.
//  5. otherwise CHARGE: convert micros → whole cents (round-half-up), create a
//     Stripe invoice item (deterministic Idempotency-Key ii-<run>) → a draft
//     invoice (charge_automatically + auto_advance, Idempotency-Key inv-<run>)
//     which Stripe finalizes + runs off-session against the default PM → mirror
//     the invoice into ms_billing.invoices → MarkBillingRun('invoiced',
//     stripe_invoice_id, total). The two deterministic Idempotency-Keys are the
//     SECOND idempotency layer: even if step 1's gate were somehow bypassed, a
//     re-run reuses the SAME Stripe objects and never double-charges.
//
// A charge error after the PM gate marks the run 'failed' (auditable; PR #7
// webhook reconciliation + risk-graded retry build on it) and returns the
// error. Money is integer micro-dollars → cents; never float.
func (s *Service) RunBillingCycle(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time, allowanceMicros int64) (*ChargeSummary, error) {
	if accountID == uuid.Nil {
		return nil, billing.InvalidInput("account_id required")
	}
	if periodStart.IsZero() || periodEnd.IsZero() || !periodEnd.After(periodStart) {
		return nil, billing.InvalidInput("period_end must be after period_start")
	}
	if allowanceMicros < 0 {
		return nil, billing.InvalidInput("allowance_micros must be non-negative")
	}
	if s.stripe == nil {
		// Charge leg requires a Stripe client; rollup-only wiring must not reach
		// here. Surface as INTERNAL (a wiring bug), not a silent no-op.
		return nil, billing.Internal("RunBillingCycle requires a Stripe client", nil)
	}

	runID, shouldCharge, err := s.store.InsertBillingRun(ctx, accountID, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("insert billing run failed", err)
	}
	if !shouldCharge {
		// Idempotency gate: the window already has an 'invoiced' (terminal-
		// success) run. Do nothing — no arrears read, no Stripe charge. Caller
		// treats FirstRun=false as success (already charged). A non-terminal run
		// (skipped_no_pm / failed / pending-died-mid-flight) is RECLAIMED by
		// InsertBillingRun instead and falls through here to re-attempt.
		return &ChargeSummary{FirstRun: false}, nil
	}

	total, err := s.store.PeriodChargedTotal(ctx, accountID, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("period charged total query failed", err)
	}

	// Allowance-netting: the meter never bills the first `allowanceMicros` of
	// usage. v1 passes 0, so arrears == total. Negative clamps to 0.
	arrears := total - allowanceMicros
	if arrears < 0 {
		arrears = 0
	}

	summary := &ChargeSummary{FirstRun: true, ArrearsMicros: arrears}

	// Zero arrears (empty/zero period, or allowance ≥ usage): mark invoiced with
	// NO Stripe call — never auto-create a Customer with nothing to charge.
	if arrears == 0 {
		if err := s.store.MarkBillingRun(ctx, runID, RunStatusInvoiced, "", 0); err != nil {
			return nil, billing.Internal("mark billing run (zero arrears) failed", err)
		}
		summary.Status = RunStatusInvoiced
		return summary, nil
	}

	// No usable default PM: skip (usage RETAINED), re-attempt next cycle.
	hasPM, err := s.store.HasUsableDefaultPM(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("usable PM check failed", err)
	}
	if !hasPM {
		if err := s.store.MarkBillingRun(ctx, runID, RunStatusSkippedNoPM, "", 0); err != nil {
			return nil, billing.Internal("mark billing run (skipped_no_pm) failed", err)
		}
		summary.Status = RunStatusSkippedNoPM
		return summary, nil
	}

	custID, err := s.store.AccountStripeCustomer(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("stripe customer lookup failed", err)
	}
	if custID == "" {
		// A usable PM implies a Stripe Customer (a card can't attach without
		// one). An empty id here is an anomaly — surface it; never auto-create a
		// Customer on the charge path.
		return nil, billing.Internal("account has a usable PM but no Stripe customer id", nil)
	}

	cents, err := centsFromMicros(arrears)
	if err != nil {
		return nil, billing.Internal("micros to cents conversion failed", err)
	}
	summary.ChargedCents = cents

	// Charge. A failure after the PM gate marks the run 'failed' (auditable) and
	// returns the error.
	inv, err := s.charge(ctx, runID, custID, cents)
	if err != nil {
		if markErr := s.store.MarkBillingRun(ctx, runID, RunStatusFailed, "", 0); markErr != nil {
			// Both failed: surface the original charge error; the failed-mark is
			// best-effort (the run stays 'pending' and is auditable / resumable).
			return nil, billing.StripeError("charge failed and could not mark run failed", err)
		}
		summary.Status = RunStatusFailed
		return nil, billing.StripeError("charge failed", err)
	}

	if err := s.store.UpsertInvoice(ctx, InvoiceMirror{
		AccountID:       accountID,
		StripeInvoiceID: inv.ID,
		Status:          inv.Status,
		AmountDueCents:  inv.AmountDue,
		AmountPaidCents: inv.AmountPaid,
		Currency:        chargeCurrency,
		PeriodStart:     periodStart,
		PeriodEnd:       periodEnd,
	}); err != nil {
		return nil, billing.Internal("invoice mirror upsert failed", err)
	}

	if err := s.store.MarkBillingRun(ctx, runID, RunStatusInvoiced, inv.ID, cents); err != nil {
		return nil, billing.Internal("mark billing run (invoiced) failed", err)
	}

	summary.Status = RunStatusInvoiced
	summary.StripeInvoiceID = inv.ID
	return summary, nil
}

// charge creates the Stripe invoice item + draft invoice for the arrears, with
// the two deterministic Idempotency-Keys (ii-<run>, inv-<run>) so a re-run
// reuses the same Stripe objects. Returns the created invoice projection
// (id/status/amounts) for the mirror upsert.
func (s *Service) charge(ctx context.Context, runID uuid.UUID, custID string, cents int64) (billingstripe.Invoice, error) {
	desc := fmt.Sprintf("MirrorStack usage — run %s", runID)
	if _, err := s.stripe.CreateInvoiceItem(ctx, custID, cents, chargeCurrency, desc, invoiceItemIdemKey(runID)); err != nil {
		return billingstripe.Invoice{}, err
	}
	return s.stripe.CreateInvoice(ctx, custID, true /* autoAdvance */, invoiceIdemKey(runID))
}

// AccountsWithUsageEvents returns the accounts with raw usage_events in the
// [periodStart, periodEnd) window — the rollup-phase (phase 1) work list
// cmd/billing-cycle iterates before charging. A thin pass-through to the store.
func (s *Service) AccountsWithUsageEvents(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error) {
	if periodStart.IsZero() || periodEnd.IsZero() || !periodEnd.After(periodStart) {
		return nil, billing.InvalidInput("period_end must be after period_start")
	}
	accounts, err := s.store.AccountsWithUsageEvents(ctx, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("list accounts with usage events failed", err)
	}
	return accounts, nil
}

// AccountsWithUnbilledUsage returns the accounts with usage_aggregates in the
// [periodStart, periodEnd) window that have no SUCCESSFUL (invoiced) billing_run
// yet — the charge-phase (phase 2) work list cmd/billing-cycle iterates. A thin
// pass-through to the store so the binary depends only on the Service.
func (s *Service) AccountsWithUnbilledUsage(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error) {
	if periodStart.IsZero() || periodEnd.IsZero() || !periodEnd.After(periodStart) {
		return nil, billing.InvalidInput("period_end must be after period_start")
	}
	accounts, err := s.store.AccountsWithUnbilledUsage(ctx, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("list unbilled accounts failed", err)
	}
	return accounts, nil
}

// invoiceItemIdemKey / invoiceIdemKey build the deterministic per-run Stripe
// Idempotency-Keys. The run id is the stable charge identity, so a re-fire
// (same run row) produces the SAME keys and Stripe returns the original objects
// instead of creating duplicates. The arrears charge is a SINGLE pooled line
// per run (Σ charged across all metrics), so the item key is ii-<run> (not
// per-metric) — matching the single combined invoice item this leg creates.
func invoiceItemIdemKey(runID uuid.UUID) string { return "ii-" + runID.String() }
func invoiceIdemKey(runID uuid.UUID) string     { return "inv-" + runID.String() }
