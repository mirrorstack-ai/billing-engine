// Package eligibility implements the SERVICE-BLOCK gate: the pure decision of
// whether an account's services may run, given its payment standing. It is a
// sibling of the collection package (internal/account/collection) and follows
// the same posture — DELIBERATELY PURE: the verdict is a total function of
// explicit Signals, with NO DB / Stripe / clock access. The caller (the
// account-api billing service) gathers the signals (via db.ServiceBlockSignals)
// and calls Evaluate; persistence + transport live in the caller. This keeps
// the policy unit-testable in isolation and puts the gate rules in one obvious,
// finance-legible place.
//
// The gate (product spec): an account is ELIGIBLE for service iff ALL of —
//
//	(1) it has at least one NON-FRAUD card on file,
//	(2) its FIRST charge did not fail,
//	(3) its consecutive failed-charge streak is < 2, and
//	(4) it has fewer than 2 unpaid invoices (funding-gates design,
//	    DECIDED 2026-07-11 — docs-temp/billing-funding-gates/design.md).
//
// If any gate fails, the account is BLOCKED. The four gates map one-for-one to
// the four Signals fields; see Evaluate for the exact boolean logic and the
// grace edges (a brand-new account with no charge yet is graced, not blocked).
package eligibility

// MaxFailedChargeStreak is the consecutive failed-charge count at which services
// are blocked. The spec is "< 2 failed" (2 excluded), so a streak of 0 or 1
// passes and 2+ blocks. The streak is RECOVERABLE — it is derived (by
// ServiceBlockSignals) as the failures since the account's last successful
// charge, so paying moves the cutoff forward and self-heals the block.
// Hardcoded (mirrors collection's inline finance thresholds); promote to a
// per-account column only if a knob is needed.
const MaxFailedChargeStreak = 2

// MaxUnpaidInvoices is the unpaid-invoice count at which services are blocked
// (funding-gates design, DECIDED 2026-07-11): "block services when the account
// has >= 2 unpaid invoices". Unpaid = a mirror invoice still collectible but
// not collected ('open' or 'uncollectible') with amount_due > 0 — derived at
// read time from the invoices mirror, like the streak, so paying an invoice
// self-heals the block as soon as the invoice.paid webhook lands. 0 or 1
// unpaid passes; 2+ blocks.
const MaxUnpaidInvoices = 2

// FirstChargeState is the outcome of the account's EARLIEST real charge — the
// oldest invoice that is not draft (never finalized) or void (cancelled). It is
// the enum form of ServiceBlockSignals.first_charge_status, resolved by the
// caller from the invoice mirror.
type FirstChargeState string

const (
	// FirstChargeNone: the account has no finalized charge yet (brand new). GRACE
	// — eligible as long as a card is present, so arrears/usage billing isn't a
	// bootstrap deadlock (the account must run to generate a charge at all).
	FirstChargeNone FirstChargeState = "none"
	// FirstChargeSucceeded: the earliest real invoice is 'paid'. Passes gate 2.
	FirstChargeSucceeded FirstChargeState = "succeeded"
	// FirstChargePending: the earliest real invoice is still 'open' (Stripe is
	// smart-retrying). GRACE during the retry window — not yet a failure.
	FirstChargePending FirstChargeState = "pending"
	// FirstChargeFailed: the earliest real invoice went 'uncollectible' (Stripe
	// gave up). This is the only first-charge state that BLOCKS (fails gate 2).
	FirstChargeFailed FirstChargeState = "failed"
)

// Signals are the live inputs the gate reasons over, gathered by the caller in
// one DB read (db.ServiceBlockSignals). None are derived here.
type Signals struct {
	// UsableNonFraudCardCount is the number of active (not soft-deleted),
	// not-fraud-flagged, not-expired cards on the account. Gate 1 requires >= 1.
	UsableNonFraudCardCount int
	// FirstCharge is the outcome of the account's earliest real charge. Gate 2
	// blocks only on FirstChargeFailed.
	FirstCharge FirstChargeState
	// FailedChargeStreak is the account's consecutive failed-charge counter
	// (resets to 0 on a successful charge). Gate 3 blocks at >= MaxFailedChargeStreak.
	FailedChargeStreak int
	// UnpaidInvoiceCount is the number of unpaid (open/uncollectible,
	// amount_due > 0) invoices on the account's mirror. Gate 4 blocks at
	// >= MaxUnpaidInvoices.
	UnpaidInvoiceCount int
}

// Reason is a stable, machine-readable code for a Verdict — the primary block
// cause (or ReasonEligible), for the wire contract, the UI, and test assertions.
type Reason string

const (
	// ReasonEligible: not blocked — all gates pass.
	ReasonEligible Reason = "ELIGIBLE"
	// ReasonNoUsableCard: gate 1 failed — zero non-fraud usable cards on file.
	ReasonNoUsableCard Reason = "NO_USABLE_CARD"
	// ReasonFirstChargeFailed: gate 2 failed — the account's first charge went
	// uncollectible.
	ReasonFirstChargeFailed Reason = "FIRST_CHARGE_FAILED"
	// ReasonTooManyFailures: gate 3 failed — the consecutive failed-charge streak
	// reached MaxFailedChargeStreak.
	ReasonTooManyFailures Reason = "TOO_MANY_FAILURES"
	// ReasonUnpaidInvoices: gate 4 failed — the account carries
	// MaxUnpaidInvoices or more unpaid invoices.
	ReasonUnpaidInvoices Reason = "UNPAID_INVOICES"
	// ReasonOutOfCredits is the additive wallet gate merged by the billing
	// service after Evaluate has applied the original four card/standing gates.
	// Keeping it out of Evaluate preserves those gates' semantics byte-for-byte
	// for accounts that do not use the credit wallet.
	ReasonOutOfCredits Reason = "OUT_OF_CREDITS"
)

// Verdict is the gate's decision. Blocked is the single field a caller must read
// to gate service; Reason is the primary cause (ReasonEligible when !Blocked);
// Reasons lists EVERY failing gate (empty when eligible) so a UI can tell a
// customer everything they must fix, not just the first thing.
type Verdict struct {
	Blocked bool
	Reason  Reason
	Reasons []Reason
}

// Evaluate applies the four gates in a fixed priority order and returns the
// Verdict. The gates, and the exact boolean each checks:
//
//	gate 1 (card):         UsableNonFraudCardCount >= 1
//	gate 2 (first charge): FirstCharge != FirstChargeFailed
//	gate 3 (failures):     FailedChargeStreak < MaxFailedChargeStreak
//	gate 4 (unpaid):       UnpaidInvoiceCount < MaxUnpaidInvoices
//	Blocked = NOT (gate1 AND gate2 AND gate3 AND gate4)
//
// Priority (card → first charge → failures → unpaid) fixes which cause becomes
// the primary Reason when more than one gate fails; Reasons collects them all in
// the same order. The order is purely presentational — Blocked is the AND of all
// four regardless.
//
// Grace edges (why gate 2 is "!= failed", not "== succeeded"): a brand-new
// account (FirstChargeNone) and one whose first invoice is still retrying
// (FirstChargePending) both PASS gate 2 — blocking them would deadlock an
// account that must run before it can ever be charged. Only an outright failed
// first charge (uncollectible) blocks. Gate 1 is unconditional, so even a graced
// new account still needs a card.
func Evaluate(s Signals) Verdict {
	var reasons []Reason

	if s.UsableNonFraudCardCount < 1 {
		reasons = append(reasons, ReasonNoUsableCard)
	}
	if s.FirstCharge == FirstChargeFailed {
		reasons = append(reasons, ReasonFirstChargeFailed)
	}
	if s.FailedChargeStreak >= MaxFailedChargeStreak {
		reasons = append(reasons, ReasonTooManyFailures)
	}
	if s.UnpaidInvoiceCount >= MaxUnpaidInvoices {
		reasons = append(reasons, ReasonUnpaidInvoices)
	}

	if len(reasons) == 0 {
		return Verdict{Blocked: false, Reason: ReasonEligible}
	}
	return Verdict{Blocked: true, Reason: reasons[0], Reasons: reasons}
}
