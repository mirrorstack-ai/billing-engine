// Package collection implements the risk-graded collection policy for the
// off-session arrears leg — the GA gate for billing-engine charging accrued
// usage at cycle close (design §7-A "risk-graded collection"; billing-tiers §3
// "prepaid is the fallback, not the default").
//
// It is deliberately pure: every decision is a function of explicit account
// SIGNALS, with NO DB / Stripe / clock access. The charge spine (cycle package)
// gathers the signals (mode + limits from the accounts row, delinquency from the
// invoices mirror per #7, accrued arrears from the rollup) and calls these
// functions; persistence + Stripe live in the caller. This keeps the policy unit-
// testable in isolation and makes the thresholds (FINANCE-OWNED) one obvious
// place to revise.
//
// Two surfaces:
//
//	RiskAssess(account, signals) → Decision
//	  The risk-judge. Decides the desired usage_billing_mode and whether this
//	  cycle's off-session arrears charge may proceed. TIGHTENS toward 'prepaid'
//	  on a delinquency signal, accrual at/over the credit limit, or a usage
//	  spike. RELAXES back toward 'arrears' only on sustained clean standing
//	  (conservative — never aggressively re-trust).
//
//	TrustRampedCreditLimit(history, tenureDays, hasVerifiedPM) → micros
//	  The trust ramp. A conservative monotonic credit limit: new / no-verified-PM
//	  / short-tenure accounts get the low floor; it grows with successful-invoice
//	  history, tenure, and a verified PM. Exact numbers are FINANCE-OWNED — the
//	  consts below are conservative placeholders (see the TODO).
//
// Money is integer micro-dollars (no float), matching the rest of billing-engine.
package collection

// Mode is the account's usage_billing_mode — mirrors
// ms_billing.usage_billing_mode one-for-one. The charge spine maps this to/from
// the db enum.
type Mode string

const (
	// ModeArrears: accrued usage above allowance is charged off-session at cycle
	// close, gated on credit_limit + spend_ceiling + a usable PM.
	ModeArrears Mode = "arrears"
	// ModePrepaid: the off-session arrears leg is NOT permitted. The cycle marks
	// the run skipped_prepaid and RETAINS the usage. The prepaid-credit wallet
	// (balance / top-ups / deduct-on-usage) is a DEFERRED follow-up.
	ModePrepaid Mode = "prepaid"
)

// Account is the persisted collection state the judge reasons over: the current
// mode + the two limits. SpendCeilingMicros uses a sentinel of NoCeiling (0 is a
// valid ceiling meaning "charge nothing"; absence is modeled by HasSpendCeiling).
type Account struct {
	Mode               Mode
	CreditLimitMicros  int64
	HasSpendCeiling    bool
	SpendCeilingMicros int64
}

// Signals are the live, per-cycle risk inputs gathered by the caller. None are
// stored on the account; they are derived at decision time (delinquency from the
// invoices mirror per #7, accrued arrears from the rollup, the spike flag from a
// cycle-over-cycle comparison the caller may supply — false when not computed).
type Signals struct {
	// Delinquent is true when the account has an unpaid (open/uncollectible)
	// invoice — the #7 delinquency signal (billing.HasUnpaidInvoice). The
	// strongest tighten trigger.
	Delinquent bool
	// AccruedArrearsMicros is the netted arrears this cycle would charge
	// (max(0, usage − allowance)). Compared against the credit limit.
	AccruedArrearsMicros int64
	// UsageSpike is an optional caller-supplied anomaly flag (this cycle's usage
	// is anomalously high vs the account's recent baseline). false when the
	// caller does not compute it; the judge treats false as "no spike", never as
	// "definitely safe".
	UsageSpike bool
}

// Action is what the charge spine must do with this cycle's off-session arrears
// leg, per the judge's decision.
type Action int

const (
	// ActionCharge: proceed with the off-session arrears charge (arrears mode,
	// within limits, no tighten trigger). The caller still applies the PM gate
	// and the spend-ceiling cap downstream.
	ActionCharge Action = iota
	// ActionSkipPrepaid: do NOT off-session-charge this cycle; mark the run
	// skipped_prepaid and RETAIN the usage. Set when the desired mode is prepaid
	// (either already prepaid, or the judge just tightened to it).
	ActionSkipPrepaid
)

// Decision is the judge's verdict: the mode the account SHOULD be in, whether
// that is a change the caller must persist, the action for this cycle's charge,
// and a stable machine-readable reason for the audit trail.
type Decision struct {
	// DesiredMode is the usage_billing_mode the account should be in after this
	// assessment. May equal the input mode (no change) or differ (a transition).
	DesiredMode Mode
	// ModeChanged is true when DesiredMode differs from the account's current
	// mode — the caller must persist it (UpdateAccountCollection).
	ModeChanged bool
	// Action is what to do with this cycle's off-session arrears leg.
	Action Action
	// Reason is a stable code for the transition / decision (audit + tests).
	Reason Reason
}

// Reason is a stable machine-readable code for a Decision, for the audit trail
// and deterministic test assertions.
type Reason string

const (
	// ReasonWithinLimits: arrears mode, accrual within the credit limit, no
	// tighten trigger → charge.
	ReasonWithinLimits Reason = "within_limits"
	// ReasonAlreadyPrepaid: the account is already prepaid; the off-session leg
	// stays skipped (no transition).
	ReasonAlreadyPrepaid Reason = "already_prepaid"
	// ReasonTightenDelinquent: tightened to prepaid because of an unpaid invoice.
	ReasonTightenDelinquent Reason = "tighten_delinquent"
	// ReasonTightenOverLimit: tightened to prepaid because accrued arrears reached
	// or exceeded the credit limit.
	ReasonTightenOverLimit Reason = "tighten_over_limit"
	// ReasonTightenSpike: tightened to prepaid because of a usage spike.
	ReasonTightenSpike Reason = "tighten_spike"
	// ReasonRelaxClean: relaxed from prepaid back to arrears on sustained clean
	// standing (no current tighten trigger + the caller vouched for good history).
	ReasonRelaxClean Reason = "relax_clean"
)

// RiskAssess is the risk-judge. Given the account's persisted collection state
// and this cycle's live signals, it decides the desired mode + the action for
// this cycle's off-session arrears leg.
//
// Tighten precedence (any one flips toward prepaid; checked in a fixed order so
// the Reason is deterministic): delinquency → over credit limit → usage spike.
// Tightening always wins over the current mode (even an arrears account tightens).
//
// Relax is conservative: an account is moved prepaid → arrears ONLY when there is
// no current tighten trigger AND the caller explicitly vouches for sustained good
// standing (cleanStanding). Absent that vouch a prepaid account STAYS prepaid —
// the judge never re-trusts on the mere absence of a fresh failure.
//
// cleanStanding is the caller's "sustained good standing" verdict (e.g.
// invoice.paid with no open delinquency over a window). It is an INPUT, not
// derived here, so the re-trust policy stays explicit and testable.
func RiskAssess(acct Account, sig Signals, cleanStanding bool) Decision {
	// 1. Tighten triggers — any one forces prepaid, regardless of current mode.
	switch {
	case sig.Delinquent:
		return tighten(acct, ReasonTightenDelinquent)
	case overCreditLimit(acct, sig):
		return tighten(acct, ReasonTightenOverLimit)
	case sig.UsageSpike:
		return tighten(acct, ReasonTightenSpike)
	}

	// 2. No tighten trigger. If already prepaid, decide whether to relax.
	if acct.Mode == ModePrepaid {
		if cleanStanding {
			// Conservative re-trust: only on an explicit sustained-good-standing
			// vouch from the caller.
			return Decision{
				DesiredMode: ModeArrears,
				ModeChanged: true,
				Action:      ActionCharge,
				Reason:      ReasonRelaxClean,
			}
		}
		// Stay prepaid: absence of a fresh failure is NOT a reason to re-trust.
		return Decision{
			DesiredMode: ModePrepaid,
			ModeChanged: false,
			Action:      ActionSkipPrepaid,
			Reason:      ReasonAlreadyPrepaid,
		}
	}

	// 3. Arrears, within limits, no trigger → charge.
	return Decision{
		DesiredMode: ModeArrears,
		ModeChanged: false,
		Action:      ActionCharge,
		Reason:      ReasonWithinLimits,
	}
}

// overCreditLimit reports whether this cycle's accrued arrears reach or exceed
// the account's credit limit (the "nearing/over the limit" tighten trigger). A
// zero credit limit means "no off-session arrears headroom" — any positive
// accrual is over the limit (a deliberately prepaid-by-policy account).
func overCreditLimit(acct Account, sig Signals) bool {
	return sig.AccruedArrearsMicros >= acct.CreditLimitMicros && sig.AccruedArrearsMicros > 0
}

// tighten builds the prepaid transition Decision, marking ModeChanged only when
// the account was not already prepaid (so an already-prepaid + delinquent account
// doesn't issue a redundant persist).
func tighten(acct Account, reason Reason) Decision {
	return Decision{
		DesiredMode: ModePrepaid,
		ModeChanged: acct.Mode != ModePrepaid,
		Action:      ActionSkipPrepaid,
		Reason:      reason,
	}
}

// --- large auto-collect disclosure --------------------------------------------

// DefaultAutoCollectThresholdMicros is the platform-default size threshold above
// which a SUCCESSFUL off-session charge is disclosed as "large" on the billing
// page: $100.00. An account with no per-account override (NULL
// auto_collect_threshold_micros, migration 034) uses this. FINANCE-OWNED like
// the other collection thresholds; kept here (a risk/disclosure concept), not in
// the pricing consts.
//
// This is DISCLOSURE ONLY: unlike the spend ceiling (which SKIPS a charge that
// would breach it), the threshold changes NO charging behaviour — it only marks,
// after the fact, that a charge which ALREADY succeeded was large, so the
// customer isn't surprised by the auto-debit.
const DefaultAutoCollectThresholdMicros int64 = 100_000_000 // $100.00

// ResolveAutoCollectThreshold resolves the effective per-account disclosure
// threshold: the account override when set (non-nil), else the platform default.
// A nil override models the migration-031 NULL column ("use the default"). This
// MUST be resolved AT CHARGE TIME so the flag reflects the threshold that applied
// when the charge fired, not one edited afterward.
func ResolveAutoCollectThreshold(overrideMicros *int64) int64 {
	if overrideMicros != nil {
		return *overrideMicros
	}
	return DefaultAutoCollectThresholdMicros
}

// IsLargeAutoCollect reports whether a SUCCESSFUL off-session charge of
// chargedMicros (netted arrears + advance base, pre-cents-conversion) crosses the
// account's disclosure threshold (override when non-nil, else the default) and so
// must be disclosed as "large".
//
// The comparison is strict-greater-than (>): a charge EXACTLY EQUAL to the
// threshold is NOT flagged. "Large" means ABOVE the threshold — a $100 threshold
// discloses charges over $100, not a charge of exactly $100. This matches
// ExceedsSpendCeiling's precise-dollar-cap reading (equal-to is not "above") and
// is intentionally distinct from overCreditLimit's inclusive (>=) trust trigger.
//
// The comparison MUST happen in the SAME unit Stripe actually charges: whole
// cents, round-half-up (cycle.centsFromMicros — 1 cent = 10_000 micros). chargedMicros
// is the raw pre-cents-conversion micros value, so a chargedMicros strictly between
// the threshold and threshold+5,000 micros (half a cent) rounds DOWN to the
// threshold's own cents value when actually charged — comparing raw micros would
// flag it "large" even though the money that hit the card is IDENTICAL to the
// threshold's dollar amount. Rounding BOTH sides to cents before comparing (rather
// than comparing raw micros) makes the flag agree with the real charge by
// construction. This package cannot import cycle.centsFromMicros directly (cycle
// imports collection; that would cycle), so centsRoundHalfUp below duplicates the
// identical round-half-up rule in plain integer arithmetic.
func IsLargeAutoCollect(chargedMicros int64, overrideMicros *int64) bool {
	chargedCents := centsRoundHalfUp(chargedMicros)
	thresholdCents := centsRoundHalfUp(ResolveAutoCollectThreshold(overrideMicros))
	return chargedCents > thresholdCents
}

// microsPerCent is the micro-dollar value of one cent (1e-2 USD = 10_000 ×
// 1e-6 USD) — mirrors cycle.microsPerCent. Duplicated rather than imported: see
// the IsLargeAutoCollect doc on the import-cycle constraint.
const microsPerCent = 10_000

// centsRoundHalfUp converts a non-negative micro-dollar amount to whole cents,
// round-half-up — the SAME conversion Stripe amounts undergo at the charge
// boundary (cycle.centsFromMicros, which does the equivalent computation via
// big.Rat). For non-negative int64 operands and an EVEN divisor (10_000),
// floor((micros + microsPerCent/2) / microsPerCent) is exactly round-half-up
// (ties round up), so plain integer arithmetic reproduces that package's
// rounding without depending on it. Charged amounts and thresholds are
// non-negative at every call site (a successful charge, a finance-set
// threshold); a negative input is defensive-clamped to 0 rather than producing
// a sign-flipped cents value from truncating integer division.
func centsRoundHalfUp(micros int64) int64 {
	if micros <= 0 {
		return 0
	}
	return (micros + microsPerCent/2) / microsPerCent
}

// ExceedsSpendCeiling reports whether the netted arrears would breach the
// account's hard per-cycle bill-shock cap. NoCeiling (HasSpendCeiling=false) →
// never breaches. This is a HARD cap independent of the mode/credit-limit judge:
// the charge spine must NOT auto-charge above the ceiling in a single cycle
// (billing-tiers §3), so a breach skips the off-session charge and retains the
// usage rather than charging a shocking amount.
//
// The comparison is strict-greater-than (>): arrears EXACTLY EQUAL to the ceiling
// are charged, not skipped. This is the spec-correct reading of "never auto-charge
// ABOVE it" — equal-to is not above. Note this is intentionally asymmetric with
// overCreditLimit (>=, where equal triggers a tighten): the ceiling is a precise
// dollar cap the customer sets ("charge me at most $100" → $100 is allowed),
// whereas the credit limit is a trust threshold where reaching it is the trigger.
func ExceedsSpendCeiling(acct Account, arrearsMicros int64) bool {
	if !acct.HasSpendCeiling {
		return false
	}
	return arrearsMicros > acct.SpendCeilingMicros
}

// --- trust-ramped credit limit ------------------------------------------------

// Trust-ramp consts. CONSERVATIVE placeholders — the exact ramp is FINANCE-OWNED
// (billing-tiers §4: "Finance-owned later: bad-debt reserve %, credit-limit ramp
// + anomaly thresholds"). Keep newAccountFloorMicros in sync with migration
// 016's accounts.credit_limit_micros DEFAULT.
//
// TODO(finance): replace these with the signed-off ramp curve + anomaly
// thresholds. Until then the floor is low and growth is slow, so the platform
// errs toward under-extending credit (the safe direction).
const (
	// newAccountFloorMicros is the credit limit for a brand-new account with no
	// payment history and no verified PM: $25.00. Matches migration 016's
	// accounts.credit_limit_micros DEFAULT.
	newAccountFloorMicros int64 = 25_000_000

	// verifiedPMBonusMicros is added once the account has a verified payment
	// method (SCA-capable card on file): +$75.00 → a $100 base for a carded
	// account.
	verifiedPMBonusMicros int64 = 75_000_000

	// perSuccessfulInvoiceMicros grows the limit per successfully-paid invoice in
	// the account's history: +$50.00 each. Slow, monotonic trust accrual.
	perSuccessfulInvoiceMicros int64 = 50_000_000

	// perTenureMonthMicros grows the limit per full 30-day month of account
	// tenure: +$10.00/month. Rewards longevity independent of volume.
	perTenureMonthMicros int64 = 10_000_000

	// tenureDaysPerMonth is the tenure quantum (a "month" for the ramp).
	tenureDaysPerMonth int = 30

	// maxCreditLimitMicros caps the ramped limit so no account auto-earns
	// unbounded off-session exposure: $5,000.00. Larger limits are a manual /
	// finance decision (billing-tiers §4 MAX+ bridge), never auto-ramped.
	maxCreditLimitMicros int64 = 5_000_000_000
)

// TrustRampedCreditLimit computes a conservative, monotonic off-session credit
// limit (in micros) from the account's standing:
//
//	limit = floor
//	      + (verified PM ? verifiedPMBonus : 0)
//	      + successfulInvoices × perSuccessfulInvoice
//	      + (tenureDays / 30)  × perTenureMonth
//	capped at maxCreditLimit.
//
// It is monotonic in every input (more history / longer tenure / a verified PM
// never LOWERS the limit) and floors at the conservative new-account value, so a
// brand-new / no-PM / short-tenure account gets the low floor. Negative inputs
// are clamped to zero (defensive; callers pass non-negative counts). All money is
// integer micros; the accumulation saturates at maxCreditLimitMicros at every
// step so a pathologically large input count can never overflow int64 (it just
// pins to the cap) — the result is always in [floor, maxCreditLimit].
//
// TODO(#9-followup): WIRE A CALLER. v1 has none — the charge cycle's mode tighten
// carries the EXISTING credit_limit through unchanged, and a brand-new account
// gets the floor from migration 016's column DEFAULT. This ramp must be invoked
// by a tenure/history-driven recompute (a periodic job, or on the verified-PM
// attach + invoice.paid signals) that writes the result back via
// UpdateAccountCollection. Until then the limit never GROWS — which errs toward
// under-extending credit (the safe direction), but caps accounts at the floor.
func TrustRampedCreditLimit(successfulInvoices, tenureDays int, hasVerifiedPM bool) int64 {
	if successfulInvoices < 0 {
		successfulInvoices = 0
	}
	if tenureDays < 0 {
		tenureDays = 0
	}

	limit := newAccountFloorMicros
	if hasVerifiedPM {
		limit = addCapped(limit, verifiedPMBonusMicros)
	}
	limit = addCapped(limit, mulCapped(int64(successfulInvoices), perSuccessfulInvoiceMicros))
	limit = addCapped(limit, mulCapped(int64(tenureDays/tenureDaysPerMonth), perTenureMonthMicros))
	return limit
}

// addCapped returns min(a+b, maxCreditLimitMicros), treating b as non-negative.
// Saturating add: a and b are both bounded contributions, so a+b is computed
// only when it cannot exceed the cap; otherwise the cap is returned directly. No
// int64 overflow because a is always ≤ maxCreditLimitMicros on entry.
func addCapped(a, b int64) int64 {
	if b >= maxCreditLimitMicros-a {
		return maxCreditLimitMicros
	}
	return a + b
}

// mulCapped returns min(n×unit, maxCreditLimitMicros) for non-negative n, unit.
// Saturates instead of overflowing: once n exceeds the cap/unit ratio the product
// is pinned to the cap, so a huge n never wraps int64.
func mulCapped(n, unit int64) int64 {
	if n <= 0 || unit <= 0 {
		return 0
	}
	if n > maxCreditLimitMicros/unit {
		return maxCreditLimitMicros
	}
	return n * unit
}
