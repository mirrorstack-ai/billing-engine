package usage

import "time"

const microsPerCent = 10_000

// This file is the SINGLE home of the base-fee + overage display math (owner
// spec 2026-07-05, DESIGN.md "Base fee — v2: creation grace + per-module overage
// timers"). Both consumers — the display read (GetAppBill / GetAccountBill, this
// package) and the charge spine (cycle: the creation/combined charge, the
// boundary advance leg, the per-module grace sweep) — compute the per-app FLAT
// base and the account overage through these functions, so the bill page, the
// invoice, and the mirror can never disagree by construction. All money is
// integer micro-dollars; the arithmetic here is pure int64 (no big.Rat needed:
// the operands are bounded — see ProratedBaseMicros).
//
// The flat $20/app base is per-app; the $3/module surcharge applies to the
// account's over-count, max(0, live module count − IncludedModules). Under the
// per-module-instance model (migration 033) the charge legs tier per install
// TIMER (each on its own grace), while the DISPLAY reads the live timer count
// through AccountOverageMicros. There is deliberately NO per-app overage helper:
// an app's base is just the flat (plan-resolved) fee.

// AccountOverageMicros is the account's module overage shown for one period:
//
//	ModuleOverageFeeMicros × max(0, liveModuleCount − IncludedModules)
//
// liveModuleCount is the account's live installed-module count (the count of live
// install timers, migration 033 — one pool of IncludedModules for the WHOLE
// account, not per app). The first IncludedModules live installs (by FIFO) are
// "included"; the rest are "over", so max(0, live − included) is exactly the live
// over-count. A liveModuleCount ≤ IncludedModules yields 0 (the max(0, …) clamp
// makes the function total; a negative count cannot occur — a live-row count).
func AccountOverageMicros(liveModuleCount int) int64 {
	if extra := liveModuleCount - IncludedModules; extra > 0 {
		return ModuleOverageFeeMicros * int64(extra)
	}
	return 0
}

// GraceExpiry is the single home of the "grace elapses at t + GraceDays" rule
// (creation grace AND per-module install timers use the same window).
func GraceExpiry(t time.Time) time.Time {
	return t.Add(GraceDays * 24 * time.Hour)
}

// CreationChargeBaseMicros is the EXACT base amount the creation-proration
// sweep (cycle.ChargeCreationProration) charges an app created at createdAt
// whose anchored creation window is [periodStart, periodEnd): the
// creation-period proration, plus the straddled period's FULL base when the
// creation grace elapses at/after periodEnd (coverage contract H2 — the
// boundary advance leg excludes an in-grace app, so this charge owns that
// period). The preview (ListNewCreationCharges pending rows) and the charge
// callback both price through THIS function, so they agree to the micro by
// construction.
func CreationChargeBaseMicros(createdAt, periodStart, periodEnd time.Time) int64 {
	m := ProratedBaseMicros(BaseFeeMicros, createdAt, periodStart, periodEnd)
	if !GraceExpiry(createdAt.UTC()).Before(periodEnd) {
		m += BaseFeeMicros
	}
	return m
}

// CreationChargeOverageMicros is the per-co-created-over-module surcharge the
// Stripe creation-proration rail bills on the combined creation invoice for ONE
// timer. The wallet rail draws the base only and leaves this surcharge to Leg 1.
// The amount is ModuleOverageFeeMicros prorated to the creation window (the same
// shape as CreationChargeBaseMicros), plus the full fee for the straddled period
// when the creation grace crosses the period boundary — then ROUNDED TO WHOLE
// CENTS (the Stripe boundary the sweep charges at). Returned back in micros
// (cents × microsPerCent) so a multi-timer projection is per-timer-cents × count,
// never round(micros × count) — matching the Stripe sweep's
// overageCents × len(overTimers) to the micro.
func CreationChargeOverageMicros(createdAt, periodStart, periodEnd time.Time) int64 {
	m := ProratedBaseMicros(ModuleOverageFeeMicros, createdAt, periodStart, periodEnd)
	if !GraceExpiry(createdAt.UTC()).Before(periodEnd) {
		m += ModuleOverageFeeMicros
	}
	// Round-half-up micros→cents. m is a non-negative surcharge, so integer
	// half-up is exact and equals cycle.centsFromMicros' big.Rat rounding.
	cents := (m + microsPerCent/2) / microsPerCent
	return cents * microsPerCent
}

// ProratedBaseMicros prorates an app's per-period base fee for the period
// [periodStart, periodEnd) given the app's creation instant:
//
//   - created on/before periodStart → the FULL base (the app existed for the
//     whole period);
//   - created inside the period     → base × remain_days / period_days,
//     integer micros ROUND-HALF-UP ((a×b + d/2) / d — the owner-specified
//     formula), where remain_days = whole UTC days in [creation_date,
//     periodEnd) with the creation DAY inclusive (create on the 1st with the
//     period ending on the 4th → days 1–3 → 3 days), and period_days = whole
//     UTC days in [periodStart, periodEnd);
//   - created on/after periodEnd    → 0 (the app did not exist in the period;
//     only reachable from the display read on a historical period — the
//     charge legs always bill the window containing the creation).
//
// Period boundaries are midnight-UTC anchored (billingperiod), so the
// day counts are exact divisions; createdAt is truncated to its UTC date
// (creation-day inclusive). Overflow: base is bounded by the module_count
// INT column (≤ ~2^31 × $3 ≈ 6.4e15 micros) and day counts by ~31, so
// base × remain_days stays far inside int64 — plain integer math is exact.
func ProratedBaseMicros(baseMicros int64, createdAt, periodStart, periodEnd time.Time) int64 {
	coverageStart := ProrationCoverageStart(createdAt, periodStart)
	if coverageStart.Equal(periodStart) {
		return baseMicros // existed for the whole period → full base
	}
	if !coverageStart.Before(periodEnd) {
		return 0 // did not exist in the period
	}
	periodDays := wholeDaysUTC(periodStart, periodEnd)
	remainDays := wholeDaysUTC(coverageStart, periodEnd)
	if periodDays <= 0 {
		return baseMicros // defensive: a malformed window never zero-divides
	}
	return (baseMicros*remainDays + periodDays/2) / periodDays
}

// ProrationCoverageStart is the UTC day the creation proration starts
// covering: created_at truncated to its UTC date (creation day inclusive),
// clamped to periodStart so a backdated created_at never widens the window.
// ProratedBaseMicros derives remain_days from this SAME instant and the
// proration invoice mirrors it as the partial window's period_start — one
// home for the rule, so the amount billed and the displayed coverage window
// can never disagree.
func ProrationCoverageStart(createdAt, periodStart time.Time) time.Time {
	c := createdAt.UTC()
	day := time.Date(c.Year(), c.Month(), c.Day(), 0, 0, 0, 0, time.UTC)
	if day.Before(periodStart) {
		return periodStart
	}
	return day
}

// wholeDaysUTC counts the whole UTC days in [from, to). Both inputs are
// midnight-UTC instants (anchored period boundaries / a truncated creation
// date), so the division is exact — UTC has no DST to break the 24h day.
func wholeDaysUTC(from, to time.Time) int64 {
	return int64(to.Sub(from) / (24 * time.Hour))
}
