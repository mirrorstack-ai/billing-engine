package usage

import "time"

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

// creationChargeMicros is the shared per-fee creation-charge shape both the base
// leg and each co-created over-module line of the creation-proration sweep bill:
// feeMicros prorated to the creation window [periodStart, periodEnd), plus the
// straddled period's FULL fee when the creation grace elapses at/after periodEnd
// (coverage contract H2 — the boundary advance leg excludes an in-grace app, so
// this charge owns that period). CreationChargeBaseMicros passes BaseFeeMicros;
// CreationChargeAddonMicros passes ModuleOverageFeeMicros per over-module — the
// exact ProratedBaseMicros(fee,…)+straddle top-up the charge callback computes
// for each leg (proration.go), so preview and charge agree to the micro.
func creationChargeMicros(feeMicros int64, createdAt, periodStart, periodEnd time.Time) int64 {
	m := ProratedBaseMicros(feeMicros, createdAt, periodStart, periodEnd)
	if !GraceExpiry(createdAt.UTC()).Before(periodEnd) {
		m += feeMicros
	}
	return m
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
	return creationChargeMicros(BaseFeeMicros, createdAt, periodStart, periodEnd)
}

// CreationChargeAddonMicros is the co-created over-module overage the SAME
// combined creation invoice bills alongside the base (proration.go scenario 3):
// each module co-created with the app (install date == created_at) that is
// "over" the account's IncludedModules allowance rides its own $3 line at the
// IDENTICAL coverage window as the base — ProratedBaseMicros(ModuleOverageFeeMicros,
// createdAt, window) plus the straddle top-up, times the over-count. addonCount
// is the app's frozen add-on module count (addonModuleCount(created_module_count));
// like the base preview, this is the projection the sweep will charge under the
// frozen count — an over-module uninstalled mid-grace drops its timer and shrinks
// the actual charge (D1e), the same caveat the displayed count already carries.
func CreationChargeAddonMicros(createdAt, periodStart, periodEnd time.Time, addonCount int) int64 {
	if addonCount <= 0 {
		return 0
	}
	return creationChargeMicros(ModuleOverageFeeMicros, createdAt, periodStart, periodEnd) * int64(addonCount)
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
