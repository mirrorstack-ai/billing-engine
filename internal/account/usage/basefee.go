package usage

import "time"

// This file is the SINGLE home of the base-fee + pooled-overage math (owner
// spec 2026-07-05, DESIGN.md "Base fee — v1 spec"; account-wide overage
// reversal, migration 032). Both consumers — the display read (GetAppBill /
// GetAccountBill, this package) and the charge spine (cycle: the RegisterApp
// creation-proration charge, the boundary advance leg, the mid-period grace
// sweep) — compute the per-app FLAT base and the account-wide pooled overage
// through these functions, so the bill page, the invoice, and the mirror can
// never disagree by construction. All money is integer micro-dollars; the
// arithmetic here is pure int64 (no big.Rat needed: the operands are bounded —
// see ProratedBaseMicros).
//
// Overage moved from PER-APP to ACCOUNT-WIDE POOLED (migration 032): the flat
// $20/app base is per-app and unchanged, but the $3/module surcharge now
// applies ONCE per account to max(0, Σ live-app module_count − IncludedModules)
// — see AccountOverageMicros. There is deliberately NO per-app overage helper
// anymore: an app's base is just the flat (plan-resolved) fee.

// AccountOverageMicros is the account-wide POOLED module overage for one
// period:
//
//	ModuleOverageFeeMicros × max(0, pooledModuleCount − IncludedModules)
//
// pooledModuleCount is SUM(module_count) over the account's LIVE apps (one pool
// of IncludedModules for the WHOLE account, not per app). A pooledModuleCount
// ≤ IncludedModules yields 0 (the max(0, …) clamp makes the function total;
// a negative count cannot occur — the sum of non-negative DB-CHECKed counts).
func AccountOverageMicros(pooledModuleCount int) int64 {
	if extra := pooledModuleCount - IncludedModules; extra > 0 {
		return ModuleOverageFeeMicros * int64(extra)
	}
	return 0
}

// ProratedOverageMicros prorates an account-wide pooled overage amount for the
// period [periodStart, periodEnd), covering [grace-end day, periodEnd): the
// SAME day-count round-half-up math as ProratedBaseMicros (the overage amount
// is prorated exactly like a base amount), but ANCHORED on the grace-end
// instant instead of an app's creation instant. graceEnd on/before periodStart
// → the FULL overage (the account was over for the whole period); graceEnd
// on/after periodEnd → 0 (grace ends after this period — nothing to charge
// yet). Kept as a named wrapper so the semantic ("prorate FROM grace-end") is
// legible at the mid-period grace sweep's call site.
func ProratedOverageMicros(overageMicros int64, graceEnd, periodStart, periodEnd time.Time) int64 {
	return ProratedBaseMicros(overageMicros, graceEnd, periodStart, periodEnd)
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
