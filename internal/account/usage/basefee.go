package usage

import (
	"bytes"
	"sort"
	"time"

	"github.com/google/uuid"
)

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
// The flat $20/app base is per-app; the $5-per-block-of-5 surcharge applies to the
// account's over-count, max(0, live module count − IncludedModules). Under the
// per-module-instance model (migration 033) the charge legs tier per install
// TIMER (each on its own grace), while the DISPLAY reads the live timer count
// through AccountOverageMicros. There is deliberately NO per-app overage helper:
// an app's base is just the flat (plan-resolved) fee.

// ModuleOverageBlocks is the number of WHOLE overage blocks an over-count
// occupies: ceil(overCount / ModuleBlockSize), clamped to 0. Overage is sold in
// blocks, so an over-count of 1 and of ModuleBlockSize both cost one block —
// that free headroom inside a block is the point (see ModuleBlockFeeMicros).
// Total on non-positive input, so callers never pre-clamp.
func ModuleOverageBlocks(overCount int64) int64 {
	if overCount <= 0 {
		return 0
	}
	return (overCount + ModuleBlockSize - 1) / ModuleBlockSize
}

// ModuleBlockMicros prices an over-count in whole blocks —
// ModuleBlockFeeMicros × ModuleOverageBlocks(overCount). This is the ONE home of
// the RECURRING overage price: both the displayed bill line
// (AccountOverageMicros) and the boundary advance leg (cycle.charge) go through
// it, so the page and the invoice cannot disagree. Overflow is not reachable —
// overCount is a live-row count, bounded by the timer table.
func ModuleBlockMicros(overCount int64) int64 {
	return ModuleOverageBlocks(overCount) * ModuleBlockFeeMicros
}

// AccountOverageMicros is the account's module overage shown for one period:
//
//	ModuleBlockFeeMicros × ceil(max(0, liveModuleCount − IncludedModules) / ModuleBlockSize)
//
// liveModuleCount is the account's live installed-module count (the count of live
// install timers, migration 033 — one pool of IncludedModules for the WHOLE
// account, not per app). The first IncludedModules live installs (by FIFO) are
// "included"; the rest are "over", so max(0, live − included) is exactly the live
// over-count, which ModuleBlockMicros then rounds UP to whole blocks. A
// liveModuleCount ≤ IncludedModules yields 0 (ModuleOverageBlocks is total on
// non-positive input; a negative count cannot occur — a live-row count).
// GetAccountBill uses this current-live steady-state amount as the next-period
// recurring overage forecast inside ProjectedTotalMicros; unresolved one-time
// timer proration is layered on separately and any exact next-period straddle
// overlap is counted once — at the per-module amortized rate, which is exactly
// one module's marginal share of a full block.
func AccountOverageMicros(liveModuleCount int) int64 {
	return ModuleBlockMicros(int64(liveModuleCount) - IncludedModules)
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

// DomainCreationChargeMicros is the exact whole-cent custom-domain activation
// line minted by cycle.ChargeDomain, returned in micros for the billing wire.
// Domain activations have no grace or straddle top-up: only the remainder of
// the activation-containing period is charged, then the domain joins recurring
// base after that settlement succeeds.
func DomainCreationChargeMicros(activatedAt, periodStart, periodEnd time.Time) int64 {
	m := ProratedBaseMicros(DomainFeeMicros, activatedAt, periodStart, periodEnd)
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
// INT column (≤ ~2^31 × $5 ≈ 1.1e16 micros) and day counts by ~31, so
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

// projectedBaseFeeByApp allocates the account's NEXT-PERIOD recurring base to
// the apps that own it, keyed by app id. Σ of the returned values equals the
// account line GetAccountBill publishes:
//
//	Σ = activatedApps × baseFeeMicros
//	  + ModuleBlockMicros(Σ overCounts)
//	  + Σ domainCounts × DomainFeeMicros
//
// exactly, for any share set — that identity is the whole point, and
// TestProjectedBaseFeeByAppSumsToAccountTotal pins it.
//
// Two of the three terms are already per-app: an activated app owns its flat
// base, and a custom domain owns its $2. The OVERAGE is not. Overage is sold in
// whole blocks against an ACCOUNT-WIDE allowance (migration 033), so the account
// pays ceil(over/ModuleBlockSize) blocks no matter how the over-modules are
// spread — two apps with two over-modules each cost ONE block between them, and
// recomputing the block price per app would bill two. The block total is
// therefore DISTRIBUTED, never recomputed: each app takes the share of the
// account's block money that its over-count earns.
//
// The split is by largest remainder, so the parts sum to the whole with no
// rounding drift, and ties break on app id — a stable, arbitrary-but-repeatable
// order beats a map-iteration one that would move money between apps run to run.
// An account with all its over-modules on one app (the common shape) gives that
// app every block, which is the intuitive answer and falls out of the general
// rule rather than being special-cased.
func projectedBaseFeeByApp(shares []AppRecurringFeeShare, baseFeeMicros int64) map[uuid.UUID]int64 {
	if len(shares) == 0 {
		return nil
	}
	byApp := make(map[uuid.UUID]int64, len(shares))
	var totalOver int64
	for _, share := range shares {
		var micros int64
		if share.Activated {
			micros += baseFeeMicros
		}
		micros += int64(share.CustomDomainCount) * DomainFeeMicros
		byApp[share.AppID] = micros
		totalOver += int64(share.OverModuleCount)
	}
	if totalOver == 0 {
		return byApp
	}

	// Largest remainder over the account's whole-block money. floor + the top
	// remainders exhausts blockTotal precisely; nothing is created or lost.
	blockTotal := ModuleBlockMicros(totalOver)
	type portion struct {
		appID     uuid.UUID
		remainder int64
	}
	portions := make([]portion, 0, len(shares))
	allocated := int64(0)
	for _, share := range shares {
		over := int64(share.OverModuleCount)
		if over == 0 {
			continue
		}
		exact := blockTotal * over
		byApp[share.AppID] += exact / totalOver
		allocated += exact / totalOver
		portions = append(portions, portion{appID: share.AppID, remainder: exact % totalOver})
	}
	sort.Slice(portions, func(i, j int) bool {
		if portions[i].remainder != portions[j].remainder {
			return portions[i].remainder > portions[j].remainder
		}
		return bytes.Compare(portions[i].appID[:], portions[j].appID[:]) < 0
	})
	for i := int64(0); i < blockTotal-allocated; i++ {
		byApp[portions[i%int64(len(portions))].appID]++
	}
	return byApp
}
