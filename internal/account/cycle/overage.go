package cycle

// Account-wide POOLED module overage (migration 030, owner spec 2026-07-05,
// confirmed reversal of the per-app overage tier). Overage moved from PER-APP
// to a single ACCOUNT-WIDE POOL of IncludedModules: overage = $3 × max(0,
// Σ live-app module_count − IncludedModules), charged ONCE per account per
// period. This file owns:
//
//   - recomputeAccountOverage: after any module_count write (RegisterApp /
//     SyncAppModules) it re-derives the pool and arms/clears the account's ONE
//     grace timer (accounts.overage_since);
//   - the mid-period GRACE SWEEP (ChargeAccountOverage + AccountsInOverageGrace):
//     when the pool has been over for the full grace window, it charges the
//     pooled overage prorated from grace-end to the period end — a DELIBERATE
//     mid-period charge (the D1b "no mid-period charges" rule is now stale for
//     the OVERAGE leg specifically; base-fee mid-period behavior is unchanged).
//
// The boundary advance leg's pooled-overage term lives in charge.go alongside
// the base leg. Both legs guard against double-charging the same period through
// the account_overage_snapshots ledger (keyed (account_id, period_start)) plus
// the deterministic per-(account, period) Stripe Idempotency-Keys below — the
// same money-safety pattern app_base_snapshots gives the per-app base.
//
// CRASH-SAFETY (PR #47 review — the cross-leg double-charge finding): the
// ledger row is written as a claim BEFORE either leg calls Stripe
// (status='pending'; see migration 030), not only after Stripe succeeds. A
// crash between "Stripe succeeded" and "the row committed" used to leave NO
// row for the OTHER leg to see, so it would independently charge the SAME
// period's overage under a completely disjoint Idempotency-Key namespace — a
// real double charge. Now the claim is visible to both legs the instant it is
// written, before any money moves, so a crash anywhere in the sequence leaves
// evidence the other leg (and the same leg's own retry) can see and must
// respect: ANY row for the period — pending or charged — means "claimed,
// never independently charge it".

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// overageGraceWindow is the ONE grace timer per account: when the pooled module
// count first crosses IncludedModules the timer starts (accounts.overage_since),
// and only after this window elapses does the mid-period sweep charge the
// overage. If the pool drops back to ≤ IncludedModules before it elapses the
// timer is cleared and nothing is charged. Owner spec 2026-07-05: 3 days.
const overageGraceWindow = 3 * 24 * time.Hour

// OverageChargeStatus is the terminal classification of a ChargeAccountOverage
// attempt, for the cron sweep's tally + logging.
type OverageChargeStatus string

const (
	// OverageCharged: the pooled overage was invoiced for the period.
	OverageCharged OverageChargeStatus = "charged"
	// OverageSkippedAlreadyCharged: this period's pooled overage already has an
	// account_overage_snapshots row (a prior sweep or the boundary billed it).
	OverageSkippedAlreadyCharged OverageChargeStatus = "already_charged"
	// OverageSkippedUnderPool: the pool dropped back to ≤ IncludedModules by the
	// time the sweep ran — nothing to charge; the timer is cleared.
	OverageSkippedUnderPool OverageChargeStatus = "under_pool"
	// OverageSkippedGraceHolding: the grace window has not elapsed yet (defensive
	// — the work-list query already excludes these).
	OverageSkippedGraceHolding OverageChargeStatus = "grace_holding"
	// OverageSkippedZeroCents: the prorated overage rounded to 0 cents (grace-end
	// lands at/after the period end, or a sub-cent remainder) — nothing to
	// invoice this period; a later period picks it up.
	OverageSkippedZeroCents OverageChargeStatus = "zero_cents"
	// OverageSkippedNoPM: no usable default payment method — retained, re-attempted
	// on the next sweep (the SAME per-(account, period) Stripe idem key stays
	// stable), never a failure.
	OverageSkippedNoPM OverageChargeStatus = "skipped_no_pm"
	// OverageToppedUp: an ALREADY-charged period's pool grew further while the
	// period was still open, and the sweep charged the incremental delta
	// (finding #3, a judgment call — see topUpGraceOverage's doc).
	OverageToppedUp OverageChargeStatus = "topped_up"
)

// OverageChargeSummary reports what one ChargeAccountOverage call did.
type OverageChargeSummary struct {
	Status       OverageChargeStatus
	PeriodStart  time.Time
	OverCount    int
	ChargedCents int64
	// StripeInvoiceID is set only when Status == OverageCharged.
	StripeInvoiceID string
}

// AccountsInOverageGrace returns the accounts whose grace timer has EXPIRED as
// of `at` (overage_since <= at − overageGraceWindow) and are chargeable — the
// mid-period sweep's work list. A thin pass-through to the store.
func (s *Service) AccountsInOverageGrace(ctx context.Context, at time.Time) ([]OverageGraceCandidate, error) {
	cands, err := s.store.AccountsInOverageGrace(ctx, at.Add(-overageGraceWindow))
	if err != nil {
		return nil, billing.Internal("list overage-grace accounts failed", err)
	}
	return cands, nil
}

// recomputeAccountOverage re-derives the account's pooled module count after a
// module_count write and arms/clears its ONE grace timer accordingly: pool >
// IncludedModules and not yet armed → stamp overage_since (StartAccountOverage
// is first-crossing-wins); pool ≤ IncludedModules → clear it (ClearAccountOverage
// is idempotent). No refund on the clear (D1e) — it only stops FUTURE accrual;
// overage already charged this period stays billed via its snapshot row. Called
// by RegisterApp (after the initial insert) and SyncAppModules (after any count
// / delete write) so the timer always reflects the live pool.
func (s *Service) recomputeAccountOverage(ctx context.Context, accountID uuid.UUID) error {
	pooled, err := s.store.PooledModuleCount(ctx, accountID)
	if err != nil {
		return billing.Internal("pooled module count lookup failed", err)
	}
	if pooled > usage.IncludedModules {
		if err := s.store.StartAccountOverage(ctx, accountID, s.nowFn().UTC()); err != nil {
			return billing.Internal("start account overage timer failed", err)
		}
		return nil
	}
	if err := s.store.ClearAccountOverage(ctx, accountID); err != nil {
		return billing.Internal("clear account overage timer failed", err)
	}
	return nil
}

// ChargeAccountOverage is the mid-period grace charge for ONE account whose
// pooled overage has survived the grace window. It:
//
//  1. resolves the account's CURRENT anchored period (from activated_at) and the
//     grace-end instant (overage_since + overageGraceWindow);
//  2. reads this period's account_overage_snapshots row, if any (the
//     double-charge guard):
//     - source='advance' (the boundary claimed it, pending or charged) →
//     skip, never independently charge it;
//     - source='grace', status='charged' → already settled; the only further
//     work is a possible TOP-UP if the pool grew further within this SAME
//     still-open period (topUpGraceOverage, finding #3);
//     - source='grace', status='pending' → THIS leg's own attempt died
//     between claiming the period and Stripe confirming (or before Stripe
//     was ever called) — resume it, reusing the FROZEN over_count /
//     charged_micros so the retry recomputes the IDENTICAL amount and the
//     deterministic Idempotency-Key stays valid;
//  3. otherwise (no row): reads the CURRENT pool — ≤ IncludedModules clears the
//     timer and skips (no refund of anything already charged, D1e); prices the
//     pooled overage PRORATED from grace-end to the period end
//     (ProratedOverageMicros) → whole cents; a 0-cent result skips (grace ends
//     at/after the period end);
//  4. CLAIMS the period (a 'pending' account_overage_snapshots row) BEFORE
//     calling Stripe — the crash-safe marker described in this file's header —
//     then charges via the SAME Stripe plumbing as the other legs with the
//     deterministic per-(account, period) Idempotency-Keys, mirrors the
//     invoice, and flips the row to 'charged' with the genuine Stripe item id.
//
// Gated on a usable default PM exactly like the spine (the candidate is already
// activated). A failure after the claim leaves the row 'pending'; the next
// sweep resumes through the SAME idem key (Stripe dedupes) — retry-safe, never
// a double charge.
func (s *Service) ChargeAccountOverage(ctx context.Context, cand OverageGraceCandidate, at time.Time) (*OverageChargeSummary, error) {
	if cand.ID == uuid.Nil {
		return nil, billing.InvalidInput("account_id required")
	}
	if s.stripe == nil {
		return nil, billing.Internal("ChargeAccountOverage requires a Stripe client", nil)
	}

	graceEnd := cand.OverageSince.Add(overageGraceWindow)
	if at.UTC().Before(graceEnd) {
		// Defensive: the work-list query already excludes still-in-grace accounts.
		return &OverageChargeSummary{Status: OverageSkippedGraceHolding}, nil
	}

	anchorDay := billingperiod.AnchorDay(cand.ActivatedAt)
	periodStart, periodEnd := billingperiod.AnchoredPeriodWindow(at.UTC(), anchorDay)

	existing, found, err := s.store.AccountOverageSnapshot(ctx, cand.ID, periodStart)
	if err != nil {
		return nil, billing.Internal("account overage snapshot lookup failed", err)
	}
	if found {
		if existing.Source == "advance" {
			// The boundary claimed (pending or charged) this period's overage —
			// the mid-period sweep must NEVER independently charge it, crash
			// window or not (the cross-leg double-charge guard, PR #47 review).
			return &OverageChargeSummary{Status: OverageSkippedAlreadyCharged, PeriodStart: periodStart, OverCount: existing.OverCount}, nil
		}
		if existing.Status == OverageSnapshotCharged {
			return s.topUpGraceOverage(ctx, cand, existing, at, periodStart, periodEnd)
		}
		// existing.Status == pending, existing.Source == "grace": resume OUR
		// own crashed attempt, reusing the frozen amount.
		return s.completeGraceCharge(ctx, cand, existing.OverCount, existing.ChargedMicros, graceEnd, periodStart, periodEnd, true /* alreadyClaimed */)
	}

	pooled, err := s.store.PooledModuleCount(ctx, cand.ID)
	if err != nil {
		return nil, billing.Internal("pooled module count lookup failed", err)
	}
	overCount := pooled - usage.IncludedModules
	if overCount <= 0 {
		// Dropped back under the pool since the timer was read — clear the stale
		// timer and charge nothing (no refund of anything already billed, D1e).
		if err := s.store.ClearAccountOverage(ctx, cand.ID); err != nil {
			return nil, billing.Internal("clear account overage timer failed", err)
		}
		return &OverageChargeSummary{Status: OverageSkippedUnderPool, PeriodStart: periodStart}, nil
	}

	// Prorate the pooled overage from grace-end to the period end. A 0-cent
	// result (grace ends at/after this period's end) means nothing to bill this
	// period — a later period picks it up; leave the timer armed, no snapshot.
	proratedMicros := usage.ProratedOverageMicros(usage.AccountOverageMicros(pooled), graceEnd, periodStart, periodEnd)
	cents, err := centsFromMicros(proratedMicros)
	if err != nil {
		return nil, billing.Internal("micros to cents conversion failed", err)
	}
	if cents == 0 {
		return &OverageChargeSummary{Status: OverageSkippedZeroCents, PeriodStart: periodStart, OverCount: overCount}, nil
	}

	return s.completeGraceCharge(ctx, cand, overCount, proratedMicros, graceEnd, periodStart, periodEnd, false /* alreadyClaimed */)
}

// completeGraceCharge runs the PM/customer gate + the actual Stripe charge for
// the mid-period grace leg, given an already-resolved (overCount,
// chargedMicros) — either freshly computed (alreadyClaimed=false: this call
// still needs to CLAIM the period before Stripe) or reused from an existing
// 'pending' row (alreadyClaimed=true: THIS leg already claimed it on a prior
// attempt; resume straight to the PM/Stripe steps with the SAME frozen amount
// so the deterministic Idempotency-Key never sees a different total).
func (s *Service) completeGraceCharge(ctx context.Context, cand OverageGraceCandidate, overCount int, chargedMicros int64, graceEnd, periodStart, periodEnd time.Time, alreadyClaimed bool) (*OverageChargeSummary, error) {
	summary := &OverageChargeSummary{PeriodStart: periodStart, OverCount: overCount}
	cents, err := centsFromMicros(chargedMicros)
	if err != nil {
		return nil, billing.Internal("micros to cents conversion failed", err)
	}
	summary.ChargedCents = cents

	hasPM, err := s.store.HasUsableDefaultPM(ctx, cand.ID)
	if err != nil {
		return nil, billing.Internal("usable PM check failed", err)
	}
	if !hasPM {
		summary.Status = OverageSkippedNoPM
		return summary, nil // retained; re-attempted next sweep through the same idem key
	}
	custID, err := s.store.AccountStripeCustomer(ctx, cand.ID)
	if err != nil {
		return nil, billing.Internal("stripe customer lookup failed", err)
	}
	if custID == "" {
		return nil, billing.Internal("account has a usable PM but no Stripe customer id", nil)
	}

	if !alreadyClaimed {
		// CLAIM the period BEFORE calling Stripe (the crash-safe marker — see
		// this file's header). inserted=false means we lost a race to a
		// concurrent claim (another sweep invocation, or the boundary) — defer
		// to the winner instead of charging under our own stale claim.
		inserted, err := s.store.InsertAccountOverageSnapshot(ctx, AccountOverageSnapshot{
			AccountID:     cand.ID,
			PeriodStart:   periodStart,
			PeriodEnd:     periodEnd,
			OverCount:     overCount,
			ChargedMicros: chargedMicros,
			Source:        "grace",
			Status:        OverageSnapshotPending,
		})
		if err != nil {
			return nil, billing.Internal("account overage snapshot claim failed", err)
		}
		if !inserted {
			return s.resumeClaimedOverage(ctx, cand, graceEnd, periodStart, periodEnd)
		}
	}

	desc := fmt.Sprintf("MirrorStack module overage (account pool, %d over) — account %s", overCount, cand.ID)
	item, err := s.stripe.CreateInvoiceItem(ctx, custID, cents, chargeCurrency, desc, accountOverageItemIdemKey(cand.ID, periodStart))
	if err != nil {
		return nil, billing.StripeError("overage invoice item failed", err)
	}
	inv, err := s.stripe.CreateInvoice(ctx, custID, true /* autoAdvance */, accountOverageInvoiceIdemKey(cand.ID, periodStart))
	if err != nil {
		return nil, billing.StripeError("overage invoice failed", err)
	}

	if err := s.store.UpsertInvoice(ctx, InvoiceMirror{
		AccountID:       cand.ID,
		StripeInvoiceID: inv.ID,
		Status:          inv.Status,
		AmountDueCents:  inv.AmountDue,
		AmountPaidCents: inv.AmountPaid,
		Currency:        chargeCurrency,
		// The mirrored window is the PARTIAL coverage [grace-end day, period end),
		// the SAME instant ProratedOverageMicros priced, so the shown window and
		// the charged amount agree by construction.
		PeriodStart: usage.ProrationCoverageStart(graceEnd, periodStart),
		PeriodEnd:   periodEnd,
	}); err != nil {
		return nil, billing.Internal("invoice mirror upsert failed", err)
	}

	// Flip the claim to 'charged' now that Stripe confirmed it, recording the
	// GENUINE Stripe invoice item id (never an idempotency-key string).
	if err := s.store.MarkAccountOverageSnapshotCharged(ctx, cand.ID, periodStart, item.ID); err != nil {
		return nil, billing.Internal("account overage snapshot mark-charged failed", err)
	}

	summary.Status = OverageCharged
	summary.StripeInvoiceID = inv.ID
	return summary, nil
}

// resumeClaimedOverage handles the (rare, only-possible-under-true-concurrency)
// case where completeGraceCharge's own claim-insert LOST a race: by the time it
// tried to claim the period, some OTHER caller (a concurrent sweep invocation,
// or the boundary) had already inserted a row first. Re-reads the winning row
// and defers to it — an 'advance' winner means the boundary owns this period
// (skip); a 'grace' winner that is already 'charged' means another sweep
// invocation finished first (skip); a 'grace' winner still 'pending' means
// another invocation is mid-flight — resume IT (same frozen amount).
func (s *Service) resumeClaimedOverage(ctx context.Context, cand OverageGraceCandidate, graceEnd, periodStart, periodEnd time.Time) (*OverageChargeSummary, error) {
	existing, found, err := s.store.AccountOverageSnapshot(ctx, cand.ID, periodStart)
	if err != nil {
		return nil, billing.Internal("account overage snapshot lookup failed", err)
	}
	if !found {
		// The conflict that sent us here proves a row exists; a missing read
		// immediately after is a driver/consistency anomaly.
		return nil, billing.Internal("account overage snapshot claim lost but no row found on re-read", nil)
	}
	if existing.Source == "advance" || existing.Status == OverageSnapshotCharged {
		return &OverageChargeSummary{Status: OverageSkippedAlreadyCharged, PeriodStart: periodStart, OverCount: existing.OverCount}, nil
	}
	return s.completeGraceCharge(ctx, cand, existing.OverCount, existing.ChargedMicros, graceEnd, periodStart, periodEnd, true)
}

// topUpGraceOverage is finding #3's fix — A JUDGMENT CALL (PR #47 review): no
// product decision was available on the exact top-up policy, so this
// implements the safest technically-correct interpretation, flagged here and
// in the PR description. recomputeAccountOverage only ever arms/clears the
// grace timer; it never re-evaluates an ALREADY-CHARGED period's snapshot, so
// pooled-module growth after the period's first grace charge went permanently
// unbilled for that period (and the display, which is snapshot-first, never
// reflected it either). Because the timer stays armed until the pool drops
// back under the pool (recomputeAccountOverage), the sweep keeps re-invoking
// ChargeAccountOverage for this account on every pass — so THIS function now
// re-checks a 'charged' period's current pool against what was billed and, if
// it grew, charges the INCREMENTAL delta, conservatively prorated from THIS
// sweep's instant (`at`) to the period end — never retroactively for time
// before this sweep noticed the growth (the safest, never-overcharge
// interpretation; it may under-bill by the gap between the actual install and
// the next sweep tick, which is bounded by the sweep's cron interval).
//
// A pool that merely dropped (or stayed the same) since the last charge is
// D1e (no refund) — this only ever ADDS to what is billed, never subtracts.
// The top-up's own Idempotency-Keys are derived from the TARGET cumulative
// over-count, so a crash-and-retry of the SAME top-up (pool unchanged since)
// reuses the SAME Stripe objects; if the pool grows AGAIN before this one
// resolves, the next pass computes a NEW target and a NEW (legitimately
// different) key — never a collision with the one still in flight.
func (s *Service) topUpGraceOverage(ctx context.Context, cand OverageGraceCandidate, existing AccountOverageSnapshot, at, periodStart, periodEnd time.Time) (*OverageChargeSummary, error) {
	pooled, err := s.store.PooledModuleCount(ctx, cand.ID)
	if err != nil {
		return nil, billing.Internal("pooled module count lookup failed", err)
	}
	overCount := pooled - usage.IncludedModules
	if overCount <= existing.OverCount {
		if overCount <= 0 {
			if err := s.store.ClearAccountOverage(ctx, cand.ID); err != nil {
				return nil, billing.Internal("clear account overage timer failed", err)
			}
		}
		return &OverageChargeSummary{Status: OverageSkippedAlreadyCharged, PeriodStart: periodStart, OverCount: existing.OverCount}, nil
	}

	deltaOverCount := overCount - existing.OverCount
	deltaFullMicros := usage.ModuleOverageFeeMicros * int64(deltaOverCount)
	deltaMicros := usage.ProratedOverageMicros(deltaFullMicros, at, periodStart, periodEnd)
	deltaCents, err := centsFromMicros(deltaMicros)
	if err != nil {
		return nil, billing.Internal("micros to cents conversion failed", err)
	}
	summary := &OverageChargeSummary{PeriodStart: periodStart, OverCount: overCount}
	if deltaCents == 0 {
		summary.Status = OverageSkippedZeroCents
		return summary, nil
	}
	summary.ChargedCents = deltaCents

	hasPM, err := s.store.HasUsableDefaultPM(ctx, cand.ID)
	if err != nil {
		return nil, billing.Internal("usable PM check failed", err)
	}
	if !hasPM {
		summary.Status = OverageSkippedNoPM
		return summary, nil // retained; re-attempted next sweep through the same idem key
	}
	custID, err := s.store.AccountStripeCustomer(ctx, cand.ID)
	if err != nil {
		return nil, billing.Internal("stripe customer lookup failed", err)
	}
	if custID == "" {
		return nil, billing.Internal("account has a usable PM but no Stripe customer id", nil)
	}

	desc := fmt.Sprintf("MirrorStack module overage top-up (account pool, %d over, +%d) — account %s", overCount, deltaOverCount, cand.ID)
	item, err := s.stripe.CreateInvoiceItem(ctx, custID, deltaCents, chargeCurrency, desc, accountOverageTopUpItemIdemKey(cand.ID, periodStart, overCount))
	if err != nil {
		return nil, billing.StripeError("overage top-up invoice item failed", err)
	}
	inv, err := s.stripe.CreateInvoice(ctx, custID, true /* autoAdvance */, accountOverageTopUpInvoiceIdemKey(cand.ID, periodStart, overCount))
	if err != nil {
		return nil, billing.StripeError("overage top-up invoice failed", err)
	}

	if err := s.store.UpsertInvoice(ctx, InvoiceMirror{
		AccountID:       cand.ID,
		StripeInvoiceID: inv.ID,
		Status:          inv.Status,
		AmountDueCents:  inv.AmountDue,
		AmountPaidCents: inv.AmountPaid,
		Currency:        chargeCurrency,
		// The incremental coverage starts at THIS sweep's instant (the
		// conservative proration basis above), never retroactively.
		PeriodStart: usage.ProrationCoverageStart(at, periodStart),
		PeriodEnd:   periodEnd,
	}); err != nil {
		return nil, billing.Internal("invoice mirror upsert failed", err)
	}

	if err := s.store.TopUpAccountOverageSnapshot(ctx, AccountOverageSnapshot{
		AccountID:     cand.ID,
		PeriodStart:   periodStart,
		PeriodEnd:     periodEnd,
		OverCount:     overCount,
		ChargedMicros: existing.ChargedMicros + deltaMicros, // cumulative total actually billed for the period
		Source:        "grace",
		InvoiceItemID: item.ID,
	}); err != nil {
		return nil, billing.Internal("account overage snapshot top-up failed", err)
	}

	summary.Status = OverageToppedUp
	summary.StripeInvoiceID = inv.ID
	return summary, nil
}

// accountOverageItemIdemKey / accountOverageInvoiceIdemKey build the
// deterministic per-(account, period) Stripe Idempotency-Keys for the pooled
// overage charge. The (account, period_start) pair is the stable charge
// identity — each account's pooled overage bills at most once per period (the
// account_overage_snapshots guard) — so both the mid-period grace sweep and the
// boundary leg, if they ever target the SAME period, reuse the SAME Stripe
// objects and can never double-charge. Mirrors the app-ii-/app-inv- pattern.
func accountOverageItemIdemKey(accountID uuid.UUID, periodStart time.Time) string {
	return "acct-overage-ii-" + accountID.String() + "-" + strconv.FormatInt(periodStart.UTC().Unix(), 10)
}

func accountOverageInvoiceIdemKey(accountID uuid.UUID, periodStart time.Time) string {
	return "acct-overage-inv-" + accountID.String() + "-" + strconv.FormatInt(periodStart.UTC().Unix(), 10)
}

// accountOverageTopUpItemIdemKey / accountOverageTopUpInvoiceIdemKey build the
// deterministic Stripe Idempotency-Keys for an INCREMENTAL top-up charge
// (finding #3), keyed additionally by the TARGET cumulative over-count so a
// retry of the SAME top-up (pool unchanged since) reuses the SAME Stripe
// objects, while a further pool change before it resolves computes a NEW
// (legitimately distinct) key rather than colliding with the one in flight.
func accountOverageTopUpItemIdemKey(accountID uuid.UUID, periodStart time.Time, targetOverCount int) string {
	return accountOverageItemIdemKey(accountID, periodStart) + "-topup-" + strconv.Itoa(targetOverCount)
}

func accountOverageTopUpInvoiceIdemKey(accountID uuid.UUID, periodStart time.Time, targetOverCount int) string {
	return accountOverageInvoiceIdemKey(accountID, periodStart) + "-topup-" + strconv.Itoa(targetOverCount)
}
