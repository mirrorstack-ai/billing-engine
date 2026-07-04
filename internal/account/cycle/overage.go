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
//  2. skips if this period's pooled overage already has a snapshot (a prior
//     sweep or the boundary billed it — the double-charge guard);
//  3. reads the CURRENT pool; if it dropped back to ≤ IncludedModules, clears
//     the timer and skips (no refund of anything already charged);
//  4. prices the pooled overage PRORATED from grace-end to the period end
//     (ProratedOverageMicros) → whole cents at the Stripe boundary; a 0-cent
//     result skips (grace ends at/after the period end);
//  5. charges via the SAME Stripe plumbing as the other legs with the
//     deterministic per-(account, period) Idempotency-Keys, mirrors the invoice,
//     and freezes the account_overage_snapshots row (source='grace').
//
// Gated on a usable default PM exactly like the spine (the candidate is already
// activated). A failure after the charge leaves no snapshot; the next sweep
// re-attempts through the SAME idem key (Stripe dedupes) — retry-safe, never a
// double charge.
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
	summary := &OverageChargeSummary{PeriodStart: periodStart}

	// Double-charge guard: this period's pooled overage was already billed (by a
	// prior sweep run OR the boundary that closed a prior period into this one).
	if _, snapped, err := s.store.AccountOverageSnapshot(ctx, cand.ID, periodStart); err != nil {
		return nil, billing.Internal("account overage snapshot lookup failed", err)
	} else if snapped {
		summary.Status = OverageSkippedAlreadyCharged
		return summary, nil
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
		summary.Status = OverageSkippedUnderPool
		return summary, nil
	}
	summary.OverCount = overCount

	// Prorate the pooled overage from grace-end to the period end. A 0-cent
	// result (grace ends at/after this period's end) means nothing to bill this
	// period — a later period picks it up; leave the timer armed, no snapshot.
	proratedMicros := usage.ProratedOverageMicros(usage.AccountOverageMicros(pooled), graceEnd, periodStart, periodEnd)
	cents, err := centsFromMicros(proratedMicros)
	if err != nil {
		return nil, billing.Internal("micros to cents conversion failed", err)
	}
	if cents == 0 {
		summary.Status = OverageSkippedZeroCents
		return summary, nil
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

	// Freeze the ledger row keyed by the FULL anchored period_start (the display +
	// double-charge identity) — source='grace', the prorated amount actually
	// invoiced. ON CONFLICT DO NOTHING makes a retry idempotent.
	if err := s.store.InsertAccountOverageSnapshot(ctx, AccountOverageSnapshot{
		AccountID:     cand.ID,
		PeriodStart:   periodStart,
		PeriodEnd:     periodEnd,
		OverCount:     overCount,
		ChargedMicros: proratedMicros,
		Source:        "grace",
		InvoiceItemID: item.ID,
	}); err != nil {
		return nil, billing.Internal("account overage snapshot insert failed", err)
	}

	summary.Status = OverageCharged
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
