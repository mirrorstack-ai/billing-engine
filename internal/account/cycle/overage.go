package cycle

// Per-module-instance overage — Leg 1: the per-module grace charge (migration
// 033, DESIGN.md "Base fee — v2: creation grace + per-module overage timers").
// This REPLACES the account-wide single-timer pooled overage sweep entirely.
//
// The model: overage is no longer ONE account-level grace timer tiering on
// SUM(module_count); it is ONE independently-anchored 3-day grace timer per
// module INSTALL EVENT (ms_billing.app_module_overage_timers), each priced from
// its OWN install date. RegisterApp / SyncAppModules synthesize the timer rows
// (the RPC layer carries only an integer module_count); this file owns the
// mid-period sweep that charges them:
//
//   - SweepModuleOverage lists the live, unresolved timers whose grace has
//     elapsed (ModuleOverageTimersPastGrace) and runs ChargeModuleOverage on each.
//   - ChargeModuleOverage determines, LIVE and fresh at every grace-check (never
//     cached), whether the install is "included" or "over": across the account's
//     currently-live timers ordered (installed_at ASC, id ASC), the first
//     IncludedModules (5) are included, the rest are over.
//       * included → mark grace_resolved, never charge. Monotonicity (a new
//         install always gets the latest installed_at, so an existing row's rank
//         can only improve over→included, never included→over) makes this a
//         PERMANENT verdict — the row is never re-evaluated.
//       * over → charge ModuleOverageFeeMicros ($3) prorated from the install's
//         UTC day to the install period's end (install-anchored — the correction
//         vs. the prior account-wide attempt, which anchored to grace-elapse),
//         via a per-timer Stripe invoice with deterministic idem keys derived
//         from the timer id, then stamp grace_charged_at / grace_resolved and the
//         GENUINE Stripe ids.
//
// The boundary per-module overage precharge for ongoing modules (scenario 6) and
// the combined creation-invoice overage line (scenario 3) are Stage B follow-ups.

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// moduleOverageGraceWindow is the per-install grace window: a module's own timer
// starts at its install instant and its overage is only charged once this window
// has elapsed (owner spec 2026-07-05: 3 days, the SAME GraceDays as the creation
// grace). A module removed before its own grace elapses is never charged.
const moduleOverageGraceWindow = usage.GraceDays * 24 * time.Hour

// moduleGraceExpiry is the single home of the "grace_expires_at = installed_at +
// window" rule, used by RegisterApp / SyncAppModules when they synthesize timer
// rows and (implicitly, via the stored column) by the sweep's work-list.
func moduleGraceExpiry(installedAt time.Time) time.Time {
	return installedAt.Add(moduleOverageGraceWindow)
}

// ModuleOverageStatus is the terminal classification of one ChargeModuleOverage
// attempt, for the sweep's tally + per-timer log line.
type ModuleOverageStatus string

const (
	// ModuleOverageCharged: the install was "over" and its overage was invoiced.
	ModuleOverageCharged ModuleOverageStatus = "charged"
	// ModuleOverageIncluded: the live FIFO put the install within the included
	// IncludedModules — resolved permanently, never charged.
	ModuleOverageIncluded ModuleOverageStatus = "included"
	// ModuleOverageSkippedNoPM: "over" but no usable default PM — left unresolved,
	// re-attempted on the next sweep through the SAME per-timer idem keys.
	ModuleOverageSkippedNoPM ModuleOverageStatus = "skipped_no_pm"
	// ModuleOverageSkippedPrepaid: "over" but the account is in PREPAID collection
	// mode — off-session auto-charges are not permitted (H10, the same gate the
	// boundary spine applies). Left unresolved and re-attempted: a webhook-driven
	// relax back to arrears lets the deferred charge fire through the same keys.
	ModuleOverageSkippedPrepaid ModuleOverageStatus = "skipped_prepaid"
	// ModuleOverageSkippedZeroCents: "over" but the prorated overage rounded to 0
	// cents (unreachable for a real ≥1-day over module at $3) — resolved with no
	// charge so it never re-sweeps forever.
	ModuleOverageSkippedZeroCents ModuleOverageStatus = "zero_cents"
	// ModuleOveragePeriodClosed: "over" but the account only activated AT OR AFTER
	// this install's anchored period had already closed — charging now would be a
	// retroactive catch-up (D1d), so it is resolved terminally WITH NO charge (the
	// per-module analogue of ProrationStatusPeriodClosed on the creation leg).
	ModuleOveragePeriodClosed ModuleOverageStatus = "period_closed"
	// ModuleOverageDeferredToCombined: "over" AND co-created with an app whose
	// creation proration has not yet resolved — this timer's overage belongs on the
	// app's ONE combined creation invoice (scenario 3), not a separate Leg 1 one, so
	// it is DEFERRED (left unresolved) for the proration sweep to charge and mark.
	ModuleOverageDeferredToCombined ModuleOverageStatus = "deferred_to_combined"
	// ModuleOverageSkippedStale: the charge-time re-verification (M2) found the
	// timer no longer live/unresolved — removed, or resolved by a concurrent
	// sweep — between the work-list read and this candidate's turn. Nothing done.
	ModuleOverageSkippedStale ModuleOverageStatus = "skipped_stale"
)

// ModuleOverageResult reports what one ChargeModuleOverage call did.
type ModuleOverageResult struct {
	TimerID      uuid.UUID
	Status       ModuleOverageStatus
	ChargedCents int64
	// StripeInvoiceID is set only when Status == ModuleOverageCharged.
	StripeInvoiceID string
}

// moduleOverageCoverage resolves the deterministic coverage + amount for one
// timer under the 2026-07-06 coverage contract — install day → the END of the
// period the grace elapses into (install period prorated + the straddled
// period in full when the grace crosses the boundary). Every input
// (installed_at, grace_expires_at, activation anchor) is immutable, so the
// SAME amount is recomputed by a fresh charge, an idem-key replay, and the
// post-idem-key-window recovery path — one home for the math keeps the three
// from drifting.
func moduleOverageCoverage(cand ModuleOverageCandidate) (proratedMicros int64, periodStart, periodEnd, coverageEnd time.Time, closed bool) {
	periodStart, periodEnd, closed = periodClosedByActivation(cand.InstalledAt, cand.ActivatedAt)
	coverageEnd = periodEnd
	proratedMicros = usage.ProratedBaseMicros(usage.ModuleOverageFeeMicros, cand.InstalledAt, periodStart, periodEnd)
	if !cand.GraceExpiresAt.Before(periodEnd) {
		_, coverageEnd = billingperiod.AnchoredPeriodWindow(cand.GraceExpiresAt.UTC(), billingperiod.AnchorDay(cand.ActivatedAt))
		proratedMicros += usage.ModuleOverageFeeMicros
	}
	return proratedMicros, periodStart, periodEnd, coverageEnd, closed
}

// ChargeModuleOverage evaluates + (if "over") charges ONE per-module install
// timer whose grace has elapsed. Gated on the collection mode and a usable
// default PM exactly like the proration leg (the candidate account is already
// activated — the work-list query filters activated_at IS NOT NULL).
// Idempotent + race-safe WITHOUT a lock, via three layers:
//
//   - the grace_resolved first-write-wins guard records the terminal verdict;
//   - the deterministic per-timer Stripe Idempotency-Keys dedupe the charge
//     across SHORT-window retries (a crash between Stripe succeeding and the
//     mark committing resumes through the SAME keys);
//   - the migration-036 charge_attempted_at marker + the ms_charge_ref
//     metadata anchor cover retries PAST Stripe's ~24h idempotency-key window
//     (H5): an attempted candidate reconciles against what Stripe actually has
//     BEFORE recomputing any live verdict — so money moved by a crashed
//     attempt is mirrored+marked even if the timer's FIFO rank has since
//     improved to "included" (H9), and a pruned-key retry never mints a second
//     set of Stripe objects.
func (s *Service) ChargeModuleOverage(ctx context.Context, cand ModuleOverageCandidate, at time.Time) (*ModuleOverageResult, error) {
	if cand.ID == uuid.Nil {
		return nil, billing.InvalidInput("timer id required")
	}
	if s.stripe == nil {
		return nil, billing.Internal("ChargeModuleOverage requires a Stripe client", nil)
	}
	res := &ModuleOverageResult{TimerID: cand.ID}

	// CHARGE-TIME RE-VERIFICATION (review 2026-07-06, M2): the sweep's work list
	// was read once and this candidate may be minutes stale — re-check live +
	// unresolved immediately before acting, so a module removed (or resolved by
	// a concurrent sweep) mid-batch is not charged.
	pending, err := s.store.ModuleTimerStillPending(ctx, cand.ID)
	if err != nil {
		return nil, billing.Internal("module timer pending re-check failed", err)
	}
	if !pending {
		res.Status = ModuleOverageSkippedStale
		return res, nil
	}

	// CRASH RECOVERY (review 2026-07-06, H5/H9) — BEFORE the live FIFO verdict.
	// A set charge_attempted_at means a prior attempt reached its Stripe section
	// and may have moved money before crashing short of the mark. Reconcile
	// against Stripe by the ms_charge_ref anchor: whatever is found is finished
	// (mirrored + marked) regardless of what the timer's rank says NOW — a rank
	// that improved over→included since the crash must not orphan a real charge.
	// Nothing found ⇒ the crashed attempt never created its invoice; fall
	// through and charge fresh.
	if !cand.ChargeAttemptedAt.IsZero() {
		recovered, err := s.recoverModuleOverageCharge(ctx, cand, at, res)
		if err != nil {
			return nil, err
		}
		if recovered {
			return res, nil
		}
	}

	// LIVE FIFO determination, computed fresh (never cached): this install's rank
	// among the account's currently-live timers ordered (installed_at, id).
	rank, err := s.store.LiveModuleTimerRankBefore(ctx, cand.AccountID, cand.ID, cand.InstalledAt)
	if err != nil {
		return nil, billing.Internal("module timer FIFO rank lookup failed", err)
	}
	if rank < usage.IncludedModules {
		// Included → PERMANENT verdict (monotonicity: an existing row's rank only
		// ever improves over→included, never the reverse). Mark resolved, never
		// charge, never re-check.
		if err := s.store.MarkModuleTimerIncluded(ctx, cand.ID); err != nil {
			return nil, billing.Internal("mark module timer included failed", err)
		}
		res.Status = ModuleOverageIncluded
		return res, nil
	}

	// "Over": price $3 prorated from the install's UTC day over the install's own
	// anchored period (ADR 0005 anchor from activation) — install-anchored, NOT
	// grace-elapse-anchored and NOT now-anchored — plus, for a grace that
	// STRADDLES the period boundary, the full fee for the period the grace
	// elapses into (moduleOverageCoverage; the boundary precharge deliberately
	// excludes straddlers via its grace_expires_at < period_end cutoff, so that
	// period is THIS leg's to bill, and only from the boundary after that does
	// the precharge take over).
	proratedMicros, periodStart, _, coverageEnd, closed := moduleOverageCoverage(cand)

	// D1d — no retroactive catch-up (the SAME posture ChargeCreationProration
	// enforces on the creation leg, proration.go). RegisterApp synthesizes an app's
	// timers at install time regardless of whether the owning account has activated
	// yet, and the work-list only gates on activation at CHARGE time — so a timer
	// can sit installed + past-grace for arbitrarily long while unactivated, then
	// get swept the instant the account finally binds a card. If the account only
	// activated AT OR AFTER this install's anchored period had already closed, the
	// account was never chargeable for that whole period; charging its overage now
	// — however late the sweep runs — is exactly the retroactive catch-up D1d
	// forbids. Resolve the timer terminally WITHOUT charging (grace_resolved,
	// first-write-wins via MarkModuleTimerIncluded) so it never resurfaces, rather
	// than minting a historical, never-chargeable invoice. Compared against
	// ActivatedAt, NOT `at`: an ordinary late sweep on a HEALTHY already-activated
	// account (grace pushing the charge a few days past periodEnd) still charges,
	// exactly like the creation leg's ActivatedBeforePeriodCloses case.
	if closed {
		if err := s.store.MarkModuleTimerIncluded(ctx, cand.ID); err != nil {
			return nil, billing.Internal("mark module timer resolved (period closed) failed", err)
		}
		res.Status = ModuleOveragePeriodClosed
		return res, nil
	}

	// Scenario 3 combined-invoice ownership guard. A co-created over-module timer
	// (installed AT the app's created_at) whose app's creation proration is still
	// UNRESOLVED is the COMBINED-invoice path's responsibility (proration.go), NOT
	// Leg 1's: the creation-proration charge pins this timer's overage line onto
	// the app's ONE creation invoice (app-inv-<appID>) using the SHARED per-timer
	// item key. cmd/billing-cycle runs the proration sweep BEFORE this one so the
	// happy path resolves these timers first (they never reach here). But if that
	// sweep's persist phase FAILED after its Stripe calls already finalized the
	// combined invoice (money moved, lines pinned), the timer is still unresolved
	// when this sweep runs in the SAME process — and minting our OWN invoice
	// (mod-overage-inv-<timerID>) here would double-charge overage the combined
	// invoice already collected. So DEFER (skip WITHOUT resolving): the proration
	// sweep retries every cycle and converges on the SAME combined invoice via
	// the deterministic keys, then marks this timer resolved, dropping it from
	// this work list. A LATER install (installed_at != created_at) is never
	// co-created, so it charges here normally.
	app, found, err := s.store.AppMirror(ctx, cand.AppID)
	if err != nil {
		return nil, billing.Internal("app mirror lookup failed", err)
	}
	if found && cand.InstalledAt.Equal(app.CreatedAt) &&
		app.ProrationInvoiceID == "" && !app.ProrationSkipped && !app.Deleted {
		res.Status = ModuleOverageDeferredToCombined
		return res, nil
	}

	// Coverage (review 2026-07-06 contract, moduleOverageCoverage): install day →
	// the END of the period the grace ELAPSES INTO — install period prorated +
	// the straddled period in full when the grace crosses the boundary. Both
	// inputs (installed_at, activation anchor) are immutable, so the amount stays
	// deterministic across retries — the per-timer Stripe idem keys stay stable.
	// The precharge picks the timer up from the FIRST boundary after its grace
	// elapsed, so coverage is complete and disjoint by construction.
	cents, err := centsFromMicros(proratedMicros)
	if err != nil {
		return nil, billing.Internal("micros to cents conversion failed", err)
	}
	if cents == 0 {
		// Rounds to 0 cents — resolve with no charge so the sweep never revisits it
		// forever (grace_resolved would otherwise stay false).
		if err := s.store.MarkModuleTimerIncluded(ctx, cand.ID); err != nil {
			return nil, billing.Internal("mark module timer resolved (zero cents) failed", err)
		}
		res.Status = ModuleOverageSkippedZeroCents
		return res, nil
	}
	res.ChargedCents = cents

	// COLLECTION-MODE gate (review 2026-07-06, H10): a prepaid account is never
	// auto-charged off-session by ANY leg. Skip WITHOUT resolving — a relax back
	// to arrears re-attempts through the same keys.
	if permitted, err := s.offSessionChargePermitted(ctx, cand.AccountID); err != nil {
		return nil, err
	} else if !permitted {
		res.Status = ModuleOverageSkippedPrepaid
		return res, nil
	}

	// PM gate (same posture as the proration leg): no usable PM → skip WITHOUT
	// resolving, re-attempted next sweep (the per-timer idem keys stay stable).
	custID, ok, err := s.resolveChargeableCustomer(ctx, cand.AccountID)
	if err != nil {
		return nil, err
	}
	if !ok {
		res.Status = ModuleOverageSkippedNoPM
		return res, nil
	}

	// Stamp the migration-036 recovery marker BEFORE the first Stripe call
	// (first-write-wins): from here on, any retry — however late — reconciles
	// against Stripe rather than trusting a recomputed live verdict.
	if err := s.store.MarkModuleTimerChargeAttempted(ctx, cand.ID, at.UTC()); err != nil {
		return nil, billing.Internal("mark module timer charge attempted failed", err)
	}

	// Charge via a per-timer draft→pinned-item→finalize flow with deterministic
	// idem keys derived from the timer id (the stable charge identity — each
	// install charges at most once, the grace_resolved guard), so a crash-retry
	// reuses the SAME Stripe objects. The item is PINNED to this timer's own
	// draft (C2 — a floating pending item could be swept onto another leg's
	// invoice); only the finalize step moves money.
	desc := fmt.Sprintf("MirrorStack module overage (prorated) — app %s", cand.AppID)
	draft, err := s.stripe.CreateDraftInvoice(ctx, custID, moduleOverageChargeRef(cand.ID), moduleOverageInvoiceIdemKey(cand.ID))
	if err != nil {
		return nil, billing.StripeError("module overage draft invoice failed", err)
	}
	item, err := s.stripe.CreateInvoiceItem(ctx, custID, draft.ID, cents, chargeCurrency, desc, moduleOverageItemIdemKey(cand.ID))
	if err != nil {
		return nil, billing.StripeError("module overage invoice item failed", err)
	}
	inv, err := s.stripe.FinalizeInvoice(ctx, draft.ID, moduleOverageFinalizeIdemKey(cand.ID))
	if err != nil {
		return nil, billing.StripeError("module overage invoice finalize failed", err)
	}

	// Resolve the large-charge disclosure threshold AT CHARGE TIME, immediately
	// after Stripe confirms (scenario 5 / migration 034) — the SAME resolution
	// point every off-session charge site uses.
	acct, err := s.store.AccountCollection(ctx, cand.AccountID)
	if err != nil {
		return nil, billing.Internal("account collection lookup failed", err)
	}

	if err := s.store.UpsertInvoice(ctx, InvoiceMirror{
		AccountID:       cand.AccountID,
		StripeInvoiceID: inv.ID,
		Status:          inv.Status,
		AmountDueCents:  inv.AmountDue,
		AmountPaidCents: inv.AmountPaid,
		Currency:        chargeCurrency,
		// Partial coverage window [install day (UTC midnight), coverage end) — the
		// SAME window the amount above priced (coverage end extends past the install
		// period only for a boundary-straddling grace), so the mirrored window and
		// the charged amount agree by construction.
		PeriodStart:        usage.ProrationCoverageStart(cand.InstalledAt, periodStart),
		PeriodEnd:          coverageEnd,
		IsLargeAutoCollect: flagLargeAutoCollect(proratedMicros, acct),
	}); err != nil {
		return nil, billing.Internal("invoice mirror upsert failed", err)
	}

	// Stamp the terminal "over and charged" verdict with the GENUINE Stripe ids
	// (item.ID — never the idempotency-key string).
	if err := s.store.MarkModuleTimerCharged(ctx, cand.ID, at.UTC(), inv.ID, item.ID); err != nil {
		return nil, billing.Internal("mark module timer charged failed", err)
	}

	res.Status = ModuleOverageCharged
	res.StripeInvoiceID = inv.ID
	return res, nil
}

// SweepModuleOverageResult tallies one SweepModuleOverage batch for the
// cmd/billing-cycle log line + exit code.
type SweepModuleOverageResult struct {
	Pending  int // live unresolved timers past grace (the work-list size)
	Charged  int // over-modules invoiced this sweep
	Included int // resolved-as-included (no charge)
	Skipped  int // no-PM / 0-cent (left for next sweep, or resolved zero)
	Failed   int // per-timer errors — retried next sweep, never abort the batch
}

// SweepModuleOverage charges (or resolves) every per-module install timer whose
// grace has elapsed as of `at`. Idempotent + resumable: a resolved timer drops
// out of the work list (grace_resolved), and a per-timer failure is counted but
// never aborts the batch (the next sweep retries it through the same
// deterministic Stripe keys).
func (s *Service) SweepModuleOverage(ctx context.Context, at time.Time) (*SweepModuleOverageResult, error) {
	if at.IsZero() {
		return nil, billing.InvalidInput("sweep instant required")
	}
	cands, err := s.store.ModuleOverageTimersPastGrace(ctx, at.UTC())
	if err != nil {
		return nil, billing.Internal("list module overage timers past grace failed", err)
	}
	res := &SweepModuleOverageResult{Pending: len(cands)}
	for _, c := range cands {
		r, err := s.ChargeModuleOverage(ctx, c, at)
		if err != nil {
			slog.ErrorContext(ctx, "module overage charge failed",
				"timer_id", c.ID, "app_id", c.AppID, "error", err)
			res.Failed++
			continue
		}
		switch r.Status {
		case ModuleOverageCharged:
			res.Charged++
		case ModuleOverageIncluded:
			res.Included++
		default:
			res.Skipped++
		}
		slog.InfoContext(ctx, "module overage grace sweep",
			"timer_id", c.ID, "app_id", c.AppID, "status", string(r.Status),
			"charged_cents", r.ChargedCents, "stripe_invoice_id", r.StripeInvoiceID)
	}
	return res, nil
}

// moduleOverageItemIdemKey / moduleOverageInvoiceIdemKey build the deterministic
// per-timer Stripe Idempotency-Keys for the Leg 1 overage charge. The timer id is
// the stable charge identity (each install charges at most once — the
// grace_resolved guard), so a re-attempt (a retried sweep after a crash between
// the Stripe call and the mark) reuses the SAME Stripe objects and can never
// double-charge even before the row is marked resolved.
// recoverModuleOverageCharge is the H5/H9 recovery path for a candidate whose
// charge_attempted_at marker is set: look the timer's invoice up on Stripe by
// its ms_charge_ref anchor and finish whatever the crashed attempt left —
// finalized invoice → mirror + mark; inert draft → complete it (attach the
// deterministic line if it never landed, then finalize) → mirror + mark.
// Returns recovered=false when Stripe has nothing under the ref (the crashed
// attempt never created its invoice — the caller charges fresh). The PM gate
// is deliberately NOT applied: reconciling possibly-moved money must never be
// blocked by a PM removed after the crash (an idem/finalize failure lands in
// the sweep's retried-error path instead).
func (s *Service) recoverModuleOverageCharge(ctx context.Context, cand ModuleOverageCandidate, at time.Time, res *ModuleOverageResult) (bool, error) {
	custID, err := s.store.AccountStripeCustomer(ctx, cand.AccountID)
	if err != nil {
		return false, billing.Internal("stripe customer lookup failed (module overage recovery)", err)
	}
	if custID == "" {
		// No Customer ⇒ no prior attempt could have created Stripe objects.
		return false, nil
	}
	found, ok, err := s.stripe.FindInvoiceByRef(ctx, custID, moduleOverageChargeRef(cand.ID))
	if err != nil {
		return false, billing.StripeError("module overage recovery lookup failed", err)
	}
	if !ok {
		return false, nil
	}
	if found.Status == "void" {
		// The charge was CANCELED (support voided it during an incident) —
		// adopting it as 'charged' would silently forgive the overage and
		// terminally consume the timer's charge identity. Fail loudly into the
		// sweep's retried-error path so ops decides.
		return false, billing.Internal(fmt.Sprintf(
			"module overage recovery: invoice %s under %s is VOID — refusing to adopt a canceled charge (timer %s needs ops resolution)",
			found.ID, moduleOverageChargeRef(cand.ID), cand.ID), nil)
	}

	proratedMicros, periodStart, _, coverageEnd, _ := moduleOverageCoverage(cand)
	cents, err := centsFromMicros(proratedMicros)
	if err != nil {
		return false, billing.Internal("micros to cents conversion failed", err)
	}

	inv := found
	if found.Status == "draft" {
		// The crashed attempt never finalized. Complete ITS draft — never mint a
		// second one. The line either never landed (AmountDue 0 → attach it, with
		// the deterministic amount the crashed attempt would have used) or already
		// did (AmountDue == cents → just finalize). Anything else means the draft
		// was tampered with or the deterministic math changed — refuse loudly.
		switch found.AmountDue {
		case 0:
			desc := fmt.Sprintf("MirrorStack module overage (prorated) — app %s", cand.AppID)
			if _, err := s.stripe.CreateInvoiceItem(ctx, custID, found.ID, cents, chargeCurrency, desc, moduleOverageItemIdemKey(cand.ID)); err != nil {
				return false, billing.StripeError("module overage recovery invoice item failed", err)
			}
		case cents:
			// line already attached — nothing to add
		default:
			return false, billing.Internal(fmt.Sprintf(
				"module overage recovery: draft %s carries %d cents but the deterministic amount is %d — refusing to finalize a mismatched draft (timer %s)",
				found.ID, found.AmountDue, cents, cand.ID), nil)
		}
		inv, err = s.stripe.FinalizeInvoice(ctx, found.ID, moduleOverageFinalizeIdemKey(cand.ID))
		if err != nil {
			return false, billing.StripeError("module overage recovery finalize failed", err)
		}
	}

	// Mirror + mark exactly like the fresh path. The invoice-item id is unknown
	// on recovery (the search projection carries the invoice only) — the mark
	// stores NULL for it, with the genuine invoice id as the correlation anchor.
	acct, err := s.store.AccountCollection(ctx, cand.AccountID)
	if err != nil {
		return false, billing.Internal("account collection lookup failed", err)
	}
	if err := s.store.UpsertInvoice(ctx, InvoiceMirror{
		AccountID:          cand.AccountID,
		StripeInvoiceID:    inv.ID,
		Status:             inv.Status,
		AmountDueCents:     inv.AmountDue,
		AmountPaidCents:    inv.AmountPaid,
		Currency:           chargeCurrency,
		PeriodStart:        usage.ProrationCoverageStart(cand.InstalledAt, periodStart),
		PeriodEnd:          coverageEnd,
		IsLargeAutoCollect: flagLargeAutoCollect(proratedMicros, acct),
	}); err != nil {
		return false, billing.Internal("invoice mirror upsert failed (module overage recovery)", err)
	}
	if err := s.store.MarkModuleTimerCharged(ctx, cand.ID, at.UTC(), inv.ID, ""); err != nil {
		return false, billing.Internal("mark module timer charged failed (module overage recovery)", err)
	}

	res.Status = ModuleOverageCharged
	res.ChargedCents = inv.AmountDue
	res.StripeInvoiceID = inv.ID
	return true, nil
}

// moduleOverageChargeRef is the deterministic ms_charge_ref metadata anchor for
// one timer's Leg-1 invoice — what FindInvoiceByRef recovers by.
func moduleOverageChargeRef(timerID uuid.UUID) string { return "timer:" + timerID.String() }

func moduleOverageItemIdemKey(timerID uuid.UUID) string {
	return "mod-overage-ii-" + timerID.String()
}

func moduleOverageInvoiceIdemKey(timerID uuid.UUID) string {
	return "mod-overage-inv-" + timerID.String()
}

func moduleOverageFinalizeIdemKey(timerID uuid.UUID) string {
	return "mod-overage-fin-" + timerID.String()
}
