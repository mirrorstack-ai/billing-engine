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
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// moduleGraceExpiry keeps the cycle's module-timer call sites named locally
// while delegating the shared creation/install grace rule to usage.GraceExpiry.
func moduleGraceExpiry(installedAt time.Time) time.Time {
	return usage.GraceExpiry(installedAt)
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
	// ModuleOverageWalletCharged (credit mode, billing-engine Job 3): the "over"
	// install was SETTLED from the credit wallet (an append-only ledger draw), not
	// a Stripe invoice — the credits-mode analogue of ModuleOverageCharged. The
	// result's StripeInvoiceID carries the synthetic wallet charge reference that
	// armed the per-timer guard.
	ModuleOverageWalletCharged ModuleOverageStatus = "wallet_charged"
	// ModuleOverageWalletUnsettled (credit mode, billing-engine Job 3): the wallet
	// could not fully settle the overage. Nothing was drawn or armed, and this call
	// does NOT fall through to Stripe; the next sweep re-selects the rail from the
	// durable billing mode.
	ModuleOverageWalletUnsettled ModuleOverageStatus = "skipped_wallet_unsettled"
)

// ModuleOverageWalletOutcome is the store's report from the locked module-overage
// wallet draw (DrawModuleOverageFromWallet), decided UNDER the timer row lock
// where the terminal removed/resolved/attempted state is authoritative. It mirrors
// ProrationOutcome for the creation leg (billing-engine #99).
type ModuleOverageWalletOutcome int

const (
	// ModuleOverageWalletLockedCharged: the overage was drawn from the wallet and
	// the per-timer guard armed with the synthetic wallet ref, committed atomically.
	ModuleOverageWalletLockedCharged ModuleOverageWalletOutcome = iota
	// ModuleOverageWalletShort: the wallet could not fully cover the overage.
	// NOTHING was drawn and the guard is UNARMED; the caller stays unsettled
	// instead of falling through to Stripe. The standard-mode case is a defensive
	// credits→standard mode-flip race.
	ModuleOverageWalletShort
	// ModuleOverageWalletDeferToStripe: the locked timer row shows a concurrent
	// attempt already reached the Stripe leg (charge_attempted_at set). The wallet
	// must not draw beside money that may have moved; defer to the Stripe leg.
	ModuleOverageWalletDeferToStripe
	// ModuleOverageWalletLockedStale: the timer was removed, or resolved by a
	// concurrent sweep, under the lock — nothing to settle (the M2 stale posture).
	ModuleOverageWalletLockedStale
	// ModuleOverageWalletLockedNoCharge: the amount rounded to nothing (defensive;
	// unreachable for a real over-module). Nothing drawn, guard unarmed.
	ModuleOverageWalletLockedNoCharge
)

// ModuleOverageWalletCharge is the credit-wallet-settled analogue of the Leg-1
// Stripe charge (billing-engine Job 3, credit mode; mirrors ProrationWalletCharge).
// For a credits-mode account the per-module overage is DEBITED from the append-only
// credit wallet instead of billed to Stripe: AmountMicros is drawn from the wallet
// and the per-timer guard is armed with Ref (a synthetic wallet reference, never a
// Stripe invoice id) + ChargedAt. The draw + guard arm commit in ONE store
// transaction.
type ModuleOverageWalletCharge struct {
	// Ref arms grace_invoice_id in place of a Stripe invoice id.
	Ref string
	// AmountMicros is the prorated overage — the SAME amount the Stripe leg charges.
	AmountMicros int64
	// ChargedAt is the sweep instant stamped as grace_charged_at.
	ChargedAt time.Time
}

// ModuleOverageResult reports what one ChargeModuleOverage call did.
type ModuleOverageResult struct {
	TimerID      uuid.UUID
	Status       ModuleOverageStatus
	ChargedCents int64
	// StripeInvoiceID is set only when Status == ModuleOverageCharged.
	StripeInvoiceID string
}

// moduleOverageChargeShape resolves the deterministic charge for one timer
// under the 2026-07-06 coverage contract — install day → the END of the period
// the grace elapses into (install period prorated + the straddled period in
// full when the grace crosses the boundary) — INCLUDING the D1d decision
// (wave 2, D4): the pre-activation install period is forgiven, but a grace
// that straddles into a period the account WAS activated during leaves that
// straddled period chargeable — it is billed in full with the coverage window
// narrowed to it; only when the ENTIRE coverage is pre-activation-closed is
// the timer fully forgiven. Every input (installed_at, grace_expires_at,
// activation anchor) is immutable, so a fresh charge, an idem-key replay, and
// the post-idem-key-window recovery path all recompute the identical shape —
// one home for the math keeps the three from drifting.
func moduleOverageChargeShape(cand ModuleOverageCandidate) (proratedMicros int64, coverageStart, coverageEnd time.Time, fullyForgiven bool) {
	periodStart, periodEnd, closed := periodClosedByActivation(cand.InstalledAt, cand.ActivatedAt)
	coverageStart = usage.ProrationCoverageStart(cand.InstalledAt, periodStart)
	coverageEnd = periodEnd
	proratedMicros = usage.ProratedBaseMicros(usage.ModuleOverageFeeMicros, cand.InstalledAt, periodStart, periodEnd)
	if !cand.GraceExpiresAt.Before(periodEnd) {
		_, coverageEnd = billingperiod.AnchoredPeriodWindow(cand.GraceExpiresAt.UTC(), billingperiod.AnchorDay(cand.ActivatedAt))
		proratedMicros += usage.ModuleOverageFeeMicros
	}
	if closed {
		if coverageEnd.After(periodEnd) && cand.ActivatedAt.Before(coverageEnd) {
			// D1d forgives only the install period; the straddled period is
			// post-activation and owed in full.
			proratedMicros = usage.ModuleOverageFeeMicros
			coverageStart = periodEnd
			return proratedMicros, coverageStart, coverageEnd, false
		}
		return 0, time.Time{}, time.Time{}, true
	}
	return proratedMicros, coverageStart, coverageEnd, false
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
	// elapses into (moduleOverageChargeShape; the boundary precharge deliberately
	// excludes straddlers via its grace_expires_at < period_end cutoff, so that
	// period is THIS leg's to bill, and only from the boundary after that does
	// the precharge take over).
	proratedMicros, coverageStart, coverageEnd, fullyForgiven := moduleOverageChargeShape(cand)

	// D1d — no retroactive catch-up (the SAME posture ChargeCreationProration
	// enforces on the creation leg, proration.go). RegisterApp synthesizes an app's
	// timers at install time regardless of whether the owning account has activated
	// yet, and the work-list only gates on activation at CHARGE time — so a timer
	// can sit installed + past-grace for arbitrarily long while unactivated, then
	// get swept the instant the account finally binds a card. If the account only
	// activated AT OR AFTER the timer's ENTIRE coverage had already closed, the
	// account was never chargeable for any of it; charging now — however late the
	// sweep runs — is exactly the retroactive catch-up D1d forbids. Resolve the
	// timer terminally WITHOUT charging (grace_resolved, first-write-wins via
	// MarkModuleTimerIncluded) so it never resurfaces, rather than minting a
	// historical, never-chargeable invoice. Compared against ActivatedAt, NOT
	// `at`: an ordinary late sweep on a HEALTHY already-activated account (grace
	// pushing the charge a few days past periodEnd) still charges. A grace that
	// straddles into a post-activation period is NOT fully forgiven (wave 2, D4)
	// — the shape narrows the charge to that straddled period in full; only the
	// pre-activation install period is ever forgiven.
	if fullyForgiven {
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

	// CREDITS-MODE MODULE-OVERAGE SETTLEMENT (billing-engine Job 3 — mirrors the
	// creation-proration credit leg, #99). A credits-mode account (durable
	// ms_billing.accounts.billing_mode = 'credits') settles its per-module overage
	// through the credit wallet and NEVER creates a Stripe invoice. Standard
	// accounts — even with a gifted balance — keep the Stripe overage path below;
	// the rail is keyed off the DURABLE billing_mode, not a transient balance, so a
	// credits account can never flip to Stripe mid-retry when its balance drains.
	// Only a FRESH charge routes here — a timer whose prior attempt already reached
	// Stripe (charge_attempted_at: handled by the recovery leg at the top of this
	// method, and re-checked UNDER the timer-row lock in the store) defers to
	// Stripe, so a mid-flight mode flip can never draw the wallet beside money that
	// may already have moved. The block is dark unless the credit-wallet flag is set
	// (fail-closed): with the flag off nothing here runs and the OFF path is the
	// byte-for-byte existing Stripe overage behavior below.
	if s.walletEnabled && cand.ChargeAttemptedAt.IsZero() {
		walletStart, walletEnd := billingperiod.AnchoredPeriodWindow(cand.InstalledAt.UTC(), billingperiod.AnchorDay(cand.ActivatedAt))
		walletState, err := s.store.WalletCreditState(ctx, cand.AccountID, walletStart, walletEnd)
		if err != nil {
			return nil, billing.Internal("wallet state lookup failed", err)
		}
		if walletState.Mode == CreditBillingModeCredits {
			wres, deferToStripe, err := s.chargeModuleOverageFromWallet(ctx, cand, proratedMicros, at)
			if err != nil {
				return nil, err
			}
			if !deferToStripe {
				return wres, nil
			}
			// deferToStripe: the locked timer row showed a concurrent attempt already
			// reached Stripe — fall through to the Stripe leg below, whose deterministic
			// per-timer idem keys + first-write-wins grace guard dedupe against it.
		}
	}

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
	// against Stripe rather than trusting a recomputed live verdict. The stamp
	// ALSO guards grace_resolved = false, making it the serialization point against
	// a concurrent credit-wallet settlement (billing-engine Job 3 hardening):
	// DrawModuleOverageFromWallet arms grace_resolved inside its own row-locked
	// transaction, so if that wallet draw committed between this candidate's
	// unlocked pre-checks and here, the stamp affects 0 rows. A 0 means the timer
	// is already settled — ABORT as stale rather than draft/finalize a second charge
	// (a Stripe charge beside a wallet debit is a double charge). This holds for
	// standard accounts too: a resolved timer must never be re-charged by any rail.
	stamped, err := s.store.MarkModuleTimerChargeAttempted(ctx, cand.ID, at.UTC())
	if err != nil {
		return nil, billing.Internal("mark module timer charge attempted failed", err)
	}
	if stamped == 0 {
		res.Status = ModuleOverageSkippedStale
		return res, nil
	}

	// Charge via a per-timer draft→pinned-item→finalize flow with deterministic
	// idem keys derived from the timer id (the stable charge identity — each
	// install charges at most once, the grace_resolved guard), so a crash-retry
	// reuses the SAME Stripe objects. The item is PINNED to this timer's own
	// draft (C2 — a floating pending item could be swept onto another leg's
	// invoice); only the finalize step moves money.
	desc := fmt.Sprintf("MirrorStack module overage (prorated) — %s", appLineLabel(app.Name, cand.AppID))
	draft, err := s.stripe.CreateDraftInvoice(ctx, custID, moduleOverageChargeRef(cand.ID), moduleOverageInvoiceIdemKey(cand.ID))
	if err != nil {
		return nil, billing.StripeError("module overage draft invoice failed", err)
	}
	linePeriod := billingstripe.LinePeriod{Start: coverageStart, End: coverageEnd}
	item, err := s.stripe.CreateInvoiceItem(ctx, custID, draft.ID, cents, chargeCurrency, desc, linePeriod, moduleOverageItemIdemKey(cand.ID))
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
		// The coverage window the shape priced — [install day, coverage end) in
		// the normal case; narrowed to the straddled period alone under the D1d
		// straddle rule — so the mirrored window and the charged amount agree by
		// construction.
		PeriodStart:        coverageStart,
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

// chargeModuleOverageFromWallet is ChargeModuleOverage's credit-mode leg
// (billing-engine Job 3 — mirrors chargeCreationProrationFromWallet, #99). It
// DEBITS the SAME prorated overage amount the Stripe leg would charge from the
// credit wallet instead of minting a Stripe invoice. The store draws the full
// amount and, ONLY if the wallet fully covers it, arms the SAME per-timer guard
// the Stripe leg arms (grace_charged_at + a synthetic wallet grace_invoice_id ref,
// NULL invoice-item id), all in one transaction. installed_at + the activation
// anchor are immutable, so amountMicros is deterministic across retries. Returns
// deferToStripe=true when the locked timer row showed a concurrent Stripe attempt,
// so the caller falls through to the Stripe leg (mirroring #99's caller).
func (s *Service) chargeModuleOverageFromWallet(ctx context.Context, cand ModuleOverageCandidate, amountMicros int64, at time.Time) (*ModuleOverageResult, bool, error) {
	res := &ModuleOverageResult{TimerID: cand.ID}
	if amountMicros <= 0 {
		// Unreachable after the caller's cents check (a real over-module owes ≥ 1¢);
		// resolve as a no-charge zero rather than draw, mirroring the Stripe zero path.
		res.Status = ModuleOverageSkippedZeroCents
		return res, false, nil
	}

	outcome, armedRef, err := s.store.DrawModuleOverageFromWallet(ctx, cand.ID, ModuleOverageWalletCharge{
		Ref:          appModuleOverageWalletRef(cand.ID),
		AmountMicros: amountMicros,
		ChargedAt:    at.UTC(),
	})
	if err != nil {
		// A billing.Error from the store is already classified — surface verbatim;
		// anything else is a store/tx failure.
		if _, ok := err.(*billing.Error); ok {
			return nil, false, err
		}
		return nil, false, billing.Internal("wallet module-overage draw failed", err)
	}

	switch outcome {
	case ModuleOverageWalletLockedCharged:
		cents, err := centsFromMicros(amountMicros)
		if err != nil {
			return nil, false, billing.Internal("micros to cents conversion failed", err)
		}
		res.Status = ModuleOverageWalletCharged
		res.ChargedCents = cents
		res.StripeInvoiceID = armedRef
		return res, false, nil
	case ModuleOverageWalletShort:
		res.Status = ModuleOverageWalletUnsettled
		return res, false, nil
	case ModuleOverageWalletDeferToStripe:
		return nil, true, nil
	case ModuleOverageWalletLockedStale:
		res.Status = ModuleOverageSkippedStale
		return res, false, nil
	default: // ModuleOverageWalletLockedNoCharge
		res.Status = ModuleOverageSkippedZeroCents
		return res, false, nil
	}
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
		case ModuleOverageCharged, ModuleOverageWalletCharged:
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
	custID, err := s.recoveryCustomer(ctx, cand.AccountID)
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

	proratedMicros, coverageStart, coverageEnd, fullyForgiven := moduleOverageChargeShape(cand)
	if fullyForgiven {
		// The shape is deterministic, so an attempted timer can only be fully
		// forgiven if the attempt itself would have been — nothing chargeable to
		// recover; the caller's fresh path resolves it period_closed.
		return false, nil
	}
	cents, err := centsFromMicros(proratedMicros)
	if err != nil {
		return false, billing.Internal("micros to cents conversion failed", err)
	}

	inv := found
	if found.Status == "draft" {
		// An inert draft moved NO money — finalizing it is a fresh off-session
		// debit, so the prepaid collection gate applies exactly as on a first
		// attempt (wave 2, D6). Skip WITHOUT resolving; the draft stays inert
		// and a relax back to arrears completes it through the same keys.
		if permitted, err := s.offSessionChargePermitted(ctx, cand.AccountID); err != nil {
			return false, err
		} else if !permitted {
			res.Status = ModuleOverageSkippedPrepaid
			return true, nil
		}
		// The crashed attempt never finalized. Complete ITS draft — never mint a
		// second one. The line either never landed (AmountDue 0 → attach it, with
		// the deterministic amount the crashed attempt would have used) or already
		// did (AmountDue == cents → just finalize). Anything else means the draft
		// was tampered with or the deterministic math changed — refuse loudly.
		switch found.AmountDue {
		case 0:
			app, _, mErr := s.store.AppMirror(ctx, cand.AppID)
			appName := ""
			if mErr == nil {
				appName = app.Name
			}
			desc := fmt.Sprintf("MirrorStack module overage (prorated) — %s", appLineLabel(appName, cand.AppID))
			linePeriod := billingstripe.LinePeriod{Start: coverageStart, End: coverageEnd}
			if _, err := s.stripe.CreateInvoiceItem(ctx, custID, found.ID, cents, chargeCurrency, desc, linePeriod, moduleOverageItemIdemKey(cand.ID)); err != nil {
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
		PeriodStart:        coverageStart,
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

// appModuleOverageWalletRef is the deterministic wallet settlement reference that
// arms one timer's per-timer guard (grace_invoice_id) when its overage is settled
// from the credit wallet rather than Stripe (billing-engine Job 3). The "wallet:"
// prefix keeps it unambiguously NOT a Stripe invoice id for any reader of the guard
// column, mirroring appProrationWalletRef on the creation leg.
func appModuleOverageWalletRef(timerID uuid.UUID) string {
	return "wallet:mod-overage:" + timerID.String()
}
