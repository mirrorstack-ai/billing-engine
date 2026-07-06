package cycle

// Creation-proration charge + sweep (creation grace, owner spec 2026-07-05,
// D1e follow-up). RegisterApp used to charge an app's creation-period base
// synchronously at creation; it no longer does (see apps.go). Instead a newly
// created app enters a GRACE window and is charged only once it has SURVIVED it:
//
//   - RegisterApp mirrors the roster row (created_at, account, module_count) and
//     charges NOTHING;
//   - a periodic sweep (SweepCreationProrations, driven by cmd/billing-cycle)
//     finds apps past grace (created_at <= now − GraceDays) that are still LIVE
//     (deleted_at IS NULL) and NOT yet charged (proration_invoice_id IS NULL),
//     and charges each the SAME creation-period proration RegisterApp used to —
//     identical ProratedBaseMicros math, anchored to the TRUE created_at, so the
//     app pays only for the days it actually existed. Grace delays WHEN the
//     charge fires, never WHAT it covers.
//
// An app soft-deleted within grace is thus NEVER charged (the sweep excludes
// deleted rows), and the charge is race-safe against a concurrent delete via a
// brief FOR UPDATE row lock that is released BEFORE the Stripe call (see
// ChargeProrationLocked in store.go).
//
// D1d — no retroactive catch-up: an app whose account never activated (or had
// no usable PM) sits pending on every sweep. If the account only becomes
// chargeable AFTER the app's anchored creation period has already closed,
// charging it then would be exactly the retroactive catch-up D1d forbids —
// ChargeCreationProration detects this (activatedAt at/after the period's end)
// and PERMANENTLY skips the charge (proration_skipped_at, migration 031)
// rather than charging it or leaving it pending forever.
//
// module_count is a LIVE snapshot SyncAppModules can move at any time,
// including during grace. The creation-proration charge must never price its
// historical window off whatever module_count happens to read at sweep time —
// it prices off created_module_count (migration 030), frozen once at
// RegisterApp and never touched again.

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

// ProrationStatus classifies one ChargeCreationProration outcome for the sweep's
// tally + per-app log line. Only ProrationStatusCharged mints a new invoice; the
// rest are legitimate no-charge outcomes (D1d/D1e) or the idempotent guard.
type ProrationStatus string

const (
	// ProrationStatusCharged: the creation-proration invoice was created and the
	// one-shot guard armed on THIS call.
	ProrationStatusCharged ProrationStatus = "charged"
	// ProrationStatusAlreadyCharged: the guard was already armed (a prior sweep,
	// or a concurrent one, charged) — idempotent success, no second invoice.
	ProrationStatusAlreadyCharged ProrationStatus = "already_charged"
	// ProrationStatusDeleted: the app is soft-deleted (within grace, or a delete
	// that won the race under the lock) → never charged (D1e, no refunds).
	ProrationStatusDeleted ProrationStatus = "skipped_deleted"
	// ProrationStatusUnactivated: the owner account never bound a card → never
	// charged (D1d, no retroactive catch-up on later activation in v1).
	ProrationStatusUnactivated ProrationStatus = "skipped_unactivated"
	// ProrationStatusNoPM: activated but no usable default PM → skipped, same
	// posture as the boundary spine; re-attempted on the next sweep.
	ProrationStatusNoPM ProrationStatus = "skipped_no_pm"
	// ProrationStatusPrepaid: the account is in PREPAID collection mode —
	// off-session auto-charges are not permitted (H10, the same gate the
	// boundary spine applies). Transient like no-PM: re-attempted once a
	// webhook-driven relax flips the account back to arrears.
	ProrationStatusPrepaid ProrationStatus = "skipped_prepaid"
	// ProrationStatusNoCharge: the proration rounded to 0 cents (effectively
	// unreachable for a real survived app whose base is ≥ $20) → nothing to
	// invoice, guard left unarmed.
	ProrationStatusNoCharge ProrationStatus = "no_charge"
	// ProrationStatusNotFound: no roster row for the app id (never registered).
	ProrationStatusNotFound ProrationStatus = "not_found"
	// ProrationStatusPeriodClosed: the account only activated at/after the
	// app's anchored creation period had already closed — charging it now
	// would be a retroactive catch-up (D1d). PERMANENTLY skipped: the
	// proration_skipped_at marker is armed so the app never resurfaces on a
	// later sweep.
	ProrationStatusPeriodClosed ProrationStatus = "skipped_period_closed"
)

// ProrationResult reports what ChargeCreationProration did. ProrationInvoiceID is
// set on ProrationStatusCharged (the new invoice) and ProrationStatusAlreadyCharged
// (the pre-existing one); ProrationCents only on a fresh charge.
type ProrationResult struct {
	AppID              uuid.UUID
	Status             ProrationStatus
	ProrationInvoiceID string
	ProrationCents     int64
}

// ProrationOutcome is the store's report from the locked charge section
// (ChargeProrationLocked), decided UNDER the row lock where the terminal
// deleted/guard state is authoritative.
type ProrationOutcome int

const (
	// ProrationLockedNotFound: the row vanished between the sweep's read and the
	// lock (unregistered / cascade-deleted).
	ProrationLockedNotFound ProrationOutcome = iota
	// ProrationLockedDeleted: deleted_at is set under the lock — a delete won.
	ProrationLockedDeleted
	// ProrationLockedAlreadyCharged: proration_invoice_id is set under the lock.
	ProrationLockedAlreadyCharged
	// ProrationLockedNoCharge: the charge callback declined (0 cents) — nothing
	// persisted, guard unarmed.
	ProrationLockedNoCharge
	// ProrationLockedCharged: the charge fired, was mirrored + snapshotted, and
	// the guard armed, all committed atomically.
	ProrationLockedCharged
)

// ProrationCharge is the persistence payload the charge callback returns from
// inside the locked transaction: the created Stripe invoice to mirror, the base
// snapshot to freeze (migration 028, source='proration'), and the invoice id
// that arms the one-shot guard. Cents is echoed to the caller. A nil return from
// the callback means "nothing to charge" (0 cents) — the store persists nothing.
//
// TimerCharges (scenario 3) are the co-created over-module install timers billed
// as ADDITIONAL line items on this SAME invoice. persistProrationCharge stamps
// each grace_resolved/grace_charged in the SAME transaction that arms the app
// guard, so the combined charge is all-or-nothing: a co-created over-module and
// the app base fee are billed and marked together, never one without the other.
type ProrationCharge struct {
	InvoiceID string
	Cents     int64
	Invoice   InvoiceMirror
	Snapshot  AppBaseSnapshot
	// StraddleSnapshot freezes the straddled period billed IN FULL on this same
	// invoice when the app's creation grace crossed its period boundary (the
	// coverage contract, review 2026-07-06) — nil otherwise.
	StraddleSnapshot *AppBaseSnapshot
	TimerCharges     []ModuleTimerCharge
}

// ModuleTimerCharge is one co-created over-module install timer's terminal
// "over and charged" mark (scenario 3): the timer id + the REAL Stripe
// invoice/invoice-item ids of the line it rode on the combined creation invoice.
type ModuleTimerCharge struct {
	TimerID       uuid.UUID
	ChargedAt     time.Time
	InvoiceID     string
	InvoiceItemID string
}

// ChargeCreationProration charges (once) the creation-period base proration for
// ONE app that has survived the grace window — the shared charge leg the sweep
// invokes per pending app. It is idempotent (the one-shot proration_invoice_id
// guard) and race-safe against a concurrent soft-delete (the FOR UPDATE section).
//
// The amount is the FLAT per-app base, prorated to the creation window:
//
//	ProratedBaseMicros(BaseFeeMicros, created_at,
//	                   the anchored period CONTAINING created_at)
//
// anchored to the TRUE created_at (NOT now), so the app pays only for the whole
// UTC days it existed in its creation period, creation day inclusive — grace
// only delayed WHEN this fires. Module overage is NO LONGER folded into this
// base (migration 032): it is billed per module instance on its own grace timer,
// and modules co-created with the app (install date == created_at) are added as
// a SEPARATE overage line on this SAME invoice (scenario 3). created_module_count
// stays frozen at RegisterApp time and is recorded on the base snapshot for
// display, but no longer moves the base amount.
//
// It IS gated on whether the account only became chargeable after the
// creation period had already closed (D1d): that would be a retroactive
// catch-up charge for time the account was never eligible to be billed for,
// and is PERMANENTLY skipped rather than charged (see the period-closed check
// below). Short of that, the creation period is billed by NO other leg (the
// boundary advance leg only ever bills an app's SUBSEQUENT periods, never the
// one containing its creation), so charging it whenever the guard is unarmed
// and the period-closed check passes is correct and can never double-bill.
//
// Cheap gates that don't need the row lock (unregistered / already-charged /
// deleted / unactivated / period-closed / no-PM) short-circuit first; the
// actual charge + arm runs under the lock (ChargeProrationLocked), which
// re-verifies the deleted + guard state authoritatively.
func (s *Service) ChargeCreationProration(ctx context.Context, appID uuid.UUID) (*ProrationResult, error) {
	if appID == uuid.Nil {
		return nil, billing.InvalidInput("app_id required")
	}

	app, found, err := s.store.AppMirror(ctx, appID)
	if err != nil {
		return nil, billing.Internal("app mirror lookup failed", err)
	}
	if !found {
		return &ProrationResult{AppID: appID, Status: ProrationStatusNotFound}, nil
	}
	// Idempotent short-circuit: a prior (or concurrent) sweep already charged.
	if app.ProrationInvoiceID != "" {
		return &ProrationResult{AppID: appID, Status: ProrationStatusAlreadyCharged, ProrationInvoiceID: app.ProrationInvoiceID}, nil
	}
	// Permanently skipped on a prior sweep (D1d retroactive-catch-up guard,
	// migration 031) — never re-evaluated.
	if app.ProrationSkipped {
		return &ProrationResult{AppID: appID, Status: ProrationStatusPeriodClosed}, nil
	}
	// Deleted WITHIN grace → never charged (scenario 1). Deleted AFTER the
	// grace elapsed SURVIVED it and still owes the creation charge (wave 2,
	// D11) — grace only delays WHEN the charge fires, and the H2 boundary
	// exclusion leaves no other leg as a backstop, so skipping any deleted app
	// was a user-timable ~$22 dodge in the grace-elapse→sweep window. The
	// locked section re-checks this authoritatively; this is the cheap
	// early-out.
	if app.Deleted && app.DeletedAt.Before(moduleGraceExpiry(app.CreatedAt.UTC())) {
		return &ProrationResult{AppID: appID, Status: ProrationStatusDeleted}, nil
	}

	// Activation gate (D1d), same posture as the boundary spine: an
	// unactivated account (never bound a card) is never charged.
	activatedAt, activated, err := s.store.AccountActivation(ctx, app.AccountID)
	if err != nil {
		return nil, billing.Internal("account activation lookup failed", err)
	}
	if !activated {
		return &ProrationResult{AppID: appID, Status: ProrationStatusUnactivated}, nil
	}

	// D1d — no retroactive catch-up: derive the anchored period CONTAINING the
	// app's created_at from the account's (now-known) activation anchor. If the
	// account only activated AT OR AFTER that period's end, the account was
	// unactivated — and therefore never chargeable — for the app's ENTIRE
	// creation period; charging it now, however late the sweep runs, would be
	// exactly the retroactive catch-up D1d forbids. Permanently mark it skipped
	// (never re-evaluated again) rather than charge it, and rather than leaving
	// it pending forever (proration_invoice_id would stay NULL, so without this
	// marker AppsPendingProration would resurface it on every future sweep).
	//
	// This check deliberately compares against activatedAt, NOT "now": grace +
	// ordinary sweep cadence can itself push the charge attempt a few days past
	// this SAME periodEnd for a perfectly healthy, already-activated account
	// (an app created near its period boundary) — that is expected, intended
	// delayed billing (still the ONLY leg that ever bills this period), not a
	// retroactive catch-up, and must still charge normally.
	// A creation grace that straddles into a period the account WAS activated
	// during is NOT fully forgiven (wave 2, D4): D1d forgives the pre-activation
	// creation period only — the straddled post-activation period is owed in
	// full (the charge callback narrows the amount + window to it), and the
	// advance leg only picks the app up from the NEXT boundary.
	if _, periodEnd, closed := periodClosedByActivation(app.CreatedAt, activatedAt); closed {
		graceExpiry := moduleGraceExpiry(app.CreatedAt.UTC())
		straddleChargeable := false
		if !graceExpiry.Before(periodEnd) {
			_, coverageEnd := billingperiod.AnchoredPeriodWindow(graceExpiry, billingperiod.AnchorDay(activatedAt))
			straddleChargeable = activatedAt.Before(coverageEnd)
		}
		if !straddleChargeable {
			if err := s.store.SetAppProrationSkipped(ctx, appID); err != nil {
				return nil, billing.Internal("mark proration permanently skipped failed", err)
			}
			return &ProrationResult{AppID: appID, Status: ProrationStatusPeriodClosed}, nil
		}
	}

	if s.stripe == nil {
		return nil, billing.Internal("ChargeCreationProration requires a Stripe client", nil)
	}

	// Gates + recovery resolution. Once a prior attempt reached its Stripe
	// section (proration_attempted_at set), look its invoice up NOW by the
	// ms_charge_ref anchor (once — the charge callback consumes the result):
	// a FINALIZED invoice means money moved and this call's only job is to
	// RECONCILE (gates bypassed — a prepaid tighten or removed PM after the
	// crash must not strand the charge unmirrored); a VOID one is refused
	// loudly (D10); an inert DRAFT or nothing at all moved NO money (wave 2,
	// D6) — finalizing/minting is a fresh off-session debit and every gate
	// applies, exactly as on a first attempt.
	var custID string
	var recoveredInv *billingstripe.Invoice
	moneyMayHaveMoved := false
	if app.ProrationAttempted {
		custID, err = s.store.AccountStripeCustomer(ctx, app.AccountID)
		if err != nil {
			return nil, billing.Internal("stripe customer lookup failed", err)
		}
		if custID == "" {
			return nil, billing.Internal("app has an attempted proration charge but the account has no Stripe customer id", nil)
		}
		if found, ok, err := s.stripe.FindInvoiceByRef(ctx, custID, appProrationChargeRef(appID)); err != nil {
			return nil, billing.StripeError("proration recovery lookup failed", err)
		} else if ok {
			if found.Status == "void" {
				return nil, billing.Internal(fmt.Sprintf(
					"proration recovery: invoice %s under %s is VOID — refusing to adopt a canceled charge (app %s needs ops resolution)",
					found.ID, appProrationChargeRef(appID), appID), nil)
			}
			recoveredInv = &found
			moneyMayHaveMoved = found.Status != "draft"
		}
	}
	if !moneyMayHaveMoved {
		// COLLECTION-MODE gate (review 2026-07-06, H10): a prepaid account is
		// never auto-charged off-session by ANY leg. Transient skip (guard
		// unarmed), like no-PM — re-attempted once the account relaxes back to
		// arrears. A recovered inert draft stays inert across the skip.
		if permitted, err := s.offSessionChargePermitted(ctx, app.AccountID); err != nil {
			return nil, err
		} else if !permitted {
			return &ProrationResult{AppID: appID, Status: ProrationStatusPrepaid}, nil
		}

		// PM gate (D1d), same posture as the boundary spine: activated but no
		// usable default PM is skipped and re-attempted next sweep (unlike the
		// period-closed case above, "no PM right now" is not itself evidence the
		// account was ever ineligible for this specific period, so it stays a
		// transient, retried skip rather than a permanent one — see the judgment
		// call noted in the PR description for the limits of this).
		var ok bool
		custID, ok, err = s.resolveChargeableCustomer(ctx, app.AccountID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return &ProrationResult{AppID: appID, Status: ProrationStatusNoPM}, nil
		}
	}

	// The charge callback runs AFTER the row lock is released (ChargeProrationLocked,
	// store.go): the not-deleted re-check happens under a brief lock, then the
	// Stripe charge runs unlocked, then the guard-arm persists the result.
	var cents int64
	outcome, invID, err := s.store.ChargeProrationLocked(ctx, appID, func(locked AppMirror) (*ProrationCharge, error) {
		// Window = the anchored period CONTAINING the app's created_at (ADR 0005
		// anchor from the activation day). Derived from created_at, NEVER from now,
		// so the amount is deterministic regardless of when the sweep fires.
		periodStart, periodEnd := billingperiod.AnchoredPeriodWindow(locked.CreatedAt.UTC(), billingperiod.AnchorDay(activatedAt))
		// The creation proration is the FLAT per-app base (migration 032 — module
		// overage is no longer a per-app tier; it is billed per module instance via
		// its own grace timer). created_module_count is still frozen at RegisterApp
		// time and recorded on the snapshot for display, but it no longer moves the
		// base amount — a create with 0 or 50 modules prorates the identical flat base.
		creationPeriodMicros := usage.ProratedBaseMicros(usage.BaseFeeMicros, locked.CreatedAt, periodStart, periodEnd)

		// Coverage contract (review 2026-07-06, H2): this charge covers creation
		// day → the END of the period the creation grace ELAPSES INTO. Normally
		// that is the creation period itself. An app created within GraceDays of
		// its period boundary is still IN GRACE at that boundary, so the advance
		// leg deliberately excludes it there (LiveAppsCreatedBefore's grace
		// cutoff — a grace-deleted app must never pay a month of base); when it
		// SURVIVES, this charge bills the straddled period in full on top of the
		// creation-period proration, and the app joins the advance leg at the
		// NEXT boundary. Deterministic across retries (created_at + activation
		// anchor are immutable) — the app-keyed Stripe idem keys stay stable.
		coverageEnd := periodEnd
		prorated := creationPeriodMicros
		straddle := !moduleGraceExpiry(locked.CreatedAt.UTC()).Before(periodEnd)
		if straddle {
			_, coverageEnd = billingperiod.AnchoredPeriodWindow(moduleGraceExpiry(locked.CreatedAt.UTC()), billingperiod.AnchorDay(activatedAt))
			prorated += usage.BaseFeeMicros
		}
		// D1d straddle narrowing (wave 2, D4). With a pre-activation-closed
		// creation period this point is only reachable when the grace straddles
		// into a post-activation period (the outer period-closed gate permanently
		// skips every other closed case): forgive the creation period, bill the
		// straddled one in full, and narrow the mirror window to it.
		coverageStart := usage.ProrationCoverageStart(locked.CreatedAt, periodStart)
		creationPeriodClosed := !activatedAt.Before(periodEnd)
		if creationPeriodClosed {
			creationPeriodMicros = 0
			prorated = usage.BaseFeeMicros
			coverageStart = periodEnd
		}
		c, err := centsFromMicros(prorated)
		if err != nil {
			return nil, billing.Internal("micros to cents conversion failed", err)
		}
		if c == 0 {
			return nil, nil // rounds to 0 cents → nothing to invoice (guard stays unarmed)
		}

		// Scenario 3 — the combined creation invoice. Modules co-created with the
		// app (install date == created_at) that are "over" per the live FIFO have
		// their OWN grace elapse at this SAME instant (same GraceDays anchor), so
		// bill them as ADDITIONAL line items PINNED to this SAME draft rather than
		// minting a second invoice. Each is $3 over the IDENTICAL coverage window
		// as the base (same created_at, so every co-created module is the same
		// amount). They use the SAME per-timer item idem keys (mod-overage-ii-<id>)
		// Leg 1 would use; the deferred-to-combined guard (overage.go) keeps Leg 1
		// off co-created timers while this charge is pending, and the
		// grace_resolved guard (persistProrationCharge) records the winner.
		overTimers, err := s.store.CoCreatedOverModuleTimers(ctx, locked.AccountID, locked.AppID, locked.CreatedAt, usage.IncludedModules)
		if err != nil {
			return nil, billing.Internal("co-created over-module timers lookup failed", err)
		}
		// Same coverage as the base: creation period prorated, plus the straddled
		// period in full when the (shared, co-created) grace crosses the boundary
		// — the boundary precharge's grace_expires_at cutoff excluded these timers
		// there, so the straddled period is this combined invoice's to bill.
		overageMicros := usage.ProratedBaseMicros(usage.ModuleOverageFeeMicros, locked.CreatedAt, periodStart, periodEnd)
		if straddle {
			overageMicros += usage.ModuleOverageFeeMicros
		}
		if creationPeriodClosed {
			// Same D1d narrowing as the base: only the straddled period is owed.
			overageMicros = usage.ModuleOverageFeeMicros
		}
		overageCents, err := centsFromMicros(overageMicros)
		if err != nil {
			return nil, billing.Internal("overage micros to cents conversion failed", err)
		}
		expectedTotalCents := c
		if overageCents > 0 {
			expectedTotalCents += overageCents * int64(len(overTimers))
		}

		// CRASH RECOVERY (review 2026-07-06, H5): the outer gate section already
		// looked the attempted charge up by its ms_charge_ref anchor (once —
		// void refused there, gates re-applied when nothing/only-a-draft was
		// found, D6). A finalized invoice → the money moved; adopt it. An inert
		// draft → complete THAT draft below instead of creating one. Past
		// Stripe's ~24h idempotency-key window a bare key replay would have
		// created a SECOND draft + items + charge.
		var inv billingstripe.Invoice
		var draft billingstripe.Invoice
		var recoveredFinal bool
		if recoveredInv != nil {
			if recoveredInv.Status == "draft" {
				draft = *recoveredInv
			} else {
				inv = *recoveredInv
				recoveredFinal = true
			}
		}

		var timerCharges []ModuleTimerCharge
		if !recoveredFinal {
			// Draft→pinned-items→finalize (C2): the empty draft is created FIRST so
			// the base line and every co-created overage line are pinned to THIS
			// invoice explicitly — never floating customer-level pending items that a
			// concurrently-finalizing leg's invoice could sweep up (or that a crash
			// here would leak onto the account's next unrelated invoice). Only the
			// finalize step at the end moves money. The migration-036 attempt marker
			// is stamped BEFORE the first Stripe call (first-write-wins).
			if draft.ID == "" {
				if err := s.store.MarkAppProrationAttempted(ctx, locked.AppID, s.nowFn().UTC()); err != nil {
					return nil, billing.Internal("mark proration attempted failed", err)
				}
				draft, err = s.stripe.CreateDraftInvoice(ctx, custID, appProrationChargeRef(locked.AppID), appProrationInvoiceIdemKey(locked.AppID))
				if err != nil {
					return nil, billing.StripeError("proration draft invoice failed", err)
				}
			}

			// Attach the lines — unless a recovered draft already carries a
			// COMPLETE deterministic line set: base + k overage lines with
			// len(overTimers) ≤ k ≤ created_module_count (wave 2, D2). The live
			// overTimers set can only SHRINK between the crash and the retry (an
			// uninstall, or a rank flip via an earlier removal), so demanding
			// equality with the LIVE recomputation livelocked every such retry
			// forever — the crashed attempt's larger (then-correct) line set is
			// equally valid: those timers were live and over when the lines were
			// pinned, and D1e forbids unwinding them. k below the live set means
			// the draft is genuinely INCOMPLETE (crashed mid-attach) — completing
			// it past the idem-key window risks duplicate lines, so that (and any
			// amount fitting no k) is refused loudly for ops.
			completeForSomeK := false
			if overageCents > 0 && draft.AmountDue >= c {
				rem := draft.AmountDue - c
				if rem%overageCents == 0 {
					k := rem / overageCents
					completeForSomeK = k >= int64(len(overTimers)) && k <= int64(locked.CreatedModuleCount)
				}
			}
			switch {
			case draft.AmountDue == expectedTotalCents || completeForSomeK:
				// every line already attached — collect the marks for the LIVE
				// still-unresolved timers with the known invoice id (item ids
				// unrecoverable from the search projection; a timer removed since
				// the crash keeps its pinned line — D1e — but needs no mark)
				if overageCents > 0 {
					for _, timerID := range overTimers {
						timerCharges = append(timerCharges, ModuleTimerCharge{
							TimerID: timerID, ChargedAt: s.nowFn().UTC(), InvoiceID: draft.ID,
						})
					}
				}
			case draft.AmountDue == 0:
				desc := fmt.Sprintf("MirrorStack app base fee (prorated) — app %s", locked.AppID)
				if _, err := s.stripe.CreateInvoiceItem(ctx, custID, draft.ID, c, chargeCurrency, desc, appProrationItemIdemKey(locked.AppID)); err != nil {
					return nil, billing.StripeError("proration invoice item failed", err)
				}
				if overageCents > 0 {
					overDesc := fmt.Sprintf("MirrorStack module overage (prorated) — app %s", locked.AppID)
					for _, timerID := range overTimers {
						item, err := s.stripe.CreateInvoiceItem(ctx, custID, draft.ID, overageCents, chargeCurrency, overDesc, moduleOverageItemIdemKey(timerID))
						if err != nil {
							return nil, billing.StripeError("combined module overage invoice item failed", err)
						}
						timerCharges = append(timerCharges, ModuleTimerCharge{
							TimerID:       timerID,
							ChargedAt:     s.nowFn().UTC(),
							InvoiceID:     draft.ID,
							InvoiceItemID: item.ID,
						})
					}
				}
			default:
				return nil, billing.Internal(fmt.Sprintf(
					"proration recovery: draft %s carries %d cents, which is neither 0 nor base(%d)+k×overage(%d) for any k ≤ %d — refusing to finalize a corrupt combined draft (app %s)",
					draft.ID, draft.AmountDue, c, overageCents, locked.CreatedModuleCount, locked.AppID), nil)
			}

			inv, err = s.stripe.FinalizeInvoice(ctx, draft.ID, appProrationFinalizeIdemKey(locked.AppID))
			if err != nil {
				return nil, billing.StripeError("proration invoice finalize failed", err)
			}
		} else {
			// Recovered a finalized invoice: rebuild the co-created timer marks
			// against it (item ids unrecoverable from the search projection).
			if overageCents > 0 {
				for _, timerID := range overTimers {
					timerCharges = append(timerCharges, ModuleTimerCharge{
						TimerID: timerID, ChargedAt: s.nowFn().UTC(), InvoiceID: inv.ID,
					})
				}
			}
		}
		overageTotalMicros := overageMicros * int64(len(timerCharges))

		// Resolve the account's large-charge disclosure threshold AT CHARGE TIME,
		// immediately AFTER the Stripe calls above succeeded — the SAME point (via
		// the shared flagLargeAutoCollect helper, scenario 5) every off-session
		// charge site uses, so a threshold edit landing concurrently with a charge is
		// honored identically everywhere. The flag reflects the FULL combined debit
		// (base + every co-created overage line).
		acct, err := s.store.AccountCollection(ctx, locked.AccountID)
		if err != nil {
			return nil, billing.Internal("account collection lookup failed", err)
		}

		// Mirror the window the amount priced — [creation day, coverage end)
		// normally; narrowed to the straddled period alone under the D1d straddle
		// rule — so mirror and amount agree by construction.
		cents = c
		pc := &ProrationCharge{
			InvoiceID: inv.ID,
			Cents:     c,
			Invoice: InvoiceMirror{
				AccountID:          locked.AccountID,
				StripeInvoiceID:    inv.ID,
				Status:             inv.Status,
				AmountDueCents:     inv.AmountDue,
				AmountPaidCents:    inv.AmountPaid,
				Currency:           chargeCurrency,
				PeriodStart:        coverageStart,
				PeriodEnd:          coverageEnd,
				IsLargeAutoCollect: flagLargeAutoCollect(prorated+overageTotalMicros, acct),
			},
			// Freeze what was billed keyed by the FULL anchored period_start (the
			// display identity, migration 028); BaseMicros is the prorated BASE amount
			// for the CREATION period only (the co-created overage rides the
			// per-module timers, not the base snapshot).
			Snapshot: AppBaseSnapshot{
				AppID:       locked.AppID,
				PeriodStart: periodStart,
				PeriodEnd:   periodEnd,
				ModuleCount: locked.CreatedModuleCount,
				BaseMicros:  creationPeriodMicros,
			},
			TimerCharges: timerCharges,
		}
		if creationPeriodClosed {
			// D1d straddle (wave 2, D4): only the straddled period was billed —
			// its snapshot is the primary one; the forgiven creation period gets
			// no row (nothing was charged for it).
			pc.Snapshot = AppBaseSnapshot{
				AppID:       locked.AppID,
				PeriodStart: periodEnd,
				PeriodEnd:   coverageEnd,
				ModuleCount: locked.CreatedModuleCount,
				BaseMicros:  usage.BaseFeeMicros,
			}
		} else if straddle {
			// The straddled period was billed IN FULL on this same invoice — freeze
			// its own snapshot row so the display shows that period's base as
			// charged (the boundary leg excluded the app there and writes nothing).
			pc.StraddleSnapshot = &AppBaseSnapshot{
				AppID:       locked.AppID,
				PeriodStart: periodEnd,
				PeriodEnd:   coverageEnd,
				ModuleCount: locked.CreatedModuleCount,
				BaseMicros:  usage.BaseFeeMicros,
			}
		}
		return pc, nil
	})
	if err != nil {
		// A billing.Error from the charge callback (Stripe / conversion) is already
		// classified — surface it verbatim; anything else is a store/tx failure.
		if _, ok := err.(*billing.Error); ok {
			return nil, err
		}
		return nil, billing.Internal("locked creation-proration charge failed", err)
	}

	switch outcome {
	case ProrationLockedCharged:
		return &ProrationResult{AppID: appID, Status: ProrationStatusCharged, ProrationInvoiceID: invID, ProrationCents: cents}, nil
	case ProrationLockedAlreadyCharged:
		return &ProrationResult{AppID: appID, Status: ProrationStatusAlreadyCharged, ProrationInvoiceID: invID}, nil
	case ProrationLockedDeleted:
		return &ProrationResult{AppID: appID, Status: ProrationStatusDeleted}, nil
	case ProrationLockedNotFound:
		return &ProrationResult{AppID: appID, Status: ProrationStatusNotFound}, nil
	default: // ProrationLockedNoCharge
		return &ProrationResult{AppID: appID, Status: ProrationStatusNoCharge}, nil
	}
}

// SweepProrationsResult tallies one SweepCreationProrations batch for the
// cmd/billing-cycle log line + exit code.
type SweepProrationsResult struct {
	Pending int // apps past grace with an unarmed guard (the work list size)
	Charged int // creation-proration invoices minted this sweep
	Skipped int // legitimate no-charge outcomes (deleted / unactivated / no-PM / already / 0¢)
	Failed  int // per-app errors (charge failures) — retried next sweep
}

// SweepCreationProrations charges the creation-period base for every app that has
// survived the grace window as of `at`: it lists the pending apps (created_at <=
// at − GraceDays, guard unarmed, not deleted) and runs ChargeCreationProration on
// each. Idempotent + resumable: an app charged on a prior sweep drops out of the
// work list (guard armed), and a per-app failure is counted but never aborts the
// batch (the next sweep retries it through the same deterministic Stripe keys).
func (s *Service) SweepCreationProrations(ctx context.Context, at time.Time) (*SweepProrationsResult, error) {
	if at.IsZero() {
		return nil, billing.InvalidInput("sweep instant required")
	}
	createdBefore := at.UTC().AddDate(0, 0, -usage.GraceDays)
	appIDs, err := s.store.AppsPendingProration(ctx, createdBefore)
	if err != nil {
		return nil, billing.Internal("list pending prorations failed", err)
	}

	res := &SweepProrationsResult{Pending: len(appIDs)}
	for _, id := range appIDs {
		r, err := s.ChargeCreationProration(ctx, id)
		if err != nil {
			slog.ErrorContext(ctx, "creation-proration charge failed",
				"app_id", id, "error", err)
			res.Failed++
			continue
		}
		if r.Status == ProrationStatusCharged {
			res.Charged++
		} else {
			res.Skipped++
		}
		slog.InfoContext(ctx, "creation-proration",
			"app_id", id, "status", string(r.Status),
			"invoice_id", r.ProrationInvoiceID, "cents", r.ProrationCents)
	}
	return res, nil
}

// appProrationItemIdemKey / appProrationInvoiceIdemKey /
// appProrationFinalizeIdemKey build the deterministic Stripe Idempotency-Keys
// for the creation-proration charge's draft→items→finalize flow. The APP id is
// the stable charge identity (each app prorates at most once — the one-shot
// proration_invoice_id guard), so a re-attempt (a retried sweep after a crash
// between the Stripe calls and the guard-arm) reuses the SAME Stripe objects
// and can never double-charge even before the guard is armed.
func appProrationItemIdemKey(appID uuid.UUID) string     { return "app-ii-" + appID.String() }
func appProrationInvoiceIdemKey(appID uuid.UUID) string  { return "app-inv-" + appID.String() }
func appProrationFinalizeIdemKey(appID uuid.UUID) string { return "app-fin-" + appID.String() }

// appProrationChargeRef is the deterministic ms_charge_ref metadata anchor for
// one app's combined creation invoice — what FindInvoiceByRef recovers by.
func appProrationChargeRef(appID uuid.UUID) string { return "app-proration:" + appID.String() }
