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
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// ProrationStatus classifies one ChargeCreationProration outcome for the sweep's
// tally + per-app log line.
//
// No outcome mints an invoice any more: a fresh charge is SEALED
// (ProrationStatusProposed) or drawn from the credit wallet
// (ProrationStatusWalletCharged), and ProrationStatusCharged now means only
// that an invoice a LEGACY run had already finalized was adopted into this
// database. The rest are legitimate no-charge outcomes (D1d/D1e) or the
// idempotent guard.
type ProrationStatus string

const (
	// ProrationStatusCharged: a creation-proration invoice a legacy run had
	// already finalized at the provider was ADOPTED on this call — mirrored,
	// its timers marked against it, and the one-shot guard armed with its id.
	// Nothing was created or collected here; this leg has no such call left.
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

	// ProrationStatusProposed: the intent cutover was armed, so this
	// attempt's charge was SEALED AS AN INTENT and nothing was collected.
	//
	// Distinct from no_charge, which means there was nothing to bill. This
	// attempt had an amount and the intent rail now owns it, so a caller
	// that conflated the two would report a real charge as absent.
	ProrationStatusProposed ProrationStatus = "proposed"
	// ProrationStatusNotFound: no roster row for the app id (never registered).
	ProrationStatusNotFound ProrationStatus = "not_found"
	// ProrationStatusWalletCharged (credit mode, billing-engine #99): the
	// creation proration was SETTLED from the credit wallet (an append-only
	// ledger draw), not a Stripe invoice — the credits-mode analogue of
	// ProrationStatusCharged. ProrationInvoiceID carries the
	// synthetic wallet charge reference that armed the one-shot guard.
	ProrationStatusWalletCharged ProrationStatus = "wallet_charged"
	// ProrationStatusWalletUnsettled (credit mode, billing-engine #99): the
	// wallet transaction could not fully settle the creation base. Nothing was
	// drawn or armed, and this call does not fall through to Stripe; the next
	// sweep selects the rail again from the durable billing mode.
	ProrationStatusWalletUnsettled ProrationStatus = "skipped_wallet_unsettled"
	// ProrationStatusPeriodClosed: the account only activated at/after the
	// app's anchored creation period had already closed — charging it now
	// would be a retroactive catch-up (D1d). PERMANENTLY skipped: the
	// proration_skipped_at marker is armed so the app never resurfaces on a
	// later sweep.
	ProrationStatusPeriodClosed ProrationStatus = "skipped_period_closed"
)

// ProrationResult reports what ChargeCreationProration did. ProrationInvoiceID is
// set on ProrationStatusCharged (the new invoice), ProrationStatusWalletCharged
// (the wallet settlement reference), and ProrationStatusAlreadyCharged (the
// pre-existing one); ProrationCents only on a fresh charge.
type ProrationResult struct {
	AppID              uuid.UUID
	Status             ProrationStatus
	ProrationInvoiceID string
	ProrationCents     int64
	// IntentDigest is the intent this attempt's charge was sealed as. It is
	// the link from the proration row to the document that replaced its
	// charge, and it is set on every fresh Stripe-rail charge.
	//
	// Empty on the two outcomes that are not a seal: a wallet settlement
	// (ProrationStatusWalletCharged) and the adoption of an in-flight legacy
	// invoice (ProrationStatusCharged).
	IntentDigest string
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
	// ProrationWalletShort (credit mode, billing-engine #99): the wallet could
	// not fully cover the creation proration. NOTHING was drawn and the guard is
	// UNARMED; this call stays unsettled instead of falling through to Stripe.
	// Reserved for an all-or-nothing credits allocator that cannot cover; the
	// current credits policy normally covers through its unsecured remainder.
	ProrationWalletShort
	// ProrationWalletDeferToStripe (credit mode, billing-engine #99): the locked
	// app row shows a prior Stripe attempt, or the locked account mode is no
	// longer credits. The wallet performs no draw and defers the full charge to
	// the Stripe rail.
	ProrationWalletDeferToStripe
)

// ProrationCharge is the persistence payload the charge callback returns from
// inside the locked transaction: the ADOPTED Stripe invoice to mirror (one a
// legacy run finalized before it crashed — nothing here creates one), the base
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
	// ResolvedAt is the captured terminal persistence instant for the durable
	// combined attempt and all of its frozen timer guards.
	ResolvedAt time.Time
	Invoice    InvoiceMirror
	Snapshot   AppBaseSnapshot
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

// ProrationWalletCharge is the credit-wallet-settled analogue of ProrationCharge
// (billing-engine #99, credit mode). For a credits-mode account the creation
// proration base is DEBITED from the append-only credit wallet instead of billed
// to Stripe: AmountMicros is drawn from the wallet and the one-shot guard is
// armed with Ref (a synthetic wallet reference, never a Stripe invoice id).
// Snapshot / StraddleSnapshot freeze the SAME display base rows the Stripe leg
// writes. The draw + snapshots + guard arm all commit in ONE store transaction.
type ProrationWalletCharge struct {
	// Ref arms apps.proration_invoice_id in place of a Stripe invoice id.
	Ref string
	// AmountMicros is the prorated creation base only.
	AmountMicros int64
	Snapshot     AppBaseSnapshot
	// StraddleSnapshot freezes a boundary-straddled period billed in full on the
	// same debit — nil otherwise, mirroring ProrationCharge.StraddleSnapshot.
	StraddleSnapshot *AppBaseSnapshot
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
//
// 🔴 THE CHARGE ITSELF IS NO LONGER COLLECTED HERE. The amount above is
// derived exactly as it always was, then SEALED AS AN INTENT for something
// holding the write port to collect. The one thing this leg still finishes at
// the provider is an invoice a legacy run finalized before it crashed, and it
// finishes that by mirroring it, not by writing anything back.
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
		if app.ProrationAttempted {
			return nil, billing.Internal(fmt.Sprintf(
				"proration recovery: app %s has both a legacy attempt marker and skip guard; exact Stripe ownership is unknown",
				appID,
			), ErrCombinedProrationAttemptUnknown)
		}
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

	// UNBILLED org roster row (NULL account, migration 041): no payer exists
	// yet. The sweep's work list (AppsPendingProration) already excludes these;
	// this guards the direct path. Same transient posture as the unactivated
	// skip — the RepointOrgUsage attach sweep backfills account_id and a later
	// sweep evaluates the app normally.
	if app.AccountID == uuid.Nil {
		return &ProrationResult{AppID: appID, Status: ProrationStatusUnactivated}, nil
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
	if _, periodEnd, closed := periodClosedByActivation(app.CreatedAt, activatedAt); closed && !app.ProrationAttempted {
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

	// CREDITS-MODE CREATION SETTLEMENT (billing-engine #99). A credits-mode account
	// (durable ms_billing.accounts.billing_mode = 'credits') settles its creation-period
	// base through the credit wallet and NEVER creates a Stripe invoice. Standard accounts —
	// even with a gifted balance — keep the Stripe creation path here; their wallet credit is
	// applied at the boundary spine (charge.go), NOT at creation. This asymmetry is deliberate:
	// the rail is keyed off the DURABLE billing_mode, not a transient balance, so a credits
	// account can never flip to Stripe mid-retry when its balance drains. Only a FRESH charge
	// routes here — an app whose prior attempt already reached Stripe (proration_attempted_at
	// re-checked UNDER the app-row lock in the store) defers to the Stripe recovery leg below,
	// so a mid-flight mode flip can never draw the wallet beside money that may already have moved.
	// The whole block is dark unless the credit-wallet flag + schema capability are BOTH ready.
	if !app.ProrationAttempted {
		walletStart, walletEnd := billingperiod.AnchoredPeriodWindow(app.CreatedAt.UTC(), billingperiod.AnchorDay(activatedAt))
		walletState, walletAllowed, err := s.creditWalletChargeState(ctx, app.AccountID, walletStart, walletEnd)
		if err != nil {
			return nil, billing.Internal("wallet route classification failed", err)
		}
		if walletAllowed && walletState.Mode == CreditBillingModeCredits {
			res, deferToStripe, err := s.chargeCreationProrationFromWallet(ctx, app, activatedAt)
			if err != nil {
				return nil, err
			}
			if !deferToStripe {
				return res, nil
			}
			// The defer outcome covers two distinct locked facts: a prior Stripe
			// attempt (recovery authoritative) or a credits→standard mode change
			// (fresh Stripe charge). Re-read the app marker instead of inventing
			// attempted=true for the latter; migration-050 intentionally treats a
			// marker without a header as legacy/unknown.
			latest, found, err := s.store.AppMirror(ctx, appID)
			if err != nil {
				return nil, billing.Internal("app mirror re-read after wallet defer failed", err)
			}
			if !found {
				return &ProrationResult{AppID: appID, Status: ProrationStatusNotFound}, nil
			}
			app = latest
		}
	}

	if s.stripe == nil {
		return nil, billing.Internal("ChargeCreationProration requires a Stripe client", nil)
	}
	combinedStripe, ok := s.stripe.(billingstripe.CombinedProrationClient)
	if !ok {
		return nil, billing.Internal("Stripe client lacks combined-proration resource-truth support", nil)
	}

	// A legacy app-level marker alone cannot prove which co-created timer lines
	// the crashed request owned. Resolve the migration-050 header BEFORE any
	// Stripe recovery read and fail closed when it is absent or inconsistent.
	var preflightAttempt *CombinedProrationAttempt
	if app.ProrationAttempted {
		attempt, found, err := s.store.CombinedProrationAttempt(ctx, appID)
		if err != nil {
			return nil, billing.Internal("combined proration attempt lookup failed", err)
		}
		if !found {
			return nil, billing.Internal(fmt.Sprintf(
				"proration recovery: app %s has a legacy attempt marker without exact combined ownership; ops resolution required",
				appID,
			), ErrCombinedProrationAttemptUnknown)
		}
		if attempt.ResolvedInvoiceID != "" || attempt.Shape.AccountID != app.AccountID {
			return nil, billing.Internal(fmt.Sprintf(
				"proration recovery: app %s combined attempt disagrees with its unarmed app guard",
				appID,
			), nil)
		}
		preflightAttempt = &attempt
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
		if preflightAttempt == nil || preflightAttempt.ChargeFundingAccountID == uuid.Nil {
			return nil, billing.Internal("app has an attempted proration charge without a pinned funding account", nil)
		}
		custID, err = s.store.AccountStripeCustomer(ctx, preflightAttempt.ChargeFundingAccountID)
		if err != nil {
			return nil, billing.Internal("stripe customer lookup failed", err)
		}
		if custID == "" {
			return nil, billing.Internal("app has an attempted proration charge but the funding account has no Stripe customer id", nil)
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

	// The charge callback runs AFTER the row lock is released
	// (ChargeProrationLocked). It freezes exact timer ownership + request shape
	// in a second short transaction, then and only then reaches Stripe.
	var cents int64
	var concurrentlyResolvedInvoice string
	// proposedDigest is set when the cutover branch sealed this attempt as an
	// intent instead of collecting it. It is the link from the app's proration
	// row to the document that replaced its charge.
	var proposedDigest string
	atomicNoPM := false
	outcome, invID, err := s.store.ChargeProrationLocked(ctx, appID, func(locked AppMirror) (*ProrationCharge, error) {
		var candidate CombinedProrationChargeShape
		if locked.ProrationAttempted {
			// Recovery is driven solely by the immutable first-write header.
			// Recomputing before FreezeCombinedProrationAttempt reads that
			// header can incorrectly return zero after a later D1d gate change,
			// deletion, or other mutable roster drift and strand money that may
			// already have moved at Stripe.
			if preflightAttempt != nil {
				candidate = preflightAttempt.Shape
			} else {
				attempt, found, lookupErr := s.store.CombinedProrationAttempt(ctx, locked.AppID)
				if lookupErr != nil {
					return nil, billing.Internal("combined proration attempt re-read failed", lookupErr)
				}
				if !found {
					return nil, billing.Internal(fmt.Sprintf(
						"proration recovery: app %s has no exact combined ownership after the app lock",
						locked.AppID,
					), ErrCombinedProrationAttemptUnknown)
				}
				candidate = attempt.Shape
			}
		} else {
			var err error
			candidate, err = combinedProrationChargeShape(locked, activatedAt)
			if err != nil {
				return nil, err
			}
		}
		if candidate.BaseChargeCents == 0 {
			return nil, nil
		}
		attempt, claim, err := s.store.FreezeCombinedProrationAttempt(
			ctx,
			locked.AppID,
			s.nowFn().UTC(),
			candidate,
			s.creditWalletRailEnabled(locked.AccountID),
		)
		if err != nil {
			switch {
			case errors.Is(err, ErrCombinedProrationAttemptUnknown):
				return nil, billing.Internal(fmt.Sprintf(
					"proration recovery: app %s has no provable combined timer ownership; ops resolution required",
					locked.AppID,
				), err)
			case errors.Is(err, ErrCombinedProrationSelectionChanged):
				return nil, billing.Internal("combined proration timer selection changed before freeze; retry required", err)
			default:
				return nil, billing.Internal("freeze combined proration attempt failed", err)
			}
		}
		switch claim {
		case StripeRailWalletRequired:
			return nil, ErrCreditRailRequired
		case StripeRailNoPaymentMethod:
			atomicNoPM = true
			return nil, nil
		case StripeRailStale:
			return nil, nil
		}
		if attempt.ResolvedInvoiceID != "" {
			// Another worker passed ChargeProrationLocked's phase-1 read first,
			// then completed while this worker waited on the freeze locks.
			concurrentlyResolvedInvoice = attempt.ResolvedInvoiceID
			return nil, nil
		}
		if attempt.ChargeFundingAccountID == uuid.Nil {
			return nil, billing.Internal("combined proration attempt has no pinned funding account", nil)
		}
		custID, err = s.store.AccountStripeCustomer(ctx, attempt.ChargeFundingAccountID)
		if err != nil {
			return nil, billing.Internal("combined proration funding customer lookup failed", err)
		}
		if custID == "" {
			return nil, billing.Internal("combined proration funder has a usable PM but no Stripe customer id", nil)
		}

		// 🔴 THE CUTOVER, after the durable arming claim and after the
		// recovery read — the same position it has always held.
		//
		// It is no longer a BRANCH on an installed proposer: the legacy
		// collector below it is gone, so this leg proposes, always. The only
		// thing it does not propose is a charge that ALREADY MOVED MONEY at
		// the provider, and that is the guard below, not this one.
		//
		// moneyMayHaveMoved is narrower than the `recoveredInv == nil` it
		// replaces, deliberately. recoveredInv also covers an inert DRAFT, and
		// this leg's own recovery contract (wave 2, D6, ~90 lines above) says
		// what a draft is: nothing moved, and finalizing it is a FRESH
		// off-session debit with every gate applying, exactly as on a first
		// attempt. A fresh debit is precisely what the intent rail now owns.
		// The draft is created with auto_advance=false (inertDraftInvoiceParams),
		// so it cannot finalize or collect on its own — abandoning it strands
		// no charge and can double-bill nobody.
		if !moneyMayHaveMoved {
			sealed, perr := s.proposeCombinedProration(ctx, attempt)
			if perr != nil {
				return nil, perr
			}
			// 🔴 THE TERMINAL STAMP, so the app stops being re-swept.
			//
			// AppsPendingProration selects on proration_invoice_id AND
			// proration_skipped_at both being NULL. Without this the attempt
			// is re-derived on every sweep and seals a NEW intent each time,
			// for one charge — and on a disarm the legacy branch would then
			// mint a real Stripe invoice for a period the intent rail already
			// sealed. The domain and overage legs each stamp here; this is the
			// same act, and its absence was the defect.
			if merr := s.store.MarkCombinedProrationProposed(
				ctx, appID, s.nowFn().UTC(), "intent:"+sealed.Digest(),
			); merr != nil {
				return nil, billing.Internal("mark combined proration proposed failed", merr)
			}
			proposedDigest = sealed.Digest()
			return nil, nil
		}

		expectedTotalCents, err := combinedProrationTotalCents(attempt)
		if err != nil {
			return nil, billing.Internal("combined proration total cents overflow", err)
		}
		inv, landed, err := s.adoptFinalizedProrationInvoice(
			ctx,
			combinedStripe,
			custID,
			attempt,
			*recoveredInv,
			expectedTotalCents,
		)
		if err != nil {
			return nil, err
		}

		resolvedAt := s.nowFn().UTC()
		timerCharges := make([]ModuleTimerCharge, 0, len(attempt.TimerIDs))
		for _, timerID := range attempt.TimerIDs {
			item, ok := landed[combinedProrationTimerItemKey(timerID)]
			if !ok {
				return nil, billing.Internal(fmt.Sprintf(
					"combined proration invoice %s omitted frozen timer %s after reconciliation",
					inv.ID, timerID,
				), nil)
			}
			timerCharges = append(timerCharges, ModuleTimerCharge{
				TimerID:       timerID,
				ChargedAt:     resolvedAt,
				InvoiceID:     inv.ID,
				InvoiceItemID: item.ID,
			})
		}
		if len(attempt.TimerIDs) > 0 &&
			attempt.Shape.ModuleChargeMicros > (math.MaxInt64-attempt.Shape.BaseChargeMicros)/int64(len(attempt.TimerIDs)) {
			return nil, billing.Internal("combined proration raw amount overflows int64", nil)
		}
		totalMicros := attempt.Shape.BaseChargeMicros +
			attempt.Shape.ModuleChargeMicros*int64(len(attempt.TimerIDs))

		// Resolve the disclosure threshold only after Stripe succeeds, matching
		// every other off-session charge site.
		acct, err := s.store.AccountCollection(ctx, locked.AccountID)
		if err != nil {
			return nil, billing.Internal("account collection lookup failed", err)
		}
		cents = attempt.Shape.BaseChargeCents
		return &ProrationCharge{
			InvoiceID:  inv.ID,
			Cents:      attempt.Shape.BaseChargeCents,
			ResolvedAt: resolvedAt,
			Invoice: InvoiceMirror{
				AccountID:               attempt.Shape.AccountID,
				ChargeFundingAccountID:  attempt.ChargeFundingAccountID,
				ChargeFundingGeneration: attempt.ChargeFundingGeneration,
				StripeInvoiceID:         inv.ID,
				Status:                  inv.Status,
				AmountDueCents:          inv.AmountDue,
				AmountPaidCents:         inv.AmountPaid,
				Currency:                attempt.Shape.Currency,
				PeriodStart:             attempt.Shape.CoverageStart,
				PeriodEnd:               attempt.Shape.CoverageEnd,
				IsLargeAutoCollect:      flagLargeAutoCollect(totalMicros, acct),
			},
			Snapshot:         attempt.Shape.Snapshot,
			StraddleSnapshot: attempt.Shape.StraddleSnapshot,
			TimerCharges:     timerCharges,
		}, nil
	})
	if err != nil {
		if errors.Is(err, ErrCreditRailRequired) {
			walletResult, deferToStripe, walletErr :=
				s.chargeCreationProrationFromWallet(ctx, app, activatedAt)
			if walletErr != nil {
				return nil, walletErr
			}
			if !deferToStripe {
				return walletResult, nil
			}
			return nil, billing.Internal(
				"creation-proration rail changed again before wallet claim; retry required",
				ErrCreditRailRequired,
			)
		}
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
		if atomicNoPM {
			return &ProrationResult{AppID: appID, Status: ProrationStatusNoPM}, nil
		}
		if concurrentlyResolvedInvoice != "" {
			return &ProrationResult{
				AppID:              appID,
				Status:             ProrationStatusAlreadyCharged,
				ProrationInvoiceID: concurrentlyResolvedInvoice,
			}, nil
		}
		if proposedDigest != "" {
			// Sealed, not collected. Distinct from NoCharge, which means
			// there was nothing to bill: this attempt HAD an amount and the
			// intent rail now owns it.
			return &ProrationResult{
				AppID:        appID,
				Status:       ProrationStatusProposed,
				IntentDigest: proposedDigest,
			}, nil
		}
		return &ProrationResult{AppID: appID, Status: ProrationStatusNoCharge}, nil
	}
}

// combinedProrationChargeShape computes every immutable request/snapshot field
// before the store selects ownership. The store persists this first-write shape
// together with the exact timer IDs; every retry then consumes the persisted
// winner rather than re-running this math or reading a mutable app name.
func combinedProrationChargeShape(app AppMirror, activatedAt time.Time) (CombinedProrationChargeShape, error) {
	periodStart, periodEnd := billingperiod.AnchoredPeriodWindow(
		app.CreatedAt.UTC(),
		billingperiod.AnchorDay(activatedAt),
	)
	creationPeriodMicros := usage.ProratedBaseMicros(
		usage.BaseFeeMicros,
		app.CreatedAt,
		periodStart,
		periodEnd,
	)
	coverageEnd := periodEnd
	straddle := !moduleGraceExpiry(app.CreatedAt.UTC()).Before(periodEnd)
	if straddle {
		_, coverageEnd = billingperiod.AnchoredPeriodWindow(
			moduleGraceExpiry(app.CreatedAt.UTC()),
			billingperiod.AnchorDay(activatedAt),
		)
	}
	baseMicros := usage.CreationChargeBaseMicros(app.CreatedAt, periodStart, periodEnd)
	coverageStart := usage.ProrationCoverageStart(app.CreatedAt, periodStart)
	creationPeriodClosed := !activatedAt.Before(periodEnd)
	if creationPeriodClosed {
		creationPeriodMicros = 0
		baseMicros = usage.BaseFeeMicros
		coverageStart = periodEnd
	}
	baseCents, err := centsFromMicros(baseMicros)
	if err != nil {
		return CombinedProrationChargeShape{}, billing.Internal("micros to cents conversion failed", err)
	}

	moduleMicros := usage.ProratedBaseMicros(
		usage.ModuleOverageFeeMicros,
		app.CreatedAt,
		periodStart,
		periodEnd,
	)
	if straddle {
		moduleMicros += usage.ModuleOverageFeeMicros
	}
	if creationPeriodClosed {
		moduleMicros = usage.ModuleOverageFeeMicros
	}
	moduleCents, err := centsFromMicros(moduleMicros)
	if err != nil {
		return CombinedProrationChargeShape{}, billing.Internal("overage micros to cents conversion failed", err)
	}
	// A sub-cent timer line is not a Stripe charge identity. Keep both frozen
	// module fields zero so the timer stays available to Leg 1's zero-resolution
	// path instead of claiming ownership for a line that cannot be created.
	if moduleCents == 0 {
		moduleMicros = 0
	}

	snapshot := AppBaseSnapshot{
		AppID:       app.AppID,
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		ModuleCount: app.CreatedModuleCount,
		BaseMicros:  creationPeriodMicros,
	}
	var straddleSnapshot *AppBaseSnapshot
	if creationPeriodClosed {
		snapshot = AppBaseSnapshot{
			AppID:       app.AppID,
			PeriodStart: periodEnd,
			PeriodEnd:   coverageEnd,
			ModuleCount: app.CreatedModuleCount,
			BaseMicros:  usage.BaseFeeMicros,
		}
	} else if straddle {
		straddleSnapshot = &AppBaseSnapshot{
			AppID:       app.AppID,
			PeriodStart: periodEnd,
			PeriodEnd:   coverageEnd,
			ModuleCount: app.CreatedModuleCount,
			BaseMicros:  usage.BaseFeeMicros,
		}
	}
	label := appLineLabel(app.Name, app.AppID)
	return CombinedProrationChargeShape{
		AccountID:          app.AccountID,
		Currency:           chargeCurrency,
		BaseChargeMicros:   baseMicros,
		BaseChargeCents:    baseCents,
		ModuleChargeMicros: moduleMicros,
		ModuleChargeCents:  moduleCents,
		CoverageStart:      coverageStart,
		CoverageEnd:        coverageEnd,
		BaseDescription:    fmt.Sprintf("MirrorStack app base fee (prorated) — %s", label),
		ModuleDescription:  fmt.Sprintf("MirrorStack module overage (prorated) — %s", label),
		Snapshot:           snapshot,
		StraddleSnapshot:   straddleSnapshot,
	}, nil
}

func combinedProrationTotalCents(attempt CombinedProrationAttempt) (int64, error) {
	timerCount := int64(len(attempt.TimerIDs))
	if timerCount > 0 &&
		attempt.Shape.ModuleChargeCents > (math.MaxInt64-attempt.Shape.BaseChargeCents)/timerCount {
		return 0, errors.New("combined proration cents overflow")
	}
	return attempt.Shape.BaseChargeCents + attempt.Shape.ModuleChargeCents*timerCount, nil
}

const combinedProrationBaseItemKey = "base"

func combinedProrationTimerItemKey(timerID uuid.UUID) string {
	return "timer:" + timerID.String()
}

// combinedProrationExpectedItem is one line the frozen attempt says the
// provider invoice must carry. It is now a VERIFICATION shape only: the
// idempotency key and the item identity it also held were the arguments to
// CreateCombinedProrationInvoiceItem, and this leg creates no invoice items.
type combinedProrationExpectedItem struct {
	key         string
	amountCents int64
	currency    string
	description string
	period      billingstripe.LinePeriod
}

func combinedProrationExpectedItems(attempt CombinedProrationAttempt) []combinedProrationExpectedItem {
	period := billingstripe.LinePeriod{
		Start: attempt.Shape.CoverageStart,
		End:   attempt.Shape.CoverageEnd,
	}
	items := make([]combinedProrationExpectedItem, 0, len(attempt.TimerIDs)+1)
	items = append(items, combinedProrationExpectedItem{
		key:         combinedProrationBaseItemKey,
		amountCents: attempt.Shape.BaseChargeCents,
		currency:    attempt.Shape.Currency,
		description: attempt.Shape.BaseDescription,
		period:      period,
	})
	for _, timerID := range attempt.TimerIDs {
		items = append(items, combinedProrationExpectedItem{
			key:         combinedProrationTimerItemKey(timerID),
			amountCents: attempt.Shape.ModuleChargeCents,
			currency:    attempt.Shape.Currency,
			description: attempt.Shape.ModuleDescription,
			period:      period,
		})
	}
	return items
}

// validateCombinedProrationInvoiceItems proves the provider's invoice carries
// EXACTLY the frozen attempt's lines: every one of them, each at its frozen
// amount, currency, description, and period, and nothing else.
//
// It used to take a requireComplete flag, false while a draft was still being
// assembled. There is no assembly any more — the only invoice this leg reads is
// one a legacy run already finalized — so an incomplete set is always a
// disagreement between the provider and the frozen attempt.
func validateCombinedProrationInvoiceItems(
	attempt CombinedProrationAttempt,
	actual []billingstripe.InvoiceItem,
) (map[string]billingstripe.InvoiceItem, error) {
	expectedList := combinedProrationExpectedItems(attempt)
	expected := make(map[string]combinedProrationExpectedItem, len(expectedList))
	for _, item := range expectedList {
		expected[item.key] = item
	}
	landed := make(map[string]billingstripe.InvoiceItem, len(actual))
	for _, item := range actual {
		var key string
		switch item.CombinedProrationComponent {
		case billingstripe.CombinedProrationComponentAppBase:
			if item.CombinedProrationAppID != attempt.AppID.String() ||
				item.CombinedProrationTimerID != "" {
				return nil, fmt.Errorf(
					"invoice item %s carries an invalid combined base identity",
					item.ID,
				)
			}
			key = combinedProrationBaseItemKey
		case billingstripe.CombinedProrationComponentModuleOverage:
			if item.CombinedProrationAppID != attempt.AppID.String() ||
				item.CombinedProrationTimerID == "" {
				return nil, fmt.Errorf(
					"invoice item %s carries an invalid combined timer identity",
					item.ID,
				)
			}
			key = "timer:" + item.CombinedProrationTimerID
		default:
			return nil, fmt.Errorf(
				"invoice item %s has unknown/missing combined-proration metadata",
				item.ID,
			)
		}
		want, ok := expected[key]
		if !ok {
			return nil, fmt.Errorf(
				"invoice item %s claims unfrozen combined identity %s",
				item.ID, key,
			)
		}
		if _, duplicate := landed[key]; duplicate {
			return nil, fmt.Errorf(
				"combined invoice has duplicate resource identity %s",
				key,
			)
		}
		if item.ID == "" ||
			item.AmountCents != want.amountCents ||
			item.Currency != want.currency ||
			item.Description != want.description ||
			!item.Period.Start.Equal(want.period.Start) ||
			!item.Period.End.Equal(want.period.End) {
			return nil, fmt.Errorf(
				"invoice item %s does not match frozen shape for %s",
				item.ID, key,
			)
		}
		landed[key] = item
	}
	if len(landed) != len(expected) {
		return nil, fmt.Errorf(
			"combined invoice has %d of %d frozen items",
			len(landed), len(expected),
		)
	}
	return landed, nil
}

// adoptFinalizedProrationInvoice completes a combined creation-proration charge
// that ALREADY MOVED MONEY at the provider, and completes it in this
// database only.
//
// 🔴 IT WRITES NOTHING AT STRIPE, AND THAT IS THE WHOLE POINT.
//
// This is what is left of the legacy collector. The leg used to create a
// draft, attach its lines, and finalize; every one of those calls is gone,
// because every fresh charge is now sealed as an intent. What could not go is
// the case where a legacy run finalized an invoice and died before its
// terminal transaction: Stripe is collecting that invoice, or already has, and
// abandoning it would leave a charge on the customer's statement that this
// database has never heard of — no mirror, no snapshot, the one-shot guard
// unarmed, and the app swept again forever.
//
// So it reads the invoice back, proves it is the frozen attempt's invoice down
// to each line's identity and the total, and hands it to the caller to MIRROR.
// A draft cannot arrive here (the caller's moneyMayHaveMoved guard), and if one
// somehow does this refuses rather than finalizing it — nothing in this leg
// finalizes anything any more.
//
// The exception drains: once no app row carries an unresolved
// proration_attempted_at, this function has nothing left to adopt. That is one
// of the questions scripts/legacy-drop-preconditions.sql asks production.
func (s *Service) adoptFinalizedProrationInvoice(
	ctx context.Context,
	combinedStripe billingstripe.CombinedProrationClient,
	custID string,
	attempt CombinedProrationAttempt,
	recovered billingstripe.Invoice,
	expectedTotalCents int64,
) (billingstripe.Invoice, map[string]billingstripe.InvoiceItem, error) {
	if recovered.ID == "" {
		return billingstripe.Invoice{}, nil, billing.Internal("combined proration invoice id is empty", nil)
	}
	current, err := s.stripe.GetInvoice(ctx, recovered.ID)
	if err != nil {
		return billingstripe.Invoice{}, nil, billing.StripeError("combined proration invoice refresh failed", err)
	}
	if current.Status == "void" {
		return billingstripe.Invoice{}, nil, billing.Internal(fmt.Sprintf(
			"proration recovery: invoice %s is VOID; app %s needs ops resolution",
			current.ID, attempt.AppID,
		), nil)
	}
	if current.CustomerID != "" && current.CustomerID != custID {
		return billingstripe.Invoice{}, nil, billing.Internal(fmt.Sprintf(
			"proration recovery: invoice %s belongs to customer %s, expected %s",
			current.ID, current.CustomerID, custID,
		), nil)
	}
	if current.Status == "draft" {
		// It was not a draft when the caller read it, or the caller would have
		// proposed instead. Something un-finalized it, which no code here does.
		return billingstripe.Invoice{}, nil, billing.Internal(fmt.Sprintf(
			"proration recovery: invoice %s is a draft; this leg no longer finalizes, so app %s needs ops resolution",
			current.ID, attempt.AppID,
		), nil)
	}

	items, err := combinedStripe.ListInvoiceItems(ctx, current.ID)
	if err != nil {
		return billingstripe.Invoice{}, nil, billing.StripeError("combined proration invoice item list failed", err)
	}
	landed, err := validateCombinedProrationInvoiceItems(attempt, items)
	if err != nil {
		return billingstripe.Invoice{}, nil, billing.Internal(fmt.Sprintf(
			"proration recovery: invoice %s item truth is invalid",
			current.ID,
		), err)
	}
	if current.AmountDue != expectedTotalCents {
		return billingstripe.Invoice{}, nil, billing.Internal(fmt.Sprintf(
			"proration recovery: finalized invoice %s carries %d cents, expected frozen %d",
			current.ID, current.AmountDue, expectedTotalCents,
		), nil)
	}
	return current, landed, nil
}

// chargeCreationProrationFromWallet is ChargeCreationProration's credit-mode leg
// (billing-engine #99). It prices the creation proration EXACTLY as the Stripe
// callback above does — the prorated base, the boundary-straddle full period,
// and the D1d pre-activation narrowing — but DEBITS the prorated base only from
// the credit wallet instead of minting a Stripe invoice. Co-created over-module
// overage remains for the existing per-module overage sweep. The store draws the
// full base amount and, ONLY if the wallet fully covers it, freezes the display
// snapshot(s) and arms the one-shot guard, all in one transaction. created_at +
// the activation anchor are immutable, so this pricing is deterministic across
// retries.
func (s *Service) chargeCreationProrationFromWallet(ctx context.Context, app AppMirror, activatedAt time.Time) (*ProrationResult, bool, error) {
	// Window = the anchored period CONTAINING created_at (ADR 0005), derived from
	// created_at never from now — identical to the Stripe callback.
	periodStart, periodEnd := billingperiod.AnchoredPeriodWindow(app.CreatedAt.UTC(), billingperiod.AnchorDay(activatedAt))
	creationPeriodMicros := usage.ProratedBaseMicros(usage.BaseFeeMicros, app.CreatedAt, periodStart, periodEnd)

	// Coverage end = the END of the period the creation grace elapses into (the
	// coverage contract, review 2026-07-06) — the creation period itself unless the
	// grace straddles the boundary, exactly as the Stripe callback computes it.
	coverageEnd := periodEnd
	straddle := !moduleGraceExpiry(app.CreatedAt.UTC()).Before(periodEnd)
	if straddle {
		_, coverageEnd = billingperiod.AnchoredPeriodWindow(moduleGraceExpiry(app.CreatedAt.UTC()), billingperiod.AnchorDay(activatedAt))
	}
	prorated := usage.CreationChargeBaseMicros(app.CreatedAt, periodStart, periodEnd)
	// D1d straddle narrowing (wave 2, D4): only reachable here for a grace that
	// straddles into a post-activation period (the outer period-closed gate
	// permanently skips every other closed case) — forgive the creation period,
	// bill the straddled one in full.
	creationPeriodClosed := !activatedAt.Before(periodEnd)
	if creationPeriodClosed {
		creationPeriodMicros = 0
		prorated = usage.BaseFeeMicros
	}

	amountMicros := prorated
	if amountMicros <= 0 {
		// Rounds to nothing (unreachable for a survived app whose base ≥ $20) —
		// nothing to draw, guard stays unarmed.
		return &ProrationResult{AppID: app.AppID, Status: ProrationStatusNoCharge}, false, nil
	}

	// Freeze the SAME display base snapshot the Stripe leg writes (migration 028,
	// source='proration'), keyed by the FULL anchored period_start; the D1d/straddle
	// snapshot shape is identical to the Stripe callback's.
	snapshot := AppBaseSnapshot{
		AppID:       app.AppID,
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		ModuleCount: app.CreatedModuleCount,
		BaseMicros:  creationPeriodMicros,
	}
	var straddleSnapshot *AppBaseSnapshot
	if creationPeriodClosed {
		// Only the straddled period was billed — its snapshot is the primary one.
		snapshot = AppBaseSnapshot{
			AppID:       app.AppID,
			PeriodStart: periodEnd,
			PeriodEnd:   coverageEnd,
			ModuleCount: app.CreatedModuleCount,
			BaseMicros:  usage.BaseFeeMicros,
		}
	} else if straddle {
		straddleSnapshot = &AppBaseSnapshot{
			AppID:       app.AppID,
			PeriodStart: periodEnd,
			PeriodEnd:   coverageEnd,
			ModuleCount: app.CreatedModuleCount,
			BaseMicros:  usage.BaseFeeMicros,
		}
	}

	ref := appProrationWalletRef(app.AppID)

	outcome, armedRef, err := s.store.DrawCreationProrationFromWallet(ctx, app.AppID, ProrationWalletCharge{
		Ref:              ref,
		AmountMicros:     amountMicros,
		Snapshot:         snapshot,
		StraddleSnapshot: straddleSnapshot,
	})
	if err != nil {
		// A billing.Error from the store is already classified — surface verbatim;
		// anything else is a store/tx failure.
		if _, ok := err.(*billing.Error); ok {
			return nil, false, err
		}
		return nil, false, billing.Internal("wallet creation-proration draw failed", err)
	}

	switch outcome {
	case ProrationLockedCharged:
		// The store transaction has committed the debit and armed the one-shot
		// guard. Observe only this winning transition; replay/already/short
		// outcomes must never emit another standing push.
		s.observeWalletMutation(ctx, app.AccountID)
		cents, err := centsFromMicros(amountMicros)
		if err != nil {
			return nil, false, billing.Internal("micros to cents conversion failed", err)
		}
		return &ProrationResult{AppID: app.AppID, Status: ProrationStatusWalletCharged, ProrationInvoiceID: armedRef, ProrationCents: cents}, false, nil
	case ProrationWalletShort:
		return &ProrationResult{AppID: app.AppID, Status: ProrationStatusWalletUnsettled}, false, nil
	case ProrationWalletDeferToStripe:
		return nil, true, nil
	case ProrationLockedAlreadyCharged:
		return &ProrationResult{AppID: app.AppID, Status: ProrationStatusAlreadyCharged, ProrationInvoiceID: armedRef}, false, nil
	case ProrationLockedDeleted:
		return &ProrationResult{AppID: app.AppID, Status: ProrationStatusDeleted}, false, nil
	case ProrationLockedNotFound:
		return &ProrationResult{AppID: app.AppID, Status: ProrationStatusNotFound}, false, nil
	default: // ProrationLockedNoCharge
		return &ProrationResult{AppID: app.AppID, Status: ProrationStatusNoCharge}, false, nil
	}
}

// SweepProrationsResult tallies one SweepCreationProrations batch for the
// cmd/billing-cycle log line + exit code.
type SweepProrationsResult struct {
	Pending int // apps past grace with an unarmed guard (the work list size)
	Charged int // creation prorations SETTLED this sweep (wallet draws)
	// Proposed is the charges this sweep SEALED AS INTENTS — the normal
	// outcome for a Stripe-rail app now that this leg collects nothing.
	//
	// 🔴 It is not Skipped, and folding it in there was the reason for
	// splitting it out: a sweep that bills twelve apps would have reported
	// "charged 0, skipped 12", which reads as a quiet outage. Nor is it
	// Charged: nothing was collected, and a tally that says otherwise is the
	// same overstatement docs treat as a defect.
	Proposed int
	Skipped  int // legitimate no-charge outcomes (deleted / unactivated / no-PM / already / 0¢)
	Failed   int // per-app errors (charge failures) — retried next sweep
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
		switch r.Status {
		case ProrationStatusCharged, ProrationStatusWalletCharged:
			res.Charged++
		case ProrationStatusProposed:
			res.Proposed++
		default:
			res.Skipped++
		}
		slog.InfoContext(ctx, "creation-proration",
			"app_id", id, "status", string(r.Status),
			"invoice_id", r.ProrationInvoiceID, "cents", r.ProrationCents)
	}
	return res, nil
}

// appLineLabel renders the app suffix for an invoice-line description. It always
// carries the app id (stable machine attribution) and prepends the frozen display
// name (AppMirror.Name, migration 037 — a deleted app keeps its last-known name)
// when known, so a Stripe invoice line reads e.g. "My App (app <uuid>)" instead of
// a bare "app <uuid>".
func appLineLabel(name string, appID uuid.UUID) string {
	if name == "" {
		return fmt.Sprintf("app %s", appID)
	}
	return fmt.Sprintf("%s (app %s)", name, appID)
}

// The draft→items→finalize idempotency keys (app-ii- / app-inv- / app-fin-)
// were deleted with the calls that took them: this leg creates no draft,
// attaches no line, and finalizes nothing. A charge sealed as an intent gets
// its idempotency from the intent's own digest, and the one invoice this leg
// still reads is found by appProrationChargeRef below, not minted under a key.

// appProrationChargeRef is the deterministic ms_charge_ref metadata anchor for
// one app's combined creation invoice — what FindInvoiceByRef recovers by.
func appProrationChargeRef(appID uuid.UUID) string { return "app-proration:" + appID.String() }

// appProrationWalletRef is the deterministic wallet settlement reference that arms the
// one-shot creation-proration guard (apps.proration_invoice_id) when the charge
// is settled from the credit wallet rather than Stripe (billing-engine #99). The
// "wallet:" prefix keeps it unambiguously NOT a Stripe invoice id for any reader
// of the guard column; the usage query also uses the prefix to recover wallet
// settlements without fetching the value from Stripe.
func appProrationWalletRef(appID uuid.UUID) string { return "wallet:app-proration:" + appID.String() }
