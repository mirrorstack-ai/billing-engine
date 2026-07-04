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
type ProrationCharge struct {
	InvoiceID string
	Cents     int64
	Invoice   InvoiceMirror
	Snapshot  AppBaseSnapshot
}

// ChargeCreationProration charges (once) the creation-period base proration for
// ONE app that has survived the grace window — the shared charge leg the sweep
// invokes per pending app. It is idempotent (the one-shot proration_invoice_id
// guard) and race-safe against a concurrent soft-delete (the FOR UPDATE section).
//
// The amount is the SAME as the pre-grace RegisterApp charge:
//
//	ProratedBaseMicros(AppBaseFeeMicros(BaseFeeMicros, created_module_count),
//	                   created_at, the anchored period CONTAINING created_at)
//
// anchored to the TRUE created_at (NOT now), so the app pays only for the whole
// UTC days it existed in its creation period, creation day inclusive — grace
// only delayed WHEN this fires. created_module_count (migration 030) is the
// module count FROZEN at RegisterApp time: SyncAppModules can move the LIVE
// module_count at any point during (or after) grace, but the historical
// creation-period days billed here must price at the tier that actually
// applied on those days, never whatever count happens to read at sweep time.
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
	// Deleted within grace (or after) → never charged (D1e). The locked section
	// re-checks this authoritatively; this is the cheap early-out.
	if app.Deleted {
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
	_, periodEnd := billingperiod.AnchoredPeriodWindow(app.CreatedAt.UTC(), billingperiod.AnchorDay(activatedAt))
	if !activatedAt.Before(periodEnd) {
		if err := s.store.SetAppProrationSkipped(ctx, appID); err != nil {
			return nil, billing.Internal("mark proration permanently skipped failed", err)
		}
		return &ProrationResult{AppID: appID, Status: ProrationStatusPeriodClosed}, nil
	}

	// PM gate (D1d), same posture as the boundary spine: activated but no
	// usable default PM is skipped and re-attempted next sweep (unlike the
	// period-closed case above, "no PM right now" is not itself evidence the
	// account was ever ineligible for this specific period, so it stays a
	// transient, retried skip rather than a permanent one — see the judgment
	// call noted in the PR description for the limits of this).
	hasPM, err := s.store.HasUsableDefaultPM(ctx, app.AccountID)
	if err != nil {
		return nil, billing.Internal("usable PM check failed", err)
	}
	if !hasPM {
		return &ProrationResult{AppID: appID, Status: ProrationStatusNoPM}, nil
	}
	if s.stripe == nil {
		return nil, billing.Internal("ChargeCreationProration requires a Stripe client", nil)
	}
	custID, err := s.store.AccountStripeCustomer(ctx, app.AccountID)
	if err != nil {
		return nil, billing.Internal("stripe customer lookup failed", err)
	}
	if custID == "" {
		// A usable PM implies a Customer (same anomaly posture as the spine).
		return nil, billing.Internal("account has a usable PM but no Stripe customer id", nil)
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
		// Priced off created_module_count — the count FROZEN at RegisterApp time
		// — never the live module_count, which SyncAppModules may have moved
		// during (or since) grace (finding 1: no retroactive tier drift).
		prorated := usage.ProratedBaseMicros(
			usage.AppBaseFeeMicros(usage.BaseFeeMicros, locked.CreatedModuleCount),
			locked.CreatedAt, periodStart, periodEnd,
		)
		c, err := centsFromMicros(prorated)
		if err != nil {
			return nil, billing.Internal("micros to cents conversion failed", err)
		}
		if c == 0 {
			return nil, nil // rounds to 0 cents → nothing to invoice (guard stays unarmed)
		}

		desc := fmt.Sprintf("MirrorStack app base fee (prorated) — app %s", locked.AppID)
		if _, err := s.stripe.CreateInvoiceItem(ctx, custID, c, chargeCurrency, desc, appProrationItemIdemKey(locked.AppID)); err != nil {
			return nil, billing.StripeError("proration invoice item failed", err)
		}
		inv, err := s.stripe.CreateInvoice(ctx, custID, true /* autoAdvance */, appProrationInvoiceIdemKey(locked.AppID))
		if err != nil {
			return nil, billing.StripeError("proration invoice failed", err)
		}

		// Mirror the PARTIAL window [creation day, period end) — the same coverage
		// start ProratedBaseMicros priced, so mirror and amount agree by construction.
		partialStart := usage.ProrationCoverageStart(locked.CreatedAt, periodStart)
		cents = c
		return &ProrationCharge{
			InvoiceID: inv.ID,
			Cents:     c,
			Invoice: InvoiceMirror{
				AccountID:       locked.AccountID,
				StripeInvoiceID: inv.ID,
				Status:          inv.Status,
				AmountDueCents:  inv.AmountDue,
				AmountPaidCents: inv.AmountPaid,
				Currency:        chargeCurrency,
				PeriodStart:     partialStart,
				PeriodEnd:       periodEnd,
			},
			// Freeze what was billed keyed by the FULL anchored period_start (the
			// display identity, migration 028); BaseMicros is the prorated amount.
			Snapshot: AppBaseSnapshot{
				AppID:       locked.AppID,
				PeriodStart: periodStart,
				PeriodEnd:   periodEnd,
				ModuleCount: locked.CreatedModuleCount,
				BaseMicros:  prorated,
			},
		}, nil
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

// appProrationItemIdemKey / appProrationInvoiceIdemKey build the deterministic
// Stripe Idempotency-Keys for the creation-proration charge. The APP id is the
// stable charge identity (each app prorates at most once — the one-shot
// proration_invoice_id guard), so a re-attempt (a retried sweep after a crash
// between the Stripe call and the guard-arm) reuses the SAME Stripe objects and
// can never double-charge even before the guard is armed.
func appProrationItemIdemKey(appID uuid.UUID) string    { return "app-ii-" + appID.String() }
func appProrationInvoiceIdemKey(appID uuid.UUID) string { return "app-inv-" + appID.String() }
