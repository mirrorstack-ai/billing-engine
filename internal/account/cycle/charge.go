package cycle

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/collection"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// RunBillingCycle charges one account for one closed billing period — the
// USAGE (arrears) leg of the cycle (design §4 Axis 4). It is the charge spine:
//
//  1. InsertBillingRun — the FIRST idempotency layer. If a run for this exact
//     (account, period) window already exists, this call is a no-op (FirstRun=
//     false): the work was already done by the original run, so we NEVER
//     re-read arrears or re-charge. A re-fire / partial-failure resume lands
//     here and safely skips.
//  2. arrears = max(0, PeriodChargedTotal − allowanceMicros). The
//     allowance-netting MATH is implemented here with allowanceMicros as an
//     INPUT; v1 callers pass 0. (TODO: a dedicated subscription/tier PR sources
//     the allowance from the account's tier `included_allowance`.)
//  3. ADVANCE base leg (base-fee v1, owner spec 2026-07-05): the NEW period's
//     base fee, billed in advance on the same invoice = Σ over the account's
//     LIVE ms_billing.apps rows (deleted_at IS NULL — a deleted app stops
//     accruing base, D1e, though its usage arrears above still bill) that
//     EXISTED BEFORE the new period opened (created_at < the closed window's
//     period_end) of the FLAT BaseFeeMicros. An app created INSIDE the new
//     period is excluded — RegisterApp's creation-proration leg already charged
//     its new-period base (full or prorated); it joins the advance leg at the
//     NEXT boundary. module_count is snapshotted AT CHARGE TIME, and each billed
//     app-period is frozen into ms_billing.app_base_snapshots (migration 028) so
//     the display always shows what was invoiced. The allowance nets USAGE only,
//     never the base (it offsets ModuleUsage+Infra in the display math too). An
//     account with NO mirror rows (pre-backfill) gets base 0 — exactly the
//     pre-027 arrears-only invoice — until the api-platform backfill populates
//     the roster.
//     3b. ADVANCE overage leg (scenario 6, Leg 2): the NEW period's FULL $3-per-
//     module precharge for every ONGOING over-module — a live install timer that
//     is "over" per the live FIFO AND already charged at least once (survived its
//     grace in an earlier period, continuing into the new one). On the SAME
//     invoice, guarded by the SAME billing_run idempotency. A timer still inside
//     its own grace stays purely on Leg 1's timer and is never double-counted here.
//  4. arrears + base + overage == 0 → MarkBillingRun('invoiced') with NO
//     Stripe call. We NEVER auto-create a Stripe Customer with nothing to
//     charge (design §4 Axis 4).
//  5. no usable default PM → MarkBillingRun('skipped_no_pm'). The usage is
//     RETAINED (usage_aggregates untouched); the next cycle re-attempts. NOT a
//     failure, NOT lost usage.
//  6. otherwise CHARGE: convert micros → whole cents (round-half-up), create a
//     Stripe invoice item (deterministic Idempotency-Key ii-<run>) → a draft
//     invoice (charge_automatically + auto_advance, Idempotency-Key inv-<run>)
//     which Stripe finalizes + runs off-session against the default PM → mirror
//     the invoice into ms_billing.invoices → MarkBillingRun('invoiced',
//     stripe_invoice_id, total). The two deterministic Idempotency-Keys are the
//     SECOND idempotency layer: even if step 1's gate were somehow bypassed, a
//     re-run reuses the SAME Stripe objects and never double-charges.
//
// A charge error after the PM gate marks the run 'failed' (auditable; PR #7
// webhook reconciliation + risk-graded retry build on it) and returns the
// error. Money is integer micro-dollars → cents; never float.
func (s *Service) RunBillingCycle(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time, allowanceMicros int64) (*ChargeSummary, error) {
	if accountID == uuid.Nil {
		return nil, billing.InvalidInput("account_id required")
	}
	if periodStart.IsZero() || periodEnd.IsZero() || !periodEnd.After(periodStart) {
		return nil, billing.InvalidInput("period_end must be after period_start")
	}
	if allowanceMicros < 0 {
		return nil, billing.InvalidInput("allowance_micros must be non-negative")
	}
	if s.stripe == nil {
		// Charge leg requires a Stripe client; rollup-only wiring must not reach
		// here. Surface as INTERNAL (a wiring bug), not a silent no-op.
		return nil, billing.Internal("RunBillingCycle requires a Stripe client", nil)
	}

	runID, shouldCharge, err := s.store.InsertBillingRun(ctx, accountID, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("insert billing run failed", err)
	}
	if !shouldCharge {
		// Idempotency gate: the window already has an 'invoiced' (terminal-
		// success) run. Do nothing — no arrears read, no Stripe charge. Caller
		// treats FirstRun=false as success (already charged). A non-terminal run
		// (skipped_no_pm / failed / pending-died-mid-flight) is RECLAIMED by
		// InsertBillingRun instead and falls through here to re-attempt.
		return &ChargeSummary{FirstRun: false}, nil
	}

	// RISK-GRADED COLLECTION GATE (PR #9, design §7-A / billing-tiers §3). Load
	// the account's collection state up front. The off-session arrears leg may
	// only ship behind this gate (the GA gate); the run row already exists, so a
	// skip here is auditable as skipped_prepaid and the deterministic Stripe
	// idem keys stay stable if the mode later flips back to arrears.
	acct, err := s.store.AccountCollection(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("account collection lookup failed", err)
	}

	// Fast path: an account ALREADY in prepaid mode never reads aggregates or
	// touches Stripe — the off-session arrears leg is not permitted. The usage is
	// RETAINED (usage_aggregates untouched); the prepaid-credit wallet that would
	// settle it is a DEFERRED follow-up.
	if acct.Mode == BillingModePrepaid {
		if err := s.store.MarkBillingRun(ctx, runID, RunStatusSkippedPrepaid, "", 0); err != nil {
			return nil, billing.Internal("mark billing run (skipped_prepaid) failed", err)
		}
		return &ChargeSummary{FirstRun: true, Status: RunStatusSkippedPrepaid}, nil
	}

	total, err := s.store.PeriodChargedTotal(ctx, accountID, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("period charged total query failed", err)
	}

	// Allowance-netting: the meter never bills the first `allowanceMicros` of
	// usage. v1 passes 0, so arrears == total. Negative clamps to 0. The
	// allowance nets USAGE ONLY — the advance base below is never offset by it
	// (base-fee v1: the PaaS credit / allowance offsets ModuleUsage+Infra,
	// matching bill.go's display math).
	arrears := total - allowanceMicros
	if arrears < 0 {
		arrears = 0
	}

	// ADVANCE base leg: the NEW period's base fee for every LIVE app on the
	// roster that had JOINED the advance mechanism before the new period opened,
	// snapshotted at charge time (D1b/D1e — see the method comment). An app
	// created INSIDE the new period is EXCLUDED (its creation-proration leg
	// already charged that period's base — adding it here would double-bill on
	// the same-day cron race, and deterministically on a reclaimed
	// skipped_no_pm/failed run), and so is an app still INSIDE its creation
	// grace at the boundary (review 2026-07-06, H2): it hasn't survived grace —
	// an app deleted in grace is NEVER charged (scenario 1), so precharging its
	// next-period base would bill a full month for an app still deletable for
	// free — and when it survives, its creation charge covers through the END of
	// the period its grace elapses into, making this boundary's new period that
	// leg's coverage. Either way it joins the advance leg at the NEXT boundary.
	// Deleted apps drop out of the base but their usage arrears (already in
	// `total` above) still bill. Empty roster (pre-backfill) → base 0.
	apps, err := s.store.LiveAppsCreatedBefore(ctx, accountID, periodEnd, usage.GraceDays)
	if err != nil {
		return nil, billing.Internal("live app roster read failed", err)
	}
	// Each live app contributes ONLY its FLAT base. Module overage is billed
	// SEPARATELY below (the advance-overage / Leg 2 precharge), not folded into an
	// app's base — it rides per-module-instance grace timers (migration 033).
	var advanceBase int64
	for range apps {
		advanceBase += usage.BaseFeeMicros
	}

	// ADVANCE OVERAGE leg (scenario 6, Leg 2): the NEW period's $3-per-module
	// precharge for every ONGOING over-module — a live install timer that is both
	// "over" per the live FIFO AND already charged at least once (grace_charged_at
	// set), i.e. a module that survived its own grace in an earlier period and
	// continues into the new one. It is billed FULL (not prorated — the module
	// exists for the whole new period), on the SAME boundary invoice as arrears +
	// base, guarded by the SAME billing_run idempotency (keyed per-run, decided
	// per-module-row now). The coverage contract with the grace legs (review
	// 2026-07-06) — a timer counts iff installed_at < periodEnd (installed before
	// the new period opened; the same cutoff the advance-base leg applies, without
	// which a reclaimed skipped_no_pm/failed run double-bills a module whose own
	// grace charge already covered the new period), grace_expires_at < periodEnd
	// (a boundary-straddling grace's new period is Leg 1's coverage, never this
	// precharge's), and grace_resolved (charged — or resolved-uncharged under the
	// D1d period-closed posture, which forgives only the pre-activation install
	// period, never the periods after; the old grace_charged_at proxy exempted
	// those modules from ALL overage billing forever). Empty/pre-backfill → 0.
	overCount, err := s.store.CountOngoingOverModuleTimers(ctx, accountID, usage.IncludedModules, periodEnd)
	if err != nil {
		return nil, billing.Internal("ongoing over-module timer count failed", err)
	}
	advanceOverage := usage.ModuleOverageFeeMicros * int64(overCount)

	// The whole boundary invoice: closed period's netted usage arrears + the new
	// period's advance base + the new period's advance overage. The allowance nets
	// USAGE only (never base or overage — both ride ON TOP, matching bill.go).
	boundaryTotal := arrears + advanceBase + advanceOverage

	summary := &ChargeSummary{FirstRun: true, ArrearsMicros: arrears, AdvanceBaseMicros: advanceBase, AdvanceOverageMicros: advanceOverage}

	// Zero-skip: only when arrears, base AND overage are all zero (empty/zero
	// period with no live apps or ongoing over-modules) is there nothing to
	// invoice — mark invoiced with NO Stripe call, never auto-create a Customer
	// with nothing to charge. A zero total can never breach a limit/ceiling, so
	// this short-circuits ahead of the risk gate.
	if boundaryTotal == 0 {
		if err := s.store.MarkBillingRun(ctx, runID, RunStatusInvoiced, "", 0); err != nil {
			return nil, billing.Internal("mark billing run (zero arrears) failed", err)
		}
		summary.Status = RunStatusInvoiced
		return summary, nil
	}

	// SPEND CEILING (hard bill-shock cap, billing-tiers §3): the off-session leg
	// must NEVER auto-charge accrued arrears above the customer-set per-cycle
	// ceiling. A breach skips the charge (usage RETAINED) rather than charging a
	// shocking amount — checked against the NETTED USAGE arrears only, so the
	// allowance is credited first and the predictable, customer-visible base fee +
	// overage never trip a cap that exists to guard against USAGE surprises. (When
	// a breach skips, the whole invoice — base + overage included — waits for the
	// re-attempt, keeping one-invoice-per-boundary.) Independent of mode/credit-
	// limit (a hard cap, not a trust judgment).
	if collection.ExceedsSpendCeiling(toCollectionAccount(acct), arrears) {
		// skipped_ceiling, NOT skipped_prepaid: the ceiling is a per-cycle cap, not
		// a mode transition — the account stays in arrears mode and the next cycle
		// re-attempts once the ceiling is raised or the arrears net below it. The
		// distinct status keeps "spend-ceiling breach" legible apart from "prepaid
		// mode" in the audit trail.
		if err := s.store.MarkBillingRun(ctx, runID, RunStatusSkippedCeiling, "", 0); err != nil {
			return nil, billing.Internal("mark billing run (spend_ceiling) failed", err)
		}
		summary.Status = RunStatusSkippedCeiling
		return summary, nil
	}

	// RISK-JUDGE (design §7-A): tighten an arrears account toward prepaid on a
	// delinquency signal (an unpaid invoice, #7), accrual at/over the credit
	// limit, or a usage spike. A tighten PERSISTS the prepaid transition and
	// skips this cycle's off-session charge (usage RETAINED). v1 supplies no
	// usage-spike detector yet, so that input is conservative (spike=false).
	//
	// The charge cycle is TIGHTEN-ONLY (cleanStanding=false): it NEVER auto-relaxes
	// prepaid → arrears. The relax driver lives in the webhook (invoice.paid with
	// no remaining open delinquency → RelaxCollectionOnPaidInvoice) so a relax is
	// driven by a real successful-payment signal and is decoupled from charging —
	// an account is never relaxed and charged in the same beat. TODO(#9-followup):
	// wire a usage-spike anomaly signal + a sustained-clean-standing window.
	delinquent, err := s.store.HasUnpaidInvoice(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("delinquency lookup failed", err)
	}
	decision := collection.RiskAssess(
		toCollectionAccount(acct),
		collection.Signals{Delinquent: delinquent, AccruedArrearsMicros: arrears},
		false, // cleanStanding: the charge cycle never auto-relaxes (relax is webhook-driven)
	)
	if decision.Action == collection.ActionSkipPrepaid {
		summary.Status = RunStatusSkippedPrepaid
		if decision.ModeChanged {
			// A fresh tighten: persist the prepaid mode AND mark the run skipped in
			// ONE transaction (TightenAndMarkRun) so a crash can't strand the account
			// tightened with the run row still 'pending'.
			updated := acct
			updated.Mode = BillingMode(decision.DesiredMode)
			if err := s.store.TightenAndMarkRun(ctx, accountID, updated, runID, RunStatusSkippedPrepaid); err != nil {
				return nil, billing.Internal("tighten and mark billing run failed", err)
			}
			return summary, nil
		}
		// Already prepaid (no transition to persist): just mark the run skipped.
		if err := s.store.MarkBillingRun(ctx, runID, RunStatusSkippedPrepaid, "", 0); err != nil {
			return nil, billing.Internal("mark billing run (skipped_prepaid) failed", err)
		}
		return summary, nil
	}

	// No usable default PM (or the usable-PM-implies-Customer anomaly): skip
	// (usage RETAINED), re-attempt next cycle.
	custID, ok, err := s.resolveChargeableCustomer(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if !ok {
		if err := s.store.MarkBillingRun(ctx, runID, RunStatusSkippedNoPM, "", 0); err != nil {
			return nil, billing.Internal("mark billing run (skipped_no_pm) failed", err)
		}
		summary.Status = RunStatusSkippedNoPM
		return summary, nil
	}

	// Resolve the NEW period's window for the base snapshots BEFORE any Stripe
	// call (fail early on a lookup error). periodEnd is always the anchored
	// boundary (the straddle-clamp only ever moves the START), so the new
	// window is AnchoredPeriodWindow(periodEnd, anchorDay) = [periodEnd, next
	// boundary). The anchor day comes from activated_at (ADR 0005); the
	// boundary's own day-of-month is the defensive fallback for the
	// direct-call-on-an-unactivated-account case the cron never produces.
	var newPeriodEnd time.Time
	if len(apps) > 0 {
		anchorDay := billingperiod.AnchorDay(periodEnd)
		if activatedAt, activated, err := s.store.AccountActivation(ctx, accountID); err != nil {
			return nil, billing.Internal("account activation lookup failed", err)
		} else if activated {
			anchorDay = billingperiod.AnchorDay(activatedAt)
		}
		_, newPeriodEnd = billingperiod.AnchoredPeriodWindow(periodEnd, anchorDay)
	}

	// One invoice: closed period's netted usage arrears + the new period's advance
	// base + the new period's advance overage, converted micros → whole cents ONCE
	// at the Stripe boundary (a single deterministic rounding point for the total).
	cents, err := centsFromMicros(boundaryTotal)
	if err != nil {
		return nil, billing.Internal("micros to cents conversion failed", err)
	}
	withBase := advanceBase+advanceOverage > 0

	// FREEZE-OR-REUSE the boundary Stripe request (crash-safe idempotency,
	// migration 035). The idem keys ii-<run>/inv-<run> are STABLE across a reclaim
	// of this run, so the request sent under them must be stable too. A prior
	// attempt that already reached Stripe froze its computed (cents, withBase);
	// REUSE those frozen values rather than the ones just recomputed from LIVE
	// state — drift between the crash and this retry (a module uninstalled flipping
	// an over-module to included, an app deleted) could have moved the live total,
	// and re-sending the same idem key with a different amount/description is the
	// permanent Stripe idempotency-conflict stall this guards against (the bug
	// ee5043c fixed once for the account-wide model, whose freeze migration 033
	// dropped). Reconciled BEFORE the cents==0 short-circuit so a retry whose live
	// total collapsed to 0 still re-charges — and records — the non-zero amount the
	// crashed attempt already put through Stripe.
	if frozen, ok, err := s.store.BillingRunFrozenCharge(ctx, runID); err != nil {
		return nil, billing.Internal("frozen boundary charge lookup failed", err)
	} else if ok {
		cents = frozen.Cents
		withBase = frozen.WithBase
	}

	if cents == 0 {
		// A sub-half-cent arrears total (an advance base/overage, when present, is
		// always ≥ $3 and can never round to 0) — never call Stripe for $0. The
		// reuse above found no prior frozen charge (it would have set cents > 0), so
		// nothing was ever put through Stripe for this run.
		if err := s.store.MarkBillingRun(ctx, runID, RunStatusInvoiced, "", 0); err != nil {
			return nil, billing.Internal("mark billing run (zero cents) failed", err)
		}
		summary.Status = RunStatusInvoiced
		return summary, nil
	}

	// Freeze the amount + description this run will charge BEFORE the first Stripe
	// call. First-write-wins (a reclaim that already froze is a no-op — the reuse
	// above already adopted its values), so a crash after Stripe succeeds but before
	// MarkBillingRun commits leaves the frozen request durable for the retry.
	if err := s.store.FreezeBillingRunCharge(ctx, runID, FrozenBoundaryCharge{Cents: cents, WithBase: withBase}); err != nil {
		return nil, billing.Internal("freeze boundary charge failed", err)
	}
	summary.ChargedCents = cents

	// Charge. A failure after the PM gate marks the run 'failed' (auditable) and
	// returns the error.
	inv, err := s.charge(ctx, runID, custID, cents, withBase)
	if err != nil {
		if markErr := s.store.MarkBillingRun(ctx, runID, RunStatusFailed, "", 0); markErr != nil {
			// Both failed: surface the original charge error; the failed-mark is
			// best-effort (the run stays 'pending' and is auditable / resumable).
			return nil, billing.StripeError("charge failed and could not mark run failed", err)
		}
		summary.Status = RunStatusFailed
		return nil, billing.StripeError("charge failed", err)
	}

	// Post-hoc large-charge disclosure (migration 034, scenario 5): the charge
	// SUCCEEDED above; flag it as "large" iff the amount just charged (netted
	// usage arrears + advance base + advance overage, in micros — the SAME
	// boundaryTotal converted to cents above) exceeds the account's threshold
	// resolved AT CHARGE TIME (its per-account override, or the platform default
	// when NULL) via the shared flagLargeAutoCollect helper. Pure disclosure — it
	// changes NO charging behaviour, only surfaces the already-successful debit.
	//
	// The threshold is RE-RESOLVED HERE — immediately after the Stripe call
	// succeeded — rather than reusing `acct` loaded at the top of this
	// function (before the risk gate / PM check / the two Stripe HTTP calls
	// above). Resolving up front would let a threshold edit that lands
	// CONCURRENTLY with this charge be honored differently than
	// RegisterApp's creation-proration leg, which resolves its threshold
	// immediately after ITS Stripe charge succeeds (apps.go). Both charge
	// legs now resolve at the SAME point relative to the actual charge
	// (immediately after Stripe confirms success), so a concurrent edit
	// mid-charge is honored identically by both, never one way on the
	// boundary leg and another on the proration leg.
	postChargeAcct, err := s.store.AccountCollection(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("account collection lookup failed (post-charge threshold resolve)", err)
	}
	if err := s.store.UpsertInvoice(ctx, InvoiceMirror{
		AccountID:          accountID,
		StripeInvoiceID:    inv.ID,
		Status:             inv.Status,
		AmountDueCents:     inv.AmountDue,
		AmountPaidCents:    inv.AmountPaid,
		Currency:           chargeCurrency,
		PeriodStart:        periodStart,
		PeriodEnd:          periodEnd,
		IsLargeAutoCollect: flagLargeAutoCollect(boundaryTotal, postChargeAcct),
	}); err != nil {
		return nil, billing.Internal("invoice mirror upsert failed", err)
	}

	// Freeze what this boundary actually billed per app for the NEW window
	// (migration 028, source='advance'): the display's authoritative base for
	// the period, so a later SyncAppModules can never drift the shown base
	// away from this invoice. ON CONFLICT (app_id, period_start) DO NOTHING —
	// an existing proration row wins. A failure here leaves the run 'pending';
	// the reclaim re-charges through the SAME Stripe idem keys and re-writes
	// idempotently, so money and snapshots can never diverge.
	for _, a := range apps {
		if err := s.store.InsertAdvanceBaseSnapshot(ctx, AppBaseSnapshot{
			AppID:       a.AppID,
			PeriodStart: periodEnd, // the new period opens where the closed one ends
			PeriodEnd:   newPeriodEnd,
			ModuleCount: a.ModuleCount,
			BaseMicros:  usage.BaseFeeMicros, // FLAT per-app base (module overage rides per-module timers, migration 033)
		}); err != nil {
			return nil, billing.Internal("advance base snapshot insert failed", err)
		}
	}

	if err := s.store.MarkBillingRun(ctx, runID, RunStatusInvoiced, inv.ID, cents); err != nil {
		return nil, billing.Internal("mark billing run (invoiced) failed", err)
	}

	summary.Status = RunStatusInvoiced
	summary.StripeInvoiceID = inv.ID
	return summary, nil
}

// charge creates the Stripe invoice item + draft invoice for the boundary total
// (usage arrears + advance base + advance overage), with the two deterministic
// Idempotency-Keys (ii-<run>, inv-<run>) so a re-run reuses the same Stripe
// objects. withBase only widens the line DESCRIPTION when the total includes an
// advance base fee and/or ongoing-module overage — a pure-usage invoice keeps the
// historical line text. Returns the created invoice projection (id/status/amounts)
// for the mirror upsert.
func (s *Service) charge(ctx context.Context, runID uuid.UUID, custID string, cents int64, withBase bool) (billingstripe.Invoice, error) {
	desc := fmt.Sprintf("MirrorStack usage — run %s", runID)
	if withBase {
		desc = fmt.Sprintf("MirrorStack usage + app base fees — run %s", runID)
	}
	if _, err := s.stripe.CreateInvoiceItem(ctx, custID, cents, chargeCurrency, desc, invoiceItemIdemKey(runID)); err != nil {
		return billingstripe.Invoice{}, err
	}
	return s.stripe.CreateInvoice(ctx, custID, true /* autoAdvance */, invoiceIdemKey(runID))
}

// AccountsWithUsageEvents returns the accounts with raw usage_events in the
// [periodStart, periodEnd) window — the rollup-phase (phase 1) work list
// cmd/billing-cycle iterates before charging. A thin pass-through to the store.
func (s *Service) AccountsWithUsageEvents(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error) {
	if periodStart.IsZero() || periodEnd.IsZero() || !periodEnd.After(periodStart) {
		return nil, billing.InvalidInput("period_end must be after period_start")
	}
	accounts, err := s.store.AccountsWithUsageEvents(ctx, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("list accounts with usage events failed", err)
	}
	return accounts, nil
}

// AccountsWithUnbilledUsage returns the accounts with usage_aggregates in the
// [periodStart, periodEnd) window that have no SUCCESSFUL (invoiced) billing_run
// yet — the charge-phase (phase 2) work list cmd/billing-cycle iterates. A thin
// pass-through to the store so the binary depends only on the Service.
func (s *Service) AccountsWithUnbilledUsage(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error) {
	if periodStart.IsZero() || periodEnd.IsZero() || !periodEnd.After(periodStart) {
		return nil, billing.InvalidInput("period_end must be after period_start")
	}
	accounts, err := s.store.AccountsWithUnbilledUsage(ctx, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("list unbilled accounts failed", err)
	}
	return accounts, nil
}

// AccountHasLiveApps reports whether the account has at least one LIVE
// (non-deleted) ms_billing.apps roster row created BEFORE createdBefore (the
// NEW period's start, i.e. the closed window's period_end) — cmd/billing-
// cycle's gate for running the boundary charge on a NO-USAGE period: an
// account with live pre-existing apps still owes the advance base fee, while
// a no-usage, no-apps (pre-backfill) account keeps the historical skip (no
// billing_run at all). Apps created INSIDE the new period — or still inside
// their creation grace at the boundary (H2, same rule as the advance leg
// itself) — don't arm the gate: their new-period base is the creation-
// proration leg's, and they join the advance leg at the NEXT boundary —
// running a boundary for them here would only mint a zero-charge run row.
func (s *Service) AccountHasLiveApps(ctx context.Context, accountID uuid.UUID, createdBefore time.Time) (bool, error) {
	if accountID == uuid.Nil {
		return false, billing.InvalidInput("account_id required")
	}
	apps, err := s.store.LiveAppsCreatedBefore(ctx, accountID, createdBefore, usage.GraceDays)
	if err != nil {
		return false, billing.Internal("live app roster read failed", err)
	}
	return len(apps) > 0, nil
}

// ActivatedAccounts returns every card-bound account with its billing-period
// anchor instant — the per-account close driver's work list (each closes on its
// own card-binding day, ADR 0005). A thin pass-through to the store.
func (s *Service) ActivatedAccounts(ctx context.Context) ([]AccountAnchor, error) {
	accounts, err := s.store.ActivatedAccounts(ctx)
	if err != nil {
		return nil, billing.Internal("list activated accounts failed", err)
	}
	return accounts, nil
}

// LatestClosedPeriodEnd returns an account's newest billing_periods.period_end
// and whether one exists — the cutover straddle-clamp input. A thin pass-through
// to the store.
func (s *Service) LatestClosedPeriodEnd(ctx context.Context, accountID uuid.UUID) (time.Time, bool, error) {
	if accountID == uuid.Nil {
		return time.Time{}, false, billing.InvalidInput("account_id required")
	}
	end, found, err := s.store.LatestClosedPeriodEnd(ctx, accountID)
	if err != nil {
		return time.Time{}, false, billing.Internal("latest closed period lookup failed", err)
	}
	return end, found, nil
}

// invoiceItemIdemKey / invoiceIdemKey build the deterministic per-run Stripe
// Idempotency-Keys. The run id is the stable charge identity, so a re-fire
// (same run row) produces the SAME keys and Stripe returns the original objects
// instead of creating duplicates. The arrears charge is a SINGLE pooled line
// per run (Σ charged across all metrics), so the item key is ii-<run> (not
// per-metric) — matching the single combined invoice item this leg creates.
func invoiceItemIdemKey(runID uuid.UUID) string { return "ii-" + runID.String() }
func invoiceIdemKey(runID uuid.UUID) string     { return "inv-" + runID.String() }

// flagLargeAutoCollect is the ONE large-charge disclosure resolver (scenario 5,
// migration 034), called from EVERY off-session charge site — the boundary leg
// (charge.go), the creation/combined leg (proration.go), and the per-module grace
// leg (overage.go / Leg 1) — so the "large auto-collect" flag on a mirrored
// invoice row is computed identically everywhere and never reimplemented per leg.
// chargedMicros is the RAW pre-cents-conversion amount that just successfully
// charged; acct MUST be the account state read immediately AFTER the Stripe call
// succeeded (its per-account threshold override, or the platform default when
// nil), so a threshold edit landing concurrently with the charge is honored the
// same way at every site.
func flagLargeAutoCollect(chargedMicros int64, acct AccountCollection) bool {
	return collection.IsLargeAutoCollect(chargedMicros, acct.AutoCollectThresholdMicros)
}

// toCollectionAccount maps the cycle store's AccountCollection to the pure-policy
// collection.Account the risk-judge reasons over. Kept here so the collection
// package stays free of any persistence type.
func toCollectionAccount(a AccountCollection) collection.Account {
	return collection.Account{
		Mode:               collection.Mode(a.Mode),
		CreditLimitMicros:  a.CreditLimitMicros,
		HasSpendCeiling:    a.HasSpendCeiling,
		SpendCeilingMicros: a.SpendCeilingMicros,
	}
}
