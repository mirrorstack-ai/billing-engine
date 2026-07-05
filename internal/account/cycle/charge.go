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
//     period_end) of the FLAT BaseFeeMicros (module overage is account-wide
//     pooled now, migration 032 — no longer a per-app tier here). PLUS a
//     SEPARATE account-level POOLED overage term for the CLOSING period ($3 ×
//     max(0, Σ live-app module_count − included)), charged ONCE only when the
//     mid-period grace sweep did NOT already bill it (no
//     account_overage_snapshots row for the period — the double-charge guard);
//     when the boundary does charge it, it writes its own 'advance' snapshot.
//     An app created INSIDE the new period is excluded — RegisterApp's
//     creation-proration leg already charged its new-period base (full or
//     prorated); it joins the advance leg at the NEXT boundary. module_count is
//     snapshotted AT CHARGE TIME, and each billed app-period is frozen into
//     ms_billing.app_base_snapshots (migration 028) so the display always shows
//     what was invoiced. The allowance nets USAGE only, never the base/overage
//     (it offsets ModuleUsage+Infra in the display math too). An account with
//     NO mirror rows (pre-backfill) gets base 0 — exactly the pre-027
//     arrears-only invoice — until the api-platform backfill populates the
//     roster.
//  4. arrears + base + pooled overage == 0 (ALL zero) → MarkBillingRun('invoiced') with NO
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
	// roster that EXISTED BEFORE the new period opened (created_at < periodEnd
	// — the closed window's end IS the new period's start), snapshotted at
	// charge time (D1b/D1e — see the method comment). An app created INSIDE
	// the new period is EXCLUDED: RegisterApp's creation-proration leg already
	// charged its new-period base (full or prorated), so adding it here would
	// double-bill the same period — on the same-day cron race, and
	// deterministically whenever a skipped_no_pm/failed run is reclaimed after
	// the app registered. It joins the advance leg at the NEXT boundary.
	// Deleted apps drop out of the base but their usage arrears (already in
	// `total` above) still bill. Empty roster (pre-backfill) → base 0.
	apps, err := s.store.LiveAppsCreatedBefore(ctx, accountID, periodEnd)
	if err != nil {
		return nil, billing.Internal("live app roster read failed", err)
	}
	// Each live app contributes ONLY its FLAT base (migration 032 — module
	// overage is account-wide pooled, no longer a per-app tier). The pooled
	// module count for the ACCOUNT overage term below is Σ these apps' counts
	// (the same roster the base bills — apps created inside the new period are
	// excluded, so the closing period's pool is exact).
	var advanceBase int64
	var pooledModuleCount int
	for _, a := range apps {
		advanceBase += usage.BaseFeeMicros
		pooledModuleCount += a.ModuleCount
	}

	// SEPARATE account-level POOLED overage term for the closing period, charged
	// ONCE per account. If this period's pooled overage already has an
	// account_overage_snapshots row, its SOURCE decides how the boundary treats
	// it — the cross-leg double-charge guard (PR #47 review):
	//
	//   - source='grace': the mid-period sweep already CLAIMED it (pending or
	//     charged — a crash between Stripe succeeding and the row flipping to
	//     'charged' must NOT let the boundary independently charge it too, so
	//     ANY row, not just a settled one, excludes it here);
	//   - source='advance': THIS boundary run's OWN prior attempt (a crash
	//     between claiming the period and MarkBillingRun succeeding reclaims the
	//     SAME run id) — reuse the FROZEN over_count/charged_micros so the retry
	//     recomputes the IDENTICAL combined total and the deterministic
	//     ii-<run>/inv-<run> Idempotency-Keys never see a different amount
	//     (finding #2 — the boundary retry livelock).
	//
	// No row yet (grace never expired within the period, or overage started too
	// late for the sweep to run): compute the FULL pooled overage for the
	// account; it is CLAIMED (a 'pending' row written BEFORE Stripe is called)
	// further down, right before the charge — so every over-the-pool period is
	// billed exactly once, crash-safe.
	existingOverage, overageFound, err := s.store.AccountOverageSnapshot(ctx, accountID, periodStart)
	if err != nil {
		return nil, billing.Internal("account overage snapshot lookup failed", err)
	}
	var (
		advanceOverage        int64
		overCount             int
		overageAlreadyClaimed bool // true → no fresh claim-insert needed before charging
	)
	switch {
	case overageFound && existingOverage.Source == "grace":
		overageAlreadyClaimed = true
	case overageFound && existingOverage.Source == "advance":
		advanceOverage = existingOverage.ChargedMicros
		overCount = existingOverage.OverCount
		overageAlreadyClaimed = true
	default:
		overCount = pooledModuleCount - usage.IncludedModules
		if overCount > 0 {
			advanceOverage = usage.AccountOverageMicros(pooledModuleCount)
		}
	}

	summary := &ChargeSummary{FirstRun: true, ArrearsMicros: arrears, AdvanceBaseMicros: advanceBase, AccountOverageMicros: advanceOverage}

	// Zero-skip: only when arrears, base AND pooled overage are ALL zero
	// (empty/zero period with no live apps) is there nothing to invoice — mark
	// invoiced with NO Stripe call, never auto-create a Customer with nothing to
	// charge. A zero total can never breach a limit/ceiling, so this
	// short-circuits ahead of the risk gate.
	if arrears == 0 && advanceBase == 0 && advanceOverage == 0 {
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
	// allowance is credited first and the predictable, customer-visible base fee
	// never trips a cap that exists to guard against USAGE surprises. (When a
	// breach skips, the whole invoice — base included — waits for the re-attempt,
	// keeping one-invoice-per-boundary.) Independent of mode/credit-limit (a hard
	// cap, not a trust judgment).
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

	// No usable default PM: skip (usage RETAINED), re-attempt next cycle.
	hasPM, err := s.store.HasUsableDefaultPM(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("usable PM check failed", err)
	}
	if !hasPM {
		if err := s.store.MarkBillingRun(ctx, runID, RunStatusSkippedNoPM, "", 0); err != nil {
			return nil, billing.Internal("mark billing run (skipped_no_pm) failed", err)
		}
		summary.Status = RunStatusSkippedNoPM
		return summary, nil
	}

	custID, err := s.store.AccountStripeCustomer(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("stripe customer lookup failed", err)
	}
	if custID == "" {
		// A usable PM implies a Stripe Customer (a card can't attach without
		// one). An empty id here is an anomaly — surface it; never auto-create a
		// Customer on the charge path.
		return nil, billing.Internal("account has a usable PM but no Stripe customer id", nil)
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

	// CLAIM the boundary's pooled-overage line BEFORE calling Stripe (the
	// crash-safe marker — cycle/overage.go's header). Skipped when there is no
	// overage to charge, or it is already claimed (source='grace' owns it, or
	// this run's own reclaimed retry is reusing its own prior 'advance' claim).
	if advanceOverage > 0 && !overageAlreadyClaimed {
		inserted, err := s.store.InsertAccountOverageSnapshot(ctx, AccountOverageSnapshot{
			AccountID:     accountID,
			PeriodStart:   periodStart,
			PeriodEnd:     periodEnd,
			OverCount:     overCount,
			ChargedMicros: advanceOverage,
			Source:        "advance",
			Status:        OverageSnapshotPending,
		})
		if err != nil {
			return nil, billing.Internal("account overage snapshot claim failed", err)
		}
		if !inserted {
			// Lost the race to a concurrent claim (almost certainly the mid-period
			// grace sweep) — re-read and defer to the winner: never charge an
			// overage amount someone else already claimed.
			winning, _, err := s.store.AccountOverageSnapshot(ctx, accountID, periodStart)
			if err != nil {
				return nil, billing.Internal("account overage snapshot lookup failed", err)
			}
			advanceOverage, overCount = 0, 0
			if winning.Source == "advance" {
				advanceOverage, overCount = winning.ChargedMicros, winning.OverCount
			}
			overageAlreadyClaimed = true
		}
	}
	summary.AccountOverageMicros = advanceOverage

	// One invoice, one pooled line: closed period's netted usage arrears + the
	// new period's advance base + the closing period's account-wide pooled
	// overage, converted micros → whole cents ONCE at the Stripe boundary (a
	// single deterministic rounding point for the total).
	cents, err := centsFromMicros(arrears + advanceBase + advanceOverage)
	if err != nil {
		return nil, billing.Internal("micros to cents conversion failed", err)
	}
	if cents == 0 {
		// The claim-race adjustment above dropped the combined total to zero (a
		// narrow concurrent-invocation edge case) — never call Stripe for $0.
		if err := s.store.MarkBillingRun(ctx, runID, RunStatusInvoiced, "", 0); err != nil {
			return nil, billing.Internal("mark billing run (zero after overage claim race) failed", err)
		}
		summary.Status = RunStatusInvoiced
		return summary, nil
	}
	summary.ChargedCents = cents

	// Charge. A failure after the PM gate marks the run 'failed' (auditable) and
	// returns the error.
	inv, item, err := s.charge(ctx, runID, custID, cents, advanceBase > 0 || advanceOverage > 0)
	if err != nil {
		if markErr := s.store.MarkBillingRun(ctx, runID, RunStatusFailed, "", 0); markErr != nil {
			// Both failed: surface the original charge error; the failed-mark is
			// best-effort (the run stays 'pending' and is auditable / resumable).
			return nil, billing.StripeError("charge failed and could not mark run failed", err)
		}
		summary.Status = RunStatusFailed
		return nil, billing.StripeError("charge failed", err)
	}

	if err := s.store.UpsertInvoice(ctx, InvoiceMirror{
		AccountID:       accountID,
		StripeInvoiceID: inv.ID,
		Status:          inv.Status,
		AmountDueCents:  inv.AmountDue,
		AmountPaidCents: inv.AmountPaid,
		Currency:        chargeCurrency,
		PeriodStart:     periodStart,
		PeriodEnd:       periodEnd,
	}); err != nil {
		return nil, billing.Internal("invoice mirror upsert failed", err)
	}

	// Flip the overage claim to 'charged' now that Stripe confirmed it, using
	// the GENUINE Stripe invoice item id `item.ID` — never the ii-<run>
	// idempotency-key string (finding #4). Covers both a fresh claim (won
	// above) and a reused prior 'advance' claim (a reclaimed retry) — both
	// need the row settled once this combined charge succeeds.
	if advanceOverage > 0 {
		if err := s.store.MarkAccountOverageSnapshotCharged(ctx, accountID, periodStart, item.ID); err != nil {
			return nil, billing.Internal("account overage snapshot mark-charged failed", err)
		}
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
			BaseMicros:  usage.BaseFeeMicros, // FLAT per-app base (overage is pooled, migration 032)
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

// charge creates the Stripe invoice item + draft invoice for the boundary
// total (usage arrears + advance base + pooled overage), with the two
// deterministic Idempotency-Keys (ii-<run>, inv-<run>) so a re-run reuses the
// same Stripe objects. withBase only widens the line DESCRIPTION when the
// total includes an advance base fee — a pure-usage invoice keeps the
// historical line text. Returns the created invoice projection (id/status/
// amounts) for the mirror upsert AND the created invoice item (the caller
// needs its GENUINE id to freeze into account_overage_snapshots when the
// combined line includes pooled overage — finding #4 — rather than the
// idempotency-key string the item was created with).
func (s *Service) charge(ctx context.Context, runID uuid.UUID, custID string, cents int64, withBase bool) (billingstripe.Invoice, billingstripe.InvoiceItem, error) {
	desc := fmt.Sprintf("MirrorStack usage — run %s", runID)
	if withBase {
		desc = fmt.Sprintf("MirrorStack usage + app base fees — run %s", runID)
	}
	item, err := s.stripe.CreateInvoiceItem(ctx, custID, cents, chargeCurrency, desc, invoiceItemIdemKey(runID))
	if err != nil {
		return billingstripe.Invoice{}, billingstripe.InvoiceItem{}, err
	}
	inv, err := s.stripe.CreateInvoice(ctx, custID, true /* autoAdvance */, invoiceIdemKey(runID))
	return inv, item, err
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
// billing_run at all). Apps created INSIDE the new period don't arm the gate:
// their new-period base is RegisterApp's proration leg's, and they join the
// advance leg at the NEXT boundary — running a boundary for them here would
// only mint a zero-charge run row.
func (s *Service) AccountHasLiveApps(ctx context.Context, accountID uuid.UUID, createdBefore time.Time) (bool, error) {
	if accountID == uuid.Nil {
		return false, billing.InvalidInput("account_id required")
	}
	apps, err := s.store.LiveAppsCreatedBefore(ctx, accountID, createdBefore)
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
