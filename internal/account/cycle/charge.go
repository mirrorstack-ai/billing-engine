package cycle

import (
	"context"
	"fmt"
	"log/slog"
	"math"
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
//     NEXT boundary. module_count is snapshotted AT CHARGE TIME. 🔴 The
//     app_base_snapshots freeze (migration 028) that made the display show what
//     was invoiced was written AFTER the provider call, so a boundary that
//     PROPOSES writes none — nothing in this leg writes one any more, and
//     whatever settles the intents has to. The allowance nets USAGE only,
//     never the base (it offsets ModuleUsage+Infra in the display math too). An
//     account with NO mirror rows (pre-backfill) gets base 0 — exactly the
//     pre-027 arrears-only invoice — until the api-platform backfill populates
//     the roster.
//     3b. ADVANCE overage leg (scenario 6, Leg 2): the NEW period's FULL $5-per-block-of-5-
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
//  6. otherwise PROPOSE: freeze the request (the durable arming claim, which
//     also pins the funding instrument and converts micros → whole cents,
//     round-half-up), then seal the boundary as TWO intents — the closed
//     period's usage arrears and the next period's subscription — and
//     MarkBillingRun('proposed'). NO money moves and no invoice exists. This
//     leg's draft→item→finalize collector is DELETED: it holds no write port,
//     so cmd/billing-cycle cannot charge anyone through it, which is a stronger
//     statement than any check over its call graph could make.
//  7. the ONE exception, and it collects nothing either: a run whose crashed
//     predecessor already FINALIZED an invoice under this run's ms_charge_ref
//     is finished rather than re-derived — the invoice is mirrored into
//     ms_billing.invoices and the run marked 'invoiced'. Abandoning it would
//     strand a charge the customer can see and nothing here recorded. A
//     recovered INERT draft is not that case (it moved no money) and is
//     proposed like any other boundary.
//
// A failure while finishing an in-flight charge marks the run 'failed'
// (auditable; PR #7 webhook reconciliation + risk-graded retry build on it) and
// returns the error. A failed PROPOSAL marks nothing — nothing was attempted at
// a provider, so the run stays pending and the next reclaim retries it. Money
// is integer micro-dollars → cents; never float.
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

	runID, shouldCharge, reclaimed, err := s.store.InsertBillingRun(ctx, accountID, periodStart, periodEnd)
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

	// FROZEN-CHARGE RECONCILIATION — resolved FIRST, before ANY gate or skip
	// (review 2026-07-06, H8). A frozen charge means a prior attempt of this run
	// committed to a Stripe request under the deterministic idem keys — and may
	// have already MOVED MONEY before crashing short of MarkBillingRun. From that
	// point the run's ONE job is to finish: ADOPT the invoice that attempt left,
	// mirror it, mark invoiced. (It is adoption, not replay: nothing re-sends
	// those idem keys now that the collector is deleted.) Every early-out that
	// used to run first (prepaid fast-path, zero-skip, spend ceiling, risk
	// judge, PM gate) would record the run as skipped/invoiced WITHOUT
	// mirroring the charge that already fired — unmirrored money, and then the
	// intent rail sealing the same boundary a second time. So each of those
	// gates below applies ONLY when no frozen charge exists.
	frozen, hasFrozen, err := s.store.BillingRunFrozenCharge(ctx, runID)
	if err != nil {
		return nil, billing.Internal("frozen boundary charge lookup failed", err)
	}
	hadFrozenAtStart := hasFrozen

	// Compatibility recovery for a draw created by an earlier active candidate
	// before the atomic run marker existed: while the wallet schema gate remains
	// active, a reclaimed run may look up an existing period draw with
	// allowNew=false. New candidates atomically freeze the exact Stripe
	// remainder on billing_runs with the draw below, so true master-off recovery
	// uses that legacy run marker and executes no migration-048 SQL.
	recoveredWalletDraw := WalletDrawdown{Mode: CreditBillingModeStandard}
	if reclaimed && s.walletEnabled && !hasFrozen {
		recoveredWalletDraw, err = s.store.DrawWalletCredits(
			ctx,
			accountID,
			periodStart,
			periodEnd,
			0,
			false,
		)
		if err != nil {
			return nil, billing.Internal("recover prior wallet draw failed", err)
		}
	}

	// THE CRASH-RECOVERY READ. It outlives the collector it was written for,
	// because what it looks for outlives the collector too: an invoice a
	// crashed attempt left at the provider.
	//
	// The freeze was stamped BEFORE that attempt's first Stripe call, so
	// "frozen" alone never meant money moved — a crash between the freeze and
	// the draft creation left nothing on Stripe at all. Resolve which case this
	// reclaim is NOW (wave 2, D6): an invoice found under the run's
	// ms_charge_ref means a prior attempt reached Stripe and this run's only
	// job is to finish it (gates bypassed, below); nothing found means the
	// boundary is proposed like any other and every collection gate applies —
	// bypassing them let a prepaid-tightened / PM-removed account be charged
	// fresh. Skips taken on that path are non-terminal; the frozen amount
	// survives for a later reclaim.
	//
	// 🔴 The search-lag backstop is GONE, and it went with the collector. The
	// Search API lags writes by ≲1min; the old note said a lag-missed invoice
	// was re-found by replaying the ~24h idem keys, so a false "nothing" could
	// not double-charge. Nothing replays those keys any more — a false
	// "nothing" now proposes, and if the missed invoice was finalized the rail
	// can collect the same boundary twice. The window is small and it is real:
	// the boundary must not be armed for collection while a run this recent
	// can be reclaimed.
	var recovered *billingstripe.Invoice
	if hasFrozen && frozen.Cents < 0 {
		return nil, billing.Internal("frozen boundary charge is negative", nil)
	}
	if hasFrozen && frozen.Cents > 0 {
		if frozen.ChargeFundingAccountID == uuid.Nil {
			return nil, billing.Internal("frozen boundary charge has no pinned funding account", nil)
		}
		custID, err := s.store.AccountStripeCustomer(ctx, frozen.ChargeFundingAccountID)
		if err != nil {
			return nil, billing.Internal("stripe customer lookup failed", err)
		}
		if custID != "" {
			if found, ok, err := s.stripe.FindInvoiceByRef(ctx, custID, boundaryChargeRef(runID)); err != nil {
				return nil, billing.StripeError("boundary recovery lookup failed", err)
			} else if ok {
				recovered = &found
			}
		}
	}
	// moneyMayHaveMoved is what actually justifies bypassing the gates: a
	// FINALIZED invoice (or a void one — refused loudly downstream, D10). A
	// recovered inert DRAFT moved no money yet, so finalizing it is still a
	// fresh off-session debit and every gate applies; a gate skip leaves the
	// draft inert and the run non-terminal for a later re-attempt.
	moneyMayHaveMoved := recovered != nil && recovered.Status != "draft"

	// RISK-GRADED COLLECTION GATE (PR #9, design §7-A / billing-tiers §3). Load
	// the account's collection state up front. The off-session arrears leg may
	// only ship behind this gate (the GA gate); the run row already exists, so a
	// skip here is auditable as skipped_prepaid and the deterministic Stripe
	// idem keys stay stable if the mode later flips back to arrears.
	acct, err := s.store.AccountCollection(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("account collection lookup failed", err)
	}
	walletState := WalletCreditState{Mode: CreditBillingModeStandard}
	walletAllowed := false
	if recoveredWalletDraw.DrawnMicros > 0 {
		walletState.Mode = recoveredWalletDraw.Mode
		walletState.PeriodDrawnMicros = recoveredWalletDraw.DrawnMicros
		walletAllowed = true
	} else if !hasFrozen {
		walletState, walletAllowed, err = s.creditWalletChargeState(ctx, accountID, periodStart, periodEnd)
		if err != nil {
			return nil, billing.Internal("wallet route classification failed", err)
		}
	}
	walletActive := walletAllowed && (walletState.Mode == CreditBillingModeCredits ||
		walletState.SpendableBalanceMicros > 0 || walletState.PeriodDrawnMicros > 0)

	// Preserve the historical prepaid fast path byte-for-byte for a standard
	// account with no spendable wallet balance and no draw already recorded for
	// this period. A credits account (or a standard account with credit) must
	// instead price the true boundary and debit it before this collection gate.
	// Never apply the fast path over a frozen charge (H8): a mode tightened after
	// a crashed attempt charged must not strand moved money unmirrored.
	if !moneyMayHaveMoved &&
		!(hasFrozen && frozen.Cents == 0) &&
		acct.Mode == BillingModePrepaid &&
		!walletActive {
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

	// ADVANCE OVERAGE leg (scenario 6, Leg 2): the NEW period's $5-per-block
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
	// RECURRING leg → priced in WHOLE BLOCKS (usage.ModuleBlockMicros), never
	// per module: this is the customer-visible monthly overage. The one-time
	// grace legs stay per-module at the amortized rate — see
	// usage.ModuleOverageFeeMicros for why the split is by leg.
	advanceOverage := usage.ModuleBlockMicros(int64(overCount))

	// ADVANCE DOMAINS leg: every live custom domain activated before the new
	// period opened contributes one full $2 fee for that new period. Deliberately
	// ignore charge_resolved: the sweep owns only the activation-containing
	// period, while this leg owns every subsequent period, so the windows are
	// disjoint without depending on sweep ordering.
	domainCount, err := s.store.CountLiveDomainsActivatedBefore(ctx, accountID, periodEnd)
	if err != nil {
		return nil, billing.Internal("live custom-domain count failed", err)
	}
	advanceDomains := usage.DomainFeeMicros * int64(domainCount)

	// The whole boundary invoice: closed period's netted usage arrears + the new
	// period's advance base + module overage + custom domains. The allowance nets
	// USAGE only; all recurring account fees ride on top.
	boundaryTotal := arrears + advanceBase + advanceOverage + advanceDomains
	withBase := advanceBase+advanceOverage+advanceDomains > 0

	summary := &ChargeSummary{
		FirstRun:             true,
		ArrearsMicros:        arrears,
		AdvanceBaseMicros:    advanceBase,
		AdvanceOverageMicros: advanceOverage,
		AdvanceDomainsMicros: advanceDomains,
	}

	// UNIVERSAL WALLET DRAWDOWN. The true boundary total is now fixed, so the
	// wallet is debited before every zero/ceiling/risk/PM gate. The store owns the
	// atomic lot allocation and period idempotency. A frozen Stripe attempt may
	// already have moved money, so it may REUSE a prior draw but may never start a
	// new one beside that frozen charge.
	stripeTotal := boundaryTotal
	remainingArrears := arrears
	walletMode := CreditBillingModeStandard
	if walletAllowed {
		walletMode = walletState.Mode
	}
	if walletActive {
		draw := recoveredWalletDraw
		if draw.DrawnMicros == 0 || !hasFrozen {
			draw, err = s.store.DrawBillingRunWalletCredits(
				ctx,
				runID,
				accountID,
				periodStart,
				periodEnd,
				boundaryTotal,
				withBase,
				!hadFrozenAtStart && draw.DrawnMicros == 0,
			)
			if err != nil {
				return nil, billing.Internal("wallet drawdown failed", err)
			}
		}
		if draw.DrawnMicros < 0 {
			return nil, billing.Internal("wallet drawdown returned a negative magnitude", nil)
		}
		walletMode = draw.Mode
		summary.WalletDrawnMicros = draw.DrawnMicros
		if draw.BoundaryChargeFrozen {
			frozen = draw.BoundaryCharge
			hasFrozen = true
		}
		stripeTotal -= draw.DrawnMicros
		if stripeTotal < 0 {
			// A reclaimed period can have a larger already-durable draw than a
			// later live recomputation. Never refund/reallocate it implicitly and
			// never turn the difference into a negative Stripe line.
			stripeTotal = 0
		}
		remainingArrears -= draw.DrawnMicros
		if remainingArrears < 0 {
			remainingArrears = 0
		}
		if walletMode == CreditBillingModeCredits && s.estimate != nil {
			// The draw above is committed before this best-effort callback.
			// Seed the NEW period at zero unpaid exposure. Closed-period
			// arrears and the new period's recurring fees were both settled by
			// the draw, so neither may leak into the new key.
			if err := s.estimate.ReconcileBoundary(ctx, accountID, periodEnd, 0); err != nil {
				slog.WarnContext(ctx, "boundary estimate/standing reconciliation failed (continuing)",
					"account_id", accountID, "period_start", periodEnd, "error", err)
			}
		}
	}

	// A run-level marker is authoritative even when this deployment is truly
	// wallet-off (or the account is now excluded), so no migration-048 read is
	// needed on reclaim. It stores the exact Stripe cents that survived the
	// wallet debit. Rebuild the effective remainder from that legacy row for
	// collection gates and the eventual idempotent Stripe request.
	if hasFrozen && summary.WalletDrawnMicros == 0 {
		if frozen.Cents > math.MaxInt64/microsPerCent {
			return nil, billing.Internal("frozen boundary charge overflows micros", nil)
		}
		stripeTotal = frozen.Cents * microsPerCent
		inferredDraw := boundaryTotal - stripeTotal
		if inferredDraw > 0 {
			remainingArrears -= inferredDraw
			if remainingArrears < 0 {
				remainingArrears = 0
			}
		}
	}

	// Credits mode is wallet-only: the store debits the full boundary amount,
	// including an unallocated residual when positive lots are exhausted. It
	// never sends a remainder to Stripe. A pre-existing frozen Stripe request is
	// the sole exception: recovery must finish money that may already have moved,
	// and allowNew=false above prevents adding a new wallet debit beside it.
	if !hadFrozenAtStart && walletMode == CreditBillingModeCredits && stripeTotal != 0 {
		return nil, billing.Internal("credits-mode wallet did not debit the full boundary total", nil)
	}

	// Zero-skip: only when arrears, base AND overage are all zero (empty/zero
	// period with no live apps or ongoing over-modules) is there nothing to
	// invoice — mark invoiced with NO Stripe call, never auto-create a Customer
	// with nothing to charge. A zero total can never breach a limit/ceiling, so
	// this short-circuits ahead of the risk gate. Never applied over a frozen
	// charge (H8): a reclaimed run whose LIVE total collapsed to 0 (a module
	// uninstalled, an app deleted since the crash) still owes the mirror + mark
	// for the non-zero amount the crashed attempt already put through Stripe.
	// The terminal zero-mark is GUARDED on the run still being unfrozen (wave 2,
	// D7): our hasFrozen read is stale under the two-daemons model, and a
	// concurrent reclaim may have frozen + charged since — an unguarded terminal
	// 'invoiced' would bury that charge forever. Guard lost → error out; the run
	// stays reclaimable and the next reclaim reconciles the frozen charge.
	if stripeTotal == 0 && (!hasFrozen || frozen.Cents == 0) {
		// A wallet-settled advance base is still a real billed base. Persist the
		// same display snapshot the Stripe path would have written; a failure
		// leaves the run reclaimable and the period draw is safely reused.
		if (summary.WalletDrawnMicros > 0 || hasFrozen) && advanceBase > 0 {
			anchorDay := billingperiod.AnchorDay(periodEnd)
			if activatedAt, activated, err := s.store.AccountActivation(ctx, accountID); err != nil {
				return nil, billing.Internal("account activation lookup failed", err)
			} else if activated {
				anchorDay = billingperiod.AnchorDay(activatedAt)
			}
			_, walletPeriodEnd := billingperiod.AnchoredPeriodWindow(periodEnd, anchorDay)
			for _, a := range apps {
				if err := s.store.InsertAdvanceBaseSnapshot(ctx, AppBaseSnapshot{
					AppID:       a.AppID,
					PeriodStart: periodEnd,
					PeriodEnd:   walletPeriodEnd,
					ModuleCount: a.ModuleCount,
					BaseMicros:  usage.BaseFeeMicros,
				}); err != nil {
					return nil, billing.Internal("advance base snapshot insert failed", err)
				}
			}
		}
		if hasFrozen {
			// frozen==0 is the durable full-wallet/sub-half-cent settlement
			// marker. Unlike a genuinely fresh zero, it must be adopted rather
			// than rejected by the unfrozen-only race guard.
			if err := s.store.MarkBillingRun(ctx, runID, RunStatusInvoiced, "", 0); err != nil {
				return nil, billing.Internal("mark billing run (wallet settled) failed", err)
			}
		} else {
			ok, err := s.store.MarkBillingRunInvoicedIfUnfrozen(ctx, runID)
			if err != nil {
				return nil, billing.Internal("mark billing run (zero arrears) failed", err)
			}
			if !ok {
				return nil, billing.Internal("zero-skip lost to a concurrent freeze — run left pending for the next reclaim to reconcile", nil)
			}
		}
		summary.Status = RunStatusInvoiced
		return summary, nil
	}

	// The legacy usage-prepaid gate now runs after wallet drawdown whenever a
	// wallet was active. Credits already consumed stay settled; only the unpaid
	// remainder is retained for a later reclaim.
	if !moneyMayHaveMoved && acct.Mode == BillingModePrepaid {
		if err := s.store.MarkBillingRun(ctx, runID, RunStatusSkippedPrepaid, "", 0); err != nil {
			return nil, billing.Internal("mark billing run (skipped_prepaid) failed", err)
		}
		summary.Status = RunStatusSkippedPrepaid
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
	// limit (a hard cap, not a trust judgment). Never applied over a frozen
	// charge (H8) — the crashed attempt's money may already have moved.
	if !moneyMayHaveMoved && collection.ExceedsSpendCeiling(toCollectionAccount(acct), remainingArrears) {
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
	// an account is never relaxed and charged in the same beat. Never applied
	// over a frozen charge (H8) — a delinquency signal raised by the crashed
	// attempt's OWN open invoice must not strand that invoice unmirrored.
	// TODO(#9-followup): wire a usage-spike anomaly signal + a
	// sustained-clean-standing window.
	if !moneyMayHaveMoved {
		delinquent, err := s.store.HasUnpaidInvoice(ctx, accountID)
		if err != nil {
			return nil, billing.Internal("delinquency lookup failed", err)
		}
		decision := collection.RiskAssess(
			toCollectionAccount(acct),
			collection.Signals{Delinquent: delinquent, AccruedArrearsMicros: remainingArrears},
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
	}

	// PM gate. It stays exactly where it was: a boundary with no way to be paid
	// is skipped and RETAINED, whether the next step collects it or seals it.
	// Removing it because "a proposal moves no money" would start sealing
	// documents against accounts that cannot fund them.
	//
	// custID is resolved but no longer CONSUMED — nothing here sends a request.
	// What survives is the assertion it carries: a usable PM implies a provider
	// customer, so an empty id is an anomaly this leg refuses rather than
	// papers over.
	//
	// Fresh run — including a frozen reclaim whose prior attempt never
	// reached Stripe (D6) — no usable default PM (or the usable-PM-implies-
	// Customer anomaly) → skip (usage RETAINED), re-attempt next cycle. Only
	// when the prior attempt's invoice EXISTS on Stripe is the gate bypassed
	// (H8): completing already-created Stripe objects needs no fresh
	// authorization, and a finalize with the PM since removed fails loudly into
	// the 'failed' (retryable, auditable) path below rather than being recorded
	// as a skip over moved money. Only the Customer id is required there.
	var custID string
	if moneyMayHaveMoved {
		custID, err = s.store.AccountStripeCustomer(ctx, frozen.ChargeFundingAccountID)
		if err != nil {
			return nil, billing.Internal("stripe customer lookup failed", err)
		}
		if custID == "" {
			return nil, billing.Internal("billing run has a recovered Stripe invoice but the funding account has no Stripe customer id", nil)
		}
	} else {
		ok := false
		if hasFrozen {
			if frozen.ChargeFundingAccountID == uuid.Nil {
				return nil, billing.Internal("frozen boundary charge has no pinned funding account", nil)
			}
			ok, err = s.store.HasUsableDefaultPM(ctx, frozen.ChargeFundingAccountID)
			if err == nil && ok {
				custID, err = s.store.AccountStripeCustomer(ctx, frozen.ChargeFundingAccountID)
			}
		} else {
			custID, ok, err = s.resolveChargeableCustomer(ctx, accountID)
		}
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
	}

	// Resolve the NEW period's window for the boundary line and base snapshots
	// BEFORE any Stripe call (fail early on a lookup error). periodEnd is always
	// the anchored boundary (the straddle-clamp only ever moves the START), so the new
	// window is AnchoredPeriodWindow(periodEnd, anchorDay) = [periodEnd, next
	// boundary). The anchor day comes from activated_at (ADR 0005); the
	// boundary's own day-of-month is the defensive fallback for the
	// direct-call-on-an-unactivated-account case the cron never produces.
	anchorDay := billingperiod.AnchorDay(periodEnd)
	if activatedAt, activated, err := s.store.AccountActivation(ctx, accountID); err != nil {
		return nil, billing.Internal("account activation lookup failed", err)
	} else if activated {
		anchorDay = billingperiod.AnchorDay(activatedAt)
	}
	_, newPeriodEnd := billingperiod.AnchoredPeriodWindow(periodEnd, anchorDay)

	// One invoice: closed period's netted usage arrears + the new period's advance
	// base + the new period's advance overage, converted micros → whole cents ONCE
	// at the Stripe boundary (a single deterministic rounding point for the total).
	cents, err := centsFromMicros(stripeTotal)
	if err != nil {
		return nil, billing.Internal("micros to cents conversion failed", err)
	}
	liveCents := cents // what the LIVE state says; may be replaced by a frozen amount below
	// FREEZE-OR-REUSE the boundary Stripe request (crash-safe idempotency,
	// migration 035). The idem keys inv-/ii-/fin-<run> are STABLE across a
	// reclaim of this run, so the request sent under them must be stable too.
	//
	//   - Frozen (read at the very top, before every gate — H8): a prior attempt
	//     already committed to a Stripe request; REUSE the frozen (cents,
	//     withBase) verbatim rather than the values just recomputed from LIVE
	//     state — drift between the crash and this retry (a module uninstalled
	//     flipping an over-module to included, an app deleted) could have moved
	//     the live total, and re-sending the same idem key with a different
	//     amount/description is the permanent Stripe idempotency-conflict stall
	//     this guards against (the bug ee5043c fixed once for the account-wide
	//     model, whose freeze migration 033 dropped).
	//   - Fresh: the cents==0 sub-half-cent short-circuit applies (never call
	//     Stripe for $0 — an advance base/overage, when present, is always ≥ $5
	//     (a whole block, or the $20 base; leg 2 never prorates)
	//     and can never round to 0; nothing was ever put through Stripe for this
	//     run), then freeze BEFORE the first Stripe call. The freeze is
	//     first-write-wins AND returns the SURVIVING row value (H6): a concurrent
	//     second daemon that reclaimed the same run and froze first wins, and
	//     THIS process adopts the winner's amount — two racing processes can
	//     never send different bodies under the shared idem keys.
	if hasFrozen {
		cents = frozen.Cents
		withBase = frozen.WithBase
	} else {
		if cents == 0 {
			// Same D7 guard as the zero-skip above — terminal only while unfrozen.
			ok, err := s.store.MarkBillingRunInvoicedIfUnfrozen(ctx, runID)
			if err != nil {
				return nil, billing.Internal("mark billing run (zero cents) failed", err)
			}
			if !ok {
				return nil, billing.Internal("zero-cents skip lost to a concurrent freeze — run left pending for the next reclaim to reconcile", nil)
			}
			summary.Status = RunStatusInvoiced
			return summary, nil
		}
		surviving, claim, err := s.store.FreezeBillingRunCharge(ctx, runID, FrozenBoundaryCharge{Cents: cents, WithBase: withBase})
		if err != nil {
			return nil, billing.Internal("freeze boundary charge failed", err)
		}
		if claim == StripeRailNoPaymentMethod {
			if err := s.store.MarkBillingRun(ctx, runID, RunStatusSkippedNoPM, "", 0); err != nil {
				return nil, billing.Internal("mark billing run (atomic skipped_no_pm) failed", err)
			}
			summary.Status = RunStatusSkippedNoPM
			return summary, nil
		}
		if claim != StripeRailClaimed {
			return nil, billing.Internal("boundary funding arm lost its durable claim", nil)
		}
		cents = surviving.Cents
		withBase = surviving.WithBase
		frozen = surviving
		hasFrozen = true
		custID, err = s.store.AccountStripeCustomer(ctx, surviving.ChargeFundingAccountID)
		if err != nil {
			return nil, billing.Internal("boundary pinned funding customer lookup failed", err)
		}
		if custID == "" {
			return nil, billing.Internal("boundary pinned funder has a usable PM but no Stripe customer id", nil)
		}
	}
	summary.ChargedCents = cents

	// 🔴 THIS LEG NO LONGER COLLECTS. Everything above ran: the amounts are
	// derived, the wallet is drawn, the funding is armed and the request is
	// FROZEN. What used to sit below — draft → pinned item → finalize — is
	// DELETED, so the proposal is UNCONDITIONAL: there is no `s.proposer !=
	// nil` fallback left to fall back TO. A service wired without a proposer
	// now fails loudly inside proposeBoundary instead of quietly charging.
	//
	// The proposal still sits here, after the freeze and after the recovery
	// read, for the same reason the other legs put theirs after their own
	// arming claim (domain_charges.go:140, overage.go:452): a run whose money
	// MAY ALREADY HAVE MOVED must be FINISHED, not re-derived as an intent.
	// Abandoning a finalized invoice would strand a charge the customer can
	// see, that nothing in this tree mirrored, while the rail sealed a second
	// intent for the same money.
	//
	// That case is `moneyMayHaveMoved`, and it is EXACTLY the case — no wider.
	// A recovered INERT DRAFT is on the proposing side of this branch: its
	// params carry AutoAdvance(false) (shared/stripe/client.go
	// inertDraftInvoiceParams), so Stripe never finalizes it on its own and no
	// money has moved or can move. Finalizing one here would have been a FRESH
	// off-session debit — the very collect this cutover removes — which is why
	// the gates above already refused to bypass themselves for a draft. The
	// draft is left inert; the boundary is sealed as intents like any other.
	//
	// The finalized-invoice exception drains as those runs complete, and
	// scripts/legacy-drop-preconditions.sql row 1 (billing_runs holding a
	// frozen charge while not 'invoiced') is the query that asks production
	// when it has.
	if !moneyMayHaveMoved {
		return s.proposeBoundary(ctx, runID, accountID, summary, boundaryComponents{
			// summary.ArrearsMicros is the ORIGINAL arrears. remainingArrears
			// cannot be used to recover it: it is clamped at zero, so a wallet
			// draw larger than the arrears loses the difference and the split
			// would understate the closed period.
			ArrearsMicros:        summary.ArrearsMicros,
			AdvanceBaseMicros:    summary.AdvanceBaseMicros,
			AdvanceOverageMicros: summary.AdvanceOverageMicros,
			AdvanceDomainsMicros: summary.AdvanceDomainsMicros,
			WalletDrawnMicros:    summary.WalletDrawnMicros,
			// 🔴 The frozen figure, when a prior attempt committed to one.
			//
			// The proposal below is derived from LIVE state, and a reclaim can
			// see different live state than the attempt that froze: a module
			// uninstalled, an app deleted. The freeze exists precisely so two
			// processes working the same run commit to the SAME number — and
			// on the intent rail that matters more, not less, because the
			// amount is sealed into a digest. Two daemons deriving different
			// live totals would seal two different documents for one boundary,
			// and ON CONFLICT DO NOTHING dedupes only IDENTICAL digests.
			//
			// So it is passed in and CHECKED, not silently substituted: the
			// components cannot be recovered from a single frozen cents
			// figure, so a drifted reclaim must refuse rather than seal an
			// amount nobody committed to.
			FrozenCents:    frozenCentsForProposal(hasFrozen, frozen),
			HasFrozenCents: hasFrozen,
		}, periodStart, periodEnd, newPeriodEnd)
	}

	// IN-FLIGHT COMPLETION, AND IT MOVES NO MONEY. Reaching here means a prior
	// attempt of this run already FINALIZED an invoice under the run's
	// ms_charge_ref (found once, next to the frozen read — D6), so the debit
	// may already have happened at the provider. Finishing it is a mirror and
	// a mark: boundaryInvoice makes no provider write at all now that the
	// draft→item→finalize collect underneath it is deleted. Its only failure
	// modes are the two refusals it must keep making — a VOID invoice and (as
	// a defence of the branch above) a draft — and either marks the run
	// 'failed' (auditable) and returns the error.
	// The window the adopted invoice's line disclosed, reconstructed from the
	// FROZEN charge shape so the mirror records what that attempt actually
	// billed. It always begins at the closed usage period; a charge shape that
	// included advance base/overage also covered the new period, and so runs
	// through its next anchored boundary. Only the mirror reads this now — the
	// line it describes was created by the attempt being adopted.
	linePeriod := billingstripe.LinePeriod{Start: periodStart, End: periodEnd}
	if withBase {
		linePeriod.End = newPeriodEnd
	}

	inv, err := s.boundaryInvoice(runID, *recovered)
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
	// The flag describes the money that MOVED (review 2026-07-06, M4). On the
	// fresh path that is boundaryTotal (whose cents conversion is exactly what
	// was charged — sub-cent precision preserved for the threshold comparison).
	// When the charged cents came from a frozen/surviving value that DIFFERS
	// from the live recompute (a reclaim after roster drift, or a lost freeze
	// race), the live total describes a charge that never happened — flag the
	// amount actually sent to Stripe instead.
	chargedMicros := stripeTotal
	if cents != liveCents {
		chargedMicros = cents * microsPerCent
	}
	// Mirror the SAME window the Stripe line discloses (linePeriod) — [periodStart,
	// periodEnd) for a pure-usage run, widened to [periodStart, newPeriodEnd) when
	// the final charge shape includes advance base/overage. Reusing linePeriod (not
	// the closed usage window alone) keeps the web-account billing display fed from
	// ms_billing.invoices in lockstep with the hosted Stripe invoice, so the boundary
	// leg never shows a narrower window than Stripe disclosed for the same invoice.
	if err := s.store.UpsertInvoice(ctx, InvoiceMirror{
		AccountID:               accountID,
		ChargeFundingAccountID:  frozen.ChargeFundingAccountID,
		ChargeFundingGeneration: frozen.ChargeFundingGeneration,
		StripeInvoiceID:         inv.ID,
		Status:                  inv.Status,
		AmountDueCents:          inv.AmountDue,
		AmountPaidCents:         inv.AmountPaid,
		Currency:                chargeCurrency,
		PeriodStart:             linePeriod.Start,
		PeriodEnd:               linePeriod.End,
		IsLargeAutoCollect:      flagLargeAutoCollect(chargedMicros, postChargeAcct),
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

// boundaryInvoice ADOPTS a boundary run's already-finalized Stripe invoice —
// `found` is what RunBillingCycle recovered under the run's ms_charge_ref
// anchor, next to the frozen read (H5/D6), in the one state that means a
// crashed attempt may already have debited the customer. It performs NO
// provider write: the draft→item→finalize collect that used to live here (and
// the fresh s.charge it fell through to) is deleted, and the caller proposes
// for every state but this one.
//
// A finalized invoice (paid/open/uncollectible) is returned as-is, so the
// caller mirrors the charge that exists and marks the run invoiced. The two
// refusals it still has to make:
//
//   - VOID — the charge was CANCELED at the provider. Adopting it would mark
//     the run invoiced against an invoice that collects nothing, silently
//     forgiving the boundary and terminally consuming the run. Ops resolves it.
//   - DRAFT — unreachable while the caller's guard holds (a draft is inert,
//     moved no money, and is proposed instead). Kept as a defence of that
//     guard, because the failure it prevents is silent: adopting a draft would
//     record a boundary as invoiced that no invoice ever charged.
func (s *Service) boundaryInvoice(runID uuid.UUID, found billingstripe.Invoice) (billingstripe.Invoice, error) {
	switch found.Status {
	case "void":
		return billingstripe.Invoice{}, fmt.Errorf(
			"boundary recovery: invoice %s under %s is VOID — refusing to adopt a canceled charge (run %s needs ops resolution)",
			found.ID, boundaryChargeRef(runID), runID)
	case "draft":
		return billingstripe.Invoice{}, fmt.Errorf(
			"boundary recovery: invoice %s under %s is still a DRAFT — it moved no money, so the boundary is proposed, never adopted (run %s)",
			found.ID, boundaryChargeRef(runID), runID)
	}
	return found, nil // finalized (paid/open/uncollectible) — the charge exists; mirror it
}

// boundaryChargeRef is the deterministic ms_charge_ref metadata anchor a
// crashed attempt stamped on its invoice — what FindInvoiceByRef recovers by.
// Nothing writes it any more; it survives because the recovery READ still has
// to find what the deleted collector left behind.
func boundaryChargeRef(runID uuid.UUID) string { return "run:" + runID.String() }

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

// UnactivatedAccountsWithUsage returns the card-less accounts whose raw events
// need rollup in the calendar-month window. It is deliberately only a work-list
// pass-through; the driver exposes no charge capability to this phase.
func (s *Service) UnactivatedAccountsWithUsage(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error) {
	if periodStart.IsZero() || periodEnd.IsZero() || !periodEnd.After(periodStart) {
		return nil, billing.InvalidInput("period_end must be after period_start")
	}
	accounts, err := s.store.UnactivatedAccountsWithUsage(ctx, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("list unactivated accounts with usage failed", err)
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

// AccountHasLiveDomains reports whether a no-usage account still needs a
// boundary run for at least one custom domain. The cutoff matches the charge
// leg exactly: a domain activated at the boundary belongs to its activation
// sweep for that period and joins the boundary leg at the next one.
func (s *Service) AccountHasLiveDomains(ctx context.Context, accountID uuid.UUID, activatedBefore time.Time) (bool, error) {
	if accountID == uuid.Nil {
		return false, billing.InvalidInput("account_id required")
	}
	count, err := s.store.CountLiveDomainsActivatedBefore(ctx, accountID, activatedBefore)
	if err != nil {
		return false, billing.Internal("live custom-domain count failed", err)
	}
	return count > 0, nil
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
