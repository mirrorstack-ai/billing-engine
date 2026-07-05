// Command billing-cycle is the scheduled charge driver for Milestone D PR #6 —
// the USAGE (arrears) leg of the billing cycle. It runs per closed billing
// period, charging each account's metered usage off-session via Stripe
// (invoice item + draft invoice + auto-advance on the default PM).
//
// Dual-transport (same logic, two harnesses):
//   - AWS_LAMBDA_FUNCTION_NAME set → lambda.Start(handler), driven by an
//     EventBridge Scheduler singleton in production (a CloudWatchEvent).
//   - Otherwise → a one-shot local run (make dev-cycle / go run .), so dev
//     never needs Lambda or a scheduler.
//
// Resumability: the cycle is request-scoped (a Lambda invocation can be
// interrupted mid-batch). cycle.RunBillingCycle's first idempotency layer
// (billing_runs UNIQUE(account, period) ON CONFLICT DO NOTHING) makes a
// re-fire charge ONLY the accounts that hadn't completed — completed accounts
// already have their run row and are skipped. The second layer (deterministic
// Stripe Idempotency-Keys) defends the Stripe calls themselves.
//
// Period window: each account closes on its OWN billing period, anchored to the
// day-of-month it bound its first credit card (activated_at, migration 025 / ADR
// 0005) — NOT the UTC calendar month and NOT the signup date. The anchor is a
// billing event billing-engine already owns, so the driver derives every window
// in-process from ms_billing.accounts.activated_at with NO cross-schema read into
// ms_account: it lists the card-bound accounts, derives each one's anchor day,
// and closes THAT account's just-ended anchored period (billingperiod.
// AnchoredJustClosed). Because each account's close day differs, the batch can no
// longer share a single window; the window is computed per account inside the
// loop. Processing is idempotent (billing_runs UNIQUE(account, period) +
// deterministic Stripe keys), so re-firing on any day only charges periods that
// have actually closed and are not yet invoiced — the driver can run daily
// (EventBridge, once provisioned) or as a local one-shot without double-charging.
//
// allowanceMicros is 0 for v1 (the allowance-netting math is implemented in
// cycle.RunBillingCycle; tier-sourced allowance + the advance leg are DEFERRED
// to the subscription/tier PR).
//
// Spec: docs-temp/milestone-d-meter/design.md §4 Axis 4 / §5 / §6 (PR #6) and
// mirrorstack-docs/adr/0005-billing-period-anchor.md.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// allowanceMicros is the per-account usage allowance netted off the arrears
// charge. 0 in v1 (tier-sourced allowance + the advance leg are DEFERRED to the
// subscription/tier PR — they need tier pricing + per-account seat/app counts
// that do not exist in billing yet).
const allowanceMicros int64 = 0

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	svc := buildService()

	if config.IsLambda() {
		lambda.Start(handler(svc))
		return
	}

	// Local one-shot run: close every card-bound account's just-ended anchored
	// period as of now AND sweep the creation-proration grace queue, then exit.
	// No HTTP listener — the dev cycle is a single batch invocation.
	at := time.Now().UTC()
	res := runCycle(context.Background(), svc, at)
	slog.Info("billing-cycle local run complete",
		"as_of", res.AsOf,
		"activated", res.Activated, "rolled_up", res.RolledUp, "processed", res.Processed, "charged", res.Charged,
		"skipped_no_pm", res.SkippedNoPM, "zero_arrears", res.ZeroArrears,
		"already_run", res.AlreadyRun, "failed_runs", res.FailedRuns, "failed", res.Failed,
		"overage_candidates", res.OverageCandidates, "overage_charged", res.OverageCharged,
		"overage_skipped", res.OverageSkipped, "overage_failed", res.OverageFailed)
	sweepFailed := runProrationSweep(context.Background(), svc, at)
	if res.Failed > 0 || sweepFailed {
		os.Exit(1)
	}
}

// runProrationSweep charges the creation-period base for every app that has
// survived the grace window as of `at` (the second leg of the cycle job,
// alongside the per-account boundary loop). Reports whether any per-app charge
// failed so the caller can set a non-zero exit code; a failure is retried on the
// next sweep and never aborts the batch. Logged here so both transports share
// the shape.
func runProrationSweep(ctx context.Context, svc *cycle.Service, at time.Time) bool {
	sweep, err := svc.SweepCreationProrations(ctx, at)
	if err != nil {
		slog.ErrorContext(ctx, "creation-proration sweep failed", "as_of", at, "error", err)
		return true
	}
	slog.InfoContext(ctx, "creation-proration sweep complete",
		"as_of", at, "pending", sweep.Pending, "charged", sweep.Charged,
		"skipped", sweep.Skipped, "failed", sweep.Failed)
	return sweep.Failed > 0
}

// buildService wires the pgxpool + Stripe client into the cycle Service. The
// Stripe secret is required (the charge leg cannot run without it).
func buildService() *cycle.Service {
	pool := config.MustPgxPool()
	stripeKey := config.MustEnv("STRIPE_SECRET_KEY")
	return cycle.NewService(cycle.NewStore(pool), billingstripe.NewClient(stripeKey))
}

// handler is the Lambda entrypoint for an EventBridge-scheduled invocation. The
// CloudWatchEvent carries no window (the scheduler fires on a cron); the handler
// closes each card-bound account's just-ended ANCHORED period as of the event
// time. Firing daily is idempotent — an account is only charged on/after its own
// close day, and never twice for the same period.
func handler(svc *cycle.Service) func(context.Context, events.CloudWatchEvent) error {
	return func(ctx context.Context, ev events.CloudWatchEvent) error {
		at := ev.Time
		if at.IsZero() {
			at = time.Now().UTC()
		}
		res := runCycle(ctx, svc, at.UTC())
		slog.InfoContext(ctx, "billing-cycle lambda run complete",
			"as_of", res.AsOf,
			"activated", res.Activated, "rolled_up", res.RolledUp, "processed", res.Processed, "charged", res.Charged,
			"skipped_no_pm", res.SkippedNoPM, "zero_arrears", res.ZeroArrears,
			"already_run", res.AlreadyRun, "failed_runs", res.FailedRuns, "failed", res.Failed,
			"overage_candidates", res.OverageCandidates, "overage_charged", res.OverageCharged,
			"overage_skipped", res.OverageSkipped, "overage_failed", res.OverageFailed)
		// Sweep the creation-proration grace queue as of the same instant.
		runProrationSweep(ctx, svc, at.UTC())
		// A per-account charge failure (or a per-app proration failure) is recorded
		// (billing_runs status='failed')
		// and does NOT fail the batch — the next cycle retries it. The handler
		// returns nil so EventBridge doesn't replay the whole batch.
		return nil
	}
}

// cycleResult tallies a batch run for logging / exit code.
type cycleResult struct {
	AsOf        time.Time // the run's evaluation instant (UTC)
	Activated   int       // card-bound accounts considered
	RolledUp    int       // accounts whose just-closed window had usage (rolled up)
	Processed   int       // accounts processed in the charge phase
	Charged     int
	SkippedNoPM int
	ZeroArrears int
	AlreadyRun  int
	FailedRuns  int // per-account charge runs that ended status='failed'
	Failed      int // errors (rollup error, charge error, or list error)

	// Mid-period account-wide overage grace sweep (migration 030).
	OverageCandidates int // accounts past the grace window this sweep evaluated
	OverageCharged    int // accounts whose pooled overage was invoiced mid-period
	OverageSkipped    int // evaluated but not charged (already billed / under pool / no PM / 0 cents)
	OverageFailed     int // per-account overage-charge errors (counted, never abort)
}

// runCycle closes every card-bound account's just-ended ANCHORED period as of
// `at`. Each account has its own card-binding anchor day, so the window is
// derived PER ACCOUNT inside the loop — the batch cannot share one window under
// anchoring. Per account:
//
//  1. window = AnchoredJustClosed(at, anchorDay), straddle-clamped so the FIRST
//     anchored run after cutover never overlaps the last calendar-month period
//     (start := max(anchoredStart, lastClosedPeriodEnd)).
//  2. Rollup — price the window's raw usage_events into usage_aggregates. An
//     account whose just-closed window had NO usage produces no aggregates and
//     is skipped (no billing_run) UNLESS it has live ms_billing.apps roster
//     rows created before the new period opened — base-fee v1 still owes the
//     NEW period's advance base at this boundary, so the charge phase runs
//     anyway. A no-usage account with no such apps (pre-backfill, or apps
//     created inside the new period whose base RegisterApp's proration leg
//     already owns) keeps the historical skip.
//  3. Charge — RunBillingCycle nets arrears and charges. Idempotent: a re-fire
//     only charges periods without a successful (invoiced) run; billing_runs
//     reclaims non-terminal rows.
//
// A single account's error is logged + counted but never aborts the batch.
func runCycle(ctx context.Context, svc *cycle.Service, at time.Time) cycleResult {
	res := cycleResult{AsOf: at}

	accounts, err := svc.ActivatedAccounts(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "list activated accounts failed", "error", err)
		res.Failed++
		return res
	}
	res.Activated = len(accounts)

	for _, a := range accounts {
		anchorDay := billingperiod.AnchorDay(a.ActivatedAt)
		start, end := billingperiod.AnchoredJustClosed(at, anchorDay)

		// Cutover straddle-clamp: if the account's last closed period ended AFTER
		// this anchored window's start (the calendar→anchor transition month), start
		// the run at that end instead. This yields ONE clean bridge period with no
		// overlap, gap, or duplicate (account_id, period_start) key. A lookup error
		// is non-fatal — proceed unclamped (the UNIQUE key still prevents a dup).
		if lastEnd, found, err := svc.LatestClosedPeriodEnd(ctx, a.ID); err != nil {
			slog.ErrorContext(ctx, "latest closed period lookup failed (proceeding unclamped)",
				"account_id", a.ID, "error", err)
		} else if found && start.Before(lastEnd) && lastEnd.Before(end) {
			start = lastEnd
		}

		// Phase 1 — rollup this account's window. No usage → no aggregates →
		// skip the charge (no billing_run for an empty period) UNLESS the
		// account has live apps on the ms_billing.apps roster created BEFORE
		// the new period opened (created_at < end): base-fee v1 bills the NEW
		// period's base fee in advance at this boundary even when the closed
		// period metered nothing. Pre-backfill accounts (empty roster) keep
		// the historical no-usage skip, and so does an account whose only
		// apps were created inside the new period — their base is the
		// RegisterApp proration leg's; they join at the NEXT boundary.
		summary, err := svc.RollupPeriod(ctx, a.ID, start, end)
		if err != nil {
			slog.ErrorContext(ctx, "rollup failed", "account_id", a.ID,
				"period_start", start, "period_end", end, "error", err)
			res.Failed++
			continue
		}
		if len(summary.Aggregates) == 0 {
			hasApps, err := svc.AccountHasLiveApps(ctx, a.ID, end)
			if err != nil {
				slog.ErrorContext(ctx, "live app roster check failed", "account_id", a.ID, "error", err)
				res.Failed++
				continue
			}
			if !hasApps {
				continue
			}
		} else {
			res.RolledUp++
		}

		// Phase 2 — charge the just-closed window.
		res.Processed++
		chargeSummary, err := svc.RunBillingCycle(ctx, a.ID, start, end, allowanceMicros)
		if err != nil {
			// A charge error already marked the run 'failed' (auditable, retried
			// next cycle). Count it as both a failed run and a batch error so the
			// exit code is non-zero, but never abort the batch.
			slog.ErrorContext(ctx, "account billing cycle failed",
				"account_id", a.ID, "period_start", start, "period_end", end, "error", err)
			res.FailedRuns++
			res.Failed++
			continue
		}
		tally(&res, a.ID, chargeSummary)
	}

	// Mid-period account-wide overage grace sweep (migration 030): independent of
	// the boundary close above. Every account whose pooled module overage has
	// survived the grace window and whose CURRENT period has no pooled-overage
	// snapshot yet is charged the prorated overage now (a deliberate mid-period
	// charge). Idempotent per (account, period) via the snapshot ledger + the
	// deterministic Stripe idem keys, so firing daily never double-charges.
	runOverageSweep(ctx, svc, at, &res)
	return res
}

// runOverageSweep charges the mid-period account-wide pooled overage for every
// account past the grace window as of `at`. A single account's error is logged +
// counted but never aborts the sweep (like the boundary loop).
func runOverageSweep(ctx context.Context, svc *cycle.Service, at time.Time, res *cycleResult) {
	cands, err := svc.AccountsInOverageGrace(ctx, at)
	if err != nil {
		slog.ErrorContext(ctx, "list overage-grace accounts failed", "error", err)
		res.Failed++
		return
	}
	res.OverageCandidates = len(cands)
	for _, c := range cands {
		summary, err := svc.ChargeAccountOverage(ctx, c, at)
		if err != nil {
			slog.ErrorContext(ctx, "account overage charge failed", "account_id", c.ID, "error", err)
			res.OverageFailed++
			res.Failed++
			continue
		}
		if summary.Status == cycle.OverageCharged || summary.Status == cycle.OverageToppedUp {
			// A top-up (finding #3 — an ALREADY-charged period whose pool grew
			// further before the period closed) is also money charged, not a skip.
			res.OverageCharged++
		} else {
			res.OverageSkipped++
		}
		slog.Info("account overage grace sweep",
			"account_id", c.ID,
			"status", string(summary.Status),
			"over_count", summary.OverCount,
			"charged_cents", summary.ChargedCents,
			"stripe_invoice_id", summary.StripeInvoiceID)
	}
}

// tally classifies one account's charge summary for the run totals + a
// per-account info log. RunBillingCycle returns (nil, err) on a charge failure
// — that path is counted in runCycle, not here — but the RunStatusFailed case is
// covered so the classification is total even if the contract later returns a
// non-nil summary alongside the error.
func tally(res *cycleResult, accountID uuid.UUID, s *cycle.ChargeSummary) {
	switch {
	case !s.FirstRun:
		res.AlreadyRun++
	case s.Status == cycle.RunStatusSkippedNoPM:
		res.SkippedNoPM++
	case s.Status == cycle.RunStatusFailed:
		res.FailedRuns++
	case s.Status == cycle.RunStatusInvoiced && s.ChargedCents == 0:
		// Nothing was actually invoiced: usage arrears AND advance base both 0
		// (ChargedCents, not ArrearsMicros — a base-only boundary invoice has
		// zero arrears but IS a real charge and must count as Charged below).
		res.ZeroArrears++
	case s.Status == cycle.RunStatusInvoiced:
		res.Charged++
	}
	slog.Info("account billing cycle",
		"account_id", accountID,
		"first_run", s.FirstRun,
		"status", string(s.Status),
		"arrears_micros", s.ArrearsMicros,
		"charged_cents", s.ChargedCents,
		"stripe_invoice_id", s.StripeInvoiceID)
}
