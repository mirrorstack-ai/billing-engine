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
// Period window: the cycle processes a [periodStart, periodEnd) window and
// charges every account with unbilled usage_aggregates in it. The window is
// each account's just-closed SIGNUP-DAY anniversary period (design §4 Axis 4);
// per the trust boundary, billing-engine does NOT read ms_account.users
// directly to derive per-account signup days — the upstream trigger
// (api-platform cron / EventBridge payload) supplies the window. For v1 the
// window defaults to the just-closed UTC calendar month (a uniform anchor) when
// no explicit window is provided; the per-account signup-day anchor lands with
// the subscription/tier PR that also adds the ADVANCE leg.
//
// allowanceMicros is 0 for v1 (the allowance-netting math is implemented in
// cycle.RunBillingCycle; tier-sourced allowance + the advance leg are DEFERRED
// to the subscription/tier PR).
//
// Spec: docs-temp/milestone-d-meter/design.md §4 Axis 4 / §5 / §6 (PR #6).
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

	// Local one-shot run: derive the just-closed window and process it, then
	// exit. No HTTP listener — the dev cycle is a single batch invocation.
	start, end := justClosedCalendarMonth(time.Now().UTC())
	res := runCycle(context.Background(), svc, start, end)
	slog.Info("billing-cycle local run complete",
		"period_start", start, "period_end", end,
		"rolled_up", res.RolledUp, "processed", res.Processed, "charged", res.Charged,
		"skipped_no_pm", res.SkippedNoPM, "zero_arrears", res.ZeroArrears,
		"already_run", res.AlreadyRun, "failed_runs", res.FailedRuns, "failed", res.Failed)
	if res.Failed > 0 {
		os.Exit(1)
	}
}

// buildService wires the pgxpool + Stripe client into the cycle Service. The
// Stripe secret is required (the charge leg cannot run without it).
func buildService() *cycle.Service {
	pool := config.MustPgxPool()
	stripeKey := config.MustEnv("STRIPE_SECRET_KEY")
	return cycle.NewService(cycle.NewStore(pool), billingstripe.NewClient(stripeKey))
}

// handler is the Lambda entrypoint for an EventBridge-scheduled invocation. The
// CloudWatchEvent carries no window today (the scheduler fires on a cron), so
// the handler derives the just-closed UTC calendar-month window from the event
// time. A future trigger can pass an explicit window in the event detail.
func handler(svc *cycle.Service) func(context.Context, events.CloudWatchEvent) error {
	return func(ctx context.Context, ev events.CloudWatchEvent) error {
		at := ev.Time
		if at.IsZero() {
			at = time.Now().UTC()
		}
		start, end := justClosedCalendarMonth(at.UTC())
		res := runCycle(ctx, svc, start, end)
		slog.InfoContext(ctx, "billing-cycle lambda run complete",
			"period_start", start, "period_end", end,
			"rolled_up", res.RolledUp, "processed", res.Processed, "charged", res.Charged,
			"skipped_no_pm", res.SkippedNoPM, "zero_arrears", res.ZeroArrears,
			"already_run", res.AlreadyRun, "failed_runs", res.FailedRuns, "failed", res.Failed)
		// A per-account charge failure is recorded (billing_runs status='failed')
		// and does NOT fail the batch — the next cycle retries it. The handler
		// returns nil so EventBridge doesn't replay the whole batch.
		return nil
	}
}

// cycleResult tallies a batch run for logging / exit code.
type cycleResult struct {
	RolledUp    int // accounts rolled up in phase 1
	Processed   int // accounts processed in the charge phase
	Charged     int
	SkippedNoPM int
	ZeroArrears int
	AlreadyRun  int
	FailedRuns  int // per-account charge runs that ended status='failed'
	Failed      int // errors (rollup error, charge error, or list error)
}

// runCycle runs the two-phase cycle for the window:
//
//	Phase 1 (rollup): for every account with raw usage_events in the window,
//	  RollupPeriod prices the events into usage_aggregates. Without this the
//	  charge phase's PeriodChargedTotal reads 0 and silently bills nothing.
//	Phase 2 (charge): for every account whose usage_aggregates have no
//	  successful (invoiced) run yet, RunBillingCycle nets arrears and charges.
//
// A single account's error is logged + counted but never aborts the batch
// (resumable: a fresh re-fire re-rolls + re-charges only what is not yet
// invoiced; billing_runs reclaims non-terminal rows).
func runCycle(ctx context.Context, svc *cycle.Service, periodStart, periodEnd time.Time) cycleResult {
	var res cycleResult

	// Phase 1 — rollup. Price raw usage_events into usage_aggregates so the
	// charge phase has something to read.
	rollupAccounts, err := svc.AccountsWithUsageEvents(ctx, periodStart, periodEnd)
	if err != nil {
		slog.ErrorContext(ctx, "list accounts with usage events failed", "error", err)
		res.Failed++
		return res
	}
	for _, accountID := range rollupAccounts {
		if _, err := svc.RollupPeriod(ctx, accountID, periodStart, periodEnd); err != nil {
			slog.ErrorContext(ctx, "rollup failed", "account_id", accountID, "error", err)
			res.Failed++
			continue
		}
		res.RolledUp++
	}

	// Phase 2 — charge. Bill each account with unbilled priced usage.
	accounts, err := svc.AccountsWithUnbilledUsage(ctx, periodStart, periodEnd)
	if err != nil {
		slog.ErrorContext(ctx, "list unbilled accounts failed", "error", err)
		res.Failed++
		return res
	}
	for _, accountID := range accounts {
		res.Processed++
		summary, err := svc.RunBillingCycle(ctx, accountID, periodStart, periodEnd, allowanceMicros)
		if err != nil {
			// A charge error already marked the run 'failed' (auditable, retried
			// next cycle). Count it as both a failed run and a batch error so the
			// exit code is non-zero, but never abort the batch.
			slog.ErrorContext(ctx, "account billing cycle failed",
				"account_id", accountID, "error", err)
			res.FailedRuns++
			res.Failed++
			continue
		}
		tally(&res, accountID, summary)
	}
	return res
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
	case s.Status == cycle.RunStatusInvoiced && s.ArrearsMicros == 0:
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

// justClosedCalendarMonth returns the [start, end) window of the calendar month
// just BEFORE the month containing `at` (UTC). The v1 uniform anchor; the
// per-account signup-day anniversary window arrives with the subscription/tier
// PR. e.g. at=2026-06-19 → [2026-05-01, 2026-06-01).
func justClosedCalendarMonth(at time.Time) (time.Time, time.Time) {
	end := time.Date(at.Year(), at.Month(), 1, 0, 0, 0, 0, time.UTC)
	start := end.AddDate(0, -1, 0)
	return start, end
}
