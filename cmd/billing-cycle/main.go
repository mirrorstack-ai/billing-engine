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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit/rollout"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/legacyrestamp"
	"github.com/mirrorstack-ai/billing-engine/internal/account/standing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
	intentstore "github.com/mirrorstack-ai/billing-engine/internal/intent/store"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/buildinfo"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/signing"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// allowanceMicros is the per-account usage allowance netted off the arrears
// charge. 0 in v1 (tier-sourced allowance + the advance leg are DEFERRED to the
// subscription/tier PR — they need tier pricing + per-account seat/app counts
// that do not exist in billing yet).
const allowanceMicros int64 = 0

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	restampConfig, err := legacyrestamp.ParseEnvironment(
		legacyrestamp.Environment{
			RestampMode: os.Getenv(legacyrestamp.EnvRestampMode),
			Master:      os.Getenv(legacyrestamp.EnvMaster),
			WorkerMode:  os.Getenv(legacyrestamp.EnvWorkerMode),
			WorkerBPS:   os.Getenv(legacyrestamp.EnvWorkerBPS),
			CoreSHA:     os.Getenv(legacyrestamp.EnvCoreSHA),
			BillingSHA:  os.Getenv(legacyrestamp.EnvBillingSHA),
		},
	)
	if err != nil {
		slog.Error("legacy standing restamp configuration rejected", "error", err)
		os.Exit(1)
	}
	if restampConfig.Enabled {
		runner := buildLegacyRestampRunner()
		if config.IsLambda() {
			lambda.Start(legacyRestampHandler(runner, restampConfig))
			return
		}
		if err := runAllLegacyRestampPages(
			context.Background(),
			runner,
			restampConfig,
		); err != nil {
			os.Exit(1)
		}
		return
	}

	svc := buildService()

	if config.IsLambda() {
		lambda.Start(handler(svc))
		return
	}

	// Local one-shot run: close every card-bound account's just-ended anchored
	// period as of now, sweep the creation-proration grace queue, then sweep the
	// per-module overage grace queue (Leg 1), then exit. No HTTP listener — the
	// dev cycle is a single batch.
	at := time.Now().UTC()
	runOrgAttachSweep(context.Background(), svc, at)
	res := runCycle(context.Background(), svc, at)
	// Proration runs BEFORE the overage sweep. A Stripe creation charge resolves
	// its co-created overage on the combined invoice; a wallet creation charge
	// arms the app guard without resolving those timers, so Leg 1 stops deferring
	// them and charges their overage separately.
	sweepFailed := runProrationSweep(context.Background(), svc, at)
	runOverageSweep(context.Background(), svc, at, &res)
	runDomainSweep(context.Background(), svc, at, &res)
	slog.Info("billing-cycle local run complete",
		"as_of", res.AsOf,
		"activated", res.Activated, "rolled_up", res.RolledUp, "processed", res.Processed, "charged", res.Charged,
		"skipped_no_pm", res.SkippedNoPM, "zero_arrears", res.ZeroArrears,
		"already_run", res.AlreadyRun, "failed_runs", res.FailedRuns, "failed", res.Failed,
		"unactivated_candidates", res.UnactivatedCandidates, "unactivated_rolled_up", res.UnactivatedRolledUp,
		"unactivated_failed", res.UnactivatedFailed,
		"overage_candidates", res.OverageCandidates, "overage_charged", res.OverageCharged,
		"overage_skipped", res.OverageSkipped, "overage_failed", res.OverageFailed,
		"domain_candidates", res.DomainCandidates, "domain_charged", res.DomainCharged,
		"domain_skipped", res.DomainSkipped, "domain_failed", res.DomainFailed)
	if res.Failed > 0 || sweepFailed {
		os.Exit(1)
	}
}

type legacyRestampRunner interface {
	RunPage(context.Context, uuid.UUID) (legacyrestamp.Result, error)
}

func buildLegacyRestampRunner() legacyRestampRunner {
	pool := config.MustPgxPool()
	// Intentionally fresh and wallet-disabled: GetServiceStatus evaluates only
	// legacy card/invoice standing even if an account's stale billing_mode still
	// says credits. NotifyOwner performs the existing idempotent status+POST.
	legacyStatus := billing.NewService(billing.NewStore(pool), nil, "")
	notifier := standing.NewNotifierFromEnvWithStatus(
		pool,
		legacyStatus,
		slog.Default(),
	)
	return legacyrestamp.NewRunner(
		legacyrestamp.NewSource(pool),
		notifier,
		legacyrestamp.DefaultPageSize,
		legacyrestamp.DefaultConcurrency,
	)
}

type legacyRestampRequest struct {
	AfterAccountID string `json:"after_account_id"`
}

type legacyRestampResponse struct {
	Complete           bool   `json:"complete"`
	NextAfterAccountID string `json:"next_after_account_id"`
	Attempted          int    `json:"attempted"`
	Succeeded          int    `json:"succeeded"`
	Failed             int    `json:"failed"`
	Blocked            int    `json:"blocked"`
	TotalOwners        int64  `json:"total_owners"`
	CoreManifestSHA    string `json:"core_manifest_sha"`
	BillingEngineSHA   string `json:"billing_engine_sha"`
}

func legacyRestampHandler(
	runner legacyRestampRunner,
	cfg legacyrestamp.Config,
) func(context.Context, legacyRestampRequest) (legacyRestampResponse, error) {
	return func(
		ctx context.Context,
		request legacyRestampRequest,
	) (legacyRestampResponse, error) {
		after, err := parseLegacyRestampCursor(request.AfterAccountID)
		if err != nil {
			return legacyRestampResponse{}, err
		}
		result, err := runLegacyRestampPage(ctx, runner, cfg, after)
		return legacyRestampResponse{
			Complete:           result.Complete,
			NextAfterAccountID: result.NextCursor,
			Attempted:          result.Scanned,
			Succeeded:          result.Delivered,
			Failed:             result.Failed,
			Blocked:            result.Blocked,
			TotalOwners:        result.TotalOwners,
			CoreManifestSHA:    cfg.CoreSHA,
			BillingEngineSHA:   cfg.BillingSHA,
		}, err
	}
}

func parseLegacyRestampCursor(raw string) (uuid.UUID, error) {
	if raw == "" {
		return uuid.Nil, nil
	}
	cursor, err := uuid.Parse(raw)
	if err != nil || cursor == uuid.Nil || cursor.String() != raw {
		return uuid.Nil, billing.InvalidInput(
			"after_account_id must be empty or a canonical non-zero UUID",
		)
	}
	return cursor, nil
}

func runAllLegacyRestampPages(
	ctx context.Context,
	runner legacyRestampRunner,
	cfg legacyrestamp.Config,
) error {
	after := uuid.Nil
	var (
		expectedTotal int64 = -1
		succeeded     int64
	)
	for {
		result, err := runLegacyRestampPage(ctx, runner, cfg, after)
		if err != nil {
			return err
		}
		if expectedTotal == -1 {
			expectedTotal = result.TotalOwners
		}
		if result.TotalOwners != expectedTotal {
			return fmt.Errorf(
				"legacy restamp owner count changed from %d to %d; restart at empty cursor",
				expectedTotal,
				result.TotalOwners,
			)
		}
		succeeded += int64(result.Delivered)
		if result.Complete {
			if succeeded != expectedTotal {
				return fmt.Errorf(
					"legacy restamp completed %d owners, expected %d; restart at empty cursor",
					succeeded,
					expectedTotal,
				)
			}
			return nil
		}
		next, err := parseLegacyRestampCursor(result.NextCursor)
		if err != nil {
			return err
		}
		after = next
	}
}

func runLegacyRestampPage(
	ctx context.Context,
	runner legacyRestampRunner,
	cfg legacyrestamp.Config,
	after uuid.UUID,
) (legacyrestamp.Result, error) {
	result, err := runner.RunPage(ctx, after)
	log := slog.InfoContext
	if err != nil {
		log = slog.ErrorContext
	}
	log(
		ctx,
		"legacy standing restamp page result",
		"core_manifest_sha", cfg.CoreSHA,
		"billing_engine_sha", cfg.BillingSHA,
		"complete", result.Complete,
		"total_owners", result.TotalOwners,
		"pages", result.Pages,
		"scanned", result.Scanned,
		"delivered", result.Delivered,
		"blocked", result.Blocked,
		"failed", result.Failed,
		"error", err,
	)
	return result, err
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
	candidate := rollout.FromEnv(rollout.ComponentWorker, true)
	schemaReady := false
	if candidate.Active() {
		ready, err := config.CreditRuntimeSchemaReady(context.Background(), pool)
		if err != nil {
			slog.Error("credit runtime schema probe failed", "error", err)
			os.Exit(1)
		}
		schemaReady = ready
	}
	policy := rollout.FromEnv(rollout.ComponentWorker, schemaReady)
	controller := rollout.NewController(policy, rollout.NewReporter(os.Stdout))
	walletEnabled := policy.Active()
	creditAccess := func(accountID uuid.UUID) bool {
		return controller.Decide(accountID).Enforced()
	}

	stripeKey := config.MustEnv("STRIPE_SECRET_KEY")
	svc := cycle.NewService(cycle.NewStore(pool), billingstripe.NewClient(stripeKey)).
		WithCreditWallet(walletEnabled).
		WithCreditRollout(controller)
	coordinator := credit.NewCoordinatorIfReady(
		walletEnabled && controller.Mode() == rollout.ModeEnforce,
		func() *credit.Coordinator {
			counter, err := credit.NewCounter(os.Getenv("REDIS_URL"))
			if err != nil {
				slog.Error("credit estimate cache unavailable; boundary live projection fallback remains active", "error", err)
			}
			standingStore := billing.NewStore(pool)
			projection := usage.NewService(usage.NewStoreWithCreditAccess(
				pool,
				rollout.ReadOnlySelectedAccess(controller),
			))
			coordinator := credit.NewCoordinator(counter, standingStore, projection, nil)
			autoTopUpExecutor := autotopup.NewStandardExecutor(pool, stripeKey).WithSettlementObserver(coordinator)
			coordinator.WithAutoTopUpTrigger(credit.AutoTopUpTriggerFunc(
				func(ctx context.Context, accountID uuid.UUID, projectedChargeMicros int64) (credit.AutoTopUpTriggerResult, error) {
					result, err := autoTopUpExecutor.Trigger(ctx, accountID, projectedChargeMicros)
					return credit.AutoTopUpTriggerResult{
						Attempted:  result.Triggered,
						NewAttempt: result.NewAttempt,
						Terminal:   result.Status == "settled" || result.Status == "failed",
					}, err
				},
			))

			standingSvc := billing.NewService(standingStore, nil, "").
				WithCreditWallet(true).
				WithCreditAccess(creditAccess).
				WithCreditCoordinator(rollout.NewGate(controller, nil, coordinator), coordinator)
			notifier := standing.NewNotifierFromEnvWithStatus(pool, standingSvc, slog.Default())
			if notifier.Enabled() {
				coordinator.WithNotifier(notifier)
			}
			return coordinator
		})
	if coordinator != nil {
		svc.WithBoundaryEstimateReconciler(coordinator).
			WithWalletMutationObserver(coordinator)
	}
	return withIntentCutover(svc, pool, os.Getenv(intentCutoverEnv))
}

// intentCutoverEnv arms the intent cutover for every leg that has one.
//
// It must be set to the literal string below. A flag whose truthiness is
// inferred from "1", "true" or "yes" would let a typo in a deploy
// template stop this worker collecting, and a worker that proposes
// collects nothing at all.
const (
	intentCutoverEnv   = "BILLING_CYCLE_INTENT_CUTOVER"
	intentCutoverArmed = "propose-do-not-collect"
)

// withIntentCutover attaches the proposer seam, or leaves the service on
// the legacy collecting path.
//
// 🔴 Arming this STOPS THIS WORKER COLLECTING FOR NEW CHARGES. A
// cut-over leg derives the same amount, seals it as an intent, stores it
// and returns — and cmd/intent-executor, which holds the only write
// port, refuses to start while any legacy money path remains. Nothing
// downstream picks the intent up yet. So the flag's effect today is a
// revenue stop, not a migration, and it exists to make the seam
// REACHABLE rather than to be switched on.
//
// ⚠️ It is NOT "this worker collects nothing", and the difference is
// load-bearing. Each leg takes its durable arming claim BEFORE the
// proposer branch, and each leg's crash-recovery path runs BEFORE that
// claim (domain_charges.go:100, overage.go:262). A row armed by a
// legacy run that left a draft or finalized invoice at the provider is
// therefore still completed after this flag is set.
//
// That is deliberate. Abandoning a finalized invoice would strand a
// charge the customer can see and nobody can finish or prove. The
// exception drains: once no row carries an unresolved charge-attempt
// marker, the recovery path has nothing left to complete —
// scripts/legacy-drop-preconditions.sql asks production exactly that
// question, and it is one of the preconditions for deleting the
// collectors.
//
// Until 2026-08-30 WithIntentProposer had no non-test caller at all: the
// cutover branch in every leg was unreachable in production, and the two
// legs described as "cut over" could not propose anything on a
// deployed worker. That is the failure this function exists to make
// impossible to repeat — the arming path is now exercised by a test
// rather than asserted in a comment.
func withIntentCutover(svc *cycle.Service, pool *pgxpool.Pool, flag string) *cycle.Service {
	arm, err := intentCutoverDecision(flag)
	if err != nil {
		slog.Error("intent cutover flag is not a recognised value; refusing to start",
			"env", intentCutoverEnv, "error", err)
		os.Exit(1)
	}
	if !arm {
		return svc
	}
	// 🔴 Arming the cutover now REQUIRES an evidence signing key.
	//
	// Sealing an intent is the first of docs/DESIGN.md:388's eight evidence
	// events, and :398 makes an evidence record a durable side effect of the
	// money moving rather than a report something chooses to render. A
	// deployment that can seal charge documents but cannot record them
	// produces documents the customer has no independent trace of, and no
	// later reconciler can tell "never recorded" from "recorded and withheld".
	//
	// So this refuses to start rather than degrading. The alternative —
	// proposing without evidence when the key is absent — is the silent-skip
	// this design exists to remove, and it would be invisible: the legs would
	// run, intents would appear, and the outbox would simply stay empty.
	//
	// The flag is unset in every environment today, so nothing changes until
	// somebody deliberately arms it, which is exactly when they should be
	// told a key is missing.
	signer, err := signing.Load(os.Getenv)
	if err != nil {
		slog.Error("intent cutover is armed but the signing key material will not load; refusing to start",
			"env", intentCutoverEnv, "error", err.Error())
		os.Exit(1)
	}
	recorder, err := evidence.NewRecorder(signer, evidence.Options{
		Issuer:      "billing-engine",
		Audience:    "customer",
		Environment: buildinfo.Current().Environment,
		Now:         func() time.Time { return time.Now().UTC() },
	})
	if err != nil {
		slog.Error("intent cutover is armed but this deployment cannot record evidence; refusing to start",
			"env", intentCutoverEnv,
			"needs", signing.EnvBillingEvidenceKey,
			"why", "docs/DESIGN.md INV-014: an evidence record is a side effect of the money moving, not a report",
			"error", err.Error())
		os.Exit(1)
	}

	p, err := proposer.New(intentstore.New(pool), recorder, func() time.Time { return time.Now().UTC() })
	if err != nil {
		slog.Error("intent cutover is armed but the proposer will not construct; refusing to start",
			"env", intentCutoverEnv, "error", err.Error())
		os.Exit(1)
	}

	slog.Warn("INTENT CUTOVER ARMED — cut-over legs will propose sealed intents instead of charging",
		"env", intentCutoverEnv,
		"evidence_key", recorder != nil,
		"exception", "in-flight legacy charges are still completed by each leg's crash-recovery path")
	return svc.WithIntentProposer(p)
}

// errUnrecognisedCutoverFlag refuses a flag that is neither unset nor the
// exact armed value.
//
// It refuses rather than defaulting to legacy on purpose. Defaulting
// would make "BILLING_CYCLE_INTENT_CUTOVER=true" silently collect from
// customers while an operator believed the worker was only proposing,
// and a wrong belief about whether money is moving is worse than a
// worker that will not start.
var errUnrecognisedCutoverFlag = errors.New("unrecognised intent cutover flag")

// intentCutoverDecision is the whole policy, as a pure function, so the
// arming path can be exercised by a test instead of reasoned about.
func intentCutoverDecision(flag string) (arm bool, err error) {
	switch flag {
	case "":
		return false, nil
	case intentCutoverArmed:
		return true, nil
	default:
		return false, fmt.Errorf("%w: %q (expected %q or unset)",
			errUnrecognisedCutoverFlag, flag, intentCutoverArmed)
	}
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
		runOrgAttachSweep(ctx, svc, at.UTC())
		res := runCycle(ctx, svc, at.UTC())
		// Proration runs BEFORE the overage sweep (see main): Stripe creations
		// resolve co-created overage on their combined invoice; wallet creations
		// arm the guard so Leg 1 charges that overage separately.
		runProrationSweep(ctx, svc, at.UTC())
		runOverageSweep(ctx, svc, at.UTC(), &res)
		runDomainSweep(ctx, svc, at.UTC(), &res)
		slog.InfoContext(ctx, "billing-cycle lambda run complete",
			"as_of", res.AsOf,
			"activated", res.Activated, "rolled_up", res.RolledUp, "processed", res.Processed, "charged", res.Charged,
			"skipped_no_pm", res.SkippedNoPM, "zero_arrears", res.ZeroArrears,
			"already_run", res.AlreadyRun, "failed_runs", res.FailedRuns, "failed", res.Failed,
			"unactivated_candidates", res.UnactivatedCandidates, "unactivated_rolled_up", res.UnactivatedRolledUp,
			"unactivated_failed", res.UnactivatedFailed,
			"overage_candidates", res.OverageCandidates, "overage_charged", res.OverageCharged,
			"overage_skipped", res.OverageSkipped, "overage_failed", res.OverageFailed,
			"domain_candidates", res.DomainCandidates, "domain_charged", res.DomainCharged,
			"domain_skipped", res.DomainSkipped, "domain_failed", res.DomainFailed)
		// A per-account charge failure (or a per-app proration failure) is recorded
		// (billing_runs status='failed')
		// and does NOT fail the batch — the next cycle retries it. The handler
		// returns nil so EventBridge doesn't replay the whole batch.
		return nil
	}
}

// cycleResult tallies a batch run for logging / exit code.
type cycleResult struct {
	AsOf                  time.Time // the run's evaluation instant (UTC)
	Activated             int       // card-bound accounts considered
	RolledUp              int       // accounts whose just-closed window had usage (rolled up)
	Processed             int       // accounts processed in the charge phase
	Charged               int
	SkippedNoPM           int
	ZeroArrears           int
	AlreadyRun            int
	FailedRuns            int // per-account charge runs that ended status='failed'
	Failed                int // errors (rollup error, charge error, or list error)
	UnactivatedCandidates int // card-less accounts considered for rollup only
	UnactivatedRolledUp   int // card-less accounts whose calendar-month rollup completed
	UnactivatedFailed     int // card-less list or per-account rollup errors

	// Mid-period per-module overage grace sweep (migration 033, Leg 1).
	OverageCandidates int // install timers past their grace window this sweep evaluated
	OverageCharged    int // "over" installs whose overage was invoiced mid-period
	OverageSkipped    int // evaluated but not charged (resolved-included / no PM / 0 cents)
	OverageFailed     int // per-timer overage-charge errors (counted, never abort)

	// Mid-period custom-domain activation-period sweep (migration 047).
	DomainCandidates int // live unresolved domains activated by this sweep instant
	DomainCharged    int // activation-period prorations invoiced this sweep
	DomainSkipped    int // resolved without charge or transiently skipped
	DomainFailed     int // per-domain errors (counted, never abort)
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
	processed := make(map[uuid.UUID]bool)

	accounts, err := svc.ActivatedAccounts(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "list activated accounts failed", "error", err)
		res.Failed++
		return res
	}
	res.Activated = len(accounts)

	for _, a := range accounts {
		processed[a.ID] = true
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
				hasDomains, err := svc.AccountHasLiveDomains(ctx, a.ID, end)
				if err != nil {
					slog.ErrorContext(ctx, "live custom-domain check failed", "account_id", a.ID, "error", err)
					res.Failed++
					continue
				}
				if !hasDomains {
					continue
				}
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
	runUnactivatedRollup(ctx, svc, at, processed, &res)
	return res
}

// unactivatedRoller is deliberately NARROW: it exposes rollup and nothing else.
// The rollup-only phase for card-less accounts is handed this interface rather
// than *cycle.Service so that charging one of these accounts is a COMPILE ERROR,
// not a code-review promise. Do not widen it.
type unactivatedRoller interface {
	UnactivatedAccountsWithUsage(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error)
	RollupPeriod(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (*cycle.RollupSummary, error)
}

// runUnactivatedRollup uses the calendar month because AccountAnchorDay falls
// back to DefaultAnchorDay while activated_at is NULL, so aggregate writes must
// match the windows reads already request. After card binding, runCycle's
// existing straddle-clamp bridges calendar→anchor as it does for cutover 025.
func runUnactivatedRollup(ctx context.Context, svc unactivatedRoller, at time.Time, skip map[uuid.UUID]bool, res *cycleResult) {
	start, end := billingperiod.AnchoredJustClosed(at, billingperiod.DefaultAnchorDay)
	accounts, err := svc.UnactivatedAccountsWithUsage(ctx, start, end)
	if err != nil {
		slog.ErrorContext(ctx, "list unactivated accounts with usage failed", "error", err)
		res.UnactivatedFailed++
		res.Failed++
		return
	}
	res.UnactivatedCandidates = len(accounts)
	for _, accountID := range accounts {
		if skip[accountID] {
			continue
		}
		if _, err := svc.RollupPeriod(ctx, accountID, start, end); err != nil {
			slog.ErrorContext(ctx, "unactivated account rollup failed", "account_id", accountID,
				"period_start", start, "period_end", end, "error", err)
			res.UnactivatedFailed++
			res.Failed++
			continue
		}
		res.UnactivatedRolledUp++
	}
}

// runOrgAttachSweep runs before runCycle. The placement does not change which
// period recovered events bill: the attach clamp puts them in the account's
// currently open window, the first period that closes after designation.
func runOrgAttachSweep(ctx context.Context, svc *cycle.Service, at time.Time) {
	summary, err := svc.SweepUnattachedOrgUsage(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "org usage attach sweep failed", "as_of", at, "error", err)
		return
	}
	slog.InfoContext(ctx, "org usage attach sweep complete", "as_of", at,
		"orgs", summary.Orgs, "swept", summary.Swept, "attached_apps", summary.AttachedApps,
		"repointed_events", summary.RepointedEvents, "failed", summary.Failed)
}

// runOverageSweep runs Leg 1's per-module overage grace sweep as of `at`. A
// single timer's error is logged + counted inside the sweep but never aborts the
// batch (the next sweep retries it through the same deterministic Stripe keys).
func runOverageSweep(ctx context.Context, svc *cycle.Service, at time.Time, res *cycleResult) {
	sweep, err := svc.SweepModuleOverage(ctx, at)
	if err != nil {
		slog.ErrorContext(ctx, "module overage sweep failed", "as_of", at, "error", err)
		res.Failed++
		return
	}
	res.OverageCandidates = sweep.Pending
	res.OverageCharged = sweep.Charged
	// "Skipped" folds the resolved-as-included verdicts and the transient no-PM /
	// zero-cent skips — everything evaluated but not charged this sweep.
	res.OverageSkipped = sweep.Included + sweep.Skipped
	res.OverageFailed = sweep.Failed
	res.Failed += sweep.Failed
	slog.InfoContext(ctx, "module overage sweep complete",
		"as_of", at, "pending", sweep.Pending, "charged", sweep.Charged,
		"included", sweep.Included, "skipped", sweep.Skipped, "failed", sweep.Failed)
}

// runDomainSweep runs the activation-period custom-domain charge sweep after
// the module-overage sweep. Each domain is independently resumable through its
// deterministic Stripe keys and durable charge-attempt marker.
func runDomainSweep(ctx context.Context, svc *cycle.Service, at time.Time, res *cycleResult) {
	sweep, err := svc.SweepDomainCharges(ctx, at)
	if err != nil {
		slog.ErrorContext(ctx, "custom-domain sweep failed", "as_of", at, "error", err)
		res.Failed++
		return
	}
	res.DomainCandidates = sweep.Pending
	res.DomainCharged = sweep.Charged
	res.DomainSkipped = sweep.Resolved + sweep.Skipped
	res.DomainFailed = sweep.Failed
	res.Failed += sweep.Failed
	slog.InfoContext(ctx, "custom-domain sweep complete",
		"as_of", at, "pending", sweep.Pending, "charged", sweep.Charged,
		"resolved", sweep.Resolved, "skipped", sweep.Skipped, "failed", sweep.Failed)
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
