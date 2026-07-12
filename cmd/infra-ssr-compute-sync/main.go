// Command infra-ssr-compute-sync is the scheduled AWS Lambda/CloudWatch
// PULLER for app-hosting SSR compute metering — the platform-infra (Plane 1)
// chokepoint for `infra.compute.ssr.gb_seconds` / `infra.compute.ssr.request.count`.
//
// Each SSR app runs its own dedicated Lambda ("ms-apphost-<app_id>-<env>"),
// invoked directly by cdn-worker via IAM SigV4 lambda:InvokeFunction — no API
// Gateway, no dispatch layer, no existing metering chokepoint in the path at
// all. This binary closes that gap by PULLING usage back out of AWS itself:
// it enumerates the ms-apphost-* fleet via lambda:ListFunctions, queries
// cloudwatch:GetMetricData for each function's Duration/Invocations sums over
// the last few FULLY-CLOSED hour windows, and for each (app, env, window)
// calls billing-engine's RecordInfraUsage. Direction is billing-engine → AWS
// (an outbound pull, read-only) — mirrors cmd/infra-egress-sync's
// Cloudflare-pull shape exactly, substituting AWS CloudWatch/Lambda for
// Cloudflare's GraphQL Analytics API.
//
// Idempotency by construction: the event_id is a DETERMINISTIC hash of
// (metric, app_id, env, window_start) — see ssrEventID — so re-querying an
// already-ingested window produces the SAME ids and RecordInfraUsage's ON
// CONFLICT(event_id) DO NOTHING dedupes the re-run. Only CLOSED windows,
// aged past a CloudWatch propagation-lag margin, are ever queried (never the
// current partial hour, and never a window that closed less than
// propagationLag ago) — see windows.go.
//
// Dual-transport (same logic, two harnesses) — mirrors cmd/infra-egress-sync
// and cmd/billing-cycle:
//   - AWS_LAMBDA_FUNCTION_NAME set → lambda.Start(handler), driven by an
//     EventBridge Scheduler in production (a CloudWatchEvent). The schedule
//     is created State=DISABLED initially (see design doc §3 Decision B,
//     §7 Open Question 1b — an explicit product-owner decision on the
//     shared NewBillingStack's nightly teardown is still pending).
//   - Otherwise → a one-shot local run (make dev-ssr-compute-sync / go run
//     .), so dev never needs Lambda or a scheduler.
//
// AWS auth: region + credentials resolve through the SDK's default chain
// (the Lambda execution role in production) — unlike the Cloudflare puller,
// there is no separate API token/secret to hold. Requires only DATABASE_URL
// (+ optional DB_AUTH) like every other billing-engine binary.
//
// Spec: docs-temp/app-hosting/ssr-metering-design.md.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/awslambdainv"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	svc, lister, querier, idle := buildDeps()

	if config.IsLambda() {
		lambda.Start(handler(svc, lister, querier, idle))
		return
	}

	// Local one-shot run: sweep the closed windows ending at the current
	// hour (minus the propagation-lag margin), then exit.
	res := syncSSR(context.Background(), svc, lister, querier, idle, time.Now().UTC())
	logResult(context.Background(), "infra-ssr-compute-sync local run complete", res)
	if res.Failed {
		os.Exit(1)
	}
}

// buildDeps wires the pgxpool-backed usage.Service, the AWS Lambda/CloudWatch
// client (internal/shared/awslambdainv), and the idle-prefilter's direct
// pgxpool lookup, from config/the ambient AWS SDK credential chain.
func buildDeps() (*usage.Service, lambdaLister, metricsQuerier, idleChecker) {
	pool := config.MustPgxPool()
	svc := usage.NewService(usage.NewStore(pool))

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		slog.Error("ssr-compute-sync: load aws config failed", "error", err)
		os.Exit(1)
	}
	client := awslambdainv.NewClient(awsCfg)

	return svc, client, client, newPgxIdleChecker(pool)
}

// handler is the Lambda entrypoint for an EventBridge-scheduled invocation.
// The CloudWatchEvent carries no window (the scheduler fires on a cron), so
// the handler derives the closed-hour lookback from the event time.
func handler(svc *usage.Service, lister lambdaLister, querier metricsQuerier, idle idleChecker) func(context.Context, events.CloudWatchEvent) error {
	return func(ctx context.Context, ev events.CloudWatchEvent) error {
		at := ev.Time
		if at.IsZero() {
			at = time.Now().UTC()
		}
		res := syncSSR(ctx, svc, lister, querier, idle, at.UTC())
		logResult(ctx, "infra-ssr-compute-sync lambda run complete", res)
		// Enumeration failure fails the run (surfaces for EventBridge
		// retry/alerting); per-batch and per-row errors are logged + counted
		// but never abort the sweep.
		if res.Failed {
			return res.Err
		}
		return nil
	}
}

// logResult emits a single structured summary line for the sweep.
func logResult(ctx context.Context, msg string, res ssrSyncResult) {
	slog.InfoContext(ctx, msg,
		"functions", res.Functions, "skipped_idle", res.SkippedIdle,
		"batches", res.Batches, "batch_errors", res.BatchErrors,
		"not_ready", res.NotReady, "windows", res.Windows,
		"recorded", res.Recorded, "deduped", res.Deduped,
		"row_errors", res.RowErrors, "failed", res.Failed)
}
