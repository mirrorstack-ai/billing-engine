// Command intent-executor executes sealed charge intents.
//
// It is the isolated executor deployment docs/VERIFICATION.md §5 asks
// for: "Provider-write interfaces may be injected only into the
// isolated executor deployment. The planner, read, usage-ingress,
// notifier and reconciler binaries must not compile against a write
// port at all."
//
// A separate binary is what makes that checkable. The write port is
// constructed here and nowhere else, so "which deployments hold a
// mutation-capable credential" is answered by reading the cmd/
// directory rather than by tracing injection through a service graph.
//
// 🔴 It is OFF by default and does nothing until three things are true:
// INTENT_EXECUTOR_ENABLED is set, the build is stamped, and the
// deployment reports no legacy money paths. Each is checked at startup
// and refused loudly, because an executor that starts anyway and
// declines every intent looks identical to one that is working and
// finding nothing to do.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/account/capabilities"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/executor"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/predicate"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/store"
	"github.com/mirrorstack-ai/billing-engine/internal/provider/stripeadapter"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/buildinfo"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/signing"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

func main() {
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	if err := readiness(capabilities.Current(), os.Getenv("INTENT_EXECUTOR_ENABLED")); err != nil {
		slog.Error("intent executor will not start", "reason", err.Error(),
			"build", buildinfo.Current(), "capabilities", capabilities.Current())
		os.Exit(1)
	}

	pool := config.MustPgxPool()
	defer pool.Close()

	s := store.New(pool)
	// The resolver is the store's, so the subject a proposer seals and the
	// subject an executor resolves are written in one place. They were not,
	// and they disagreed — see internal/intent/store/payer.go.
	adapter := stripeadapter.New(
		billingstripe.NewIntentClient(config.MustEnv("STRIPE_SECRET_KEY")),
		s,
	)

	// 🔴 The executor cannot start without an evidence signing key.
	//
	// Every branch of Execute produces one of docs/DESIGN.md:388's eight
	// events, and :398 makes an evidence record a durable side effect of the
	// money moving rather than a report something chooses to render. So a
	// deployment that could execute but not record would settle charges the
	// customer has no independent trace of — and it would look identical to
	// one that is working, which is the failure this binary's readiness
	// checks already exist to prevent.
	signer, err := signing.Load(os.Getenv)
	if err != nil {
		slog.Error("signing key material will not load", "reason", err.Error())
		os.Exit(1)
	}
	recorder, err := evidence.NewRecorder(signer, evidence.Options{
		Issuer:      "billing-engine",
		Audience:    "customer",
		Environment: buildinfo.Current().Environment,
		Now:         func() time.Time { return time.Now().UTC() },
	})
	if err != nil {
		slog.Error("this deployment cannot record evidence, so it must not execute intents",
			"needs", signing.EnvBillingEvidenceKey,
			"why", "docs/DESIGN.md INV-014: an evidence record is a side effect of the money moving",
			"reason", err.Error())
		os.Exit(1)
	}

	exec, err := executor.New(s, adapter, recorder, buildinfo.Current().Artifact,
		func() time.Time { return time.Now().UTC() },
		environment,
	)
	if err != nil {
		slog.Error("executor will not construct", "reason", err.Error())
		os.Exit(1)
	}
	// The work loop.
	//
	// It was deliberately absent until now, and the reason it was absent is
	// worth keeping: "a poller here would be an arming path that has never
	// been exercised against a real intent, which docs/SECURITY.md treats as
	// its own kind of defect." That objection is answered by exercising it —
	// internal/intent/executor drives RunOnce against a real Postgres and a
	// real sealed intent, through a refusal, an in-flight result and a
	// settlement.
	//
	// One bounded pass per invocation, not a daemon. This binary holds the
	// only mutation-capable provider credential in the deployment, and
	// docs/VERIFICATION.md §5's write-port isolation is the reason not to
	// keep that open indefinitely.
	//
	// 🔴 On a deployment where every gate is false — which is every
	// deployment today — this finds work and refuses all of it. That is the
	// honest outcome, and the summary says so: an executor that found nothing
	// and one that refused everything it found are different states, and only
	// the second is telling the truth about a deployment whose supporting
	// records do not exist yet.
	result, err := executor.RunOnce(ctx, s, exec, executionBatchLimit, slog.Default())
	if err != nil {
		slog.Error("execution pass failed", "reason", err.Error())
		os.Exit(1)
	}

	slog.Info("execution pass complete",
		"considered", result.Considered,
		"settled", result.Settled,
		"in_progress", result.InProgress,
		"unresolved", result.Unresolved,
		"refused", result.Refused,
		"errors", result.Errors,
		"refused_clauses", result.RefusedClauses,
		"build", buildinfo.Current())
}

// executionBatchLimit bounds one pass.
//
// Bounded because an unbounded pass on a backlog would hold the write port
// open for as long as the backlog takes, and because a Lambda invocation that
// cannot finish is one that gets killed mid-collection. What is not executed
// this pass is not claimed, so the next invocation picks it up.
const executionBatchLimit = 100

// Readiness failures, distinguished so the operator sees which.
var (
	errNotEnabled         = errors.New("INTENT_EXECUTOR_ENABLED is not set")
	errBuildNotIdentified = errors.New("the build is not stamped, so a charge could not be tied to a revision")
	errLegacyPathsRemain  = errors.New("legacy money paths are still reachable in this deployment")
)

// readiness decides whether this binary may run.
//
// All three conditions, and each refuses loudly rather than degrading.
// docs/VERIFICATION.md §2: "An executor whose build identity reads
// `unknown` must refuse to execute", and §11's intent-only claim
// requires legacyMoneyPaths: 0 — "a strong new surface beside one weak
// legacy route is not a strong deployment, it is the weak route with
// better documentation."
//
// Split from main so the rule is testable without an environment.
func readiness(caps capabilities.Report, enabledFlag string) error {
	if enabledFlag == "" {
		return errNotEnabled
	}
	if !caps.Build.Identified {
		return errBuildNotIdentified
	}
	if caps.LegacyMoneyPaths != 0 {
		return errLegacyPathsRemain
	}
	return nil
}

// environment reports the gates this deployment can currently satisfy.
//
// Everything is false. That is the honest state: none of the supporting
// records exist, so the predicate refuses every intent. Returning true
// for a gate whose evidence is unbuilt would be the "declared but not
// implemented" failure docs/SECURITY.md exists to expose — and the one
// place it would do real damage is here, where the answer decides
// whether money moves.
func environment(context.Context) executor.Environment {
	return executor.Environment{
		BuildIdentified: buildinfo.Identified(),
		// The rest stay false until the record behind each one exists.
		PolicyDigestsMatch:           false,
		TimeReady:                    false,
		TaxIndependentlyReproducible: false,
		Unbuilt:                      predicate.UnbuiltEvidence{},
	}
}
