// Package capabilities reports what this deployment can do and what it
// has not yet stopped doing.
//
// docs/VERIFICATION.md §2 requires a Capabilities surface a verifier can
// pin, and states the condition the whole migration turns on:
//
//	The intent-only claim requires legacyMoneyPaths: 0. A strong new
//	surface beside one weak legacy route is not a strong deployment. It
//	is the weak route with better documentation.
//
// The full surface §2 specifies — policy digests, adapter conformance
// revisions, numeric limits, per-component readiness — describes an
// intent model that does not exist yet. What this package reports is
// the part that is true today: which build answered, and how many ways
// this tree can still take money without an intent.
//
// LegacyMoneyPaths is a constant rather than a runtime count, because
// the thing being reported is a property of the source, not of the
// process. What keeps it honest is that internal/architecture fails the
// build when it disagrees with an AST scan of the tree. So the service
// reports a number CI has proven, rather than a number it has computed
// about itself.
package capabilities

import "github.com/mirrorstack-ai/billing-engine/internal/shared/buildinfo"

// LegacyMoneyPaths is the number of call sites in this tree that can
// take money from a stored payment method without passing through an
// intent.
//
// Every one of them is named in internal/architecture's inventory with
// what it charges for. The number reaches zero when the last of them is
// deleted — not when an intent surface is added beside them.
//
// Do not edit this without deleting a money path. The architecture test
// will fail, which is the point.
// 🔴 THREE, down from eleven. The legacy collectors were deleted 2026-09-01.
//
// What remains is not a collector. Two are CRASH-RECOVERY paths that finish a
// charge a legacy run already put in front of the provider — abandoning one
// would strand an invoice the customer can already see and nobody can prove —
// and the third is a scan false positive on billing's own Service.PayInvoice.
//
// They drain rather than being deleted: nothing stamps a provider marker any
// more, so once no row carries an unresolved charge-attempt marker the
// recovery paths find nothing and only ever fall through to the proposal.
// scripts/legacy-drop-preconditions.sql is what measures when that is true.
//
// Until it reaches ZERO, cmd/intent-executor still refuses to start — which is
// correct: the recovery paths and the executor must not both be able to settle
// the same obligation.
const LegacyMoneyPaths = 3

// Report is what Capabilities returns.
type Report struct {
	Build buildinfo.Info `json:"build"`

	// LegacyMoneyPaths counts the reachable ways to charge a customer
	// that do not consume an intent.
	LegacyMoneyPaths int `json:"legacy_money_paths"`

	// IntentOnly is the claim docs/DESIGN.md §11 defines. It is false
	// while any legacy path remains, and it is deliberately computed
	// here rather than configured, so that no deployment can assert it.
	IntentOnly bool `json:"intent_only"`

	// IntentExecutionReady reports whether this deployment may execute
	// an intent. It requires an identified build: docs/VERIFICATION.md
	// §2 says an executor whose build identity reads "unknown" must
	// refuse to execute.
	IntentExecutionReady bool `json:"intent_execution_ready"`
}

// Current returns this deployment's capability report.
func Current() Report {
	build := buildinfo.Current()
	intentOnly := LegacyMoneyPaths == 0
	return Report{
		Build:                build,
		LegacyMoneyPaths:     LegacyMoneyPaths,
		IntentOnly:           intentOnly,
		IntentExecutionReady: intentOnly && build.Identified,
	}
}
