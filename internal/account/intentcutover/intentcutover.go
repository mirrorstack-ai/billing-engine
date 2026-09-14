// Package intentcutover is the ONE contract for arming the intent proposer
// seam in a deployment: the flag, its exact armed value, and the construction
// of a proposer with its evidence recorder. cmd/billing-cycle and
// cmd/account-api both arm through it, so the two Lambdas cannot drift on
// what "armed" means (core-v2#1476 — production billing-cycle refused to
// start with the flag unset while account-api never had a proposer at all).
package intentcutover

import (
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
	intentstore "github.com/mirrorstack-ai/billing-engine/internal/intent/store"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/buildinfo"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/signing"
)

// Env arms the intent cutover for every leg that has one. It must be set to
// the literal Armed. A flag whose truthiness is inferred from "1", "true" or
// "yes" would let a typo in a deploy template stop a worker collecting, and a
// worker that proposes collects nothing at all.
const (
	Env   = "BILLING_CYCLE_INTENT_CUTOVER"
	Armed = "propose-do-not-collect"
)

// ErrUnrecognisedFlag refuses a flag that is neither unset nor the exact armed
// value. It refuses rather than defaulting on purpose: defaulting would make
// "BILLING_CYCLE_INTENT_CUTOVER=true" silently do the other thing while an
// operator believed the opposite, and a wrong belief about whether money is
// moving is worse than a worker that will not start.
var ErrUnrecognisedFlag = errors.New("unrecognised intent cutover flag")

// Decision is the whole policy, as a pure function, so the arming path can be
// exercised by a test instead of reasoned about.
func Decision(flag string) (arm bool, err error) {
	switch flag {
	case "":
		return false, nil
	case Armed:
		return true, nil
	default:
		return false, fmt.Errorf("%w: %q (expected %q or unset)", ErrUnrecognisedFlag, flag, Armed)
	}
}

// ArmError says which precondition of an armed deployment failed. Needs names
// the environment variable that would satisfy it, for the start-up log line.
type ArmError struct {
	Stage string
	Needs string
	Err   error
}

func (e *ArmError) Error() string { return e.Stage + ": " + e.Err.Error() }
func (e *ArmError) Unwrap() error { return e.Err }

// Arm reads the flag through getenv and, when it arms, builds the proposer:
// the signing keys (signing.Load), the evidence recorder (docs/DESIGN.md
// INV-014: an evidence record is a side effect of the money moving, so a
// deployment that can seal documents but cannot record them refuses), and
// the proposer over the intent store on pool. armed=false with a nil error is
// the unset flag; a set flag whose preconditions fail is an *ArmError.
func Arm(pool *pgxpool.Pool, getenv func(string) string, now func() time.Time) (p *proposer.Proposer, armed bool, err error) {
	arm, err := Decision(getenv(Env))
	if err != nil {
		return nil, false, &ArmError{Stage: "flag", Needs: Env, Err: err}
	}
	if !arm {
		return nil, false, nil
	}
	signer, err := signing.Load(getenv)
	if err != nil {
		return nil, true, &ArmError{Stage: "the signing key material will not load", Needs: signing.EnvBillingEvidenceKey, Err: err}
	}
	recorder, err := evidence.NewRecorder(signer, evidence.Options{
		Issuer:      "billing-engine",
		Audience:    "customer",
		Environment: buildinfo.Current().Environment,
		Now:         now,
	})
	if err != nil {
		return nil, true, &ArmError{Stage: "this deployment cannot record evidence", Needs: signing.EnvBillingEvidenceKey, Err: err}
	}
	p, err = proposer.New(intentstore.New(pool), recorder, now)
	if err != nil {
		return nil, true, &ArmError{Stage: "the proposer will not construct", Needs: Env, Err: err}
	}
	return p, true, nil
}
