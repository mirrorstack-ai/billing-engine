// Package executor is the only thing that turns a sealed intent into
// money moving.
//
// It is the wiring, and deliberately almost nothing else. The decision
// belongs to internal/intent/predicate, the durable state to
// internal/intent/store, and the provider protocol to an adapter behind
// the Collector port. What is left here is the ORDER those happen in,
// which is the part that cannot be tested anywhere else:
//
//	refuse before claiming, claim before dispatching, and never record
//	an outcome nobody established.
//
// docs/DESIGN.md §4 draws that order as one arrow reaching the
// provider, and says of the permit that authorises it: "the permit is
// spent by the send, not by the reply."
package executor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/predicate"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/store"
)

// Collector moves money for one sealed intent.
//
// Deliberately one method taking one intent. A port with a "create
// invoice" and a "finalize" would let this package assemble a charge
// out of parts, and assembling is where an amount stops being the one
// that was sealed.
type Collector interface {
	Collect(ctx context.Context, req Debit) (CollectResult, error)
}

// Debit is everything an adapter needs, and nothing it could use to
// change what is charged.
//
// Named Debit rather than CollectRequest: internal/architecture
// reserves the Request suffix for types that arrive on the wire, so
// that its caller-supplied-money check can find them without a route
// table. An internal port argument carrying an amount is not what that
// rule is about.
//
// There is no line detail and no customer id: the adapter is told an
// amount, a currency and an identity, and the amount came from the
// sealed document. An adapter that could see the lines could decide to
// present them differently, and the total a customer approved would
// stop being the total a rail sees.
type Debit struct {
	IntentDigest string
	AmountMicros int64
	Currency     string
	// IdempotencyKey is derived from the intent digest, so a retry of
	// the same intent is the same request at the provider.
	IdempotencyKey string
}

// CollectResult is what the rail said.
type CollectResult struct {
	// Succeeded is true only on a verified debit.
	Succeeded bool
	// Reference is the provider's identifier for the movement.
	Reference string
	// Ambiguous means the answer never arrived, or arrived in a form
	// that does not establish whether money moved.
	//
	// This is the case the whole design bends around. An ambiguous
	// result is NOT a failure: treating it as one releases the claim
	// and lets a second attempt charge a customer who was already
	// charged. It retains the claim and records nothing.
	Ambiguous bool
}

// Environment is the state the predicate needs that this package cannot
// derive from stored records.
//
// It is a value rather than a set of method calls so that a caller
// assembles it once, visibly, and the executor cannot quietly consult
// something at decision time. docs/DESIGN.md's whole point about the
// predicate is that it answers from state it was handed.
type Environment struct {
	// BuildIdentified — docs/VERIFICATION.md §2: an executor whose
	// build identity reads "unknown" must refuse to execute.
	BuildIdentified bool
	// PolicyDigestsMatch reports that every policy the intent names is
	// published, effective and digest-matching.
	PolicyDigestsMatch bool
	// TimeReady reports that the trusted clock's uncertainty interval
	// lies wholly on the permitted side of every cutoff.
	TimeReady bool
	// TaxIndependentlyReproducible reports that the sealed tax figure
	// was recomputed from its named rule revision and matched.
	TaxIndependentlyReproducible bool
	// Unbuilt carries the clauses whose supporting records do not exist
	// yet. All false is the honest default, and the predicate refuses
	// on them.
	Unbuilt predicate.UnbuiltEvidence
}

// Executor turns a sealed intent into a settled one, or refuses.
type Executor struct {
	store     *store.Store
	collector Collector
	identity  string
	now       func() time.Time
	env       func(context.Context) Environment
}

// New builds an executor.
//
// now and env are injected because a decision that reads a clock or an
// ambient config cannot be replayed, and docs/VERIFICATION.md §4 wants
// a customer able to recheck a refusal offline.
func New(
	s *store.Store,
	collector Collector,
	identity string,
	now func() time.Time,
	env func(context.Context) Environment,
) *Executor {
	return &Executor{store: s, collector: collector, identity: identity, now: now, env: env}
}

// Outcome is what Execute did.
type Outcome struct {
	// Permitted is false when the predicate refused. Refused names
	// every clause that said no.
	Permitted bool
	Refused   []predicate.Clause

	// Settled is true on a verified debit.
	Settled   bool
	Reference string

	// Unresolved is true when the rail's answer did not establish
	// whether money moved. The claim is retained and no outcome is
	// recorded, so nothing else will attempt this intent.
	Unresolved bool
}

// Errors from Execute.
var (
	ErrAlreadyClaimed = store.ErrAlreadyClaimed
	ErrNotFound       = store.ErrNotFound
)

// Execute evaluates an intent and, if permitted, collects it.
//
// The order is the point:
//
//  1. Load the intent, digest-verified. A row that no longer hashes to
//     its own digest never reaches the predicate.
//  2. Assemble state and ask the predicate. This is its only caller.
//  3. On refusal, return — having taken no claim and called no
//     provider. docs/DESIGN.md §4: "A refusal here mutates no
//     provider."
//  4. Claim, which is a primary key and therefore exclusive.
//  5. Dispatch once.
//  6. Record the outcome only if the rail established one.
func (e *Executor) Execute(ctx context.Context, digest string) (Outcome, error) {
	sealed, err := e.store.LoadIntent(ctx, digest)
	if err != nil {
		return Outcome{}, err
	}

	auth, err := e.store.LoadAuthorization(ctx, sealed.AuthorizationID())
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Outcome{}, err
	}
	// A missing authorization is not an error here: it is a refusal.
	// The predicate's zero BillingAuthorization permits nothing, and
	// "not found" must never read as "allowed".

	state, err := e.store.State(ctx, digest)
	if err != nil {
		return Outcome{}, err
	}

	notice, _, err := e.store.LoadNotice(ctx, digest)
	if err != nil {
		return Outcome{}, err
	}

	env := e.env(ctx)
	verdict := predicate.Evaluate(predicate.SealedState{
		Intent:                       sealed,
		State:                        predicate.IntentState(state),
		Now:                          e.now(),
		BuildIdentified:              env.BuildIdentified,
		Authorization:                auth,
		Mode:                         e.authorityMode(auth),
		Notice:                       noticeFor(sealed, notice),
		Funding:                      fundingFor(sealed),
		PolicyDigestsMatch:           env.PolicyDigestsMatch,
		TimeReady:                    env.TimeReady,
		TaxIndependentlyReproducible: env.TaxIndependentlyReproducible,
		PriorSettlementExists:        false,
		ClaimAvailable:               true,
		Unbuilt:                      env.Unbuilt,
	})

	if !verdict.Permitted {
		// Nothing has been claimed and nothing dispatched. A refusal
		// leaves the intent exactly as it was found.
		return Outcome{Permitted: false, Refused: verdict.Refused}, nil
	}

	// The claim is taken BEFORE the provider is touched, so a crash
	// between the two leaves an intent that is claimed and unsettled —
	// which is recoverable — rather than one that is settled and
	// unclaimed, which is not.
	if err := e.store.ClaimSettlement(ctx, digest, e.identity); err != nil {
		return Outcome{}, err
	}

	result, collectErr := e.collector.Collect(ctx, Debit{
		IntentDigest:   digest,
		AmountMicros:   sealed.TotalMicros(),
		Currency:       sealed.Currency(),
		IdempotencyKey: "intent-" + digest,
	})

	switch {
	case collectErr != nil || result.Ambiguous:
		// The claim is retained and no outcome is recorded. Releasing
		// it would let a second attempt charge a customer who may
		// already have been charged, and the whole reason this branch
		// exists is that nobody knows which.
		return Outcome{Permitted: true, Unresolved: true}, nil

	case result.Succeeded:
		if err := e.store.RecordOutcome(ctx, digest, "succeeded", e.now()); err != nil {
			// The money moved. Failing to write that down is bad, but
			// it is not a reason to say it did not happen.
			return Outcome{Permitted: true, Settled: true, Reference: result.Reference},
				fmt.Errorf("collected but failed to record the outcome: %w", err)
		}
		_ = e.store.AdvanceState(ctx, digest, state, "succeeded")
		return Outcome{Permitted: true, Settled: true, Reference: result.Reference}, nil

	default:
		if err := e.store.RecordOutcome(ctx, digest, "failed", e.now()); err != nil {
			return Outcome{Permitted: true}, fmt.Errorf("record failure: %w", err)
		}
		_ = e.store.AdvanceState(ctx, digest, state, "voided")
		return Outcome{Permitted: true}, nil
	}
}

// authorityMode picks which of the two mutually exclusive gates
// applies, from the authorization rather than from a caller.
func (e *Executor) authorityMode(auth intent.BillingAuthorization) predicate.AuthorityMode {
	if auth.Scope() == intent.ScopeStanding {
		return predicate.AuthorityStandingAuto
	}
	return predicate.AuthorityCustomerPresent
}

// noticeFor maps a stored receipt into the predicate's shape.
func noticeFor(sealed intent.ChargeIntent, r store.NoticeReceipt) predicate.NoticeReceipt {
	return predicate.NoticeReceipt{
		DeliveredBytesDigest: r.DeliveredDigest,
		Policy:               r.Policy,
		TerminalStatus:       r.TerminalStatus,
		EligibilityNotBefore: r.EligibilityNotBefore,
		RevocationPathFresh:  r.RevocationPathFresh,
	}
}

// fundingFor is the provider-only plan this executor can express today.
//
// 🔴 Wallet allocation is not implemented, so every intent funds
// entirely from the rail. That is stated rather than hidden: a plan
// claiming a wallet split it cannot perform would make the predicate's
// balance clause pass over a lie.
func fundingFor(sealed intent.ChargeIntent) predicate.FundingPlan {
	return predicate.FundingPlan{
		Frozen:                  true,
		GrossMicros:             sealed.TotalMicros(),
		WalletAllocationMicros:  0,
		ProviderRemainderMicros: sealed.TotalMicros(),
	}
}
