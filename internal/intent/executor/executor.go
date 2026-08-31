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
	"sort"
	"strings"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
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
	// Payer is the subject sealed into the intent, NOT a provider
	// customer id. An adapter resolves it to its own identity.
	//
	// The distinction matters: if this carried a provider customer id,
	// whoever built the request could point a sealed intent at somebody
	// else's card. The amount would still be the sealed one, and it
	// would come out of the wrong account. Carrying the payer from the
	// document keeps who-pays derived rather than told.
	Payer        intent.Subject
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

	// InProgress means the rail accepted the request and has not
	// finished. Collection is under way and its result will arrive
	// through a callback.
	//
	// Distinct from Ambiguous, and the distinction was found by running
	// this against a real provider: Stripe's finalize returns before it
	// collects, so an invoice is routinely `open` at the moment the
	// call comes back. Reporting that as ambiguous is true but useless
	// — it is the ORDINARY path, and a system whose ordinary path is
	// "we do not know" cannot tell the ordinary case from the incident.
	//
	// Both retain the claim. Only Ambiguous means nobody knows whether
	// money moved; InProgress means the rail knows and will say.
	// docs/DESIGN.md §4 gives it the provider_in_progress state, whose
	// rule is "retain claim and reservations; a next step needs a fresh
	// full gate".
	InProgress bool
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
	recorder  *evidence.Recorder
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
	recorder *evidence.Recorder,
	identity string,
	now func() time.Time,
	env func(context.Context) Environment,
) (*Executor, error) {
	// A nil recorder is not "evidence off". Every branch of Execute produces
	// one of docs/DESIGN.md:388's events — a refusal, a nonterminal attempt
	// state, or a settlement — so an executor that cannot record is one that
	// must not run. Refusing here means the deployment finds out at startup
	// rather than the customer finding out by having no trace of a charge.
	if recorder == nil {
		return nil, ErrNoRecorder
	}
	if now == nil {
		return nil, errors.New("executor: no clock")
	}
	return &Executor{
		store: s, collector: collector, recorder: recorder,
		identity: identity, now: now, env: env,
	}, nil
}

// ErrNoRecorder refuses an executor that cannot record what it decides.
var ErrNoRecorder = errors.New("executor: refusing to construct an executor with no evidence recorder")

// refusalDetail renders a refusal's clause set as the record's detail.
//
// The clause names are a closed vocabulary (predicate.AllClauses), so this
// carries no customer prose into a table whose read path is not yet gated —
// and it is exactly what a customer asking "why was I not charged" needs,
// which docs/VERIFICATION.md §4 has them rechecking offline.
//
// A refusal with no clauses is recorded as such rather than as an empty
// string: the schema requires a non-empty detail, and "refused, reason
// unstated" is itself the finding.
func refusalDetail(refused []predicate.Clause) string {
	if len(refused) == 0 {
		return "refused_without_naming_a_clause"
	}
	names := make([]string, 0, len(refused))
	for _, c := range refused {
		names = append(names, string(c))
	}
	sort.Strings(names)
	return strings.Join(names, ",")
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

	// InProgress is true when the rail accepted the charge and has not
	// finished. The claim is retained and the intent moves to
	// provider_in_progress; a callback carries the result.
	InProgress bool
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

	funding, err := fundingFor(sealed)
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
		Funding:                      funding,
		PolicyDigestsMatch:           env.PolicyDigestsMatch,
		TimeReady:                    env.TimeReady,
		TaxIndependentlyReproducible: env.TaxIndependentlyReproducible,
		PriorSettlementExists:        false,
		ClaimAvailable:               true,
		Unbuilt:                      env.Unbuilt,
	})

	if !verdict.Permitted {
		// Nothing has been claimed and nothing dispatched. A refusal
		// leaves the intent exactly as it was found — except for its
		// evidence record, which is the one thing a refusal DOES
		// produce. docs/DESIGN.md:388 lists a refusal among the eight
		// events, and a customer told "no" is owed the same trace as
		// one who was charged.
		//
		// It is its own transaction because a refusal mutates nothing
		// else ("a refusal here mutates no provider"), and the write is
		// reported rather than swallowed: an unrecorded refusal is a
		// decision nobody can reproduce.
		if err := e.store.AppendEvidence(ctx, e.recorder, evidence.Event{
			Kind:         evidence.KindRefusal,
			Subject:      sealed.Payer(),
			IntentDigest: digest,
			Detail:       refusalDetail(verdict.Refused),
			OccurredAt:   e.now(),
		}); err != nil {
			return Outcome{Permitted: false, Refused: verdict.Refused},
				fmt.Errorf("refused, and failed to record the refusal: %w", err)
		}
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
		IntentDigest: digest,
		Payer:        sealed.Payer(),
		// 🔴 The provider remainder, NEVER the gross obligation.
		//
		// docs/DESIGN.md:1284: "The integer handed to an adapter is the
		// sealed providerRemainder — never grossObligation, and never
		// wallet funding."
		//
		// These are EQUAL today only because fundingFor hardcodes a zero
		// wallet allocation. The moment a wallet split exists, handing the
		// total here charges the rail for the whole obligation while the
		// wallet is drawn for its share as well — a 20,000,000 intent split
		// 6,000,000/14,000,000 would collect 26,000,000. Reading the
		// remainder is correct now and stays correct then.
		AmountMicros:   funding.ProviderRemainderMicros,
		Currency:       sealed.Currency(),
		IdempotencyKey: "intent-" + digest,
	})

	switch {
	case result.InProgress && collectErr == nil:
		// The rail has it and will report. The claim is retained, no
		// outcome is recorded, and the intent says out loud that a
		// charge is in flight — which is what a reconciler needs to
		// find it later.
		if err := e.store.AppendEvidence(ctx, e.recorder, evidence.Event{
			Kind:         evidence.KindNonterminalAttemptState,
			Subject:      sealed.Payer(),
			IntentDigest: digest,
			Detail:       "provider_in_progress",
			OccurredAt:   e.now(),
		}); err != nil {
			return Outcome{Permitted: true, InProgress: true, Reference: result.Reference},
				fmt.Errorf("in flight at the rail, and failed to record it: %w", err)
		}
		_ = e.store.AdvanceState(ctx, digest, state, "provider_in_progress")
		return Outcome{Permitted: true, InProgress: true, Reference: result.Reference}, nil

	case collectErr != nil || result.Ambiguous:
		// The claim is retained and no outcome is recorded. Releasing
		// it would let a second attempt charge a customer who may
		// already have been charged, and the whole reason this branch
		// exists is that nobody knows which.
		//
		// The evidence record is exactly the trace that makes the
		// ambiguity findable later, so it matters MORE here than on a
		// clean settlement, not less.
		if err := e.store.AppendEvidence(ctx, e.recorder, evidence.Event{
			Kind:         evidence.KindNonterminalAttemptState,
			Subject:      sealed.Payer(),
			IntentDigest: digest,
			Detail:       "unresolved",
			OccurredAt:   e.now(),
		}); err != nil {
			return Outcome{Permitted: true, Unresolved: true},
				fmt.Errorf("unresolved at the rail, and failed to record it: %w", err)
		}
		return Outcome{Permitted: true, Unresolved: true}, nil

	case result.Succeeded:
		if err := e.store.RecordOutcomeWithEvidence(ctx, e.recorder, digest, "succeeded", evidence.Event{
			Kind:         evidence.KindSettlement,
			Subject:      sealed.Payer(),
			IntentDigest: digest,
			Detail:       "succeeded",
			OccurredAt:   e.now(),
		}); err != nil {
			// The money moved. Failing to write that down is bad, but
			// it is not a reason to say it did not happen.
			return Outcome{Permitted: true, Settled: true, Reference: result.Reference},
				fmt.Errorf("collected but failed to record the outcome: %w", err)
		}
		_ = e.store.AdvanceState(ctx, digest, state, "succeeded")
		return Outcome{Permitted: true, Settled: true, Reference: result.Reference}, nil

	default:
		if err := e.store.RecordOutcomeWithEvidence(ctx, e.recorder, digest, "failed", evidence.Event{
			Kind:         evidence.KindSettlement,
			Subject:      sealed.Payer(),
			IntentDigest: digest,
			Detail:       "failed",
			OccurredAt:   e.now(),
		}); err != nil {
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
		DeliveredAt:          r.DeliveredAt,
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
//
// 🔴 The obligation is selected BY KIND, which docs/DESIGN.md §6 states
// and this function did not:
//
//	grossObligation = serviceGrossObligation OR fundingGrossObligation OR
//	                  collectionGrossObligation, selected by intent kind
//
// §6 gives the reason in the same breath: "so a stored-value purchase
// cannot end up with zero principal by borrowing the service-line
// formula". The service formula subtracts rating credits; the funding
// formula does not, because a credit purchase's principal is cash the
// customer is paying, not a service line credits may reduce. Applying
// one to the other is how a $20 top-up becomes a $0 top-up that still
// grants $20 of credit.
//
// The three formulas coincide TODAY, because rating credits are not
// implemented and every total is a bare sum. Writing the selection now,
// while they agree, is the only time it can be done without a migration
// of live documents — and an unknown kind refuses rather than borrowing
// whichever formula happens to be first.
func fundingFor(sealed intent.ChargeIntent) (predicate.FundingPlan, error) {
	// A PROJECTION of the sealed document, not a derivation.
	//
	// 🔴 This function used to synthesise the plan from the intent's
	// total, which made predicate.ClauseFundingPlanBalances unable to
	// fail: it verified a value this call had computed three lines
	// earlier. Every check in that clause was true by construction.
	//
	// The split is now sealed (docs/DESIGN.md:205-206, :470, :1281 — the
	// engine freezes it BEFORE disclosure), so these two integers reach
	// the predicate through a durable write and a digest. The clause can
	// now disagree with them: a row whose provider remainder no longer
	// sums to its total refuses, where before x + 0 == x for every int64.
	//
	// The by-kind funding-formula selection moved to Seal, where the kind
	// is sealed — an unknown kind now refuses to SEAL rather than
	// refusing to execute, which is strictly earlier and cheaper.
	return predicate.FundingPlan{
		Frozen:                  true,
		GrossMicros:             sealed.TotalMicros(),
		WalletAllocationMicros:  sealed.WalletAllocationMicros(),
		ProviderRemainderMicros: sealed.ProviderRemainderMicros(),
	}, nil
}
