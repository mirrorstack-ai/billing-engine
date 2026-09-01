package executor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/predicate"
)

// 🔴 THIS SUPPORTS OPTION B OF BOUNDARY-KIND-DECISION.md, WHICH IS OPEN.
// Nothing produces a group yet. If A, C or D is chosen, this file and
// migration 066 are deleted together.

// GroupCollector settles several sealed intents onto one provider invoice.
//
// Declared here rather than reusing Collector, so an executor built for the
// ordinary path cannot silently be handed a group and collect them one at a
// time — which would produce the several invoices and several roundings that
// grouping exists to prevent.
type GroupCollector interface {
	CollectGroup(ctx context.Context, debits []Debit) (CollectResult, error)
}

// ErrGroupNotCollectable refuses a group the executor must not attempt.
var ErrGroupNotCollectable = errors.New("executor: this group cannot be collected as one")

// ExecuteGroup settles a set of intents that must share one invoice.
//
// 🔴 ALL-OR-NOTHING, in both directions:
//
//   - if the predicate refuses ANY member, the whole group is refused and
//     nothing is claimed. Collecting the rest would invoice part of a charge:
//     the customer pays some of what they owe, and nothing in the system
//     records that a group was split.
//   - if the rail's answer is ambiguous, EVERY member stays claimed. Releasing
//     the ones this pass happens to look at first would let a later attempt
//     charge a customer who may already have been charged — the same reason
//     Execute retains a single claim, multiplied.
func (e *Executor) ExecuteGroup(ctx context.Context, collector GroupCollector, digests []string) ([]Outcome, error) {
	if collector == nil {
		return nil, fmt.Errorf("%w: no group collector", ErrGroupNotCollectable)
	}
	if len(digests) == 0 {
		return nil, fmt.Errorf("%w: no intents", ErrGroupNotCollectable)
	}

	// Deterministic order, so a retry evaluates, claims and records in the
	// same sequence — and so a partial failure leaves the same prefix claimed
	// rather than an arbitrary one.
	ordered := append([]string(nil), digests...)
	sort.Strings(ordered)

	type member struct {
		digest string
		debit  Debit
		state  string
		payer  string
	}

	var (
		members []member
		refused []predicate.Clause
	)

	// Phase 1 — evaluate every member through the SAME path Execute uses, so
	// a grouped intent faces exactly the gates a lone one does. Nothing is
	// claimed and no provider is touched, exactly as a single refusal
	// "mutates no provider".
	for _, digest := range ordered {
		ev, err := e.evaluate(ctx, digest)
		if err != nil {
			return nil, err
		}
		if !ev.verdict.Permitted {
			refused = appendClauses(refused, ev.verdict.Refused)
			continue
		}
		members = append(members, member{
			digest: digest,
			state:  ev.state,
			payer:  ev.sealed.Payer().Kind + ":" + ev.sealed.Payer().ID,
			debit: Debit{
				IntentDigest: digest,
				Payer:        ev.sealed.Payer(),
				// 🔴 The provider remainder, never the gross — the same
				// integer Execute hands a single collection.
				AmountMicros:   ev.funding.ProviderRemainderMicros,
				Currency:       ev.sealed.Currency(),
				Lines:          ev.sealed.Lines(),
				IdempotencyKey: "intent-" + digest,
			},
		})
	}

	if len(refused) > 0 {
		// The refusal is recorded per intent, because a customer asking why
		// they were not charged is owed the reason for THEIR document, not
		// for the group.
		outcomes := make([]Outcome, 0, len(ordered))
		for _, digest := range ordered {
			outcomes = append(outcomes, Outcome{Permitted: false, Refused: refused})
			_ = digest
		}
		return outcomes, nil
	}

	// Every member must be for one payer, or they cannot share an invoice.
	// The adapter checks this too; checking here means the group is refused
	// before any claim is taken rather than after.
	for _, m := range members[1:] {
		if m.payer != members[0].payer {
			return nil, fmt.Errorf("%w: %s and %s are different payers",
				ErrGroupNotCollectable, members[0].payer, m.payer)
		}
	}

	// Phase 2 — claim every member BEFORE the provider is touched. A crash
	// between claiming and collecting leaves a claimed, unsettled group,
	// which is recoverable; the reverse is not.
	claimed := make([]string, 0, len(members))
	for _, m := range members {
		if err := e.store.ClaimSettlement(ctx, m.digest, e.identity); err != nil {
			// A member that cannot be claimed means the group is no longer
			// whole. Nothing has been dispatched, so the claims already taken
			// are simply retained — releasing them would open the same
			// double-charge window a released ambiguous claim opens.
			return nil, fmt.Errorf("%w: %s could not be claimed (%d already were): %w",
				ErrGroupNotCollectable, m.digest, len(claimed), err)
		}
		claimed = append(claimed, m.digest)
	}

	debits := make([]Debit, 0, len(members))
	for _, m := range members {
		debits = append(debits, m.debit)
	}

	result, collectErr := collector.CollectGroup(ctx, debits)

	outcomes := make([]Outcome, 0, len(members))
	switch {
	case result.InProgress && collectErr == nil:
		for _, m := range members {
			e.recordNonterminal(ctx, m.digest, m.debit, "provider_in_progress")
			_ = e.store.AdvanceState(ctx, m.digest, m.state, "provider_in_progress")
			outcomes = append(outcomes, Outcome{Permitted: true, InProgress: true, Reference: result.Reference})
		}

	case collectErr != nil || result.Ambiguous:
		// Every claim is retained. Nobody knows whether the money moved, and
		// that is true of the whole group at once.
		for _, m := range members {
			e.recordNonterminal(ctx, m.digest, m.debit, "unresolved")
			outcomes = append(outcomes, Outcome{Permitted: true, Unresolved: true})
		}

	case result.Succeeded:
		for _, m := range members {
			// Every member of a group settles through the SAME provider object —
			// one invoice, which is what a group is — so they share the
			// reference. That is the link a receivable follows back.
			if err := e.store.RecordOutcomeWithEvidence(ctx, e.recorder, m.digest, "succeeded", result.Reference, evidence.Event{
				Kind:         evidence.KindSettlement,
				Subject:      m.debit.Payer,
				IntentDigest: m.digest,
				Detail:       "succeeded",
				OccurredAt:   e.now(),
			}); err != nil {
				// The money moved. Failing to write that down is bad, but it
				// is not a reason to say it did not happen.
				return outcomes, fmt.Errorf("group collected but failed to record %s: %w", m.digest, err)
			}
			_ = e.store.AdvanceState(ctx, m.digest, m.state, "succeeded")
			outcomes = append(outcomes, Outcome{Permitted: true, Settled: true, Reference: result.Reference})
		}

	default:
		for _, m := range members {
			if err := e.store.RecordOutcomeWithEvidence(ctx, e.recorder, m.digest, "failed", result.Reference, evidence.Event{
				Kind:         evidence.KindSettlement,
				Subject:      m.debit.Payer,
				IntentDigest: m.digest,
				Detail:       "failed",
				OccurredAt:   e.now(),
			}); err != nil {
				return outcomes, fmt.Errorf("group failed and could not record %s: %w", m.digest, err)
			}
			_ = e.store.AdvanceState(ctx, m.digest, m.state, "voided")
			outcomes = append(outcomes, Outcome{Permitted: true})
		}
	}
	return outcomes, nil
}

// recordNonterminal writes the attempt-state record, reporting nothing: the
// caller is already returning a nonterminal outcome, and an evidence failure
// here must not be mistaken for a collection failure.
func (e *Executor) recordNonterminal(ctx context.Context, digest string, d Debit, detail string) {
	_ = e.store.AppendEvidence(ctx, e.recorder, evidence.Event{
		Kind:         evidence.KindNonterminalAttemptState,
		Subject:      d.Payer,
		IntentDigest: digest,
		Detail:       detail,
		OccurredAt:   e.now(),
	})
}

// appendClauses merges refusals without duplicating them, so a group refused
// on one gate does not report it once per member.
func appendClauses(into []predicate.Clause, add []predicate.Clause) []predicate.Clause {
	seen := map[predicate.Clause]bool{}
	for _, c := range into {
		seen[c] = true
	}
	for _, c := range add {
		if !seen[c] {
			seen[c] = true
			into = append(into, c)
		}
	}
	sort.Slice(into, func(i, j int) bool { return string(into[i]) < string(into[j]) })
	return into
}

// groupIdentity is the sorted set of digests, for a log line. The provider
// idempotency key is derived independently by the adapter.
func groupIdentity(digests []string) string {
	d := append([]string(nil), digests...)
	sort.Strings(d)
	return strings.Join(d, ",")
}
