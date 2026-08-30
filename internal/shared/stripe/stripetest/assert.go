package stripetest

import (
	"strings"
	"testing"

	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// The Recorder must satisfy every provider interface in the package it
// doubles. This is not decoration: it is what makes the effect
// classification exhaustive. Adding a method to any of these interfaces
// breaks this file, and whoever adds it has to come here and decide
// whether the new call reads, mutates, or collects. A double that
// silently lags the interface would let an unclassified — and therefore
// unasserted — money path exist.
var (
	_ billingstripe.Client                  = (*Recorder)(nil)
	_ billingstripe.CombinedProrationClient = (*Recorder)(nil)
	_ billingstripe.CreditPurchaseClient    = (*Recorder)(nil)
	_ billingstripe.AutoTopUpClient         = (*Recorder)(nil)
)

// RequireNoCollection fails the test if the recorded calls include any
// that can take money.
//
// This is the assertion behind "a read is capability-safe". Use it
// around status reads, eligibility checks, and ingestion, where the
// claim under test is that nothing the caller did could reach a card.
func (r *Recorder) RequireNoCollection(t *testing.T, context string) {
	t.Helper()
	if calls := r.CallsWithEffect(EffectCollect); len(calls) > 0 {
		t.Fatalf("%s must not be able to collect, but reached %s", context, describe(calls))
	}
}

// RequireNoProviderMutation fails the test if the recorded calls changed
// any provider state at all.
//
// Stricter than RequireNoCollection and the right assertion for a pure
// read: a status endpoint that creates a draft invoice has left a
// customer-visible object behind and holds a capability it should not,
// even though that particular call collected nothing.
func (r *Recorder) RequireNoProviderMutation(t *testing.T, context string) {
	t.Helper()
	var offending []Call
	offending = append(offending, r.CallsWithEffect(EffectMutate)...)
	offending = append(offending, r.CallsWithEffect(EffectCollect)...)
	if len(offending) > 0 {
		t.Fatalf("%s must not mutate provider state, but reached %s", context, describe(offending))
	}
}

// RequireCollected fails unless exactly the expected number of
// money-moving calls were made. A test that asserts a charge happened
// should assert how many times, because "at least one" is the shape of
// every duplicate-collection bug.
func (r *Recorder) RequireCollected(t *testing.T, want int) {
	t.Helper()
	if got := r.CallsWithEffect(EffectCollect); len(got) != want {
		t.Fatalf("want %d money-moving call(s), got %d: %s", want, len(got), describe(got))
	}
}

// RequireEveryMutationKeyed fails if any state-changing call was made
// without a deterministic idempotency key.
//
// A retried mutation with no key is a second object at the provider —
// a duplicate invoice, or a duplicate charge. The methods that take no
// key argument at all are exempt, since there is nothing to check.
func (r *Recorder) RequireEveryMutationKeyed(t *testing.T, exempt ...string) {
	t.Helper()
	skip := map[string]bool{}
	for _, m := range exempt {
		skip[m] = true
	}
	for _, c := range r.Calls() {
		if c.Effect == EffectRead || skip[c.Method] {
			continue
		}
		if c.IdemKey == "" {
			t.Errorf("%s(%s) changed provider state with no idempotency key; a retry would duplicate it", c.Method, c.Ref)
		}
	}
}

func describe(calls []Call) string {
	parts := make([]string, 0, len(calls))
	for _, c := range calls {
		part := c.Method + "(" + c.Ref + ")"
		if c.Effect == EffectCollect {
			part += " [collect]"
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, ", ")
}
