package intent

import "strings"

// A policy revision names an immutable, published, accepted decision.
//
// docs/DESIGN.md §12 closes with the rule this file enforces: the
// undecided policies "must not be reconstructed from current constants,
// code comments, or the shape of today's Stripe-shaped schema". A
// proposer that has no published revision to name must therefore say so
// in the value itself, and the engine must refuse to COLLECT under one.
//
// The refusal deliberately lives at the predicate and not at Seal.
// Proposing under an unpublished revision is the whole point of the
// shadow phase in §11 — the intent is derived, sealed, stored and
// reconciled without anyone being charged. What must never happen is the
// step after that, and that is a collection-time question.
const (
	// UnpublishedRevisionPrefix marks a revision id that names a
	// decision §12 has not settled. It is reserved: a real published
	// revision must never begin with it.
	UnpublishedRevisionPrefix = "unpublished/"

	// PendingDecisionMarker appears in any placeholder minted against an
	// open §12 item, whatever prefix it carries. `not-applicable/` is the
	// case that motivates checking the whole string rather than the
	// prefix alone: as a placeholder it asserts that tax does not apply,
	// which is a claim, not a deferral.
	PendingDecisionMarker = "pending-decision"
)

// RevisionPublished reports whether a revision id names a published,
// accepted policy revision rather than an open decision.
//
// A blank id is not published. Seal already refuses blank terms, price
// book and notice ids, but the tax rule revision reaches here from a
// determination that only has to be Resolved, so the blank case is
// reachable and must not read as "fine".
func RevisionPublished(revision string) bool {
	trimmed := strings.TrimSpace(revision)
	if trimmed == "" {
		return false
	}
	if strings.HasPrefix(trimmed, UnpublishedRevisionPrefix) {
		return false
	}
	return !strings.Contains(trimmed, PendingDecisionMarker)
}

// UnpublishedRevisions returns the names of the sealed policy revisions
// that are not published, in a fixed order.
//
// It returns names rather than values so a refusal can be logged without
// putting a policy id into a diagnostic that may be customer-visible, and
// so the empty result is the only "everything is published" answer.
func UnpublishedRevisions(c ChargeIntent) []string {
	var unpublished []string
	for _, named := range []struct {
		name     string
		revision string
	}{
		{"terms_revision", c.TermsRevision()},
		{"price_book_revision", c.PriceBookRevision()},
		{"notice_policy", c.NoticePolicy()},
		{"tax_rule_revision", c.Tax().RuleRevision},
		// 🔴 Added in the SAME change that seals the routing policy.
		//
		// This list is what ClausePolicyPublished checks. A revision
		// sealed into the digest but absent here passes the published
		// check unexamined — the exact hollowness fixed in this predicate
		// on 2026-08-30. Any future sealed revision belongs here the
		// moment it is sealed, not in a follow-up.
		{"routing_policy", c.RoutingPolicyRevision()},
	} {
		if !RevisionPublished(named.revision) {
			unpublished = append(unpublished, named.name)
		}
	}
	return unpublished
}
