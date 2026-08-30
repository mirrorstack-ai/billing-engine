package predicate

import (
	"strings"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// Verdict is the predicate's answer.
//
// Refused clauses are returned together rather than short-circuiting.
// An operator fixing one condition and being refused for the next is
// how a single incident becomes an afternoon, and a customer asking why
// a charge did not go through deserves the whole answer. Ordering
// follows docs/DESIGN.md §4's conjunction so a refusal list reads down
// the predicate.
type Verdict struct {
	Permitted bool
	Refused   []Clause
}

// Refused reports whether a specific clause refused.
func (v Verdict) RefusedClause(c Clause) bool {
	for _, refused := range v.Refused {
		if refused == c {
			return true
		}
	}
	return false
}

// Evaluate decides whether an intent may execute.
//
// It is the single copy of docs/DESIGN.md §4's predicate: one exported
// total function, no I/O, no clock, no error return. A refusal mutates
// no provider, because this function cannot reach one.
//
// Every clause is evaluated — none short-circuits — so the verdict
// names every reason at once.
func Evaluate(state SealedState) Verdict {
	var refused []Clause

	hasProviderRemainder := state.Funding.ProviderRemainderMicros > 0

	for _, clause := range AllClauses {
		if providerClauses[clause] && !hasProviderRemainder {
			// docs/DESIGN.md §4: "providerRemainder == 0 OR (...)".
			// A wallet-only intent moves no money at a rail, so rail
			// evidence is not required of it.
			continue
		}
		if !satisfied(clause, state) {
			refused = append(refused, clause)
		}
	}

	return Verdict{Permitted: len(refused) == 0, Refused: refused}
}

// satisfied answers one clause. Each has exactly one owner here, which
// is the property the package exists to hold: there is one place to
// read to find out what a clause means, and one place to change it.
func satisfied(clause Clause, s SealedState) bool {
	switch clause {
	case ClauseIntentImmutable:
		// An unsealed intent is a draft. INV-003 makes sealing the
		// thing that fixes the document, so there is nothing to
		// execute against before it.
		return s.Intent.Sealed()

	case ClauseIntentStateEligible:
		return s.State == StateEligible

	case ClauseWithinExecutionWindow:
		// The window is sealed into the intent, so it is part of what
		// the customer was shown. Enforcing it here is what stops an
		// eligible intent from being settled arbitrarily later —
		// nothing else in the conjunction looks at time relative to
		// the document.
		if !s.Intent.Sealed() || s.Now.IsZero() {
			return false
		}
		notBefore, notAfter := s.Intent.ExecutionWindow()
		return !s.Now.Before(notBefore) && !s.Now.After(notAfter)

	case ClauseBuildIdentified:
		// docs/VERIFICATION.md §2: an executor whose build identity
		// reads "unknown" must refuse to execute.
		return s.BuildIdentified

	case ClauseAuthorizationValid:
		if !s.Intent.Sealed() {
			return false
		}
		return s.Authorization.
			Permits(s.Intent, s.Now, s.PriorUse).
			Permitted

	case ClauseAuthorityEvidence:
		return authorityEvidenceBinds(s)

	case ClauseNoticeDelivered:
		// Only the standing-automatic gate requires notice. INV-005
		// applies to automatic collection; a customer watching the
		// screen has just been shown the document.
		if s.Mode != AuthorityStandingAuto {
			return true
		}
		return noticeTerminallyDelivered(s)

	case ClauseNoticeWaitElapsed:
		if s.Mode != AuthorityStandingAuto {
			return true
		}
		// The wait runs from delivery. A zero eligibilityNotBefore is
		// not "no wait required", it is no receipt.
		if s.Notice.EligibilityNotBefore.IsZero() {
			return false
		}
		// 🔴 EligibilityNotBefore is supplied by whoever built the
		// state, so on its own it says only that SOMEBODY picked an
		// instant. Until 2026-08-30 that was the whole clause: a caller
		// could set it to the delivery moment and the wait elapsed
		// immediately, while the customer was told as their card was
		// charged.
		//
		// The authorization carries the lead time the customer
		// accepted, so the receipt is now checked against it: eligibility
		// may not start before delivery plus that wait. A missing
		// delivery instant refuses, because a wait measured from nothing
		// is not a wait.
		if s.Notice.DeliveredAt.IsZero() {
			return false
		}
		if lead := s.Authorization.NoticeLeadTime(); lead > 0 {
			if s.Notice.EligibilityNotBefore.Before(s.Notice.DeliveredAt.Add(lead)) {
				return false
			}
		}
		return !s.Now.Before(s.Notice.EligibilityNotBefore)

	case ClauseWithinCeilings:
		// The authorization owns the ceilings, and it has already been
		// asked. This clause exists separately so that a refusal names
		// the ceiling rather than the authorization: "over your limit"
		// and "not authorized" are different things to be told.
		if !s.Intent.Sealed() {
			return false
		}
		decision := s.Authorization.Permits(s.Intent, s.Now, s.PriorUse)
		for _, refusal := range decision.Refusals {
			if refusal == intent.RefusalOverPerCharge || refusal == intent.RefusalOverPeriod {
				return false
			}
		}
		return true

	case ClauseFundingPlanBalances:
		if !s.Funding.Frozen || !s.Intent.Sealed() {
			return false
		}
		if s.Funding.WalletAllocationMicros < 0 || s.Funding.ProviderRemainderMicros < 0 {
			return false
		}
		// The plan must account for the whole obligation and no more,
		// and it must be the obligation this intent sealed.
		if s.Funding.GrossMicros != s.Intent.TotalMicros() {
			return false
		}
		return s.Funding.WalletAllocationMicros+s.Funding.ProviderRemainderMicros == s.Funding.GrossMicros

	case ClauseTaxFinal:
		// INV-004: tax must be independently reproducible final or
		// explicitly not applicable. An unresolved determination is
		// neither, and must never be read as zero.
		//
		// Both halves are required. The sealed flag says a
		// determination was made; the reproduction says it can be
		// recomputed from the rule revision it names, which is what
		// makes the figure checkable by someone who does not trust us.
		return s.Intent.Sealed() &&
			s.Intent.Tax().Resolved &&
			s.TaxIndependentlyReproducible

	case ClausePolicyPublished:
		// The clause name states three things: published, effective,
		// and digest-matching. PolicyDigestsMatch is the caller's
		// answer to the last two — it is a reproduction result, and
		// the executor is the only thing that can compute it.
		//
		// Published is different: it is readable from the intent
		// itself, and until 2026-08-30 nothing read it. The four
		// sealed revision ids went into the canonical digest
		// unexamined, so a charge bundle could attest to
		// "unpublished/pending-decision-12" and no clause objected.
		// An authorization minted with the same placeholder satisfied
		// every equality check in Permits, so the fiction was
		// self-consistent rather than self-refuting.
		//
		// docs/DESIGN.md §12 gate G1 says production execution fails
		// closed until each policy is settled in an accepted ADR.
		// This is where that gate lives.
		return s.PolicyDigestsMatch && len(intent.UnpublishedRevisions(s.Intent)) == 0

	case ClauseTimeReadiness:
		return s.TimeReady

	case ClauseNoPriorSettlement:
		// INV-008: one intent settles at most once, across all
		// providers.
		return !s.PriorSettlementExists

	case ClauseClaimAvailable:
		return s.ClaimAvailable

	// --- clauses whose records are unbuilt ---
	case ClauseProofHeadCurrent:
		return s.Unbuilt.ProofHeadCurrent
	case ClauseProofsApplied:
		return s.Unbuilt.ProofsApplied
	case ClauseCommercialIdentity:
		return s.Unbuilt.CommercialIdentity
	case ClauseMerchantOfRecord:
		return s.Unbuilt.MerchantOfRecord
	case ClauseSourceAllocation:
		return s.Unbuilt.SourceAllocation
	case ClauseCreditLotsReserved:
		return s.Unbuilt.CreditLotsReserved
	case ClauseExposureReservation:
		return s.Unbuilt.ExposureReservation
	case ClauseFundingMatchesAccepted:
		return s.Unbuilt.FundingMatchesAccepted
	case ClauseRailSupportsPlan:
		return s.Unbuilt.RailSupportsPlan
	case ClauseProviderAutonomy:
		return s.Unbuilt.ProviderAutonomy
	case ClauseFirstStepMatchesPlan:
		return s.Unbuilt.FirstStepMatchesPlan
	case ClauseInstrumentBinding:
		// Two halves, and only one of them is the caller's to assert.
		//
		// Unbuilt.InstrumentBinding is the executor's claim that it
		// VERIFIED the instrument against the rail — it needs the rail,
		// so only the executor can answer it.
		//
		// The other half is readable here: an authorization that never
		// named a provider and mandate has no binding to verify, so a
		// caller claiming it verified one is claiming something about
		// nothing. That is the same hollowness ClausePolicyPublished
		// carried until 2026-08-30 — a clause named for a check, doing
		// none.
		return s.Unbuilt.InstrumentBinding && s.Authorization.InstrumentBound()
	case ClauseEnclaveReady:
		return s.Unbuilt.EnclaveReady
	case ClauseAttemptFrozen:
		return s.Unbuilt.AttemptFrozen
	}

	// An unrecognised clause refuses. INV-004: "A validator that
	// permits whatever it was not taught to refuse is the §1 shape,
	// arriving one field at a time."
	return false
}

// authorityEvidenceBinds checks the mode-appropriate evidence.
//
// docs/DESIGN.md §4 keeps the two gates mutually exclusive, so a state
// naming neither — or claiming both by carrying a fresh acceptance
// while in standing mode — is unclear rather than doubly authorized.
func authorityEvidenceBinds(s SealedState) bool {
	switch s.Mode {
	case AuthorityCustomerPresent:
		// "A bare accepted: true carrying no disclosure digest has no
		// effect at all." The receipt must name this document, this
		// payer, and be unexpired.
		if s.Acceptance.DisclosureDigest == "" {
			return false
		}
		if s.Acceptance.DisclosureDigest != s.Intent.Digest() {
			return false
		}
		if s.Acceptance.Payer != s.Intent.Payer() {
			return false
		}
		if strings.TrimSpace(s.Acceptance.Nonce) == "" ||
			strings.TrimSpace(s.Acceptance.Audience) == "" ||
			strings.TrimSpace(s.Acceptance.ReplayIdentity) == "" {
			return false
		}
		if s.Acceptance.ExpiresAt.IsZero() || s.Now.After(s.Acceptance.ExpiresAt) {
			return false
		}
		return s.Authorization.Scope() == intent.ScopeOneTime ||
			s.Authorization.Scope() == intent.ScopeStanding

	case AuthorityStandingAuto:
		// The standing gate rests on the authorization's own recorded
		// acceptance, not on a fresh one.
		//
		// A state carrying BOTH a standing mode and a fresh acceptance
		// receipt is refused rather than treated as extra assurance.
		// docs/DESIGN.md §4 keeps the two gates mutually exclusive, and
		// the reason is that they have different consequences: the
		// standing gate requires a delivered notice and its wait, the
		// customer-present one does not. A state claiming both would
		// let a caller present an acceptance and take the branch that
		// skips the notice, which is the notice control removed by
		// setting one extra field.
		if s.Acceptance != (AcceptanceReceipt{}) {
			return false
		}
		return s.Authorization.Scope() == intent.ScopeStanding &&
			s.Authorization.AcceptanceDigest() != ""
	}
	return false
}

// noticeTerminallyDelivered is INV-005's "delivery, not sending".
//
// "Delivery evidence means the carrier reported your configured
// destination in a terminal status the accepted policy defines as
// destination-delivered. Queue acceptance is not enough: handing a
// message to a queue proves only that we tried."
func noticeTerminallyDelivered(s SealedState) bool {
	if !s.Intent.Sealed() {
		return false
	}
	// The bytes delivered must be the bytes collected against.
	if s.Notice.DeliveredBytesDigest != s.Intent.Digest() {
		return false
	}
	if s.Notice.Policy != s.Intent.NoticePolicy() {
		return false
	}
	if !deliveredStatuses[s.Notice.TerminalStatus] {
		return false
	}
	// A notice the customer cannot act on is a disclosure, not a
	// control. docs/DESIGN.md §4 requires the revocation path to be
	// fresh and checkpoint-consistent before an automatic charge.
	return s.Notice.RevocationPathFresh
}

// deliveredStatuses is a closed allow-list of carrier states that count
// as destination-delivered.
//
// An allow-list rather than a deny-list, for the reason INV-004 gives:
// a status nobody anticipated must refuse, not pass. "queued", "sent"
// and "accepted" are deliberately absent — they describe our side of
// the handoff.
var deliveredStatuses = map[string]bool{
	"delivered": true,
	"relayed":   true,
}
