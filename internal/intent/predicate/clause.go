// Package predicate holds the one copy of the ExecuteChargeIntent
// decision.
//
// docs/DESIGN.md §4 writes it as a single conjunction and says of it:
// "This is the single copy of the predicate, and every other file links
// to this heading rather than restating a clause."
//
// That instruction is aimed at a specific failure. The counter-example
// is internal/account/credit/coordinator.go, which weaves the charge
// decision through seven call sites across four public methods — and
// that weave is exactly where docs/SECURITY.md §2's capability leak
// lives. A decision spread across seven places has seven chances to
// disagree with itself, and no place a reviewer can read to find out
// what it does.
//
// So the decision here is one exported total function over state the
// caller assembled. The package performs no I/O and reads no clock:
// every instant arrives on SealedState. That is the shape of
// internal/account/eligibility, and it is what lets the same predicate
// answer for a live execution, a replay, and the offline verifier of
// docs/VERIFICATION.md §4.
package predicate

// Clause names one line of the conjunction in docs/DESIGN.md §4.
//
// The enum is exhaustive over that text, including the lines whose
// supporting types are unbuilt. Naming an unbuilt clause is the point:
// a predicate that silently omitted it would look complete and permit
// executions the design says must be refused. Here the clause exists,
// has no evidence to satisfy it, and therefore refuses — which is
// INV-004's default made visible.
type Clause string

const (
	// --- clauses this package evaluates today ---

	ClauseIntentImmutable     Clause = "intent_immutable"
	ClauseIntentStateEligible Clause = "intent_state_eligible"
	ClauseAuthorizationValid  Clause = "authorization_valid"
	ClauseAuthorityEvidence   Clause = "authority_evidence"
	ClauseNoticeDelivered     Clause = "notice_terminally_delivered"
	ClauseNoticeWaitElapsed   Clause = "notice_wait_elapsed"
	ClauseWithinCeilings      Clause = "within_gross_ceilings"
	ClauseFundingPlanBalances Clause = "funding_plan_balances"
	ClauseTaxFinal            Clause = "tax_final_or_not_applicable"
	ClausePolicyPublished     Clause = "policy_published_effective_and_digest_matching"
	ClauseTimeReadiness       Clause = "time_readiness"
	ClauseNoPriorSettlement   Clause = "no_prior_settlement_or_attempt"
	ClauseClaimAvailable      Clause = "settlement_claim_available"
	ClauseBuildIdentified     Clause = "build_identified"

	// --- clauses whose supporting records are unbuilt ---
	//
	// Each refuses until the record exists. docs/DESIGN.md §4 lists
	// them in the same conjunction as the ones above, so omitting them
	// would make this predicate a weaker gate wearing the same name.

	ClauseProofHeadCurrent       Clause = "payer_proof_stream_head_current"
	ClauseProofsApplied          Clause = "accepted_proofs_applied_in_claim"
	ClauseCommercialIdentity     Clause = "commercial_identity_binding_matches"
	ClauseMerchantOfRecord       Clause = "merchant_of_record_binding_accepted"
	ClauseSourceAllocation       Clause = "source_allocation_and_exposure_uniquely_owned"
	ClauseCreditLotsReserved     Clause = "credit_lots_compatible_available_and_reserved"
	ClauseExposureReservation    Clause = "authorization_exposure_reservation_current"
	ClauseFundingMatchesAccepted Clause = "funding_mode_and_caps_equal_the_accepted_authorization"
	ClauseRailSupportsPlan       Clause = "rail_supports_currency_and_frozen_plan"
	ClauseProviderAutonomy       Clause = "provider_autonomy_no_broader_than_accepted"
	ClauseFirstStepMatchesPlan   Clause = "first_provider_step_matches_the_frozen_plan"
	ClauseInstrumentBinding      Clause = "payment_instrument_binding_verified"
	ClauseEnclaveReady           Clause = "credential_enclave_and_writer_ready"
	ClauseAttemptFrozen          Clause = "frozen_payment_attempt_exists_before_any_mutation"
)

// providerClauses are evaluated only when the intent has a provider
// remainder. docs/DESIGN.md §4: "providerRemainder == 0 OR (...)". A
// wallet-only intent moves no money at a rail, so requiring rail
// evidence of it would refuse every wallet settlement.
var providerClauses = map[Clause]bool{
	ClauseRailSupportsPlan:     true,
	ClauseProviderAutonomy:     true,
	ClauseFirstStepMatchesPlan: true,
	ClauseInstrumentBinding:    true,
	ClauseEnclaveReady:         true,
	ClauseAttemptFrozen:        true,
}

// AllClauses is every clause in evaluation order. Ordered so that a
// refusal list reads down the conjunction as docs/DESIGN.md §4 writes
// it, and so that a coverage test can assert none was forgotten.
var AllClauses = []Clause{
	ClauseIntentImmutable,
	ClauseIntentStateEligible,
	ClauseBuildIdentified,
	ClauseProofHeadCurrent,
	ClauseProofsApplied,
	ClauseCommercialIdentity,
	ClauseMerchantOfRecord,
	ClauseSourceAllocation,
	ClauseAuthorizationValid,
	ClauseAuthorityEvidence,
	ClauseNoticeDelivered,
	ClauseNoticeWaitElapsed,
	ClauseWithinCeilings,
	ClauseFundingPlanBalances,
	ClauseCreditLotsReserved,
	ClauseExposureReservation,
	ClauseFundingMatchesAccepted,
	ClauseTaxFinal,
	ClausePolicyPublished,
	ClauseTimeReadiness,
	ClauseRailSupportsPlan,
	ClauseProviderAutonomy,
	ClauseFirstStepMatchesPlan,
	ClauseInstrumentBinding,
	ClauseEnclaveReady,
	ClauseAttemptFrozen,
	ClauseNoPriorSettlement,
	ClauseClaimAvailable,
}
