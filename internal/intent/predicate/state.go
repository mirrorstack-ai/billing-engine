package predicate

import (
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// IntentState is the lifecycle position of docs/DESIGN.md §4's flow
// chart. Only one of them may execute.
type IntentState string

const (
	StateProposed          IntentState = "proposed"
	StateNoticePending     IntentState = "notice_pending"
	StateDisclosed         IntentState = "disclosed"
	StateEligible          IntentState = "eligible"
	StateExecuting         IntentState = "executing"
	StateProviderInProgres IntentState = "provider_in_progress"
	StateSucceeded         IntentState = "succeeded"
	StateVoided            IntentState = "voided"
	StateCanceled          IntentState = "canceled"
	StateExpired           IntentState = "expired"
)

// AuthorityMode is which of docs/DESIGN.md §4's two mutually exclusive
// gates applies.
//
// "The two gates stay mutually exclusive: a fresh intent acceptance
// receipt is the customer-present gate, while a standing authorization
// requires a NoticeReceipt and its delivery-relative wait." A state
// claiming both is not a stronger case, it is an unclear one, and it
// refuses.
type AuthorityMode string

const (
	AuthorityCustomerPresent AuthorityMode = "debit_customer_present"
	AuthorityStandingAuto    AuthorityMode = "standing_automatic"
)

// AcceptanceReceipt is what api-platform relays after the customer
// accepts a disclosure.
//
// 🔴 INV-006: the engine cannot tell a relayed acceptance from an
// invented one. What it can do is refuse one that does not name the
// document it claims to accept — docs/DESIGN.md §4: "A bare
// `accepted: true` carrying no disclosure digest has no effect at all."
// So every field here is checked against the intent, and the check is
// reproducibility rather than proof.
type AcceptanceReceipt struct {
	DisclosureDigest string
	Payer            intent.Subject
	Audience         string
	Nonce            string
	ExpiresAt        time.Time
	ReplayIdentity   string
}

// NoticeReceipt is delivery evidence for the standing-automatic gate.
//
// INV-005: "Automatic collection requires durable evidence that the
// sealed intent was delivered byte-for-byte under its notice policy,
// and that NoticeReceipt.eligibilityNotBefore has passed." Queue
// acceptance is not delivery, so TerminalStatus records what the
// carrier reported, and DeliveredBytesDigest records what it carried.
type NoticeReceipt struct {
	// DeliveredBytesDigest must equal the intent digest: the bytes the
	// customer received are the bytes collected against.
	DeliveredBytesDigest string
	// Policy must equal the intent's notice policy.
	Policy string
	// TerminalStatus is the carrier's own terminal state. Only a
	// destination-delivered status counts; "queued" and "sent" do not.
	TerminalStatus string
	// EligibilityNotBefore starts from DELIVERY, not from sealing.
	EligibilityNotBefore time.Time
	// RevocationPathFresh records that the customer's route to cancel
	// was verified working. A notice the customer cannot act on is not
	// a control.
	RevocationPathFresh bool
}

// FundingPlan is how the total is to be met.
//
// INV-008 and docs/DESIGN.md §4 require gross to equal the wallet
// allocation plus the sealed provider remainder. The plan is frozen
// before disclosure, so the split the customer saw is the split that
// settles.
type FundingPlan struct {
	Frozen                  bool
	GrossMicros             int64
	WalletAllocationMicros  int64
	ProviderRemainderMicros int64
}

// SealedState is every input the predicate needs, assembled by the
// caller.
//
// Nothing here is fetched: the package has no store and no clock, so a
// verdict is a function of this value alone. That is what makes a
// refusal reproducible, and it is the property internal/account/
// eligibility has and internal/account/credit/coordinator.go does not.
//
// Every field defaults to the refusing value. A caller that forgets to
// populate one gets a refusal, never an execution — the same inversion
// as the collection-authority grant in internal/account/credit.
type SealedState struct {
	Intent intent.ChargeIntent
	State  IntentState

	// Now is the evaluation instant, supplied rather than read.
	Now time.Time

	// BuildIdentified reports whether the running binary knows which
	// revision it is. docs/VERIFICATION.md §2: "An executor whose build
	// identity reads `unknown` must refuse to execute."
	BuildIdentified bool

	Authorization     intent.BillingAuthorization
	AuthorizationKind intent.ChargeKind
	PriorSpendMicros  int64

	Mode       AuthorityMode
	Acceptance AcceptanceReceipt
	Notice     NoticeReceipt

	Funding FundingPlan

	// PolicyDigestsMatch reports that every policy the intent names is
	// published, effective, and digest-matching.
	PolicyDigestsMatch bool

	// TaxIndependentlyReproducible reports that the sealed tax figure
	// was recomputed from the named rule revision and matched.
	//
	// docs/DESIGN.md §4 asks for "tax is independently reproducible
	// final or explicitly not_applicable", which is a stronger claim
	// than the sealed determination carrying a Resolved flag: Seal
	// already refuses an unresolved one, so reading that flag back here
	// would check something no reachable state can violate. What can be
	// violated is the reproduction, and that is what a customer
	// rechecking a charge offline is doing.
	TaxIndependentlyReproducible bool

	// TimeReady reports that the trusted clock's uncertainty interval
	// lies wholly on the permitted side of every cutoff evaluated here.
	// A clock that might be wrong in the direction that permits is not
	// ready.
	TimeReady bool

	// PriorSettlementExists records any earlier terminal or nonterminal
	// settlement, attempt or grant for this intent (INV-008: one intent
	// settles at most once, across all providers).
	PriorSettlementExists bool

	// ClaimAvailable reports that the core-owned settlement claim can
	// be acquired atomically.
	ClaimAvailable bool

	// --- evidence for clauses whose records are unbuilt ---
	//
	// These are false until the corresponding record exists, which is
	// what makes those clauses refuse. They are fields rather than
	// absent so that the conjunction in docs/DESIGN.md §4 stays
	// visible in the type, and so that building each record is a
	// matter of setting one flag from real evidence rather than
	// remembering a clause nobody wrote down.
	Unbuilt UnbuiltEvidence
}

// UnbuiltEvidence carries the clauses whose supporting records
// docs/DESIGN.md §4 requires and this tree does not yet have.
//
// All false is the honest default and the safe one: the predicate
// refuses. Setting one of these to true without the record behind it
// would be the "declared but not implemented" failure that
// docs/SECURITY.md exists to make visible.
type UnbuiltEvidence struct {
	ProofHeadCurrent       bool
	ProofsApplied          bool
	CommercialIdentity     bool
	MerchantOfRecord       bool
	SourceAllocation       bool
	CreditLotsReserved     bool
	ExposureReservation    bool
	FundingMatchesAccepted bool
	RailSupportsPlan       bool
	ProviderAutonomy       bool
	FirstStepMatchesPlan   bool
	InstrumentBinding      bool
	EnclaveReady           bool
	AttemptFrozen          bool
}
