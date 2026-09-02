package architecture

// FieldVerdict says why a caller-supplied money or authority field is
// tolerated on a request type.
type FieldVerdict string

const (
	// VerdictCeiling: the caller supplies a number that can only ever
	// reduce what it is possible to charge.
	//
	// INV-001 exists because a caller-proposed amount lets a hostile or
	// buggy caller inflate a bill. A ceiling cannot do that — the worst
	// a wrong one does is refuse a charge that should have gone
	// through, which is a failure in the safe direction. So a ceiling
	// is allowed to arrive on the wire, and is not counted as debt.
	//
	// This holds only while the field is genuinely a bound. A field
	// named like a limit that is read as an amount anywhere is not a
	// ceiling, whatever it is called.
	VerdictCeiling FieldVerdict = "ceiling"

	// VerdictPendingMigration: the field lets the caller decide, or
	// assert, something the engine is required to derive. It is debt,
	// tracked against the gap register, and the count below must fall
	// to zero before the deployment can be called intent-only.
	VerdictPendingMigration FieldVerdict = "pending-migration"
)

// requestFieldVerdicts records every caller-supplied money or authority
// field on a request type, and what it is doing there.
//
// docs/VERIFICATION.md §5 states the target rule — "No monetary or
// authority field on a public request struct. No caller-supplied amount
// may reach the executor" — and records that it fails today, naming
// GrantCreditsRequest's AmountMicros and Actor.
//
// The check built on this table is therefore a countdown rather than a
// wall. It cannot be a wall until the intent model exists, and a check
// that is red on arrival gets disabled instead of fixed. What it can do
// today is refuse anything NEW, and keep the existing debt enumerated
// where it cannot be forgotten.
var requestFieldVerdicts = map[string]struct {
	Verdict FieldVerdict
	Why     string
}{
	// --- ceilings: they only ever reduce ---
	"SetBudgetRequest.LimitMicros": {
		VerdictCeiling,
		"an app spend budget. docs/SECURITY.md §2 records that it is alert-only and not a universal bound, which is a separate gap: a displayed budget must not be mistaken for a hard authorization cap.",
	},
	"SetCustomerBillingModeRequest.CreditLimitMicros": {
		VerdictCeiling,
		"the credit ceiling for a customer on distributor billing; raising it grants no charge, it only permits spend the customer already has authority for.",
	},

	// --- debt: the caller decides something the engine must derive ---
	"StartCreditPurchaseRequest.AmountMicros": {
		VerdictPendingMigration,
		"the caller states how much to charge. This is INV-001 in its purest form: the target takes a sealed intent id and ONE catalog selection, and the engine derives the amount.",
	},
	"GrantCreditsRequest.AmountMicros": {
		VerdictPendingMigration,
		"the caller states how many credits to grant. Named in docs/VERIFICATION.md §5.",
	},
	"GrantCreditsRequest.Actor": {
		VerdictPendingMigration,
		"the caller asserts who acted. docs/SECURITY.md §3: a check the private caller can satisfy with a statement about itself is not a control, it is a claim. Named in docs/VERIFICATION.md §5.",
	},
	"SetAutoTopUpRequest.AmountMicros": {
		VerdictPendingMigration,
		"how much a future automatic charge takes. It is the customer's own standing instruction, but it arrives as a bare number rather than as a BillingAuthorization with a ceiling and a reviewable disclosure.",
	},
	"SetAutoTopUpRequest.ThresholdMicros": {
		VerdictPendingMigration,
		"when that automatic charge fires. Same shape as AmountMicros: it belongs on the authorization object, not on the wire as an integer.",
	},
	"SetMetricVersionPricesRequest.Prices": {
		VerdictPendingMigration,
		"the caller supplies prices. docs/SECURITY.md §2: pricing is not yet one complete, effective-dated, customer-disclosed policy, and legacy fallback paths remain.",
	},
	"SetInfraPriceOverridesRequest.Overrides": {
		VerdictPendingMigration,
		"the caller overrides infrastructure prices. Same policy-store gap, and it feeds the 1.2x markup line whose displayed unit price does not reconcile to its charge.",
	},
}
