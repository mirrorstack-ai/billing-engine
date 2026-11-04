// Package einvoice holds the rail-neutral buyer invoice profile
// (billing-engine#285, D530-D551): the data and policy an eventual Taiwan
// e-invoice issuance (#287) reads, never the issuance itself.
//
// Scope, deliberately narrow (D526): this package stores and validates the
// buyer's invoice identity (統編, carrier, 愛心碼) and the policy constants an
// invoice row must stamp. It calls no vendor, signs nothing, and issues
// nothing — that is the invoice-provider port (#286) and the issuance path
// (#287), both built on top of this table. Every payment rail (Stripe,
// NewebPay, TapPay, manual transfer) writes a neutral payment fact (#313);
// this package never knows which rail paid.
package einvoice

// EInvoicePolicyRevision names the accepted policy revision
// (docs/adr/0001-taiwan-einvoice-platform-service.md) every invoice row
// (#287) and payment fact (#313) must stamp at write time. A profile row
// itself is NOT stamped — the snapshot happens where the invoice/fact
// IS written, per the ADR's snapshot rule, so an edit to the profile never
// changes an issued invoice.
//
// Bumping this string is a policy decision, not a code edit: it belongs with
// a new ADR, same as the routing policy revision it mirrors
// (internal/intent/chargeintent.go RoutingPolicyRevision).
const EInvoicePolicyRevision = "einvoice-policy-2026-11-04"

// IssueTimingPolicy says when an invoice is issued relative to the payment
// fact that triggers it.
type IssueTimingPolicy string

// AtPayment issues synchronously at the payment fact, with no callback route
// (D550). It is the only timing this policy revision allows — see the ADR for
// the open MOF-table-row and prepaid-credit questions a future revision may
// have to answer differently.
const AtPayment IssueTimingPolicy = "at_payment"

// IssueTiming is the single constant #287's issuance path reads. Changing it
// requires a new EInvoicePolicyRevision and ADR, never a bare edit here.
const IssueTiming = AtPayment
