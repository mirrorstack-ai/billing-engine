package intent

// The closed charge catalog of docs/DESIGN.md §6.
//
// §6 is emphatic about what this list is for:
//
//	"A positive customer charge kind that is not listed below must not be
//	 proposed, and must not be collected."
//	"No private caller, module, adapter, webhook, tax vendor or operator may
//	 introduce a kind from free text. A kind arrives by being written here
//	 first, under a published rule you accepted."
//
// Until 2026-08-30 nothing enforced that. `Kind` was a bare string on the
// draft, `Seal` never looked at it, and the two shipped legs proposed
// "domain.custom" and "module.overage" — kinds that appear nowhere in §6,
// invented at the call site, and sealed into a digest a customer's bundle
// would attest to. That is the same shape as ClausePolicyPublished: a
// vocabulary declared closed and left unguarded.
const (
	// --- service kinds: what a customer consumed ---

	// KindPlatformBase is published platform access for one app or account
	// period.
	//
	// §12 item 12 folded installed-module capacity above the included tier
	// and the published custom-domain feature INTO this kind: the base
	// price recovers both, so neither is separately chargeable and neither
	// has a kind of its own. §6's pre-fold table listed them conditionally,
	// "if product policy keeps it" — product policy did not. Do not re-add
	// module_capacity or custom_domain.
	KindPlatformBase ChargeKind = "platform_base"
	// KindModuleUsage is one installed module's declared metered usage.
	KindModuleUsage ChargeKind = "module_usage"
	// KindTax is tax on the listed taxable lines.
	KindTax ChargeKind = "tax"

	// --- funding and collection: not service lines, per §6 ---
	//
	// "Buying credit is not a service you consumed, so it never rides on a
	// recurring bill. These four are their own intent kinds, with their own
	// authority." Each is listed here because §6 lists it; none of the legs
	// that would use them has been cut over, and each needs the authority
	// §6 names for it before it can be.

	// KindSubscriptionStart needs an accepted immutable SubscriptionOffer.
	KindSubscriptionStart ChargeKind = "subscription_start"
	// KindCreditPurchase needs acceptance of engine-signed disclosure bytes.
	KindCreditPurchase ChargeKind = "credit_purchase"
	// KindAutoTopUp needs its own standing authorization binding the balance
	// trigger, amount rule, ceilings, notice and revocation.
	KindAutoTopUp ChargeKind = "auto_topup"
	// KindCollectReceivable retries an amount already owed, under a
	// one-time authorization against the sealed receipt or a standing one
	// after notice.
	KindCollectReceivable ChargeKind = "collect_receivable"
)

// catalog is the closed set. A kind absent from it cannot be sealed.
var catalog = map[ChargeKind]struct{}{
	KindPlatformBase:      {},
	KindModuleUsage:       {},
	KindTax:               {},
	KindSubscriptionStart: {},
	KindCreditPurchase:    {},
	KindAutoTopUp:         {},
	KindCollectReceivable: {},
}

// KindInCatalog reports whether a kind is one §6 lists.
func KindInCatalog(k ChargeKind) bool {
	_, ok := catalog[k]
	return ok
}

// CatalogKinds returns the closed set, for a test or a report that needs to
// enumerate it. The returned slice is a copy.
func CatalogKinds() []ChargeKind {
	out := make([]ChargeKind, 0, len(catalog))
	for k := range catalog {
		out = append(out, k)
	}
	return out
}
