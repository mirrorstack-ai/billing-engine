package architecture

// allowedProviderMutations is the inventory docs/DESIGN.md §11 step 2
// asks for: every call in this tree that changes state at the provider,
// named, with the reason it is allowed to exist.
//
// It is keyed on file + enclosing function + method, so a call moving to
// a different function is a new entry that has to be justified, while an
// edit above it is not.
//
// Two rules govern this map:
//
//   - A call site not listed here fails the build. That is the point:
//     a new money path cannot be added without someone writing down why.
//   - An entry here with no matching call site also fails, so the
//     inventory cannot drift into describing code that is gone.
//
// The `collect` entries are the ones that can take money from a stored
// payment method. There are three, which is the number
// capabilities.LegacyMoneyPaths reports and which the test in this
// package pins against an AST scan of the tree. docs/SECURITY.md §2
// says of them that there is no single capability
// choke point proving every payment consumed the same authorization and
// notice gates. While those three remain the intent executor refuses to
// start, so this map is still the only enumeration of the surface that
// could actually take money, which is why the reasons below say what
// each one charges for rather than merely that it charges.
var allowedProviderMutations = map[string]string{
	// --- the provider adapter itself ---
	// The adapter is where the SDK is allowed to be called. These are
	// the wrapped calls, not independent money paths.
	"internal/shared/stripe/client.go (*realClient).FinalizeInvoice FinalizeInvoice":                   "the adapter's own SDK call; every collecting caller routes through here",
	"internal/shared/stripe/client.go (*realClient).FinalizeInvoiceWithoutAutoAdvance FinalizeInvoice": "same SDK call with auto_advance off, so it finalizes without handing the invoice to automatic collection",
	"internal/shared/stripe/client.go (*realClient).VoidInvoice VoidInvoice":                           "the adapter's own SDK call",

	// --- card administration (no money moves) ---
	"internal/account/billing/service.go (*Service).PrepareAddPaymentMethod CreateCustomer":              "first card attach needs a provider customer to hang it on",
	"internal/account/billing/service.go (*Service).PrepareAddPaymentMethod UpdateCustomerEmail":         "a setup-mode session cannot be confirmed without an email; backfills customers created before it was captured",
	"internal/account/billing/service.go (*Service).PrepareAddPaymentMethod CreateCheckoutSession":       "setup-mode session; returns the client secret web-account drives Stripe Elements with",
	"internal/account/billing/service.go (*Service).StartAddPaymentMethod CreateCustomer":                "the older add-card entrypoint, same reason as PrepareAddPaymentMethod",
	"internal/account/billing/service.go (*Service).StartAddPaymentMethod UpdateCustomerEmail":           "the older add-card entrypoint, same reason as PrepareAddPaymentMethod",
	"internal/account/billing/service.go (*Service).StartAddPaymentMethod CreateCheckoutSession":         "the older add-card entrypoint, same reason as PrepareAddPaymentMethod",
	"internal/account/billing/service.go (*Service).DetachPaymentMethod DetachPaymentMethod":             "customer removing a saved card",
	"internal/account/billing/service.go (*Service).SetDefaultPaymentMethod SetDefaultPaymentMethod":     "customer choosing which saved card is default",
	"cmd/account-api/main.go (*dispatcher).dispatch DetachPaymentMethod":                                 "dispatcher delegating to the service method above",
	"cmd/account-api/main.go (*dispatcher).dispatch SetDefaultPaymentMethod":                             "dispatcher delegating to the service method above",
	"cmd/pm-default-backfill/main.go main SetDefaultPaymentMethod":                                       "one-off backfill binary for customers whose default was never set",
	"internal/account/webhook/handlers.go (*Router).handleCustomerUpdated SetDefaultPaymentMethod":       "mirrors the provider's own default back onto the customer after a change made at Stripe",
	"internal/account/webhook/handlers.go (*Router).handlePaymentMethodAttached SetDefaultPaymentMethod": "first attached card becomes the default so later invoices have something to charge",

	// --- the cycle: draft assembly (no money moves) ---
	"internal/account/cycle/overage.go (*Service).recoverModuleOverageCharge CreateInvoiceItem": "crash recovery re-pinning the overage line",
	"internal/account/cycle/domain_charges.go (*Service).recoverDomainCharge CreateInvoiceItem": "crash recovery re-pinning the domain line",

	// --- credit purchase: draft assembly ---
	"internal/account/creditpurchase/executor.go (*Executor).reconcileUncollectible VoidInvoice": "closing out a purchase the provider gave up collecting",

	// --- automatic top-up: draft assembly ---
	"internal/account/autotopup/executor.go (*Executor).voidAndFail VoidInvoice":               "abandoning a top-up whose charge will not be attempted",
	"internal/account/autotopup/executor.go (*Executor).deleteDraftAndFail DeleteDraftInvoice": "discarding a draft that was never finalized",
	"internal/account/autotopup/executor.go (*Executor).ReconcileWebhookFailure VoidInvoice":   "closing out a top-up the provider reported as failed",

	// --- the three remaining legacy money-path entries ---
	//
	// There were eleven. Every leg now only proposes a sealed ChargeIntent,
	// and the collectors behind the other eight were deleted. What is left is
	// two crash-recovery resumes, which drain rather than being called afresh,
	// and one scanner false positive.
	//
	// The false positive is the dispatcher row at the end of this block: the
	// scan matches a method NAME without resolving the receiver, and that row
	// is billing's OWN Service.PayInvoice. It stays listed rather than being
	// argued away in the scanner, because lowering the count by editing the
	// scanner is what LegacyMoneyPaths's own comment forbids — the count falls
	// when a money path is deleted, not when a matcher is narrowed.
	"internal/account/cycle/overage.go (*Service).recoverModuleOverageCharge FinalizeInvoice": "COLLECT: resume of the same overage charge after a crash",
	"internal/account/cycle/domain_charges.go (*Service).recoverDomainCharge FinalizeInvoice": "COLLECT: resume of the same domain charge after a crash",
	// 🔴 NOT a provider call, and since the legacy drop not even an indirect
	// one. ScanProviderMutations matches the selector name without resolving
	// the receiver, and this is d.svc.PayInvoice — billing's OWN service
	// method, which now PROPOSES a receivable and never reaches Stripe. The
	// scan over-approximates deliberately: "a missed site is a money path
	// nobody reviewed while a spurious one costs an allow-list entry and a
	// comment explaining why it is harmless."
	"cmd/account-api/main.go (*dispatcher).dispatch PayInvoice": "NOT A PROVIDER CALL: billing's own Service.PayInvoice, which proposes a receivable",

	// --- the intent path ---
	//
	// These are the REPLACEMENT for the legacy collectors, not another
	// one of them. They are excluded from the legacy count below, and
	// the exclusion rests on a property enforced elsewhere rather than
	// on this comment: internal/provider/stripeadapter is reachable
	// only through internal/intent/executor, which is
	// predicate.Evaluate's single caller — asserted by
	// TestExecutionPredicateHasAtMostOneCaller. If a second caller ever
	// appears, that test fails and this exclusion stops being earned.
	"internal/provider/stripeadapter/adapter.go (*Adapter).Collect CreateDraftInvoice":                "INTENT: the inert draft for one sealed intent",
	"internal/provider/stripeadapter/adapter.go (*Adapter).Collect CreateInvoiceItem":                 "INTENT: one line carrying the sealed total; there is nowhere for a second",
	"internal/provider/stripeadapter/adapter.go (*Adapter).Collect FinalizeInvoiceWithoutAutoAdvance": "INTENT: finalizes WITHOUT handing the invoice to automatic collection, so the pay below stays the single money-moving step with an answer this code receives",
	"internal/provider/stripeadapter/adapter.go (*Adapter).Collect PayInvoiceWithMethod":              "INTENT COLLECT: the one money-moving step of the intent path. Keyed and against a named instrument, unlike the legacy unkeyed Invoices.Pay, so a retry after an ambiguous answer cannot be a second charge. Reachable only through the executor, which reaches it only on a permitting verdict",

	// The GROUP path, which settles several sealed intents onto one invoice.
	// Same four steps, same exclusion, and the exclusion rests on the same
	// property: stripeadapter is reachable only through the executor.
	//
	// It exists because the period-boundary invoice spans more than one §6
	// charge kind — module_usage arrears plus the forward platform_base, two
	// since §12 item 12 folded capacity and domains into the base price — and
	// an intent carries one. See docs/DESIGN.md §8. Nothing calls CollectGroup
	// yet.
	"internal/provider/stripeadapter/group.go (*Adapter).CollectGroup CreateDraftInvoice":                "INTENT: the inert draft for a group of sealed intents",
	"internal/provider/stripeadapter/group.go (*Adapter).CollectGroup CreateInvoiceItem":                 "INTENT: one line per sealed line across the group, apportioned from ONE rounding over the summed remainders",
	"internal/provider/stripeadapter/group.go (*Adapter).CollectGroup FinalizeInvoiceWithoutAutoAdvance": "INTENT: finalizes WITHOUT automatic collection, so the pay below stays the single money-moving step with an answer this code receives",
	"internal/provider/stripeadapter/group.go (*Adapter).CollectGroup PayInvoiceWithMethod":              "INTENT COLLECT: the one money-moving step for a group. Keyed on the sorted SET of the group's digests, so the same intents in any order are one charge and a retry is the same request",

	// --- test support that is not a _test.go file ---
	"internal/account/webhook/webhooktest/auto_topup_probe.go (*AutoTopUpChargeProbe).TriggerAutoTopUp PayInvoiceWithMethod": "a probe used by tests to detect whether a path reached the charge; it lives outside _test.go so other packages can use it",
}
