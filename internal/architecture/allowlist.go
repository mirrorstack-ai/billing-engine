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
// payment method. There are eleven, which is the number
// capabilities.LegacyMoneyPaths reports and which the test in this
// package pins against an AST scan of the tree. docs/SECURITY.md §2
// says of them that there is no single capability
// choke point proving every payment consumed the same authorization and
// notice gates. Until the intent executor exists, this map is the only
// enumeration of that surface, which is why the reasons below say what
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
	"internal/account/cycle/charge.go (*Service).charge CreateDraftInvoice":                                               "the boundary invoice's inert draft; a crash here leaves something that can never charge",
	"internal/account/cycle/charge.go (*Service).charge CreateInvoiceItem":                                                "the boundary invoice's lines, pinned to that draft",
	"internal/account/cycle/charge.go (*Service).boundaryInvoice CreateInvoiceItem":                                       "resume path re-pinning a line to the replayed draft",
	"internal/account/cycle/overage.go (*Service).ChargeModuleOverage CreateDraftInvoice":                                 "per-timer module overage draft",
	"internal/account/cycle/overage.go (*Service).ChargeModuleOverage CreateInvoiceItem":                                  "the module overage line",
	"internal/account/cycle/overage.go (*Service).recoverModuleOverageCharge CreateInvoiceItem":                           "crash recovery re-pinning the overage line",
	"internal/account/cycle/domain_charges.go (*Service).ChargeDomain CreateDraftInvoice":                                 "custom-domain charge draft",
	"internal/account/cycle/domain_charges.go (*Service).ChargeDomain CreateInvoiceItem":                                  "the custom-domain line",
	"internal/account/cycle/domain_charges.go (*Service).recoverDomainCharge CreateInvoiceItem":                           "crash recovery re-pinning the domain line",
	"internal/account/cycle/proration.go (*Service).reconcileCombinedProrationInvoice CreateDraftInvoice":                 "combined creation-proration draft",
	"internal/account/cycle/proration.go (*Service).reconcileCombinedProrationInvoice CreateCombinedProrationInvoiceItem": "the proration line, carrying the identity that lets a crashed leg be reconciled",

	// --- credit purchase: draft assembly ---
	"internal/account/creditpurchase/executor.go (*Executor).recoverOrCreateInvoice CreateCreditPurchaseInvoice": "the purchase draft, stamped with the ledger anchors that route its webhook",
	"internal/account/creditpurchase/executor.go (*Executor).ensureDraftLine CreateInvoiceItem":                  "the purchase line",
	"internal/account/creditpurchase/executor.go (*Executor).reconcileUncollectible VoidInvoice":                 "closing out a purchase the provider gave up collecting",

	// --- automatic top-up: draft assembly ---
	"internal/account/autotopup/executor.go (*Executor).recoverOrCreateInvoice CreateAutoTopUpInvoice":   "the top-up draft, stamped with the ledger anchors that route its webhook",
	"internal/account/autotopup/executor.go (*Executor).ensureDraftLine CreateInvoiceItem":               "the top-up line",
	"internal/account/autotopup/executor.go (*Executor).finalizeDraft FinalizeInvoiceWithoutAutoAdvance": "finalizes without automatic collection, so the pay step below stays the single money-moving call",
	"internal/account/autotopup/executor.go (*Executor).voidAndFail VoidInvoice":                         "abandoning a top-up whose charge will not be attempted",
	"internal/account/autotopup/executor.go (*Executor).deleteDraftAndFail DeleteDraftInvoice":           "discarding a draft that was never finalized",
	"internal/account/autotopup/executor.go (*Executor).ReconcileWebhookFailure VoidInvoice":             "closing out a top-up the provider reported as failed",

	// --- the ten service call sites that can take money ---
	"internal/account/cycle/charge.go (*Service).charge FinalizeInvoice":                               "COLLECT: the period-boundary invoice — the closed period's usage arrears plus the new period's advance base, overage and domains, in one charge",
	"internal/account/cycle/charge.go (*Service).boundaryInvoice FinalizeInvoice":                      "COLLECT: resume of the same boundary invoice after a crash, replaying the original finalization key",
	"internal/account/cycle/overage.go (*Service).ChargeModuleOverage FinalizeInvoice":                 "COLLECT: a module installed beyond the included allowance, after its grace",
	"internal/account/cycle/overage.go (*Service).recoverModuleOverageCharge FinalizeInvoice":          "COLLECT: resume of the same overage charge after a crash",
	"internal/account/cycle/domain_charges.go (*Service).ChargeDomain FinalizeInvoice":                 "COLLECT: a custom domain",
	"internal/account/cycle/domain_charges.go (*Service).recoverDomainCharge FinalizeInvoice":          "COLLECT: resume of the same domain charge after a crash",
	"internal/account/cycle/proration.go (*Service).reconcileCombinedProrationInvoice FinalizeInvoice": "COLLECT: the prorated remainder of the period an app was created in",
	"internal/account/creditpurchase/executor.go (*Executor).finalizeDraft FinalizeInvoice":            "COLLECT: a customer-initiated credit purchase; docs/SECURITY.md §2 records that this finalizes with auto-advance before the browser holds its client secret",
	"internal/account/autotopup/executor.go (*Executor).resume PayInvoiceWithMethod":                   "COLLECT: automatic top-up against the stored card; docs/SECURITY.md §2 records that four ordinary read and ingest paths can reach it",
	"internal/account/billing/unpaid.go (*Service).PayInvoice PayInvoice":                              "COLLECT: retrying an invoice the customer already owes",
	"cmd/account-api/main.go (*dispatcher).dispatch PayInvoice":                                        "dispatcher delegating to the service method above",

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

	// --- test support that is not a _test.go file ---
	"internal/account/webhook/webhooktest/auto_topup_probe.go (*AutoTopUpChargeProbe).TriggerAutoTopUp PayInvoiceWithMethod": "a probe used by tests to detect whether a path reached the charge; it lives outside _test.go so other packages can use it",
}
