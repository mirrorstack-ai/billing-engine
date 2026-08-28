# Security

This repository is public so that a question with real money behind it —
**"could MirrorStack charge me for something I was not told about?"** — can be
settled by reading code and reproducing a receipt rather than by trusting a
support reply.

That is the desired property, not the current production claim.

> **Status: proposed architecture; current code does not yet meet it.** The
> intended boundary is documented in [`docs/DESIGN.md`](docs/DESIGN.md) and
> [`docs/THREAT-MODEL.md`](docs/THREAT-MODEL.md). The implementation on the
> baseline commit from which those documents were written (`78b5c69`) can
> calculate and finalize automatic charges without a durable, customer-visible
> `ChargeIntent`, an engine-recorded pre-charge notice, or a
> `BillingAuthorization`. Do not read the guarantees below as claims about that
> build. The known gaps are listed below so that this status cannot be lost in a
> design document.

The target architecture is payment-provider-neutral. Stripe is one settlement
adapter, and other adapters such as NewebPay must pass the same intent,
authorization, notice, ambiguity, and receipt checks. A provider invoice or
callback is settlement evidence; it is never MirrorStack's billing ledger.

## Reporting

**Email `security@mirrorstack.ai`.** Please do not open a public issue for a
vulnerability or include customer billing data in an issue, discussion, or pull
request.

Include whatever you have:

- the source revision and file or action involved;
- what invariant you believe can be bypassed;
- the smallest sequence that demonstrates it;
- redacted intent, authorization, receipt, and provider references, if relevant;
- whether real money moved; and
- a test, fuzz input, or mutation, if you made one.

Never send card numbers, payment credentials, notice tokens, webhook secrets,
provider signatures, or unredacted customer records. Intent IDs and provider
references may still be sensitive correlation data; redact them unless they are
needed to investigate.

We will acknowledge a report within three working days and give an initial
assessment within ten. If we disagree, we will explain why in enough detail for
you to challenge the assessment.

**There is no bounty programme.** We would rather state that plainly than imply
one.

## What is in scope

Anything that breaks a claim made by this repository, once that claim is marked
implemented. The intended security invariants are:

- No automatic payment attempt occurs without an immutable `ChargeIntent`, a
  disclosed exact total, and a live `BillingAuthorization` that permits it.
- MirrorStack's private caller cannot supply the amount, currency, tax,
  customer-facing line items, payment destination, policy digest, or execution
  eligibility of a charge.
- A `ChargeIntent` cannot change after disclosure. A new price, tax result,
  payment adapter, payment method, or amount requires a replacement intent and
  a new disclosure.
- `AcceptBillingAuthorization` is customer-facing and engine-served. It is not
  an internal RPC the private caller can invoke.
- A notice failure, unknown tax result, missing immutable price, expired
  authorization, digest mismatch, unsupported provider capability, or ambiguous
  prior execution prevents automatic settlement.
- The executor is the only component with payment-provider write capability.
  Read APIs, the private dispatcher, notice delivery, callbacks, and receipt
  rendering cannot move money by type or credential.
- Provider observation, reconciliation, and cash-flow tracing use read-only
  ports and credentials that do not expose execution methods. A trace follows
  intent to attempt to provider objects to balance movement, payout, refund, or
  dispute without turning any provider object into ledger truth.
- One intent cannot settle twice, across retries, callbacks, workers, regions,
  payment methods, or payment providers.
- An ambiguous attempt remains `execution_unknown` and is reconciled against the
  same provider operation. It is never retried through another provider or a new
  idempotency identity merely because the first result is inconvenient.
- A payment adapter or callback cannot mark the billing ledger paid by assertion.
  The core verifies provider-authoritative evidence against the frozen payer,
  amount, currency, and intent reference.
- Customer receipts identify the running engine build, intent digest, price and
  tax policy revisions, authorization, notice, source evidence, adapter, and
  settlement observations needed to reproduce the result.
- An opaque provider reference is correlation data, not authority and not an
  amount, account, status, or tenant identifier to be inferred by parsing.
- Platform infrastructure cost may be retained for internal cost accounting,
  but it is not a separate customer-facing surcharge. The amount a customer
  owes is derived only from the closed, disclosed effect vocabulary in
  [`docs/CHARGES.md`](docs/CHARGES.md), which contains no infrastructure kind.
- Unknown tax is not zero tax. A zero amount is accepted only when a versioned
  tax policy positively derives zero from frozen evidence. The unresolved
  policy and fail-closed contract are in [`docs/TAX.md`](docs/TAX.md).

Useful reports include:

- replaying, mutating, substituting, or crossing tenants with an intent,
  authorization, notice acknowledgement, execution lease, or receipt;
- reaching a payment-provider write from anywhere except the executor;
- giving a receipt, callback, inquiry, reconciliation, or cash-flow trace path a
  composite provider client that also exposes writes;
- changing an amount after disclosure without creating a replacement intent;
- executing before the notice window, after cancellation, or above an accepted
  per-charge or period cap;
- moving from `execution_unknown` to a second provider attempt without
  authoritative proof that the first provider did not collect;
- accepting an unsigned, replayed, out-of-order, or mismatched callback;
- treating a provider invoice, webhook payload, or dashboard state as ledger
  truth without independent validation;
- charging through an adapter whose published capabilities cannot support the
  required idempotency and reconciliation semantics;
- applying a mutable fallback price or tax rule to an executable intent;
- exposing payment credentials, billing contact data, or cross-tenant billing
  evidence; and
- documentation or comments that overstate what the code or deployed artifact
  guarantees.

The final item matters. In a public billing engine, a confident false sentence
can cause the same kind of harm as a code defect, and we treat it as one.

## What is out of scope

- Reports about a proposed invariant that is plainly marked unimplemented in the
  status section. A new exploit of the current gap is still useful; merely
  restating the gap is not.
- Tax or legal advice, or disagreement with a published tax policy where the
  engine applied that exact policy reproducibly. Charging when tax is unresolved
  or applying a different policy **is** in scope.
- MirrorStack's private UI or tenancy logic by itself. A private-caller defect
  that makes this engine exceed an intent, authorization, or tenant boundary is
  in scope — that is the boundary this repository exists to enforce.
- Provider availability, a card issuer's decline, or a payment provider's
  documented settlement decision, unless this engine handles the result in a
  way that breaks a stated invariant.
- Compromise of the customer's email account, passkey, payment account, or
  device, except where this engine claims to remain safe despite it.
- Compromise of the cloud account, deployment signing root, tax authority, or
  payment-provider merchant credentials. The threat model states the assumptions
  around those systems. A path in this repository that unnecessarily widens
  their authority remains in scope.
- Automated scanner output with no demonstrated impact on a billing invariant,
  confidentiality boundary, or availability property.

Do not probe production with real charges. Reproduce against test adapters and
fixtures whenever possible. If a production-only issue cannot be demonstrated
otherwise, contact us before attempting it.

## Known current gaps

The following describe the `78b5c69` baseline. They are not accepted final
behavior, and this section must not be shortened until the corresponding code,
tests, deployment evidence, and public status all change.

| Current gap | Why it matters |
|---|---|
| There is no first-class `ChargeIntent` or `BillingAuthorization`. | There is no immutable customer-reviewable object that independently bounds a payment attempt. |
| Billing-cycle, module-overage, app/domain proration, credit-purchase, and automatic-top-up code can reach provider write methods through several service paths. | There is no single capability choke point proving that every payment consumed the same authorization and notice gates. |
| The primary provider client interface combines customer administration, invoice writes, payment writes, and provider reads. | A read/reconciliation consumer cannot be proven harmless from its interface alone. See [`internal/shared/stripe/types.go`](internal/shared/stripe/types.go). |
| Several flows calculate or freeze an amount and finalize the provider invoice within one run. | Calculation, disclosure, eligibility, and money movement are not separate durable transitions. |
| Large automatic collection is recorded as a post-charge disclosure. | It describes money that already moved; it is not notice or authorization. See [`migrations/billing/034_auto_collect_disclosure.up.sql`](migrations/billing/034_auto_collect_disclosure.up.sql). |
| App budgets are alert-only, and the separate spend ceiling is not a universal bound over every charge category. | A displayed budget must not be mistaken for a hard authorization cap. See [`internal/account/budget/service.go`](internal/account/budget/service.go). |
| A nominal credit-status gate (`OutOfCredits`) may synchronously trigger automatic top-up through the coordinator. | A read or eligibility check is not capability-safe when it can reach a card charge. See [`internal/account/credit/coordinator.go`](internal/account/credit/coordinator.go). |
| Per-version module metric prices have immutable snapshots, but pricing is not yet one complete, effective-dated, customer-disclosed policy and legacy fallback paths remain. | Reproducibility of some usage rows does not prove authorization of the final total. See [`migrations/billing/044_metric_version_prices.up.sql`](migrations/billing/044_metric_version_prices.up.sql). |
| Tax policy and jurisdiction evidence are not implemented as a frozen, versioned decision. | The engine cannot yet claim either a correct tax charge or a justified zero-tax result. |
| The health response does not identify the deployed source and policy artifact. | Public source cannot verify a private deployment when a receipt cannot name the code that produced it. |
| Provider invoices and callbacks are integrated before a provider-neutral intent ledger exists. | Adding another provider without a core execution lease could create cross-provider retry and double-settlement risk. |
| Public documentation before this design described the repository as a schema-only bootstrap. | The public description did not enumerate the code's real money-moving surface. |

The current code contains worthwhile operational controls: integer money,
idempotent usage ingestion, immutable per-version price snapshots, frozen retry
amounts, provider idempotency keys, and crash-recovery markers. Those reduce
calculation drift and accidental duplicate collection. They do **not** by
themselves prove that a customer was notified of and authorized the exact
charge.

## Disclosure and deployment evidence

The final service must expose `Capabilities` and `Health` without requiring a
statement from MirrorStack's private half. Together they must identify at least:

- the source commit and built-artifact digest;
- schema, intent-envelope, price-policy, and tax-policy revisions;
- enabled payment adapters and their automatic-settlement capabilities;
- provider read, reconciliation, and cash-flow trace capabilities separately
  from provider write capability;
- notice-channel readiness and the minimum enforced notice window;
- whether tax derivation is `ready`, `degraded`, or `unsupported`;
- whether automatic execution is enabled; and
- any legacy action or direct provider-write path still routed in the binary.

The intent-only production claim requires `legacyMoneyPaths: 0`; a new bounded
surface beside one older direct-write route does not make the deployment
bounded.

While this architecture is proposed and the legacy paths remain, billing-engine
changes are manually reviewed and promoted. A green CI result does not authorize
automatic merge, deployment, or collection enablement.

A deployment with an unresolved tax rule, unreachable notice channel, unknown
build identity, unsafe adapter, or legacy money-moving bypass must report itself
degraded and refuse new automatic executions. Operational pressure is not a
reason to reinterpret an unknown as permission.

The downloadable evidence and offline checks are specified in
[`docs/LEDGER-AND-RECEIPTS.md`](docs/LEDGER-AND-RECEIPTS.md) and
[`docs/VERIFICATION.md`](docs/VERIFICATION.md).

## Payment credentials and customer data

- Provider API keys, webhook secrets, merchant signing keys, customer payment
  tokens, and tax-provider credentials must come from a secrets manager or
  process environment and must never appear in source, intent payloads, logs, or
  receipts.
- A customer-visible receipt may carry a redacted or opaque provider reference,
  but possession of that value grants no operation.
- Provider callbacks are authenticated, replay-bounded, size-bounded, and
  parsed as untrusted input. Authentication proves which provider sent bytes; it
  does not prove those bytes match an intent.
- Billing contacts, jurisdiction evidence, tax identifiers, and provider
  metadata are minimized and tenant-scoped. Debug errors and public health
  responses contain no customer data.
- Adapter credentials are separated by provider, environment, merchant account,
  and capability wherever the provider supports it. A credential used to read
  and reconcile should not be write-capable merely for convenience.
- Go integrations use small provider ports composed at the application root.
  They do not inherit or embed one universal provider client into every service;
  a read-only consumer must be unable to name a write method at compile time.

## A note on customer presence

An engine-recorded delivery receipt proves that specific disclosure bytes were
sent through the configured channel. It does **not** prove that a person read or
understood them. Likewise, a customer-facing acceptance route proves only what
its authentication ceremony actually establishes.

The implementation must state that ceremony precisely. Until a customer
authentication mechanism independent of the private RPC caller is implemented
and tested, the service must not claim that a `BillingAuthorization` represents
customer presence, and automatic execution must remain disabled.
