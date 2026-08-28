# Who this service defends your money against

Every billing guarantee is a guarantee against a particular adversary. Naming
that adversary is what turns "we use idempotency" into a property a customer can
check — and it exposes the cases this repository cannot solve.

> **Status: desired architecture, not current behavior.** This document defines
> the acceptance criteria for the intent-only rebuild. On the baseline commit
> from which it was written (`78b5c69`), multiple services can calculate and
> finalize automatic provider invoices directly. There is no durable
> `ChargeIntent`, no customer-bound `BillingAuthorization`, and no enforced
> pre-charge notice transition. [`../SECURITY.md`](../SECURITY.md) lists the
> current gaps. Until the implementation and deployment status say otherwise,
> the guarantees below are requirements, not production claims.

---

## The primary adversary is MirrorStack's private caller

Not an unauthenticated browser. **Us.**

The billing engine is invoked by MirrorStack's private platform through an
IAM- or internal-secret-gated surface. An outside attacker normally has to
compromise that platform or its cloud account before reaching the engine. The
question a public billing engine is meant to answer is different:

> What prevents MirrorStack's private half — if buggy, compromised, or
> deliberately malicious — from silently charging a customer's payment method?

For this model, the private caller may:

- invoke every internal action its production credential reaches;
- submit, omit, duplicate, reorder, delay, and replay requests;
- choose every identifier and untrusted business fact present in those requests;
- lie about what its UI displayed or what a user clicked;
- attempt to substitute another payer, app, module, policy, payment adapter, or
  provider reference;
- withhold a cancellation or a customer-facing response; and
- know this repository and all of its tests.

The private caller does **not** hold the billing database key, intent-sealing
key, notice-channel key, payment-provider merchant credential, tax-policy
signing key, or executor credential. If one credential can do all of those
things, the deployment has erased the boundaries this document depends on.

The rule that follows is the same one used throughout this design:

> **A check the private caller can satisfy with a statement about itself is not
> a control.**

An internal field such as `customer_approved: true`, `notice_sent: true`,
`tax_exempt: true`, `amount: 10200`, or `provider_paid: true` therefore grants no
authority. The engine derives or observes each of those facts through a boundary
the caller cannot write.

### What the private caller can still influence

The caller is often the source of candidate usage and lifecycle facts. The
engine can validate that an event names a declared metric, belongs to an
installed module and app, falls within a billing period, deduplicates, and is
priced by a frozen public rule. Those checks prevent a caller from directly
supplying money. They do not prove that the customer really consumed the
reported service.

A private caller that can fabricate otherwise valid usage can still increase a
proposed total. This design bounds that risk with exact disclosure, a notice
window, authorization caps, source-evidence export, cancellation, and dispute
evidence. It does not call private metering an independent witness. A stronger
claim requires a separately trusted meter or customer-verifiable provider logs,
and must be stated metric by metric.

---

## The secondary adversaries are adapters, callbacks, and provider ambiguity

The core is provider-neutral. Stripe and NewebPay are examples of payment
adapters, not billing authorities. Different providers have different notions
of invoice, order, authorization, capture, finalization, idempotency, inquiry,
void, refund, and callback authenticity. The core never turns the weakest
provider's semantics into a platform-wide assumption.

For the ledger boundary, adapter return values and asynchronous callbacks are
untrusted. They may be buggy, stale, duplicated, reordered, truncated,
mistranslated, or deliberately dishonest. They can wake reconciliation; they
cannot declare an intent settled.

There is one unavoidable limit. An adapter process holding an unrestricted live
merchant credential can ask its provider to move money outside the public core.
No Go interface can restrain a deliberately replaced binary. We reduce that
authority with isolated executor credentials, least-privilege provider keys,
public adapter code, artifact attestation, narrow deployment roles, and provider
audit logs. We do **not** claim to survive compromise of the deployed adapter
binary plus its write credential. If a provider supports a narrower
per-operation token or amount-bound authorization, its adapter must use it.

What this design does claim is narrower and checkable:

- no private RPC caller can reach an adapter's write capability;
- an adapter receives one frozen execution command rather than caller fields;
- a callback or adapter response alone cannot mutate the billing ledger to
  `succeeded`;
- core reconciliation compares provider-authoritative observations with the
  frozen payer, merchant account, amount, currency, intent, and operation;
- an adapter cannot cause the core to retry an ambiguous operation through a
  different provider; and
- a provider artifact is never substituted for the intent ledger or receipt.

---

## What each party can do

| | customer | private caller | billing core | executor | payment adapter/provider |
|---|---|---|---|---|---|
| submit candidate usage/lifecycle facts | indirectly | yes | validate and retain evidence | no | no |
| choose a charge amount | only by accepting bounded terms | **no** | **derive and freeze** | no | no |
| choose customer-facing line categories | accept/reject policy | **no** | **derive from public policy** | no | render only |
| establish a `BillingAuthorization` | **yes, on a customer route** | **no internal action exists** | verify and seal | read only | may establish payment method |
| deliver a charge notice | receive it | no authority | create exact disclosure | no | notice provider transports bytes |
| decide an intent is executable | cancel or allow accepted terms | **no** | **yes, from durable gates** | consume lease | no |
| reach payment-provider writes | customer-present flows only | **no** | no | **yes, only here** | performs requested operation |
| declare the ledger settled | no | no | **only after verified evidence** | submit observations | **no** |
| stop a not-yet-executed intent | **yes** | may request, never override | cancel/expire | must obey | no |
| explain a completed charge | inspect evidence | proxy only | **produce canonical receipt** | append execution evidence | provider artifact is supporting evidence |

The important rows are the authorization, executability, provider-write, and
ledger-settlement rows. They must remain separate capabilities in code,
credentials, deployment roles, and tests.

---

## What "no silent automatic charge" means

An automatic attempt may begin only when all of the following are durably true:

1. A `ChargeIntent` freezes the payer, source evidence, customer-facing line
   items, credits, tax, total, currency, price policy, tax policy, payment
   adapter, payment method or mandate, and engine build.
2. The intent digest covers every byte capable of changing what the customer
   owes. There is no optional digest field and no post-digest line-item merge.
3. The engine generated the disclosure from that same frozen object and sent it
   through an enrolled billing channel.
4. Delivery is recorded and the published notice interval has elapsed.
5. A live one-time or standing `BillingAuthorization` covers the payer,
   currency, category, cadence, amount, period exposure, payment method or
   mandate, policy-change rules, and execution time.
6. The customer has not canceled the intent or withdrawn the authorization.
7. Tax is a versioned exact result. Unknown, timed-out, unconfigured, or
   contradictory tax evidence blocks execution; it never becomes zero.
8. The selected adapter reports all capabilities required for this operation,
   including an idempotency identity and authoritative reconciliation path.
9. No previous execution lease or provider operation for this intent is
   unresolved.
10. The executor atomically acquires the intent's one settlement lease before
    any provider write.

Failure of any gate leaves the intent non-executable and records a public reason.
There is no emergency switch that interprets missing evidence as permission.

"Notice" has a precise, limited meaning. A provider delivery receipt proves
that the engine sent specific bytes to a configured channel. It does **not**
prove that a person read, understood, or remembered them. The receipt and UI
must use the word *delivered*, not *read*, unless a separate ceremony genuinely
establishes more.

---

## The two durable objects

### `BillingAuthorization`

A `BillingAuthorization` records authority granted through the engine's
customer-facing `AcceptBillingAuthorization` route. The private dispatcher has
no action with that effect.

It is either:

- **one-time**, bound to one exact intent digest; or
- **standing**, bounded by payer, currency, allowed charge categories, maximum
  per intent, maximum per billing period, cadence, payment method or mandate,
  notice interval, policy-change rules, expiry, and revocation state.

An authorization is append-only. Revocation creates a durable terminal event;
it does not edit history. Increasing a cap, widening a category, changing a
payment method, shortening notice, or accepting a materially different price or
tax policy requires a new customer ceremony.

The route being customer-facing is necessary, but not sufficient. Its
authentication must establish customer control independently of a private RPC
assertion — for example through an engine-issued, audience-bound challenge sent
to an already verified billing contact, with replay and expiry controls. The
exact ceremony must be documented with the implementation. Until it exists,
standing authorization and automatic execution remain disabled.

### `ChargeIntent`

A `ChargeIntent` is the complete, immutable proposal to collect money. It
contains or commits to:

- intent ID, payer and billing period or business action;
- canonical source event and aggregate identifiers;
- customer-facing line items and their derivation evidence;
- subtotal, credits or adjustments, tax, total, and currency in integer minor
  units plus the canonical higher-precision derivation;
- price-book and tax-policy identifiers, versions, effective times, and digests;
- authorization reference and applicable caps;
- disclosure bytes, channel, delivery evidence, and `executeNotBefore`;
- selected provider, merchant account, payment method or mandate reference, and
  required adapter capabilities;
- engine source commit, artifact digest, schema version, and intent-format
  version; and
- a digest over all billing-relevant fields.

After disclosure, none of those fields can be updated. Any recalculation creates
a new intent that names and supersedes the old one. The old intent becomes
`canceled` or `expired`; it is never silently rewritten under the same URL.

---

## The lifecycle

The public states are:

```text
proposed
   │
   ▼
notice_pending ───────► action_required
   │                         │
   ▼                         │ resolved without mutation
disclosed                    │
   │ notice interval         │
   ▼                         │
executable ◄─────────────────┘
   │ one durable lease
   ▼
executing ─────────────► execution_unknown
   │                           │ same-provider reconciliation only
   ▼                           └───────────────┐
succeeded ◄───────────────────────────────────┘

terminal before collection: canceled · expired
terminal provider object with proven no collection: voided
```

- `proposed` means derivation completed but no disclosure claim is made.
- `notice_pending` means exact disclosure bytes exist but delivery has not been
  established. Delivery failure stays here or moves to `action_required`.
- `disclosed` means delivery evidence is recorded. It does not mean "read."
- `executable` means every gate currently passes and `executeNotBefore` has
  arrived.
- `executing` means the core owns the single lease and may call exactly the
  frozen adapter operation.
- `execution_unknown` is a sticky ambiguity latch after a timeout, process
  crash, malformed response, or conflicting observation. It permits reads and
  same-provider reconciliation, never a fresh charge or provider fallback.
- `succeeded` requires provider-authoritative evidence that exactly the frozen
  amount and currency settled for the frozen merchant operation.
- `action_required` names what is missing: customer presence, notice delivery,
  tax evidence, payment method, provider capability, or a recoverable decline.
  Resolving it cannot mutate a disclosed intent; a changed total creates a new
  one.
- `canceled` is a pre-collection customer or policy stop.
- `expired` means time, authorization, or policy validity ended before
  collection.
- `voided` requires affirmative provider evidence that a created provider
  object cannot and did not collect. A paid operation cannot be relabeled
  `voided`; refunds are separate ledger events and remain visible on the receipt.

Every transition is compare-and-swap against the prior state and append-only in
the event history. Unknown and terminal states are not reset by an operator
editing a row.

---

## The public action surface

The desired action vocabulary is deliberately small:

| action | effect | payment-provider write capability |
|---|---|---|
| `DescribeCharge` | live, explicitly non-final estimate using the same derivation rules | none |
| `ProposeChargeIntent` | validates source facts and freezes an immutable intent | none |
| `AcceptBillingAuthorization` | customer-facing route that creates one-time or standing authority | none |
| `ExecuteChargeIntent` | consumes one executable intent through the isolated executor | **yes — the only action** |
| `CancelChargeIntent` | durably stops a not-yet-collected intent | none |
| `GetChargeReceipt` | returns intent, authorization, notice, derivation, and settlement evidence | none |
| `Capabilities` | reports the running build, policies, adapters, notice, tax, and legacy surfaces | none |
| `Health` | reports whether those configured dependencies can uphold their advertised guarantees | none |

`DescribeCharge` is useful but grants nothing. A mutable estimate is not an
intent, a notice, an invoice, or an authorization.

The private transport must not dispatch `AcceptBillingAuthorization`. The
customer route may call the same pure validation functions, but its
authentication and credential are separate. Likewise, no service outside the
executor links a write-capable payment client.

### The Go capability shape

Provider integrations use ports and adapters built from small Go interfaces.
They are composed at the application root; they do not inherit from, embed, or
pass around one universal provider client whose method set mixes reads and
writes.

The exact names may evolve, but the capability split does not:

```go
// Held only by ExecuteChargeIntent's isolated executor.
type ExecutionPort interface {
	Execute(context.Context, FrozenExecutionCommand) (SubmissionEvidence, error)
}

// Safe for reconciliation and customer/operations reads.
type ObservationPort interface {
	ObserveOperation(context.Context, ProviderOperationRef) (SettlementObservation, error)
}

// Read-only evidence walk; it cannot create, finalize, capture, pay, or refund.
type CashFlowTracePort interface {
	TraceCashFlow(context.Context, ProviderOperationRef) (CashFlowTrace, error)
}

// Turns authenticated callback bytes into an untrusted observation.
type CallbackVerifier interface {
	VerifyCallback(context.Context, CallbackEnvelope) (SettlementObservation, error)
}
```

Stripe, NewebPay, and later providers adapt their own APIs to those ports. A
provider may implement several ports, but consumers receive only the narrow
interface they need. In particular:

- the receipt and trace service receives observation and trace ports, never
  `ExecutionPort`;
- callback handlers receive `CallbackVerifier` and enqueue reconciliation, never
  `ExecutionPort`;
- reconciliation receives `ObservationPort`; a retry decision returns to the
  core state machine rather than invoking a provider itself; and
- only the executor deployment is constructed with `ExecutionPort` and the
  provider write credential.

Where a provider supports separate read and write credentials, construction must
use them. Where it does not, process and role isolation still prevents a
read-only service from obtaining the write-bearing adapter instance. Compile-time
interface assertions, dispatcher tests, and a credential inventory make this
split auditable.

This is capability security, not aesthetic layering. Adding a convenience
method to a universal `Client` would silently widen every caller that holds it.
Composition keeps that change local and reviewable.

---

## Price and line-item integrity

The caller submits facts, never money. A public, effective-dated price policy
turns accepted facts into customer line items. Every price used by an intent is
immutable and named by digest.

The customer-facing vocabulary does not include a free-floating infrastructure
surcharge. Platform compute, network, database, and other infrastructure may be
metered internally for capacity planning and developer settlement, but customer
charges are presented only through the closed vocabulary in
[`CHARGES.md`](CHARGES.md), with disclosed categories such as:

- base fee;
- module capacity or declared module usage;
- custom-domain service, if retained by the accepted product policy;
- tax;
- typed credit; and
- an explicit, reason-coded correction linked to a prior monetary effect —
  never a generic positive debit.

Internal cost allocation is a separate ledger. It cannot be merged into the
customer total after authorization, exposed as an unexplained "infrastructure"
line, or used through a mutable fallback rate. If MirrorStack wants to introduce
a new customer charge category, it publishes a new policy and obtains whatever
new authorization that policy requires.

Integer arithmetic is required end to end. A provider-specific minor-unit
conversion is part of the frozen adapter command and receipt. Unsupported
currency precision, overflow, negative totals outside an explicit credit flow,
or rounding disagreement fails before disclosure.

---

## Tax is fail-closed

Tax is unresolved until the product, legal, evidence, and policy model is
chosen. The engine must say that rather than silently treating the missing
system as zero. [`TAX.md`](TAX.md) specifies the target evidence and the
decisions that still block production execution.

The desired tax decision freezes:

- jurisdiction inputs and their provenance;
- customer tax status and evidence revision;
- taxable line classifications;
- rule-set identifier, version, source, and effective time;
- per-jurisdiction calculation and rounding evidence; and
- the exact tax total included in the intent digest.

`Capabilities` reports tax as `ready`, `degraded`, or `unsupported`. Only
`ready` may produce an executable automatic intent. Timeout, conflicting
location evidence, missing classification, expired exemption evidence, unknown
jurisdiction, or policy lookup failure produces `action_required`; it never
falls back to an old rule or zero.

An explicit zero-tax result names the rule and evidence that derived zero. A
payment provider's automatically added tax, fee, or invoice line is not accepted
as tax truth. If tax changes after disclosure, the engine cancels the old intent,
creates a replacement, and repeats notice and authorization checks.

This repository can make application of a tax policy reproducible. It does not
turn software maintainers into a tax authority or promise that a published rule
is legally correct. The authority and update process for tax policy must be
named before `ready` can be advertised.

---

## Payment adapters and provider differences

Each adapter publishes machine-readable capabilities from the running build:

- supported currencies and minor-unit exponents;
- customer-present versus automatic/off-session collection;
- mandate and payment-method semantics;
- provider idempotency scope and retention;
- whether an inert order or invoice can be created and inspected before
  collection;
- authoritative inquiry/read-back support and expected consistency delay;
- synchronous and asynchronous result semantics;
- callback authentication, replay identifier, and ordering behavior;
- void, cancel, refund, dispute, and chargeback behavior; and
- merchant-account and environment identity.

The core chooses only an adapter covered by the customer's authorization and
freezes that choice before disclosure. The private caller cannot request a
fallback provider.

Automatic execution is enabled only when the adapter can demonstrate equivalent
safety, not identical API names. Where possible the sequence is:

1. create an inert provider object under a deterministic operation reference;
2. read it back and verify payer, merchant, amount, currency, and intent digest;
3. perform the collection operation once; and
4. reconcile from an authenticated provider read, not merely the immediate
   response.

If a provider cannot offer safe idempotency, a unique merchant order identity,
or an authoritative way to resolve an ambiguous result, that adapter is not
eligible for unattended automatic execution. It may support a customer-present
flow whose limitations are disclosed separately.

### Cross-provider double settlement

The database owns one settlement lease per intent, independent of provider. The
lease freezes provider, merchant account, operation identity, and attempt number
before the call. A timeout does not release it.

`execution_unknown` can be resolved only by querying the same provider and same
operation identity. A second provider is never tried until there is affirmative,
durable evidence that the first operation did not and cannot collect. If that
proof is unavailable, the engine waits for operations review rather than risking
two charges.

A replacement intent after such a proof receives a new digest and notice. It is
linked to the first so a receipt can prove why a second operation was allowed.

### Callbacks and webhooks

Callbacks are untrusted input even after signature or encryption verification.
They are:

- size- and schema-bounded;
- authenticated using the configured provider and merchant account;
- replay-deduplicated by provider event or operation identity;
- retained as immutable observations;
- prohibited from selecting an account by a caller-controlled email or display
  value; and
- followed by provider-authoritative read-back where the provider supports it.

Out-of-order events are normal. A late "pending" cannot regress `succeeded`; a
late "paid" for a canceled or voided local object raises a financial incident
rather than silently rewriting history. Callback handlers hold no authority to
construct line items, tax, credits, or customer authorization.

### Provider references and invoices

Provider references are opaque bounded strings. They may be stored and displayed
in redacted form for correlation, but the engine never parses business identity,
tenant, amount, currency, or state out of their shape.

A provider invoice, order, payment, or dashboard is settlement evidence, not the
billing ledger. The canonical `GetChargeReceipt` is generated from the immutable
intent and the core's append-only transition history, with provider observations
attached. If the provider artifact disagrees with the intent, the result is an
incident and `execution_unknown` or `action_required`, not whichever number is
larger.

### Read-only cash-flow tracing

The engine exposes an authenticated, read-only evidence walk for a customer or
financial operator:

```text
ChargeIntent
  → PaymentAttempt
  → provider invoice / order
  → payment / authorization / capture
  → balance transaction
  → payout
  ↘ refund · reversal · dispute · chargeback
```

Each edge comes from an explicit stored reference or a provider-authoritative
read. The tracer never guesses an edge from an email address, amount, timestamp,
display description, or parsed opaque ID. A missing or contradictory edge is
reported as missing or contradictory; it is not filled in heuristically.

The trace answers where money appears to have moved and supplies corroborating
provider evidence. It does not decide what the customer owed, mutate an intent,
mark the ledger settled, issue a refund, or prove that a provider report is
honest. The canonical obligation and state remain the intent ledger; refunds,
disputes, reversals, and payouts are append-only linked events.

Provider differences remain visible. A provider without a balance-transaction
or payout API reports that capability as unavailable instead of synthesizing an
equivalent object. Receipts identify which links were verified and which the
provider cannot expose.

---

## Customer control and cancellation

A customer may cancel a proposed, notice-pending, disclosed, or executable
intent through an engine-served route. Cancellation wins over execution unless
the executor already durably acquired the lease. The race is settled by one
database compare-and-swap, not wall-clock ordering between services.

Once the lease exists, the engine reports `executing` or `execution_unknown` and
does not claim cancellation succeeded. If the provider supports cancellation or
void, the executor may attempt it and records the outcome; it does not erase the
attempt.

Revoking a standing `BillingAuthorization` prevents every future intent and any
existing intent that has not acquired a lease. Provider-side revocation of the
payment mandate remains an independent stop controlled through the payment
provider. The engine must document how quickly each stop becomes effective.

---

## Assumptions, and what breaks when they are wrong

### We assume the deployed artifact is the public artifact

Public source proves nothing about a binary customers cannot identify. `Health`,
`Capabilities`, and every receipt therefore identify the source commit,
reproducible artifact digest, schema revision, and policy digests. Deployment
attestation is served without a claim supplied by the private caller.

If the deployment can lie about its artifact identity, customers cannot verify
this code is the code holding payment credentials. That signing and deployment
root is an explicit assumption, not something unit tests close.

### We assume billing-owned storage and keys are isolated

The private caller must not have direct write access to intent,
authorization, notice, execution, or receipt tables, and must not hold their
sealing/signing keys. If it can edit those rows or mint their envelopes, the
state machine is merely advisory.

Database operators remain powerful. Append-only constraints, audit export,
backups, and external log anchoring make unauthorized edits detectable; they do
not make a fully compromised database impossible.

### We assume the customer authentication ceremony means what it says

An accepted authorization is only as strong as the route's authentication. A
bearer credential the private caller can mint or read does not defend against
that caller. The final implementation must state the independent factor,
challenge audience, expiry, replay rule, contact-enrollment process, and contact
change cooling period.

Compromise of the customer's established factor or billing destination is not
prevented here. Notice delivery also does not prove human reading.

### We assume payment providers enforce their authenticated operations

The core verifies what it requests and what the provider later reports. It
cannot prevent a provider or card network from charging a different amount while
falsifying every authoritative read. Reconciliation, merchant statements, and
customer disputes are the external controls for that case.

Provider credentials are isolated by adapter and environment. A deliberately
malicious deployed adapter holding an unrestricted credential can exceed its Go
interface; source attestation and credential scope are the relevant controls.

### We assume the published pricing and tax authorities are legitimate

The engine can prove it applied a particular immutable rule. It cannot decide
whether the business was entitled to publish that price or whether a tax rule is
legally correct. Governance, effective dates, customer notice, and signatures
bind those authorities. Unknown authority means the policy is not executable.

### We assume clocks only within bounded roles

Notice windows, expiries, and policy effective times depend on time. The engine
uses a trusted server clock with a declared skew bound and records all transition
times. A clock anomaly may delay execution; it must never shorten the promised
notice interval. When ordering is uncertain, execution fails closed.

---

## What this design does not claim

- It does not prove a person read a delivered notice.
- It does not prove private metering facts describe real consumption unless a
  metric has an independently documented evidence source.
- It cannot force the private platform to show a billing page, forward a
  receipt, propose an intent, or continue providing service.
- It does not prevent a customer from approving terms they later regret; it
  proves which terms bounded the charge and preserves cancellation and cap
  evidence.
- It does not prevent compromise of the customer's authentication factor,
  payment account, billing destination, MirrorStack's cloud signing root, a tax
  authority, or a provider merchant credential.
- It does not guarantee a payment provider, issuing bank, tax authority, or
  notice carrier is available.
- It does not make tax advice or legal compliance claims beyond applying the
  named public policy reproducibly.
- It does not equate provider acceptance, invoice finalization, callback receipt,
  or dashboard status with ledger settlement.
- It does not hide refunds, disputes, reversals, credits, or manual adjustments.
  They are new, reason-coded ledger events linked to the original receipt.

---

## How a customer can check the claims

The final proof surface is the receipt, not a marketing page.
`GetChargeReceipt` must provide enough authenticated evidence for a public
verifier to:

1. verify the intent digest and append-only transition chain;
2. identify the exact public engine build and intent format;
3. recompute each line from source evidence and immutable price policy;
4. recompute tax from the frozen tax evidence and policy, or verify the explicit
   rule that produced zero;
5. check that notice delivery preceded `executeNotBefore` by the promised
   interval;
6. check that the one-time or standing authorization covered the exact total,
   categories, cadence, period exposure, provider, and execution time;
7. verify cancellation, expiry, and execution-lease ordering;
8. verify one provider and one operation identity were used;
9. compare provider-authoritative settlement observations with the frozen
   amount and currency; and
10. account for later refunds, disputes, reversals, credits, and adjustments
    without rewriting the original charge.

Sensitive values may be committed by hash and disclosed only to the affected
customer. Redaction must not remove the fields needed to verify money.

The canonical bundle, append-only ledger, provider trace, and verifier contract
are detailed in [`LEDGER-AND-RECEIPTS.md`](LEDGER-AND-RECEIPTS.md) and
[`VERIFICATION.md`](VERIFICATION.md).

The repository should back those claims with four layers:

1. example and integration tests for each lifecycle and provider adapter;
2. property and fuzz tests over money, authorization bounds, state transitions,
   callback ordering, and envelope substitution;
3. concurrency and crash tests proving one lease across workers and providers;
   and
4. mutation tests that deliberately remove each gate and record which test
   fails.

During the rebuild, those results feed a manual billing/security release gate;
they do not trigger automatic merge or production promotion. Automation may
assemble evidence, but it does not decide that a money-moving boundary is ready.

Core derivation, digest, authorization, lifecycle, and receipt verification must
run without a network or live merchant account. Provider contract suites use
test environments only. A security property customers can check only by trusting
MirrorStack's staging account is not a public security property.
