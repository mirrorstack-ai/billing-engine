# Security

This repository is public so customers can check the conforming billing-engine
path themselves. Could this path charge them for something outside its disclosed
and authorized intent? The evidence should be code and receipts, not a support
reply. That is the desired property, not the current production claim.

It is also deliberately scoped: public source cannot prevent a replaced executor,
or a holder of an unrestricted merchant credential, from charging at the provider
directly. The [Adversary model](#adversary-model) states that residual limit and
the controls that detect it, rather than claiming it is impossible.

> **Status: proposed architecture; current code does not yet meet it.** The
> intended boundary is specified in [`docs/DESIGN.md`](docs/DESIGN.md). The
> implementation on the baseline commit from which these documents were written
> (`78b5c69`) can calculate and finalize automatic charges on its own. It does so
> without a durable, customer-visible `ChargeIntent` (unbuilt), an
> engine-recorded pre-charge notice, or a `BillingAuthorization` (unbuilt). Do
> not read the requirements below as claims about that build.
> [Known current gaps](#known-current-gaps) is this repository's only
> enumeration of current defects.

The target architecture must be payment-provider-neutral. A provider invoice or
callback must count as settlement evidence only, never as the billing ledger.

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
| Production dispatch can invoke the complete account-api Lambda action dispatcher; only local HTTP separates `RecordUsage` with `X-MS-Meter-Secret`. | A metering role is not fact-only while it can select ordinary billing actions. The target requires a separate function/IAM resource or authenticated role-to-action enforcement. |
| Both HTTPS-webhook and EventBridge callback binaries currently wire Stripe-writing auto-top-up and credit-purchase executors into their routers. | A callback path is not read-only merely because its input was authenticated. Those writer links and credentials must be removed before callback reconciliation can be called capability-safe. |
| The current Stripe client initializes stripe-go's default backend without overriding its nonzero automatic network-retry setting. | One apparent SDK mutation may submit multiple HTTP writes after an ambiguous transport failure. Target writers must set retries to zero, disable redirects, and fence/count the actual outbound request before reporting ready. See [`internal/shared/stripe/client.go`](internal/shared/stripe/client.go). |
| Tax policy and jurisdiction evidence are not implemented as a frozen, versioned decision. | The engine cannot yet claim either a correct tax charge or a justified zero-tax result. |
| The health response does not identify the deployed source and policy artifact. | Public source cannot verify a private deployment when a receipt cannot name the code that produced it. |
| Provider invoices and callbacks are integrated before a provider-neutral intent ledger exists. | Adding another provider without a core settlement claim could create cross-provider retry and double-settlement risk. |
| Public documentation before this design described the repository as a schema-only bootstrap. | The public description did not enumerate the code's real money-moving surface. |
| A customer-visible infrastructure line carries a 1.2x markup that its own displayed unit price does not include. | `infraMarkupNum = 12` sets charge = cost x 12/10 ([`internal/account/cycle/types.go:59-60`](internal/account/cycle/types.go)). Displayed `UnitPriceMicros` is pre-markup COGS while `ChargedMicros` already includes the markup ([`internal/account/usage/types.go:446-448`](internal/account/usage/types.go)). Quantity x displayed unit price therefore does not reconcile to the charge, on a line the customer is shown. |
| The marked-up infrastructure total reaches customers through the live read path, not an internal cost report. | `RecordInfraUsage` ([`internal/account/usage/infra.go:326`](internal/account/usage/infra.go)) feeds `AppInfraBill` and `AppModuleInfraBill` ([`internal/account/usage/bill.go:500-530`](internal/account/usage/bill.go)), which `GetAppBill` and `GetAccountBill` serve ([`cmd/account-api/main.go:690`](cmd/account-api/main.go) and `:696`) as `infra_total_micros`, `infra_lines`, and `module_infra_lines`. Any claim that infrastructure is internal-only cost is false about this build. |
| The billing-period anchor is stamped from a provider webhook event rather than a customer authorization. | `StampAccountActivated` writes `accounts.activated_at` on the first `payment_method.attached` event ([`internal/account/webhook/handlers.go:131-135`](internal/account/webhook/handlers.go)). The stamp is best-effort: an error is logged and the attach continues, so the day every later cycle closes and charges can be set, or missed, by provider event ordering. |
| `StartCreditPurchase` finalizes an auto-advance invoice before the browser holds its client secret. | The purchase drives a finalize with `AutoAdvance: true` ([`internal/shared/stripe/client.go:454-456`](internal/shared/stripe/client.go)), which that file calls the only money-moving step, from `finalizeDraft` ([`internal/account/creditpurchase/executor.go:271`](internal/account/creditpurchase/executor.go)). The client secret is returned to the caller only afterwards ([`internal/account/billing/credit.go:624-635`](internal/account/billing/credit.go)). Stripe may charge the default card before the customer's browser has anything to confirm. |
| Four ordinary read and ingest paths can reach the auto-top-up executor. | `GetServiceStatus` calls the credit gate ([`internal/account/billing/service.go:465-476`](internal/account/billing/service.go)), `GetCreditStanding` calls `GetServiceStatus` ([`internal/account/billing/credit.go:48`](internal/account/billing/credit.go)), and usage ingress plus infra sync call `EvaluateCreditUsage` ([`internal/account/usage/service.go:216`](internal/account/usage/service.go), [`internal/account/usage/infra.go:435`](internal/account/usage/infra.go)). All four converge on `maybeTriggerAutoTopUp` ([`internal/account/credit/coordinator.go:316`](internal/account/credit/coordinator.go) and `:578`). A status read is not capability-safe when it can move money. |
| One boundary invoice mixes the closed period's usage arrears with the new period's advance charges. | The total is arrears plus advance base plus overage plus domains in a single Stripe invoice ([`internal/account/cycle/charge.go:296-299`](internal/account/cycle/charge.go) and `:592-594`). A customer cannot separate what was consumed from what is billed forward, and one collection decision covers two different periods. |

The current code contains worthwhile operational controls: integer money,
idempotent usage ingestion, immutable per-version price snapshots, frozen retry
amounts, provider idempotency keys, and crash-recovery markers. Those reduce
calculation drift and accidental duplicate collection. They do **not** by
themselves prove that a customer was notified of and authorized the exact
charge.

Re-verification rule: any commit after `78b5c69` touching `internal/` or
`migrations/` requires this section to be re-checked against the tree. That
check must run on a schedule, never as a required pull-request check.
`.github/workflows/ci.yml` has no base-branch filter on `pull_request` (see its
comment block at lines 6-18), so a required baseline check would turn every pull
request in the repository red.

## Adversary model

Every billing guarantee is a guarantee against a particular adversary. Naming it
turns "we use idempotency" into a checkable property, and exposes what we cannot
solve.

### The primary adversary is MirrorStack's private caller

Not an unauthenticated browser. **Us.**

The billing engine is invoked by MirrorStack's private platform through an IAM- or
internal-secret-gated surface. The local HTTP path checks
`X-MS-Internal-Secret`, with a separate `X-MS-Meter-Secret` for metering
([`internal/shared/auth/internal_secret.go:80-81`](internal/shared/auth/internal_secret.go),
[`cmd/account-api/main.go:15`](cmd/account-api/main.go)). An outside attacker
normally has to compromise that platform or its cloud account before reaching the
engine. The question a public-source billing engine answers is different:

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

The private caller must not hold the billing database key, intent-sealing key,
notice-channel key, merchant credential, tax-policy signing key, or executor
credential. A deployment where one credential does all of those has erased the
boundaries this document depends on.

The rule that follows is the same one used throughout this design:

> **A check the private caller can satisfy with a statement about itself is not
> a control.**

An internal field such as `customer_approved: true`, `notice_sent: true`,
`tax_exempt: true`, `amount: 10200`, or `provider_paid: true` must grant no
authority. The engine must derive or observe each of those facts through a
boundary the caller cannot write.

### What the private caller can still influence

The caller is often the source of candidate usage and lifecycle facts. The engine
must validate that an event names a declared metric and belongs to an installed
module and app. It must also fall within a billing period, deduplicate, and be
priced by a frozen public rule. Those checks stop a caller supplying money directly.
They do not prove the customer consumed the reported service.

A private caller that can fabricate otherwise valid usage can still increase a
proposed total. This design reduces that risk with a disclosure that requires
equality with the amount later collected, a notice window before collection, and
authorization caps per charge and per period. It adds source-evidence export the
customer can re-derive, cancellation before execution, and dispute evidence after
it. It does not call private metering an independent witness. A stronger claim needs
a separately trusted meter, or customer-verifiable provider logs, stated metric by
metric.

### The secondary adversaries are adapters, callbacks, and provider ambiguity

The core must stay provider-neutral. Stripe and NewebPay are payment adapters, not
billing authorities. Providers differ in their notions of invoice, order,
authorization, capture, finalization, idempotency, inquiry, void, refund, and
callback authenticity. The core must never turn the weakest provider's semantics
into a platform-wide assumption.

For the ledger boundary, executor return values and asynchronous callbacks are
untrusted. They may be buggy, stale, duplicated, reordered, truncated,
mistranslated, or dishonest. They may wake reconciliation; they must not declare an
intent settled.

- Immediate settlement must require provider-signed evidence the core verifies
  itself.
- Otherwise an independently deployed reconciler must read the same provider
  operation with a provider-enforced read-only credential.
- Or it must call a fixed-read broker inside the one `ProviderCredentialEnclave`
  (unbuilt), attested under the customer-pinned deployment signing root named in
  [Assumptions](#assumptions-and-what-breaks-when-they-are-wrong).
- That credential-bearing reader, or enclave broker, is inside the trusted
  computing base.
- Comparing fields in a normalized response cannot detect a malicious adapter
  that fabricated every matching field.

There is one unavoidable limit. The `ProviderCredentialEnclave` (unbuilt) holding an
unrestricted live merchant credential can ask its provider to move money outside the
public core; no Go interface restrains a deliberately replaced binary. We reduce that
authority with isolated executor credentials, least-privilege provider keys, public
adapter code, artifact attestation under the customer-pinned signing root, narrow
deployment roles, and provider audit logs. We do **not** claim to survive compromise
of the deployed adapter binary plus its write credential. Where a provider offers a
narrower per-operation or amount-bound token, its adapter must use it.

What this design does claim is narrower and checkable:

- no private RPC caller can reach an adapter's write capability;
- an adapter receives one frozen, purpose/step-typed command per finite-plan
  mutation rather than caller fields, and no permit hides multiple SDK writes;
- core reconciliation verifies provider-signed proof, or evidence from the
  explicitly trusted credential-separated read-back path;
- that evidence is compared with the frozen merchant account, amount, currency,
  intent, and operation;
- it is also compared with an authoritative provider payer identity, or an
  authenticated operation reference bound to the frozen payer and attempt; and
- a provider artifact never substitutes for the intent ledger or the receipt.

Notice delivery has the same evidence rule. A notifier-role signature proves which
component spoke. It does not prove that a carrier reported the disclosed bytes at
the configured destination, in a terminal status the accepted policy defines as
destination-delivered. `NoticeReceipt` (unbuilt) must require carrier-signed proof
the core verifies directly, or an authoritative read-back through a
provider-enforced read-only credential or an attested fixed-read broker. That proof
must bind content digest, enrolled-destination commitment and revision, provider
message id, terminal status, delivered time, audience, and replay identity. Queue
acceptance, submission, bounce or rejection, and any other nonterminal status must
fail closed. If only an attested reader can establish those facts, that reader and
the carrier are in the trusted computing base. A compromise that fabricates
delivered time defeats the notice guarantee, and that is not hidden behind the word
"authenticated."

### What each party can do

| | customer | private caller | billing core | executor | payment adapter/provider |
|---|---|---|---|---|---|
| submit candidate usage/lifecycle facts | indirectly | yes | validate and retain evidence | no | no |
| choose a charge amount | only by accepting terms with a stated cap | **no** | **derive and freeze** | no | no |
| choose customer-facing line categories | accept/reject policy | **no** | **derive from public policy** | no | render only |
| establish a `BillingAuthorization` (unbuilt) | **yes, after independently verifiable disclosure and customer proof** | relay only; **cannot mint proof** | verify and seal | read only | may establish payment method |
| deliver a charge notice | receive it | no authority | create the disclosure | no | notice provider transports bytes |
| decide an intent is executable | cancel or allow accepted terms | **no** | **yes, from durable gates and proof head** | consume typed capability only | no |
| reach payment-provider writes | customer-present flows only | **no** | no | **yes, only here** | performs requested operation |
| declare the ledger settled | no | no | **only after verified evidence** | submit observations | **no** |
| stop a not-yet-executed intent | **yes** | may request, never override | cancel/expire | must obey | no |
| explain a completed charge | inspect evidence | proxy only | **produce the receipt** | submit execution evidence | provider artifact is supporting evidence |

The authorization, executability, provider-write, and ledger-settlement rows carry
the weight. They must remain separate capabilities in code, credentials, deployment
roles, and tests. Adjacent proof and evidence roles have their own limits.

| role | enforced boundary | consequence if compromised |
|---|---|---|
| customer-held verifier | pinned, signed release; reproducible build and update provenance; canonical encoding of the disclosure rendered before factor use | a compromised verifier or customer device can misrender terms or misuse the factor, so customer-presence semantics fail unless a separate secure-display factor independently renders and signs the same fields |
| consent/revocation edge | narrow append-only payer-stream procedure; no account RPC or provider credential | can delay or replay attempts, but cannot mint the customer factor proof, choose an amount, skip the gap-free sequence, dispatch an account action, or reach a provider |
| provider callback ingress | raw request bytes only, under a published size limit; public-key or dedicated verification-only credential, or a fixed verifier inside the exclusive mutation-credential enclave | can delay or replay traffic, but replay checks and the core prevent attempt creation; it has no account action or provider read/write capability |
| billing-owned proof state, evidence outbox, and the `ReadEvidence` procedure (unbuilt) | verifies signatures, assigns a gap-free payer sequence and head, and serializes proof application with claim/grant CAS; state and evidence commit together; the only read capability verifies and consumes `CustomerReadProof` (unbuilt) and returns padded encrypted results | compromise is core-state compromise and is inside the billing trusted computing base; edge acceptance alone still never means engine effect |
| evidence edge | no table, list, or raw-read capability and no provider credential; it can call only `ReadEvidence` | can delay or replay requests, but the billing-owned procedure prevents private-relay trust, cross-tenant existence disclosure, evidence mutation, or provider access |

The verifier binary, its update channel, and the customer device are therefore in
the customer-presence trusted computing base, unless the enrolled factor has an
independent secure display. Releases must be signed against a pinned root,
reproducibly buildable, and carry auditable update provenance. Calling a browser
component "customer-held" does not make a compromised release safe. The evidence
edge's limits come from enforced data credentials and the billing-owned
`ReadEvidence` procedure, not from trusting the edge to check its own proofs.

### Three shortcuts that are not controls

Each is the cheapest substitute for a real control, and each leaves the
customer with nothing.

- An opaque id or private-UI URL alone is not meaningful consent and cannot be
  substituted.
- A local `last_consumed` value without an authoritative current head is not a
  cancellation control.
- The `api-platform` route being customer-facing is necessary, but not
  sufficient.

### The double-spend the semantic-class typing rule stops

Rating and tax credits must reduce the gross obligation. Stored-value wallet lots
must not reduce it; they fund it. Every source must be typed `rating_credit` or
`stored_value`, and never both. Without that typing, a malicious caller could
subtract the same lot from the obligation and then spend it again as funding. One
lot would pay twice, and the customer would be charged for value the platform
already granted. The enforcing mechanism is a unique-use constraint across those
domains, in [`docs/DESIGN.md` §3](docs/DESIGN.md#3-the-durable-model).

## What is in scope

Anything that breaks a claim made by this repository, once that claim is marked
implemented. Invariants INV-001 through INV-014 are specified normatively in
[`docs/DESIGN.md` §2](docs/DESIGN.md#2-the-invariants); they are proposals, not
production claims. The index below judges report eligibility; it does not restate
the rules.

- **INV-001** — the private caller cannot make amount, currency, tax, lines, destination, or eligibility authoritative.
- **INV-002** — one derivation must power both preview and settlement.
- **INV-003** — a sealed intent never changes; a new price, tax result, adapter, method, or amount needs a replacement intent.
- **INV-004** — an unknown derivation or eligibility input cannot dispatch an effect.
- **INV-005** — no collection before a notice that requires equality with the charge.
- **INV-006** — every debit has customer authority, from a proof envelope no relay credential can mint.
- **INV-007** — one exclusive enclave owner per mutation-capable credential; a Go interface alone is not sufficient ([`#inv-007`](docs/DESIGN.md#inv-007)).
- **INV-008** — one intent settles at most once across all providers, retries, callbacks, workers, and regions ([`#inv-008`](docs/DESIGN.md#inv-008)).
- **INV-008** — an ambiguous attempt stays `execution_unknown` and reconciles against the same provider operation.
- **INV-008** — one consumed permit emits at most one outbound mutation request; a timeout becomes reconciliation.
- **INV-009** — provider callbacks reconcile and never originate money.
- **INV-010** — infrastructure is not a customer charge dimension. Present state contradicts this; see [Known current gaps](#known-current-gaps).
- **INV-011** — settled history is append-only and settled exposure is gross-monotonic, so refund loops cannot reopen capacity.
- **INV-012** — receipts name the running build, intent digest, and policy revisions.
- **INV-013** — proof ordering and execution claim are one serialization boundary, with a generation CAS ([`#inv-013`](docs/DESIGN.md#inv-013)).
- **INV-014** — customer evidence does not depend on the private relay.
- Unknown tax is never zero tax; a zero amount needs a versioned policy that positively derives zero ([`§9`](docs/DESIGN.md#9-tax)). Provider ports and capability publication are in [`§5`](docs/DESIGN.md#5-payment-providers-are-adapters).

Useful reports include:

- replaying, mutating, substituting, or crossing tenants with an intent,
  authorization, notice acknowledgement, settlement claim, or receipt;
- reaching a payment-provider write from anywhere except the executor;
- giving a receipt, callback, inquiry, reconciliation, or cash-flow trace path a
  composite provider client that also exposes writes;
- changing an amount after disclosure without creating a replacement intent;
- executing before the notice window, after cancellation, or above an accepted
  per-charge or period cap;
- moving from `execution_unknown` to a second provider attempt without
  authoritative proof that the first provider did not collect;
- allowing an SDK, HTTP transport, redirect handler, proxy, or middleware to emit
  a second mutation request from one consumed permit;
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
  payment-provider merchant credentials. The assumptions around those systems
  are stated below. A path in this repository that unnecessarily widens their
  authority remains in scope.
- Automated scanner output with no demonstrated impact on a billing invariant,
  confidentiality boundary, or availability property.

Do not probe production with real charges. Reproduce against test adapters and
fixtures whenever possible. If a production-only issue cannot be demonstrated
otherwise, contact us before attempting it.

## Assumptions, and what breaks when they are wrong

Seven assumptions carry the design. Each names what an attacker gets when it is false.

### We assume the deployed artifact is the public artifact

Public source proves nothing about a binary customers cannot identify. `Health` and
`Capabilities` evidence (unbuilt), plus every receipt, must identify the source commit,
reproducible artifact digest, schema revision, and policy digests. Deployment
attestation must be signed under a purpose-bound leaf, chaining to a verification
root customers pin outside every runtime relay. Rotation must be append-only and
transparency-anchored; an unknown root must disable automatic execution.

**If it is wrong:** customers cannot verify that this code is the code holding
payment credentials. That root is an assumption, not something tests close.

### We assume billing-owned storage and keys are isolated

The private caller must not have direct write access to intent, authorization,
notice, execution, or receipt tables, and must not hold their sealing or signing
keys.

**If it is wrong:** the state machine is advisory only, because the caller can
edit the rows or mint the envelopes.

Database operators remain powerful. `BillingDecisionProof` (unbuilt) must sign the
supplied closed predicate and key schema, proof head, before and after commitments
and generations, transaction, build and policy identities, and outbox binding.
Global non-omission remains `state_assurance: attested`, with the deployment
attestation root as the attester. Append-only constraints, audit export, and backups
are the primary controls. The asynchronous payer-isolated transparency log detects a
rollback, equivocation, or split view only after a root is published, and it never
gates payment.

### We assume the customer authentication ceremony means what it says

An accepted authorization is only as strong as its independently verifiable
presentation and customer proof. A bearer credential the private caller can mint or
read does not defend against that caller. A signature over an opaque digest does not
prove which terms the customer saw. The implementation must state the engine-signed
disclosure format, independent rendering and verifier, customer-controlled factor,
challenge audience, expiry, replay rule, contact-enrollment process, and
contact-change cooling period.

**If it is wrong:** the authorization proves only that some credential answered,
not that a customer agreed. Compromise of the customer's factor or destination is
not prevented here, and delivery does not prove human reading.

### We assume the notice carrier evidence path is truthful

The core must verify a carrier signature where one exists. Otherwise the notice reader
and the carrier are trusted to report the disclosed content digest, destination
commitment, message id, terminal status, and delivered time. That reader must be
credential-separated and attested under the deployment signing root. A malicious
notifier assertion alone must never be accepted.

**If it is wrong:** the engine cannot prove delivery, because the carrier or the
read path fabricated those facts. Transparency checkpoints and complaints are
detection, not prevention. Such a path must be named in `Capabilities`, and unknown
evidence strength must disable automatic execution.

### We assume payment providers enforce their authenticated operations

The core must verify what it requests and what the provider later reports.
Provider credentials must be separated by adapter and environment.

**If it is wrong:** a provider or card network can charge a different amount
while falsifying every authoritative read, and the core cannot detect it.
Reconciliation, merchant statements, and customer disputes are the external
controls. A deliberately malicious deployed adapter holding an unrestricted
credential can likewise exceed its Go interface; source attestation and credential
scope are the controls there, not the interface.

### We assume the published pricing and tax authorities are legitimate

The engine will prove that it applied a particular immutable rule. It cannot decide
whether the business was entitled to publish that price, or whether a tax rule is
legally correct. Governance, effective dates, customer notice, and signatures bind
those authorities. Unknown authority means the policy is not executable.

**If it is wrong:** the engine reproducibly applies an illegitimate rule, and
produces evidence that it did so faithfully.

### We assume clocks are trusted only within declared roles

Notice windows, proof, authorization and capability expiries, price and policy
windows, service admission and seal, responsibility cutoffs, and consume transitions
all depend on time. Every money-authoritative transition must use a billing-owned
monotonic time source, disciplined by an authenticated wall clock.
`Capabilities` must publish the time-source identity, the maximum accepted
uncertainty and skew, the maximum forward step, the rollback policy, and current
readiness. `NoticeReceipt` (unbuilt) and `BillingDecisionProof` (unbuilt) must bind
that policy revision and the observed uncertainty interval.

Time readiness must be false on a forward jump, a rollback, source disagreement,
stale synchronization, or an uncertainty interval overlapping the disallowed side of
any cutoff. Recovery may move a transition later. It can never credit elapsed notice
time, pre-expiry authority, or an unproven prior service window. Admission, seal,
setup, customer-hosted issuance, wallet settlement, and provider dispatch use the
same check.

**If it is wrong:** an attacker who controls time controls expiry. When ordering
is uncertain, the transition must fail closed with no new debt or effect.

## What this design does not claim

These are the limits of the architecture in [`docs/DESIGN.md`](docs/DESIGN.md),
stated so a reader does not infer more. Even fully built, it will not:

- prove that a person read a delivered notice;
- prove that private metering facts describe real consumption, unless a metric
  has an independently documented evidence source;
- force the private platform to show a billing page, forward a receipt, propose
  an intent, or continue providing service;
- prevent a customer approving terms they later regret — it proves which terms
  constrained the charge and preserves cancellation and cap evidence;
- prevent compromise of the customer's authentication factor, payment account,
  billing destination, MirrorStack's cloud signing root, a tax authority, or a
  merchant credential;
- guarantee that a payment provider, issuing bank, tax authority, or notice
  carrier is available;
- make tax advice or legal claims beyond applying the named public policy
  reproducibly;
- equate provider acceptance, invoice finalization, callback receipt, or
  dashboard status with ledger settlement; or
- hide refunds, disputes, reversals, credits, or adjustments — they are new,
  reason-coded ledger events linked to the original receipt.

## Payment credentials and customer data

- Provider API keys, webhook secrets, merchant signing keys, customer payment
  tokens, and tax-provider credentials must come from a secrets manager or
  process environment.
- Those same credentials must never appear in source, intent payloads, logs, or
  receipts.
- Billing contacts, jurisdiction evidence, tax identifiers, and provider
  metadata must be minimized and tenant-scoped. Debug errors and public health
  responses must carry no customer data.
- A green CI result does not authorize automatic merge, deployment, or
  collection enablement. While the legacy paths remain, billing-engine changes
  are manually reviewed and promoted.
