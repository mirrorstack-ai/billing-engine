# Security

This repository is public so customers can assess whether the attested,
conforming billing-engine path could charge them for something outside its
disclosed and authorized intent, using code and receipts rather than only a
support reply.

That is the desired property, not the current production claim.
It is also deliberately scoped: public source cannot prevent a replaced executor
or any holder of an unrestricted merchant credential from charging directly at
the provider. Artifact attestation, credential isolation, provider logs, and
receipt reconciliation are the controls and detectors for that case; the threat
model does not claim it is impossible.

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

- No automatic adverse customer monetary effect occurs without bounded authority.
  Creating/increasing an obligation requires service/accrual authority that was
  effective at the service time and remains linked to every line, including the
  accepted immutable rates/terms, service window, kinds, and caps. Line-aware
  usage may accrue before the final cycle total and notice exist. Wallet or
  external settlement additionally requires current unrevoked collection
  authority, an immutable `ChargeIntent`, and a disclosed exact total. Later
  revocation stops future accrual but does not erase valid prior debt.
  Evidence-only disputes, corrections, write-offs, and value-granting credits
  remain separately typed ledger effects.
- MirrorStack's private caller cannot make amount, currency, tax,
  customer-facing lines, payment destination, policy digest, or execution
  eligibility authoritative. A product route may relay only a closed,
  non-authoritative engine-template/catalog selection; the core validates it,
  derives the exact proposal, and requires independent exact customer proof.
- A `ChargeIntent` cannot change after disclosure. A new price, tax result,
  payment adapter, payment method, or amount requires a replacement intent and
  a new disclosure.
- `AcceptBillingAuthorization` consumes only a canonical proof envelope from
  the append-only inbox behind the separate consent/revocation edge. The edge
  and `api-platform` may relay signed bytes but cannot mint the
  `CustomerAcceptanceProof`; no IAM/internal-secret credential or approval
  claim can satisfy that proof.
- Claim acquisition and provider-dispatch capability consumption both apply the
  authenticated, gap-free payer proof-stream head. A revocation accepted before
  any prior/current adverse or customer-collectible path wins atomically; a stale/
  missing/gapped head blocks. An established hold or `client_dispatched` path
  instead retains claim/exposure through frozen release/cancel/read-back cleanup.
- Every obligation-creating usage/base-window admission applies that same current
  proof head, requires ready bounded time, and races authority/window/
  responsibility cutoff before reserving exposure. A payer transfer is a dual-
  proof generation cutoff, not a mutable owner field; it cannot move mandates,
  wallet, tax, notice, old debt, or ambiguous claims implicitly. A
  `subscription_start` initial claim and every adverse/customer-collectible
  consume lock that same generation against transfer; transfer-first refuses
  before cash, while dispatch-first retains only the old-payer resolution path.
- `TimeReadinessPolicy` covers every money-authoritative expiry/effective window,
  admission/seal, transfer, setup/client issuance, claim, and consume—not only the
  notice wait. Rollback, excessive jump/skew, stale/disagreeing sources, or an
  uncertainty interval crossing a cutoff fails closed with no new effect.
- Standing automatic wallet/provider settlement requires a fresh independently
  attested revocation-path readiness receipt; stale/outage/inconsistent readiness
  blocks. Targeted censorship by all trusted probes remains an explicit TCB limit.
- A notice failure, unknown tax result, missing immutable price, expired
  authorization, digest mismatch, unsupported provider capability, or ambiguous
  prior execution prevents automatic settlement.
- A notifier assertion is not delivery evidence. `NoticeReceipt` requires a
  terminal carrier status that the accepted policy defines as
  destination-delivered, proven by core-verifiable carrier evidence or an
  attested, credential-separated authoritative read-back bound to exact content,
  destination, message, delivered time, audience, and replay identity. Queue
  acceptance, submission, or a nonterminal status fails closed.
- Each actual mutation-credential scope has one exclusive attested
  `ProviderCredentialEnclave` owner; preferred keys are provider × environment ×
  merchant-account × capability scoped, while any broader provider-enforced scope
  and blast radius is published and may fail policy readiness. The logical role has
  isolated instances rather than one global vault. Inside an instance,
  only purpose-matched guarded writers can use consumed opaque permits. A
  customer may use only an engine-signed, attempt-bound, one-use provider-hosted
  continuation. Read APIs, the private dispatcher, notice delivery, callbacks,
  and receipt rendering cannot move money by type or credential.
- Provider plans use a generated exhaustive purpose/effect matrix. Setup cannot
  hold/debit/return, payment cannot create mandates/returns, refund cannot debit or
  create mandates, and void/mandate-revoke can only release their exact source.
  Every mutation has its own envelope, consume CAS, opaque permit, egress fence,
  and evidence; no adapter call may hide multiple effects.
- Provider observation, reconciliation, and cash-flow tracing use a provider-
  enforced read-only credential, or a fixed-read broker inside that credential's
  exclusive enclave
  while the external reconciler remains credential-free. No separate reader may
  own a second broad credential. A Go interface alone is insufficient. A trace follows
  intent to attempt to provider objects to balance movement, payout, refund, or
  dispute without turning any provider object into ledger truth.
- Callback authentication declares its actual credential class. Public ingress
  may hold only public-key or provider-enforced verification-only material. If the
  callback secret shares mutation authority, bounded raw requests go to a fixed
  verifier inside the one credential enclave; the secret never leaves and the
  verifier exposes no provider read/write operation.
- One intent cannot settle twice, across retries, callbacks, workers, regions,
  payment methods, or payment providers.
- An ambiguous attempt remains `execution_unknown` and is reconciled against the
  same provider operation. It is never retried through another provider or a new
  idempotency identity merely because the first result is inconvenient.
- A consumed permit can emit at most one outbound mutation request. Mutation
  clients disable SDK/HTTP network retries and redirects, and a permit-aware
  transport fences the actual send. Timeout, reset, `429`, or `5xx` becomes
  `submitted_unknown`/`execution_unknown` plus read-only reconciliation, never a
  transparent retransmission. Provider idempotency is defense, not retry authority.
- A payment adapter or callback cannot mark the billing ledger paid by assertion.
  The core verifies provider-authoritative evidence against the frozen payer,
  amount, currency, and intent reference.
- Customer receipts identify the running engine build, intent digest, price and
  tax policy revisions, authorization, notice, source evidence, adapter, and
  settlement observations needed to reproduce the result.
- Price/module and tax policies are canonical declarative non-I/O artifacts under
  public byte/shape/fuel/memory limits shared by core and verifier. Wallet rollups
  have one canonical active index generation with range-CAS reservations. Settled
  authorization exposure is gross/monotonic by default, so refund loops cannot
  silently reopen cycle/frequency capacity.
- Every canonical state transition and customer receipt commits its signed,
  customer-encrypted evidence outbox record atomically. The independent evidence
  edge verifies `CustomerReadProof`; it does not trust `api-platform` identity.
  Authorized, absent, and unauthorized reads share the published status/error,
  padded-size, minimum-timing/jitter, and rate-limit policy. This bounds the
  response oracle but does not claim perfect network or microarchitectural
  indistinguishability; guessed foreign ids never authorize data or provider reads.
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
| Production dispatch can invoke the complete account-api Lambda action dispatcher; only local HTTP separates `RecordUsage` with `X-MS-Meter-Secret`. | A metering role is not fact-only while it can select ordinary billing actions. The target requires a separate function/IAM resource or authenticated role-to-action enforcement. |
| Both HTTPS-webhook and EventBridge callback binaries currently wire Stripe-writing auto-top-up and credit-purchase executors into their routers. | A callback path is not read-only merely because its input was authenticated. Those writer links and credentials must be removed before callback reconciliation can be called capability-safe. |
| The current Stripe client initializes stripe-go's default backend without overriding its nonzero automatic network-retry setting. | One apparent SDK mutation may submit multiple HTTP writes after an ambiguous transport failure. Target writers must set retries to zero, disable redirects, and fence/count the actual outbound request before reporting ready. See [`internal/shared/stripe/client.go`](internal/shared/stripe/client.go). |
| Tax policy and jurisdiction evidence are not implemented as a frozen, versioned decision. | The engine cannot yet claim either a correct tax charge or a justified zero-tax result. |
| The health response does not identify the deployed source and policy artifact. | Public source cannot verify a private deployment when a receipt cannot name the code that produced it. |
| Provider invoices and callbacks are integrated before a provider-neutral intent ledger exists. | Adding another provider without a core settlement claim could create cross-provider retry and double-settlement risk. |
| Public documentation before this design described the repository as a schema-only bootstrap. | The public description did not enumerate the code's real money-moving surface. |

The current code contains worthwhile operational controls: integer money,
idempotent usage ingestion, immutable per-version price snapshots, frozen retry
amounts, provider idempotency keys, and crash-recovery markers. Those reduce
calculation drift and accidental duplicate collection. They do **not** by
themselves prove that a customer was notified of and authorized the exact
charge.

## Disclosure and deployment evidence

The private engine must sign `Capabilities` and `Health` evidence that the
customer-facing control plane can relay without substituting a statement from
MirrorStack's private half. Together they must identify at least:

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
  and capability. A path may be called read-only only when the provider enforces
  a read-only credential, or when a fixed-read broker inside that credential's
  exclusive attested `ProviderCredentialEnclave` owner isolates it and exposes only
  fixed operation-bound reads to a credential-free reconciler. If neither is
  possible, the adapter cannot advertise separated reconciliation or unattended
  automatic-execution readiness.
- Go integrations use small provider ports composed at the application root.
  They do not inherit or embed one universal provider client into every service;
  a read consumer must be unable to name a write method at compile time, but this
  source property never substitutes for credential enforcement/attestation.

## A note on customer presence

An engine-recorded delivery receipt proves that specific disclosure bytes were
sent through the configured channel. It does **not** prove that a person read or
understood them. Likewise, the customer-facing `api-platform` route proves only
what its authentication ceremony actually establishes. The engine itself is not
customer-reachable: production uses IAM-gated Lambda invocation; local RPC uses
an internal secret; provider webhook ingress is separate.

The implementation must state that ceremony precisely. A customer-held signature
over an opaque digest is not sufficient if the private UI can lie about the
matching terms. Until an independently verifiable client or origin validates the
engine signature, renders the canonical fields, and produces a customer proof
that the private RPC caller cannot mint, the service must not claim that a
`BillingAuthorization` represents customer presence, and automatic execution
must remain disabled.
