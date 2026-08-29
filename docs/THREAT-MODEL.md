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
question a public-source billing engine is meant to answer is different:

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

For the ledger boundary, executor return values and asynchronous callbacks are
untrusted. They may be buggy, stale, duplicated, reordered, truncated,
mistranslated, or deliberately dishonest. They can wake reconciliation; they
cannot declare an intent settled. Immediate settlement requires provider-signed
evidence the core verifies itself. Otherwise an independently deployed
reconciler uses a provider-enforced read-only credential, or calls a fixed-read
broker inside the one attested `ProviderCredentialEnclave`, to read the exact
operation. The credential-bearing reader or enclave broker is explicitly in the
trusted computing base. Merely comparing fields
in a normalized response cannot detect a malicious adapter that fabricated every
matching field.

There is one unavoidable limit. The `ProviderCredentialEnclave` holding an
unrestricted live merchant credential can ask its provider to move money outside
the public core.
No Go interface can restrain a deliberately replaced binary. We reduce that
authority with isolated executor credentials, least-privilege provider keys,
public adapter code, artifact attestation, narrow deployment roles, and provider
audit logs. We do **not** claim to survive compromise of the deployed adapter
binary plus its write credential. If a provider supports a narrower
per-operation token or amount-bound authorization, its adapter must use it.

What this design does claim is narrower and checkable:

- no private RPC caller can reach an adapter's write capability;
- an adapter receives one frozen, purpose/step-typed command per finite-plan
  mutation rather than caller fields; no permit hides multiple SDK writes;
- a callback or adapter response alone cannot mutate the billing ledger to
  `succeeded`;
- core reconciliation verifies provider-signed proof or accepts evidence from
  the explicitly trusted, credential-separated read-back path, then compares it with the
  frozen merchant account, amount, currency, intent, operation, and either an
  authoritative provider payer identity or authenticated deterministic operation
  reference uniquely bound to the frozen local payer/attempt;
- an adapter cannot cause the core to retry an ambiguous operation through a
  different provider; and
- a provider artifact is never substituted for the intent ledger or receipt.

Notice delivery has the same evidence rule. A notifier-role signature proves
which component spoke, not that a carrier reported the exact bytes at the
configured destination in a terminal status that the accepted policy defines as
destination-delivered. `NoticeReceipt` therefore requires carrier-signed proof
the core verifies directly, or an authoritative read-back through an enforced
read-only credential or separately attested fixed-read broker. That proof binds
content digest, enrolled-destination commitment/revision, provider message id,
terminal status, delivered time, audience, and replay identity. Queue acceptance,
submission, bounce/rejection, or another nonterminal/invalid status fails closed.
If only an attested reader can establish those facts, it and the carrier are in
the trusted computing base; a compromise that fabricates delivered time can
defeat the notice guarantee and is not hidden behind the word "authenticated."

---

## What each party can do

| | customer | private caller | billing core | executor | payment adapter/provider |
|---|---|---|---|---|---|
| submit candidate usage/lifecycle facts | indirectly | yes | validate and retain evidence | no | no |
| choose a charge amount | only by accepting bounded terms | **no** | **derive and freeze** | no | no |
| choose customer-facing line categories | accept/reject policy | **no** | **derive from public policy** | no | render only |
| establish a `BillingAuthorization` | **yes, after independently verifiable disclosure and customer proof** | relay only; **cannot mint proof** | verify and seal | read only | may establish payment method |
| deliver a charge notice | receive it | no authority | create exact disclosure | no | notice provider transports bytes |
| decide an intent is executable | cancel or allow accepted terms | **no** | **yes, from durable gates and proof head** | consume typed capability only | no |
| reach payment-provider writes | customer-present flows only | **no** | no | **yes, only here** | performs requested operation |
| declare the ledger settled | no | no | **only after verified evidence** | submit observations | **no** |
| stop a not-yet-executed intent | **yes** | may request, never override | cancel/expire | must obey | no |
| explain a completed charge | inspect evidence | proxy only | **produce canonical receipt** | submit execution evidence | provider artifact is supporting evidence |

The important rows are the authorization, executability, provider-write, and
ledger-settlement rows. They must remain separate capabilities in code,
credentials, deployment roles, and tests.

Adjacent public proof/evidence roles have explicit enforcement and trust limits:

| role | enforced boundary | consequence if compromised |
|---|---|---|
| customer-held verifier | pinned, signed release; reproducible build and update provenance; canonical disclosure rendering before factor use | a compromised verifier or customer device can misrender terms or misuse the factor, so customer-presence semantics fail unless a separate secure-display factor independently renders and signs the same canonical fields |
| consent/revocation edge | narrow append-only payer-stream procedure; no account RPC or provider credential | can delay or replay attempts, but cannot mint the customer factor proof, choose an amount, skip the gap-free sequence, dispatch an account action, or reach a provider |
| provider callback ingress | bounded raw request only; public-key/dedicated verification-only credential, or fixed verifier inside the exclusive mutation-credential enclave | can delay/replay traffic, but replay checks and the core prevent attempt creation; it has no account action or provider read/write capability |
| billing-owned proof state | verifies signatures, assigns a gap-free payer sequence/head, and serializes proof application with claim/grant CAS | compromise is core-state compromise and is inside the billing trusted computing base; edge acceptance alone still never means engine effect |
| evidence outbox / `ReadEvidence` procedure | state and evidence are committed together; the only read capability verifies and consumes `CustomerReadProof` and returns padded encrypted results | compromise is core-state compromise and is inside the billing trusted computing base |
| evidence edge | no table/list/raw-read capability and no provider credential; it can call only `ReadEvidence` | can delay or replay requests, but the billing-owned procedure prevents private-relay trust, cross-tenant existence disclosure, evidence mutation, or provider access |

The verifier binary, its update channel, and the customer device are therefore
part of the customer-presence trusted computing base unless the enrolled factor
has an independent secure display. Releases must be signed against a pinned root,
reproducibly buildable, and accompanied by auditable update provenance. Merely
calling a browser component "customer-held" does not make a compromised release
safe. Conversely, the evidence edge's compromise claims come from enforced data
credentials and the billing-owned `ReadEvidence` procedure, not from trusting the
edge to run its own proof check.

---

## What "no silent automatic charge" means

A wallet/provider collection attempt may begin only when all of the following are
durably true:

1. A `ChargeIntent` freezes the payer, source evidence, customer-facing line
   items, rating/tax credits, taxable basis, tax, `grossObligation`, currency,
   price policy, tax policy, funding plan, exact `MerchantOfRecordBinding`,
   engine build, and—only for a nonzero provider remainder—the accepted payment
   instrument, exact adapter artifact/capability digest/evidence class/credential
   scope, autonomy policy, and complete finite provider plan.
2. The intent digest covers every byte capable of changing what the customer
   owes. There is no optional digest field and no post-digest line-item merge.
3. Authority evidence is exactly one of: (a) fresh customer-present proof over
   this exact intent and execution window, or (b) standing automatic authority.
4. For the standing branch, the engine generated the disclosure from the frozen
   object, terminal destination-delivery is established by core-verifiable
   carrier proof or the explicitly trusted read-back path,
   `now >= max(notBeforeFloor, providerDeliveredAt + minimumLeadDuration)`, and a
   fresh `RevocationPathReadinessReceipt` proves the independent revocation path
   is within its published readiness lease. The one-time customer-present branch
   does not pretend a prior notice/wait occurred.
5. A live one-time or standing `BillingAuthorization` covers the payer,
   currency, category, cadence, funding mode and split policy, gross/wallet/
   provider-remainder caps, period exposure, terms/price/tax change rules, and
   execution time, plus the exact `PaymentInstrumentBinding` when the provider
   remainder is nonzero.
6. Unique authorization-exposure reservations prove, for every aggregate cap
   window, `settled + active reservations excluding this intent + this intent <=
   accepted ceiling` at planning and `settled + all active reservations <=
   accepted ceiling` after reservation; concurrent intents cannot double-spend.
   Authorization lineage supersession carries that exposure across revisions.
7. Every service line has one non-reusable source allocation and was admitted
   under a deterministic gross exposure bound including maximum tax/rounding;
   prepaid service also had compatible wallet capacity reserved at admission.
8. The customer has not canceled the intent or withdrawn the authorization.
9. Tax is a versioned exact result reproducible from a content-addressed public
   rule artifact and committed/customer-held inputs. Proprietary-only,
   provider-attested, timed-out, unconfigured, or contradictory tax evidence is
   `unknown(unsupported_verification)` and blocks execution; it never becomes zero.
10. When provider remainder is nonzero, the selected adapter reports all
   capabilities required for every plan step, including deterministic egress,
   authoritative reconciliation, and verified disablement of provider-autonomous
   subscriptions/retries/dunning/auto-capture.
11. No prior terminal or nonterminal settlement/attempt/grant exists for this
    initial execution, no unresolved collectible provider path exists, and the one
    settlement claim is available for atomic acquisition.
12. In one trusted core transaction, the engine locks the payer's authenticated proof
    stream head, applies every gap-free accepted sequence, rechecks every gate,
    locks credit and authorization-exposure reservations, and acquires the one
    settlement claim before any provider write. The executor only consumes the
    resulting purpose-typed, single-use capability.
13. Immediately before wallet settlement, customer-collectible issuance, or `active`→`dispatching`, the billing-owned consume
    transaction repeats the proof-head check and revalidates authorization,
    expiry, funding/exposure, tax/policy digests, build/key/adapter/notice/evidence
    and standing-revocation-path readiness, claim generation, and competing-attempt state. A withdrawal or
    kill switch revokes/fences `active` grants; it never races as a stale token.

Failure of any gate leaves the intent non-executable and records a public reason.
There is no emergency switch that interprets missing evidence as permission.

"Notice" has a precise, limited meaning. A provider delivery receipt proves
that the engine sent specific bytes to a configured channel. It does **not**
prove that a person read, understood, or remembered them. The receipt and UI
must use the word *delivered*, not *read*, unless a separate ceremony genuinely
establishes more.

---

## Core authorization and intent objects

### `BillingAuthorization`

A `BillingAuthorization` records authority only after the engine verifies a
`CustomerAcceptanceProof` delivered through the append-only inbox behind the
separate consent/revocation edge.
The billing engine remains a private IAM/internal-secret RPC service. The
ordinary account dispatcher cannot dispatch or apply acceptance. Only the
proof-inbox role may invoke the narrow apply procedure, and it still cannot
satisfy the customer-proof check with its own credential or statement.

It is either:

- **one-time**, bound to one exact intent digest; or
- **standing**, bounded by payer, currency, allowed charge categories, maximum
  gross obligation, maximum wallet application, maximum external provider
  remainder, per intent, maximum per billing period, customer-selected funding
  mode and credit policy, cadence, payment method or mandate where applicable,
  notice interval, enrolled billing-channel revision/destination commitment,
  terms/price/tax policy-change rules, exact customer-factor-bound tax-profile
  receipt/digest, expiry, and revocation state.

Every authorization also binds an `AuthorizationScopeKey`, current lineage head,
content-addressed bounded `MerchantBindingSet` of commercial identities,
settlement routes, and compatibility edges, permitted finite-plan effect classes/
hold bounds, required provider evidence class and credential/enclave scope, and
`ProviderAutonomyPolicy = no_autonomous_future_debit`.
Activating revision N atomically supersedes N-1; exposure is keyed by scope/window
and carries forward across method/cap revisions, so a replacement cannot reset a
ceiling or let old and new grants spend concurrently.

Tax and source state bind the exact `CommercialIdentityBinding` before wallet
allocation. Only after tax-inclusive gross and wallet funding are final does the
engine select the canonical wallet-only route or one accepted provider/merchant
route and form the final `MerchantOfRecordBinding`. The bounded compatibility
proof joins that route to the same commercial identity. Route selection cannot
change tax or gross; route-sensitive tax is unsupported and fails closed.

When external funding is allowed, `PaymentInstrumentBinding` is one of two
closed variants. A saved method binds its immutable setup receipt/digest,
provider-verified readable identity (provider/entity, brand or type, masked
suffix, expiry where applicable, mandate scope), and secret opaque reference. A
customer-present one-time instrument instead binds provider/entity, merchant,
one-time/no-reuse scope, exact intent, deterministic operation identity, allowed
origins, continuation schema/policy, and expiry. After dispatch creates the
session, the core signs the actual attempt-bound continuation under that accepted
tuple; the independent verifier checks it before card entry. An opaque id or
private-UI URL alone is not meaningful consent and cannot be substituted.

An authorization is append-only. Revocation creates a durable terminal event;
it does not edit history. Increasing a cap, widening a category, changing a
payment method, shortening notice, or accepting a materially different price or
tax policy requires a new customer ceremony.

The `api-platform` route being customer-facing is necessary, but not sufficient.
The proof must establish customer control independently of a private RPC
assertion and bind an engine-issued challenge, canonical disclosure digest,
payer, account, engine audience, expiry, and replay identity. Signing an opaque
digest is not enough when the private UI may lie about the matching terms; an
independent consent verifier must validate the engine signature and render the
canonical fields before the customer-controlled factor signs. The exact
ceremony must be documented and deployment-attested. Until it exists, standing
authorization and automatic execution remain disabled.

Edge acceptance and engine effect are different receipts. The proof inbox is a
billing-owned gap-free monotonic stream per payer; the public edge has only a
narrow append role. It returns an `EdgeAcceptanceReceipt` only after durable
sequence assignment and an authenticated stream-head update. The core keeps an
applied high-watermark. The claim transaction locks the authoritative head,
rejects any stale/missing/gapped/unverifiable stream, applies every sequence
through that head, checks revocation, then creates the claim and an `active`
provider grant. The executor's consume transaction repeats the same head/apply
check before racing revocation on the `active`→`dispatching` compare-and-swap. A
revocation accepted first and before any earlier/current adverse or customer-
collectible path atomically revokes/releases. If a hold or `client_dispatched`
path already exists, it blocks the next debit/capture but retains cleanup state;
one serialized after the point of no return receives the exact cutoff. A local `last_consumed` value without
an authoritative current head is not a cancellation control.

### `ChargeIntent`

A `ChargeIntent` is the complete, immutable proposal to collect money. It
contains or commits to:

- intent ID, payer and billing period or business action;
- canonical source leaf/window allocation lineage and signed local transition
  evidence;
- customer-facing line items and their derivation evidence;
- positive-service-line subtotal, rating/tax credits, taxable basis, tax,
- for funding intents, positive cash purchase principal, credit granted, explicit
  bonus, unit/currency, restrictions, and expiry,
  `grossObligation`, and currency in integer minor units plus the canonical
  higher-precision derivation;
- price-book and tax-policy identifiers, versions, effective times, and digests;
- authorization scope/lineage reference, applicable caps, carried exposure, and
  service-time `ServiceAccrualExposure` where applicable;
- disclosure bytes, enrolled-destination commitment, notice policy,
  `notBeforeFloor`, and `minimumLeadDuration`;
- exact `MerchantOfRecordBinding`; selected provider, merchant account,
  environment, payment instrument; exact adapter artifact/version/capability
  digest, required evidence class and credential/enclave scope; frozen autonomy
  policy and complete finite `ProviderExecutionPlan` when provider remainder is
  nonzero;
- engine source commit, artifact digest, schema version, and intent-format
  version; and
- a digest over all billing-relevant fields.

After disclosure, none of those fields can be updated. Any recalculation creates
a new intent that names and supersedes the old one. The old intent becomes
`canceled` or `expired`; it is never silently rewritten under the same URL.

Rating/tax credits reduce `grossObligation`; stored-value wallet lots do not.
They fund it under `FundingPlan`, with
`grossObligation = walletFunding + providerRemainder`. Every source is typed
`rating_credit` or `stored_value`, never both, so a malicious caller cannot
subtract the same lot from the obligation and then spend it again as funding.

### Source allocation, accrual, and schedules

Usage and recurring base windows use a core-derived `SourceObligationKey`; payer,
currency, policy revision, or aggregate id cannot create a second uniqueness
namespace for the same leaf/window. Bounded chunk claims and a small seal barrier
prove complete membership under database uniqueness/exclusion constraints.
Overlapping/repartitioned sources refuse, a settled source never reopens, and an
asynchronous transparency root is audit evidence rather than execution authority.

At service admission, the core derives and reserves `ServiceAccrualExposure` for
a deterministic gross upper bound including maximum tax and rounding under the
accepted service authority. Prepaid mode also reserves compatible settled wallet
capacity. Concurrent facts/base windows lock the same scope. Period close converts
only exact gross within the hold and atomically releases surplus; an unbounded,
over-bound, backdated-after-revocation, or prepaid-shortfall fact is quarantined
and creates no debt.

For SaaS, accepted `SubscriptionOffer` cadence/time zone/anchor/first-period and
recognition fields are immutable. Acceptance stores
`pending_first_settlement`; only the exact first settlement with the same
responsibility/schedule generation atomically opens the service window and enables
service authority. A scheduler supplies a schedule/
period id, never its own dates or amount.

A payer/app ownership change uses `BillingResponsibilityTransfer` with exact
cutoff and old/new independent authority. It seals the old service window, keeps
accrued debt and every dispatching/hold/client-dispatched/ambiguous claim fenced to
the old payer, cancels a pre-adverse pending-first-settlement plan, revokes old
active grants, and starts only a new responsibility/allocation namespace without
transferring mandates, wallet lots, tax profile, or notice destination. The
transfer grants no new service or collection authority; the new payer completes
its own setup, tax, funding, notice, and authorization ceremony.
An already-dispatched old first charge that settles is recorded but opens no
post-cutoff service and enters source-linked resolution. Backdating or silently
reassigning old sources/receivables is refused.

Transfer proposal and activation are separate closed actions. The private
proposal action supplies only app/account, old/new payer ids, and a closed policy
selection; equal payer ids or an old payer not owning the current responsibility
generation are refused. The engine derives a future cutoff, proof/activation
deadlines, the closed failure disposition (old administrative responsibility,
no backfill, and no billable resume without fresh old-payer authority or a new
transfer), one common digest with hiding commitments, and separate payer-view
digests. Old and new payer verifiers independently sign the common digest/cutoff/
failure disposition plus their own view. The new view reveals the non-transfer semantics but no old-
payer amount, provider/mandate reference, tax/payment detail, or claim state. The
proof-only edge appends each proof to its own payer stream.

Apply accepts only a transfer id. In one transaction it locks both authoritative
stream heads in canonical payer order, bounded-applies each through its current
head, verifies two distinct unrevoked factor proofs, and commits only when the
trusted uncertainty interval is wholly at/after cutoff and within the activation
deadline. It compare-and-swaps responsibility/source generations with two payer-
encrypted receipt/outbox views sharing one transaction commitment. Missing proof
at the earlier proof deadline expires the transfer before it is due. Once both
proofs are effective, admission/period seal lock the same transfer generation and
block at/after cutoff until apply. A terminal activation failure retains old
administrative responsibility but keeps billable service stopped until fresh old-
payer authority bound to the failure receipt or a new transfer; no blocked facts
are backdated. An early/late worker, backlog, mismatch, revocation, or
race has no partial effect. Private IAM/session authority cannot assert either
proof, and there is no old-only/new-only activation or cross-payer evidence view.

### Authority, provider plans, revocation, and refunds

Each setup/debit bundle carries one tagged `AuthorityEvidence` branch:
`setup_customer_present`, `debit_customer_present`, or `standing_automatic`.
Setup proof cannot authorize debit; customer-present debit does not fabricate a
notice; standing execution includes its acceptance, terminal notice/wait, and
fresh independent revocation-path readiness evidence.

Every provider effect freezes a finite plan of closed effect classes
(`non_adverse_prepare`, `mandate_setup`, `funds_hold`, `debit`, `return`, or
`release`). Each mutation has its own purpose/step envelope, consume CAS, opaque
permit, egress fence, and authoritative result. The actual debit instrument must
equal the accepted instrument. Refund evidence instead binds the original
provider source object and exact return destination. Mandate removal is its own
customer-proof-driven plan and receipt; engine cutoff is immediate while provider
detach may remain pending.

`RefundIntent` is never debit authority. It reserves source-linked refundable
capacity. A refund of purchased/top-up credit additionally freezes a
`GrantedValueClawbackReservation`; provider cash return and cancellation of the
corresponding unspent granted/bonus lots commit atomically, so returned cash
cannot coexist with spendable output value.

---

## The lifecycle

The single canonical public state machine is in
[`DESIGN.md` §4](DESIGN.md#4-intent-lifecycle). This threat model does not define
a second topology. It adds these security meanings to those states:

- `proposed` means the exact intent was created and sealed, but no disclosure
  claim is made.
- `notice_pending` means exact disclosure bytes exist but delivery has not been
  established. Delivery failure stays here until retry policy expires; it does
  not become executable or a provider action.
- `disclosed` means delivery evidence is recorded. It does not mean "read."
- `eligible` means every applicable gate currently passes. A
  `standing_automatic` intent additionally requires an appended terminal
  `NoticeReceipt` and a trusted time reading at or after its
  `eligibilityNotBefore`; customer-present exact-proof intents do not fabricate
  notice evidence.
- `executing` means the core owns the single settlement claim and may authorize
  only the current step of the frozen finite provider plan.
- `execution_unknown` is a sticky ambiguity latch after a timeout, process
  crash, malformed response, or conflicting observation. It permits reads and
  same-provider reconciliation, never a fresh charge or provider fallback.
- verified prepare/hold evidence retains the claim and can only enter a freshly
  gated next step; it never means settlement.
- `succeeded` requires either an atomic wallet-only commit or provider-signed evidence the core verifies directly or
  evidence from the attested, credential-separated authoritative read-back path
  proving the one exact frozen debit amount/currency, accepted instrument, and
  merchant binding. An executor assertion alone is insufficient.
- `action_required` exists only for customer presence on a known, frozen
  provider attempt. Missing notice, tax evidence, authorization, capability, or
  payment method is a pre-execution refusal or non-executable gate, not a state
  that can bypass disclosure. Resolving provider action cannot mutate the
  disclosed intent; a changed total creates a new one.
- `canceled` is a pre-collection customer or policy stop.
- `expired` means time, authorization, or policy validity ended before
  collection.
- `voided` requires affirmative provider evidence that a created provider
  object did not and cannot collect. A paid operation cannot be relabeled
  `voided`; refunds are separate ledger events and remain visible on the receipt.

Every transition is compare-and-swap against the prior state and append-only in
the event history. Unknown and terminal states are not reset by an operator
editing a row.

---

## The action surface and customer relay

The desired action vocabulary is closed and role-bound. Provider writes are not
ordinary private RPC actions; each is a different signed capability:

| surface / action family | effect | payment-provider write capability |
|---|---|---|
| metering / `RecordUsage` | validates a constrained fact and atomically reserves core-derived `ServiceAccrualExposure` plus prepaid wallet capacity, or quarantines/refuses | none |
| private core / `DescribeCharge`, `ProposeChargeIntent` | estimates or seals from engine-selected facts/policy plus an optional closed non-authoritative catalog/template selection | none |
| private core / propose/apply responsibility transfer | proposal derives one canonical envelope from app/account + old/new payer ids + closed policy; apply accepts transfer id only and requires both current payer streams/proofs in one CAS | none |
| consent edge / `AppendCustomerProof` | durably orders exact acceptance, cancellation, contact-enrollment, or revocation proof | none |
| proof inbox / `ApplyCustomerProofs` | verifies and applies commands through an authenticated payer-stream head | none |
| private core / `ExecutePaymentMethodSetup` | after proof/plan checks, persists only the next no-debit `AuthorizedSetupStepEnvelope` | no direct write; typed step envelope only |
| private core / `ExecuteChargeIntent` / next-step authorizer | after every current gate and prior-step proof, persists only the next plan-bounded `AuthorizedPaymentStepEnvelope` | no direct write; typed step envelope only |
| private core / `RequestMandateRevocation` | first applies engine cutoff to linked authority/grants, then persists only the next source-bound `AuthorizedMandateRevokeStepEnvelope` | no direct write; typed step envelope only |
| private core / `RequestVoid` | validates a known unsettled source and persists only the next `AuthorizedVoidStepEnvelope` | no direct write; typed step envelope only |
| private core / `RequestRefund` | reserves source/refund capacity and any funding-output clawback, then persists only the next `AuthorizedRefundStepEnvelope` | no direct write; typed step envelope only |
| grant consumer / consume purpose step | full current-step gate recheck + CAS returns matching exported opaque `*StepDispatchPermit`; zero/forged values fail journal authentication | none |
| guarded setup-step writer / `SetupStepDispatchPermit` | one exact setup-plan mutation | setup only |
| guarded payment-step writer / `PaymentStepDispatchPermit` | one exact prepare, hold, debit, or release step | payment only |
| guarded mandate-revoke-step writer / `MandateRevokeStepDispatchPermit` | one exact known-mandate revoke step | mandate revoke only |
| guarded void-step writer / `VoidStepDispatchPermit` | one known-operation void/release step | void only |
| guarded refund-step writer / `RefundStepDispatchPermit` | one bounded source-linked return/release step | refund only |
| evidence ingress / submit notice, execution, or reconciliation evidence | appends an observation for core validation | none by assertion |
| trusted core / commit ledger transition | atomically commits only its purpose-typed validated state transition, reservations/claim, receipt, and outbox | no generic external write; no api-platform/executor/callback/operator access |
| private core / cancel or revoke | stops future execution while grant is `active`; cannot erase dispatch/settlement | none |
| private read / `GetChargeReceipt`, `TracePayment`, `Capabilities`, `Health` | returns signed evidence or running identity | none |
| evidence edge / `ReadEvidence` | serves immutable records under `CustomerReadProof` | none |

`DescribeCharge` is useful but grants nothing. A mutable estimate is not an
intent, a notice, an invoice, or an authorization.

Only the proof-inbox role may deliver applied customer proof; ordinary
private IAM/internal-secret credentials cannot. The consent edge forwards the
engine challenge, signed canonical disclosure, and customer proof unchanged;
the engine verifies the proof against a key/factor outside both the edge and the
private caller's authority. Likewise, no component outside the exclusive
`ProviderCredentialEnclave` links a write-capable payment client. Its guarded
executor owns mutations; only when native read-only credentials are unavailable
may a fixed-read broker inside the same enclave use that credential, while the
external reconciler remains credential-free.

### The Go capability shape

Provider integrations use the single normative port contract in
[`DESIGN.md` §5](DESIGN.md#go-structure-composition-and-narrow-ports). They are
composed at the application root; they do not inherit from, embed, or pass around
one universal provider client whose method set mixes reads and writes.

The security-critical shape is
`Authorized*StepEnvelope → GrantConsumer → exported opaque
*StepDispatchPermit → purpose-matched step writer`. The struct name is exported so another Go package can
implement the port; fields and constructors are unexported. Because another
package can still construct a zero value, the writer obtains operation fields
only by authenticating/fencing the permit id/MAC, purpose, provider scope,
claim/step generation, and unused state in the durable consume/egress journal
before an SDK call. A signed envelope is never accepted by a provider
writer. Only the billing-owned consume CAS can produce the matching permit, and a
durable egress guard prevents a second local submission. Generated inventory and
tests require exactly one SDK mutation for that closed plan step; composite
create/confirm/capture methods are forbidden. Read/trace consumers
receive only `PaymentReader`; callback verification produces untrusted evidence
and exposes no writer. This document deliberately does not define a second set of
Go interface names.

Stripe, NewebPay, and later providers adapt their own APIs to those ports. A
provider may implement several ports, but consumers receive only the narrow
interface they need. In particular:

- the receipt/trace service receives only `PaymentReader` (or its consumer-owned
  read-method subset), never a writer;
- callback handlers parse/authenticate callback bytes into untrusted evidence and
  enqueue reconciliation without receiving any provider client;
- reconciliation receives only the `LookupAttempt` subset of `PaymentReader`; any
  retry/replacement decision returns to the core state machine rather than
  invoking a writer; and
- only the executor inside `ProviderCredentialEnclave` is constructed with the
  non-coercible write ports and mutation-capable provider credential.

Where a provider supports separate read and write credentials, construction must
use them. Otherwise a fixed-read broker inside the same attested
`ProviderCredentialEnclave` must own the broad credential and expose fixed,
operation-bound reads to an external credential-free reconciler. No separate
reader is allowed to own another broad credential. If neither is
possible, the adapter cannot advertise a read-only reconciliation path or
unattended automatic-execution readiness. Process interfaces alone do not
constrain a compromised binary holding a write-bearing credential. Compile-time
interface assertions, dispatcher tests, credential inventories, and deployment
attestation make the remaining split auditable.

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
- customer-factor-bound `TaxProfileReceipt` revision/digest, proof-stream
  sequence/head, payer binding, and required issuer validation;
- taxable line classifications;
- content-addressed public rule artifact, deterministic verifier/calculation
  revision, verification class, effective time, and availability commitment;
- per-jurisdiction calculation and rounding evidence; and
- the exact tax total included in the intent digest.

`Capabilities` reports tax as `ready`, `degraded`, or `unsupported`. Only
`ready` with `verificationClass: independently_reproducible` may produce an
executable automatic intent. A proprietary or provider-attested result is
`unknown(unsupported_verification)`, even if it supplies a number. Timeout, conflicting
location evidence, missing classification, expired exemption evidence, unknown
jurisdiction, or policy lookup failure leaves tax `unknown` and the intent
non-executable; it never becomes provider `action_required` and never falls back
to an old rule or zero.

The private caller may relay encrypted candidate profile evidence but cannot
authenticate ownership, select the effective revision, or validate a tax id. A
wrong-payer profile, an unproven address, missing issuer validation, or a profile
change after acceptance is `unverified`, yields `unknown`, and requires a new
authorization/intent where the bound digest changes.

An explicit zero-tax result names the rule and evidence that derived zero. A
payment provider's automatically added tax, fee, or invoice line is not accepted
as tax truth. If tax changes after disclosure, the engine cancels the old intent
and creates a replacement with the applicable fresh customer-present disclosure/
proof or standing notice/wait ceremony plus authorization recheck.

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
- mutation-transport retry/redirect configuration and actual outbound-request
  fence, proving one consumed permit emits at most one mutation request;
- its complete closed finite-plan step inventory and whether every SDK mutation
  is separately visible and reconcilable;
- authoritative inquiry/read-back support and expected consistency delay;
- synchronous and asynchronous result semantics;
- callback authentication, replay identifier, and ordering behavior;
- settlement evidence strength (`provider_signed`,
  `native_readonly_reconciler`, `attested_enclave_broker_readback`, or
  `executor_assertion_only`) and the credential/enclave attestation that enforces
  it; the enclave-broker class is not presented as an independent credential
  boundary;
- explicit disable/cancel/read-back controls for provider subscriptions,
  auto-advance, smart retry, dunning, and delayed capture;
- setup, mandate-revoke, hold/capture/release, refund, dispute, and chargeback behavior; and
- merchant-account and environment identity.

The core chooses only an adapter covered by the customer's authorization and
freezes that choice before disclosure. The private caller cannot request a
fallback provider.

Automatic execution is enabled only when the adapter can demonstrate equivalent
safety, not identical API names. Every provider mutation is one frozen plan step
with one freshly consumed envelope/permit and deterministic egress reference.
Verified prepare or hold evidence is appended before a fresh next-step gate;
collection is one exact debit step, and reconciliation is read-only rather than a
writer step. Provider-managed subscriptions, auto-advance, smart retry, dunning,
or delayed capture must be disabled and authoritatively readable; otherwise that
flow is not ready.

If a provider cannot offer safe idempotency, a unique merchant order identity,
or an authoritative way to resolve an ambiguous result, that adapter is not
eligible for unattended automatic execution. It may support a customer-present
flow whose limitations are disclosed separately.

### Cross-provider double settlement

The database owns one settlement claim per intent, independent of provider. The
claim freezes provider, merchant account, operation identity, and attempt number
before the call. A timeout does not release it.

The request boundary is also single-shot. Mutation SDK/HTTP retries and redirects
are disabled, and an instrumented permit-aware transport refuses a second
outbound mutation request for the same permit after timeout, reset, `429`, `5xx`,
or any other inconclusive response. Such a result latches
`submitted_unknown`/`execution_unknown` and permits only read-only same-operation
reconciliation. An idempotency key is never permission to retransmit.

`execution_unknown` can be resolved only by querying the same provider and same
operation identity. A second provider is never tried until there is affirmative,
durable evidence that the first operation did not and cannot collect. If that
proof is unavailable, the engine waits for operations review rather than risking
two charges.

A replacement intent after such proof gets new funding/exposure reservations,
digest, disclosure, and independent settlement claim. The customer-present branch
requires fresh exact customer disclosure and factor proof; the standing-automatic
branch requires its exact terminal notice and delivery-relative wait. Notice alone
never substitutes for customer-present authority. The replacement remains linked
to the first so a receipt can prove why a second operation was allowed.

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
incident and `execution_unknown`, not whichever number is larger.

### Read-only cash-flow tracing

The independent evidence edge is the canonical customer trace path and serves
engine-signed, customer-encrypted bundles from the billing-owned outbox. It does
not depend on `api-platform`. An optional live provider refresh remains a private
engine read action; `api-platform` may relay the customer's exact
`CustomerReadProof`, but its own identity assertion is not authority. The
canonical sequence and response contract are in
[`LEDGER-AND-RECEIPTS.md` §6](LEDGER-AND-RECEIPTS.md#6-cash-flow-trace-api).

The default trace is served from append-only local observations. An explicit,
authorized refresh may use `PaymentReader`; it is rate-limited and follows exact
stored references. The path is called read-only only when the provider enforces
a read-only credential or a fixed-read broker inside the one attested
`ProviderCredentialEnclave` isolates the broader merchant credential from the
external reconciler. A Go interface alone is not sufficient.

`CustomerReadProof` binds the independently enrolled customer factor to the
payer/account, exact object or bounded collection scope, read/evidence audience,
nonce, expiry, replay identity, and encryption-key version. The edge and engine
verify the billing-owned factor mapping. Within each published scope class,
authorized, absent, and unauthorized requests use one status/content type/error
shape, padded-size bucket, minimum timing bucket with bounded jitter, and rate
limit. This bounds the documented response oracle; it does not claim perfect
network or microarchitectural indistinguishability. A guessed id or private-relay
assertion still cannot authorize a cross-tenant read. Live refresh validates
ownership and stored references before any provider read and returns the same
accepted/no-op shape for absent or unauthorized ids.

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

A customer cancels or revokes through the independently reachable proof-only
consent edge, not solely through `api-platform`. The customer-held verifier
signs the exact intent or authorization id and replay-bounded command; the edge
appends it to the payer's proof stream and cannot mint, widen, skip, or renumber
it. Edge acceptance is returned only after durable sequence/head assignment.
Claim acquisition and capability consumption both lock that authoritative head
and apply every gap-free sequence. Cancellation races the provider-dispatch
grant: if `active` and no earlier adverse/customer-collectible step exists, the
same transaction marks it `revoked`, cancels the pre-dispatch attempt/intent, and
releases claim/reservations. If a hold or `client_dispatched` continuation exists,
it blocks any next debit/capture but retains claim/exposure through the frozen
release/cancel/read-back cleanup. If already `dispatching`, it returns the exact
cutoff and retains the claim. Wall-clock
ordering between services is not authority. A censored product UI cannot keep an
authorization alive or fabricate a successful cancellation.

Once dispatch begins, the engine reports `executing`, `provider_pending`, or
`execution_unknown` and does not claim cancellation succeeded. If the provider
supports cancellation or void, the core may issue the separate purpose-typed
capability and records the outcome; it does not erase the attempt.

Revoking a standing `BillingAuthorization` prevents every future intent and any
existing intent whose provider-dispatch capability remains `active`.
Provider-side revocation of the payment mandate remains an independent stop
controlled through the payment provider. The engine must document how quickly
each stop becomes effective.

### Evidence availability and publication

The same billing-owned transaction that seals or changes canonical state appends
a signed, customer-encrypted outbox record. This applies to intent seal,
acceptance/revocation, notice eligibility, refusal, every nonterminal attempt
state, settlement, correction, and proof-stream cutoff. Publication workers may
retry delivery, but cannot synthesize a state or omit an earlier sequence without
an externally detectable gap.

The independent evidence edge serves those immutable records under
`CustomerReadProof`; `api-platform` is only an optional unchanged relay. The
edge's origin, read role, encryption-key rotation, append-only checkpoint, and
availability monitoring are separate deployment evidence. An outage may delay a
customer read and must block any policy that requires confirmed evidence
availability; it never authorizes execution or permits the private relay to mint
a replacement receipt.

---

## Assumptions, and what breaks when they are wrong

### We assume the deployed artifact is the public artifact

Public source proves nothing about a binary customers cannot identify. `Health`,
`Capabilities`, and every receipt therefore identify the source commit,
reproducible artifact digest, schema revision, and policy digests. Deployment
attestation is signed under a purpose-bound leaf chaining to a verification root
that customers pin outside every runtime relay. Rotation/revocation is
append-only and transparency-anchored; an unknown or substituted root disables
automatic execution.

If the deployment can lie about its artifact identity, customers cannot verify
this code is the code holding payment credentials. That signing and deployment
root is an explicit assumption, not something unit tests close.

### We assume billing-owned storage and keys are isolated

The private caller must not have direct write access to intent,
authorization, notice, execution, or receipt tables, and must not hold their
sealing/signing keys. If it can edit those rows or mint their envelopes, the
state machine is merely advisory.

Database operators remain powerful. `BillingDecisionProof` signs the supplied
closed predicate/key schema, proof head, before/after commitments/generations,
transaction/build/policy identities, and outbox binding, but global non-omission
is still `state_assurance: attested`. Append-only constraints, audit export, and
backups are primary controls. The asynchronous payer-isolated transparency log
can detect a rollback, equivocation, or split view only after a root is published;
it neither gates payment nor proves a hidden competing pre-publication history.

### We assume the customer authentication ceremony means what it says

An accepted authorization is only as strong as its independently verifiable
presentation and customer proof. A bearer credential the private caller can mint
or read does not defend against that caller, and a signature over an opaque
digest does not prove which terms the customer saw. The final implementation
must state the engine-signed disclosure format, independent rendering/verifier,
customer-controlled factor, challenge audience, expiry, replay rule,
contact-enrollment process, and contact-change cooling period.

Compromise of the customer's established factor or billing destination is not
prevented here. Notice delivery also does not prove human reading.

### We assume the notice carrier evidence path is truthful

The core can verify a carrier signature where available. Otherwise the attested,
credential-separated notice reader and carrier are trusted to report the exact
content digest, destination commitment, message id, terminal status, and
delivered time.
A malicious notifier assertion alone is never accepted. If the carrier or
attested read path fabricates those facts, the engine cannot independently prove
delivery; transparency checkpoints and customer complaints are detection, not
prevention. Such a path must be named in `Capabilities`, and unknown evidence
strength disables automatic execution.

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

Notice windows, proof/authorization/capability expiries, price/policy windows,
service admission/seal, responsibility cutoffs, and consume transitions depend on
time. Every money-authoritative transition uses a billing-owned monotonic time
source disciplined by an authenticated wall-
clock source. `Capabilities` publishes the time-source identity, maximum accepted
uncertainty/skew, maximum forward step, rollback policy, and current readiness;
`NoticeReceipt` and `BillingDecisionProof` bind that policy revision and the
observed uncertainty interval. A forward jump, rollback, source disagreement,
stale synchronization, or uncertainty interval overlapping the disallowed side of
any cutoff makes time readiness false. Recovery may move a transition later but
can never credit elapsed notice time, pre-expiry authority, or a prior service
window that was not proven. Admission, seal, setup, customer-hosted issuance,
wallet settlement, and provider dispatch use the same check. When ordering is
uncertain, the transition fails closed with no new debt/capability/effect.

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
5. recompute `eligibilityNotBefore` from terminal provider-delivered time,
   the sealed floor, and minimum lead duration, then check execution followed it;
6. check that the one-time or standing authorization covered the exact total,
   categories, cadence, period exposure, provider, exact saved-method or
   customer-present instrument binding, and execution time;
7. verify cancellation, expiry, settlement-claim ordering, and each subordinate
   dispatch lease/fence;
8. verify one semantic attempt/provider, the exact frozen ordered step/egress
   identities and evidence chain, and at most one exact debit or return;
9. verify `BillingDecisionProof` over the supplied serialized state transition
   while displaying `state_assurance: attested` and any asynchronous
   transparency status without claiming global non-omission;
10. compare provider-authoritative settlement observations with the frozen
   amount and currency; and
11. account for later refunds, disputes, reversals, credits, and adjustments
    without rewriting the original charge.

Low-entropy sensitive values use payer/object/field-bound hiding commitments with
unique random nonces and owner-only encrypted openings, never raw deterministic
hashes. Canonical digests remain appropriate only for non-sensitive or already
high-entropy artifacts. Redaction must not remove the fields needed to verify
money.

The canonical bundle, append-only ledger, provider trace, and verifier contract
are detailed in [`LEDGER-AND-RECEIPTS.md`](LEDGER-AND-RECEIPTS.md) and
[`VERIFICATION.md`](VERIFICATION.md).

The repository should back those claims with four layers:

1. example and integration tests for each lifecycle and provider adapter;
2. property and fuzz tests over money, authorization bounds, state transitions,
   callback ordering, and envelope substitution;
3. concurrency and crash tests proving one settlement claim across workers and
   providers;
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
