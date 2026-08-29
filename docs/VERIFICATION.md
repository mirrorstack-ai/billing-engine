# How customers can verify the billing engine

Public source is useful only when a customer can connect three things:

1. the rules in this repository,
2. the exact artifact that proposed and executed a charge, and
3. the evidence used for that particular charge.

> **Status: target verification contract.** The current engine does not yet ship
> the verifier, canonical charge bundle, public build identity, policy digests,
> transparency record, or intent-only capability gate described here. Commands
> labelled **planned** are acceptance targets, not claims about current `main`.

---

## 1. Evidence levels

| level | question answered | required evidence |
|---|---|---|
| source | what is this revision designed to permit? | public code, docs, schemas, tests |
| build | what code produced the artifact? | Git SHA, reproducible/signed build provenance, artifact digest |
| deployment | what artifact and policies are running? | customer-visible, engine-signed `Health`/`Capabilities` evidence and deployment statement |
| intent | how was this exact total derived and authorized? | canonical charge bundle + digest |
| runtime state | what serialized state transition did the trusted billing core attest? | signed `BillingDecisionProof`; global non-omission remains `state_assurance: attested` |
| provider | what did an external rail report happened? | normalized, verified provider evidence |
| ledger | what monetary transition did MirrorStack commit? | balanced append-only transaction + correction chain |

No lower level substitutes for a higher one. A public repository does not prove
which binary charged a card. A provider's `paid` status does not prove why the
amount was allowed.

---

## 2. Customer-visible runtime identity

`billing-engine` remains a private runtime service. Its `Health` and
`Capabilities` actions produce engine-signed evidence that `api-platform` can
relay publicly without being able to alter it.

`Health` returns on healthy and unhealthy responses:

- exact Git commit or the literal `unknown`,
- artifact/container digest,
- build provenance identity,
- binary role (`planner`, `notifier`, `executor`, `reconciler`, and so on),
- receipt/canonical-schema version, and
- deployment environment identifier that contains no secret.

An executor with unknown build identity cannot execute.

`Capabilities` returns:

- active terms, price-book, tax, notice, and routing-policy digests;
- supported currency/scale registry revision;
- each payment adapter's version and declared capabilities;
- versioned hard limits for intent lines/bytes, source-proof/bundle bytes, wallet
  lots and rollup depth/arity/constituent-proof work, provider-plan
  steps/branches/bytes/cleanup depth, and proof-apply batches;
- canonical `MerchantBindingSet` digest and limits for canonical bytes, members,
  membership/compatibility-proof bytes, proof depth, and proof hash operations;
- price-book/module-manifest interpreter identity and artifact/metric/tier/
  expression-depth/input/output/fuel/memory limits, with indexed/chunked support
  for many installed modules rather than a tiny global module cap;
- notifier, customer-facing relay, independent consent/proof verifier, executor,
  evidence edge, and receipt-verifier readiness;
- trusted-display profile/release/conformance identity per platform; native
  profiles are `unsupported` unless their versioned OS-specific suite passes;
- proof-inbox head/sequence and transactional-outbox readiness;
- provider evidence strength and whether reconciliation reads are enforced by a
  provider-native read-only credential or a credential-free reconciler calling a
  fixed-read broker inside that credential's exclusive attested enclave owner;
- callback-auth credential class/scope, bounded raw request/header/time policy,
  verifier artifact/owner, replay policy, and public-ingress versus exclusive-
  enclave verification location;
- mutation-transport retry/redirect configuration, permit-aware outbound-request
  fence identity, and last conformance revision for each adapter;
- public tax-rule artifact/verifier availability and verification class, plus
  identical artifact/input/output-byte, AST depth/node, fuel, and memory limits;
- minimum notice policy in force and `TimeReadinessPolicy` identity, source,
  maximum uncertainty/skew/forward step, rollback behavior, and current readiness;
- asynchronous billing-state transparency status/checkpoint and pinned witness-
  policy revision (audit-only, never a synchronous charge authority); and
- every reachable legacy money-moving path, with a required count.

The production intent-only claim requires `legacyMoneyPaths: 0`. A stronger new
surface beside one weaker legacy route is not a stronger deployment.

Build identity and non-sensitive capability fields must be available to
customers/developers through the public control plane as signed engine evidence,
not only through a private support request and not by exposing the account RPC.

### Verification trust root and signing profiles

The planned verifier pins a billing verification root independently of any
runtime response. Its fingerprint ships in this public repository, signed
verifier releases, and at least one separately operated transparency channel.
An `api-platform`, consent-edge, or evidence-edge response cannot introduce a
new root.

Every signed disclosure, capability statement, acceptance/revocation receipt,
and charge bundle includes a fixed algorithm identifier, key id, issuer,
audience, environment, schema, purpose-specific signature domain, payload
digest, validity interval, and transparency checkpoint. A key valid for
`billing-capabilities/v1` cannot sign `customer-acceptance/v1` or
`charge-receipt/v1`.

Leaf rotation is cross-signed, overlap-bounded, and committed before use.
Revocation and emergency rotation are root-signed, append-only events with an
effective cutoff. The verifier rejects an unknown algorithm, substituted root,
revoked key after its cutoff, missing chain, stale checkpoint, or purpose/audience
mismatch. `Capabilities` publishes active and recently revoked key ids and
rotation readiness; receipts carry the chain/checkpoint needed for offline
verification. Automatic execution remains disabled when key identity,
transparency anchoring, or customer trust-root distribution is `unknown`.

### Customer proof and evidence-edge contracts

The consent edge accepts only an engine-issued canonical envelope plus a
customer-factor signature. The payer stream is gap-free and monotonic; the edge
returns an `EdgeAcceptanceReceipt` only after durable sequence assignment and an
authenticated head update. The engine-effective receipt names the applied head
and is distinct. The settlement-claim transaction must prove it locked the
authoritative current head, consumed every sequence through it, and performed
revocation/gate checks before the claim compare-and-swap. A local high-watermark
without an authenticated current head is invalid.

`CustomerReadProof` binds the independently enrolled factor to payer/account,
exact object or bounded collection scope, evidence/read audience, nonce, expiry,
replay identity, and encryption-key version. The edge verifies the billing-owned
factor mapping, never an `api-platform` identity assertion, and returns one
published status/error shape, padded-size class, timing bucket/jitter policy, and
rate limit for authorized, absent, and unauthorized requests. Evidence records
are signed, customer-encrypted, monotonically checkpointed outputs of the
billing-owned transactional outbox; the edge cannot create or edit them.

---

## 3. Canonical charge bundle

The bundle described in
[`LEDGER-AND-RECEIPTS.md`](LEDGER-AND-RECEIPTS.md) has one versioned canonical
encoding. Canonicalization fixes:

- field names and order,
- integer and decimal representation,
- Unicode normalization/validation,
- timestamps and time zones,
- absent versus explicit zero/null,
- ordering of lines, sources, evidence, and ledger entries, and
- digest and signature domains.

Invalid Unicode, duplicate map keys, unknown critical fields, out-of-range
integers, unsupported schema versions, and non-canonical encodings are refused.
The digest binds bytes, not a lossy parser's interpretation.

Customer exports replace sensitive source fields with domain/payer/object/field-
bound hiding commitments using unique random nonces; raw deterministic hashes of
addresses, tax ids, or other low-entropy PII are forbidden. Only the owning payer
can obtain the encrypted opening. Nonces are never reused; key rotation preserves
historical openings, and corrected evidence creates a new append-only commitment.
Redaction is explicit and digest-covered.

The bundle includes the exact source/tax `CommercialIdentityBinding` and final
composite `MerchantOfRecordBinding` with bounded set/compatibility proof;
applicable tagged source authority; signed local source-allocation transition evidence and optional
asynchronous state-log inclusion; `SubscriptionOffer` and activation-gated
`SubscriptionScheduleReceipt` when applicable; and
`ServiceAccrualExposure` arithmetic for service; frozen `FundingPlan`; every
credit-lot and authorization-scope exposure reservation/allocation; gross
obligation, wallet application, sealed provider remainder, cap/window arithmetic,
funding/cap result; and the exact tagged `AuthorityEvidence` branch. The verifier
rejects a missing or swapped customer-present/standing branch. A standing branch
must carry its acceptance proof, terminal notice/wait, and full
`RevocationPathReadinessReceipt`; a customer-present branch carries its exact
intent acceptance/current one-time-or-standing authority and no fabricated
notice. Setup instead requires `setup_customer_present`, which is rejected in a
debit bundle.

An auto-top-up bundle additionally binds its trigger reservation/epoch, creation
balance/other-pending-funding snapshot, owner, consume-time recheck result/time,
and atomic stored-value/bonus grant plus trigger/pending close. `PaymentAttempt`
and provider evidence are required only when provider execution exists.

Every bundle also carries signed `BillingDecisionProof`: the versioned closed
key/predicate schema, authenticated payer proof head, before/after row commitments
and generations, transaction/build/policy identities, and matching transactional-
outbox record. An optional later evidence update carries asynchronous state-log
inclusion/consistency. The verifier can replay the supplied transition but cannot
prove a compromised live database omitted a competing row; the report therefore
sets `state_assurance: attested`.

For a provider-funded attempt, the bundle also binds exact autonomy/execution
plans and every step envelope/consume/opaque-permit/egress identity, exact
enclave/executor artifact/workload attestation, scoped provider-credential
identity, effective transparency checkpoint, adapter artifact/version, actual
instrument/source binding, and explicit provider-evidence strength/TCB class. If terminal
notice or settlement evidence uses an attested read-back path, the bundle binds
that reader artifact/workload/credential identity and checkpoint too. Current
runtime `Health` cannot replace this historical binding. Unknown, revoked,
expired, wrong-role, or substituted artifacts fail verification.

A `PaymentMethodSetupReceipt` applies the same historical rule even though setup
has no charge intent/provider remainder. It binds the setup disclosure/digest and
exact `ProviderMerchantSetupBinding`; engine-effective acceptance receipt plus underlying proof
commitment; payer sequence/head/cutoff, factor/verifier revision, and dispatch
plus terminal-completion proof-head/revocation state; exact no-debit execution
plan; every step consume/permit/egress
identity; enclave/executor/adapter/credential attestation; and either directly
core-verifiable provider-signed setup evidence or the exact trusted session/
mandate-reader evidence-class attestations/checkpoints when read-back was used.
It also binds `BillingDecisionProof`, provider evidence, and readable method
identity/scope. An unverifiable setup receipt cannot support standing authority.

`RefundReceipt` and `MandateRevocationReceipt` use distinct schemas/signature
domains. Refund verification binds source charge/ledger/provider objects, source-
capacity reservation, `RefundPlan`, exact return destination, every return/release
step, and balanced ledger/outbox. For `credit_purchase`/`auto_topup`, it also
requires `GrantedValueClawbackReservation` and atomic cancellation of matching
unspent principal/bonus lots. Mandate-revocation verification binds the customer
proof/cutoff, setup receipt/method identity, immediate engine cutoff, finite revoke
steps, provider evidence/status, and separate engine/provider timestamps. Neither
schema can satisfy debit authority.

---

## 4. Offline verifier

The planned public command is:

```bash
billing-verify verify charge-bundle.json
```

It checks, without contacting MirrorStack or a payment provider:

1. canonical encoding and bundle digest;
2. build/source and policy references;
3. signed local source-allocation transition evidence, canonical leaf/window
   subject binding, optional asynchronous transparency inclusion, plus service-
   admission gross-bound-to-exact-line conversion where applicable;
4. module billing-manifest/version binding;
5. price selection and effective windows;
6. exact integer arithmetic, tiers, credits, tax, and rounding, including
   reproduction from the content-addressed public tax-rule artifact;
7. line/subtotal/tax/total equality;
8. merchant-of-record equality, authorization scope/lineage, carried exposure,
   currency, cadence, ceilings, method, autonomy policy, and time window;
9. exact authority tag: customer-present acceptance/proof/current authority, or
   standing acceptance/proof plus notice equality/wait and revocation-path
   readiness freshness/checkpoint;
10. funding identity (`gross = wallet allocation + provider remainder`), bounded
    lot selection/rollup equivalence, credit compatibility/uniqueness, and caps;
11. subscription offer/schedule fields and atomic first-settlement activation, or
    auto-top-up trigger/recheck/grant/close when applicable;
12. complete finite provider plan with closed effect classes/cardinalities, one
    envelope/consume/permit/egress/evidence chain per step, exact actual debit
    instrument or refund source/destination, and at most one debit/return;
13. `BillingDecisionProof` closed predicate/key schema, proof head, before/after
    commitments/generations, transaction/build/policy and outbox binding; and
14. per-currency ledger balance, correction/refund/mandate-revocation structure,
    and every published hard line/byte/lot/plan bound.

The result is structured:

```text
verdict: verified | invalid | unsupported
state_assurance: attested
state_transparency: verified | pending | unsupported | invalid
historical_provider_evidence: not_applicable | verified | invalid | unsupported
live_provider_status: not_requested | verified | pending | unsupported | invalid
```

`verdict: verified` means the supplied canonical contract, arithmetic, signatures,
and transition chain passed; it never upgrades `state_assurance` to proof of
global database non-omission. Unknown required schema/policy is `unsupported`.
Missing async publication is `pending`/`unsupported` and does not change the
offline arithmetic verdict. A conflicting published history sets both
`state_transparency: invalid` and the aggregate `verdict: invalid`; a report may
not describe a known conflicting history as verified.

A terminal provider-funded `ChargeReceipt` requires its historical debit evidence.
If that evidence is missing/invalid, aggregate `verdict` is `invalid`; an unknown
required evidence schema is `unsupported`. `pending`/`unsupported` live status is
reserved for the optional later provider refresh/trace and does not weaken already
verified historical settlement evidence or change offline arithmetic.

Optional online mode may refresh provider evidence through customer-authorized
read-only APIs. Provider reachability cannot affect offline arithmetic verdicts.

---

## 5. Test layers

### Example and golden tests

Golden vectors cross package and repository boundaries. At minimum they pin:

- canonical intent and receipt bytes/digests;
- each charge kind and tax status;
- each currency settlement exponent and boundary rounding;
- standing and one-time authorization evaluation;
- `AuthorizationScopeKey` lineage supersession/carry-forward vectors;
- `SubscriptionOffer`/pending/active schedule receipts, auto-top-up trigger, refund
  clawback, mandate-revocation, and `BillingDecisionProof` schemas;
- every closed provider effect class and max/max+1 intent/lot/plan/bundle limit;
- `MerchantBindingSet` member/proof max/max+1 limits and wallet balances just
  below, equal to, and above tax-inclusive gross, proving deterministic
  `core_wallet` versus provider-route selection and typed refusal for any route-
  sensitive tax rule;
- exact notice bytes, carrier evidence, destination/message binding, and wait
  calculation;
- ledger entries and correction chains; and
- normalized Stripe and NewebPay adapter fixtures.

Changing a golden digest requires a schema/policy version change and an explicit
migration decision. Regenerating constants to make tests green is not a fix.

### Property tests and fuzzing

Properties include:

- accepted usage requests contain no monetary authority;
- rating is deterministic and independent of input order;
- price/module artifacts are canonical declarative non-I/O inputs; max+1 artifact,
  metric, tier, expression-depth, input/output, fuel, or memory limits refuse or
  quarantine identically in core/verifier, while indexed bounded-chunk processing
  remains independent of total installed-module count;
- replaying an event never increases the result;
- partitioning/combining facts preserves documented aggregation semantics;
- no integer overflow, sign inversion, or second rounding point is accepted;
- missing price/tax/applicable authority evidence always yields no executable
  intent; missing notice blocks `standing_automatic` but is not fabricated for a
  customer-present exact-proof intent;
- a notifier assertion without core-verifiable carrier proof or attested
  credential-separated read-back never creates `NoticeReceipt`;
- carrier evidence with queued/accepted/submitted/bounced/nonterminal status, or
  mismatched content, destination revision, message id, delivered time, audience,
  or replay identity never advances eligibility; a later invalidating status
  before wallet commit, server dispatch, or `client_dispatched` issuance clears
  readiness and requires re-notice; after any point of no return it retains claim/
  exposure until authoritative provider resolution and cannot authorize replacement;
- clock rollback, excessive forward step/skew, stale/disagreeing time sources, or
  an uncertainty interval crossing any cutoff makes time readiness false for proof/
  authority/capability expiry, policy windows, admission/seal, responsibility
  transfer, setup/client issuance, provider-funded and wallet-only settlement;
- a sealed intent cannot change without changing its digest;
- substituting another saved method—even for the same payer—changes the setup
  receipt/digest or rendered method identity and invalidates authorization;
- a stable provider token whose method identity or mandate scope changes fails
  consume-time validation unless an accepted narrowly bounded updater policy and
  signed update receipt cover that exact non-material change;
- setup and one-time-payment continuations require a core signature binding the
  previously accepted instrument tuple, provider/entity/merchant, allowed origin,
  setup or intent/attempt/operation, audience, expiry, nonce, and one-use/no-reuse
  scope; the customer-held verifier rejects any mismatch before card entry;
- browser conformance rejects framing/clickjacking, overlay or occlusion,
  autosubmit/synthetic or forwarded approval gestures, opener-controlled
  navigation/messages, wrong origin, missing `noopener`/COOP isolation, stale or
  substituted verifier release, and acceptance-receipt origin/release mismatch;
- no native verifier may report ready from generic “OS isolation”; a platform is
  unsupported until its public profile tests signed app/deep-link association,
  pinned release/root, substitution, overlay/occlusion/accessibility automation,
  application-bound factor gesture, and update/revocation;
- wrong-payer, private-caller-asserted, unvalidated-issuer, or later-revision tax
  profile evidence cannot satisfy the bound `TaxProfileReceipt`;
- tax rules are canonical declarative non-I/O artifacts; max+1 bytes/nodes/depth,
  unknown operators, recursion/iteration escape, attempted I/O, or fuel/memory
  exhaustion deterministically returns `unknown` in both core and verifier;
- substituting any seller, tax-registration set/market, currency, or commercial
  revision breaks exact `CommercialIdentityBinding` equality from source and tax
  through settlement; substituting provider, merchant, rail, environment, route
  revision, or compatibility proof breaks the final composite binding/funding
  equality;
- `MerchantBindingSet` parsing and membership are bounded identically in core and
  verifier; duplicate/ambiguous/max+1 members, bytes, proof depth, proof bytes, or
  hash operations refuse rather than truncate or scan without bound;
- final tax is derived before settlement-route selection; a zero provider
  remainder selects canonical `core_wallet`, a nonzero remainder selects exactly
  one accepted provider route/instrument, and route selection cannot alter tax or
  gross obligation; settlement-sensitive tax is typed unsupported;
- `gross = wallet allocation + sealed provider remainder` and every applied lot
  has matching currency/kind/ownership, unique reservation, and sufficient
  settled availability;
- deferred prepaid admission uses only a lot whose accepted terms preserve the
  exact reserved slice through terminal close/consume/release; nominal expiry
  blocks new allocation but cannot retire that slice, while a non-preserving lot
  is eligible only for immediate same-transaction settlement;
- rating/tax credits reduce `grossObligation`, stored-value lots fund it, and no
  source id/lot can be typed or consumed in both domains;
- service intents use the service equation, while credit purchase/auto-top-up use
  `cashPurchasePrincipal + tax + rounding`; positive principal, credit granted,
  bonus, unit/currency, restrictions, and expiry are digest-bound, and bonus never
  reduces principal;
- collection intents use only the uniquely reserved remaining collectible amount
  from the bound source intent/receipt/ledger after prior collections, credits,
  and write-offs; they do not re-rate, re-tax, post a second obligation, or accept
  an arbitrary collection principal;
- period close deterministically partitions sources by payer, exact commercial
  identity, tax profile, currency, service/collection authority, funding mode/
  policy, accepted route policy/instrument class, and window; after tax/wallet
  allocation each intent selects one compatible exact route, and one invalid
  group cannot alter or block another group's result;
- a shortfall or cap refusal never queues, wallet-only settlement never creates a
  `PaymentAttempt`, and no funding mode falls back to a card;
- a provider-funded success consumes reserved credits and commits the provider
  settlement atomically;
- for every authorization/cap/window, planning proves `settled exposure + active
  reservations excluding candidate + candidate <= accepted ceiling`, while
  execution proves `settled + all active reservations <= ceiling`; this includes
  two distinct intents racing the last available amount or frequency slot;
- settled amount/count exposure is gross and monotonic by default: verified
  pre-debit release frees only its reservation, while refunds/chargebacks/dispute
  credits/reversals/write-offs do not reopen spend or frequency capacity without a
  separately accepted source-bound `CapRecreditPolicy`/new authority;
- authorization-exposure reservations persist through pending/unknown and are
  consumed or released only with the matching atomic terminal transition;
- activating authorization lineage N atomically supersedes N-1, revokes old
  active grants, retains old dispatching/ambiguous work, carries the same scope-
  window exposure across revisions, and cannot reset capacity or allow old/new
  concurrent spend; a lower already-exceeded cap creates no new capacity;
- `BillingResponsibilityTransfer` requires exact old/new authority and cutoff,
  seals and partitions old/new service/source state, keeps accrued debt and
  ambiguous old claims with the old payer, revokes old active pre-adverse grants,
  and creates no new service authority until the new payer completes its own
  mandate/wallet/tax/notice/authorization ceremony;
- transfer proposal derives one canonical envelope/digest/cutoff from only the
  account, old/new payer ids, and closed policy; apply accepts only a transfer id,
  locks both payer heads in canonical order, bounded-applies both through their
  authenticated current heads, requires two distinct unrevoked factor proofs over
  that same digest/cutoff and audience-specific views, and commits either the
  whole generation CAS or nothing; equal payer ids/current-owner mismatch refuse,
  new-payer evidence reveals no old private financial detail, the cutoff barrier
  prevents cross-cutoff admission, and both views/proofs bind that late failure
  keeps service stopped pending fresh old-payer authority or a new transfer and
  never backdates blocked facts;
- canonical source leaves/windows cannot be reused by changing payer, currency,
  policy, aggregate, partition, or replacement id; overlap/repartition, incomplete
  seal membership, post-settlement reuse, and competing chunk/seal workers refuse;
- every accrued service line binds service/accrual authority effective at its
  service time, later revocation stops future accrual without erasing prior debt,
  and wallet/provider settlement still requires current collection authority;
- service admission derives a deterministic gross bound including maximum tax/
  rounding, reserves it under concurrent usage/base windows, and in prepaid mode
  also reserves wallet capacity; close proves exact gross within the hold and
  atomically releases surplus, while over-bound/shortfall/unsafe tier or late-event
  ordering is quarantined with no debt;
- every obligation-creating usage/base-window admission locks and applies the
  authenticated current payer proof head under the bounded apply budget before
  checking authority/window and reserving exposure; a stale/gapped/backlogged head
  requeues or refuses with no fact, debt, exposure, or wallet hold;
- producer-supplied occurrence time cannot backdate authority: only billing
  admission time or an independently verified bounded-lateness source clock/high-
  watermark may apply, and post-revocation/post-close facts are quarantined;
- subscription offer acceptance leaves service authority inactive; only exact
  first settlement with the same responsibility/schedule generation atomically
  activates the schedule/anchor/window, while usage racing pending/refusal/
  revocation/crash remains nonbillable and re-anchor needs a replacement proof;
  initial claim and every adverse/customer-collectible consume lock that same
  current generation against transfer, so transfer-first refuses before cash and
  dispatch-first retains an old-payer claim without post-cutoff service;
  an old-payer first charge settling after transfer remains old-payer cash evidence
  but activates no post-cutoff service and enters its source-linked refund/credit/
  manual-resolution policy;
- a linked `collect_receivable` intent reuses the immutable source lines/tax,
  reserves only remaining collection capacity, posts no second receivable, and
  retains that reservation through pending/unknown;
- a payer proof stream is gap-free and claim acquisition applies the
  authenticated current head before deciding revocation;
- invalid/oversized/bad-signature/duplicate-replay envelopes receive no sequence,
  while bounded incremental application never rescans from sequence 1 and claim/
  consume fails closed whenever `appliedHead != currentHead` after the strict
  batch/transaction budget;
- first-factor enrollment rejects api-platform bearer/session/IAM assertions and
  requires an engine challenge, new-factor possession, and independently
  verifiable account/organization authority; rotation/recovery enforces threshold,
  cooling, notification, cancellation, issuer revocation, and old-key fencing;
- standing automatic wallet/provider settlement requires a fresh, origin/root/
  artifact/head/checkpoint-bound revocation-path readiness receipt; stale,
  inconsistent, outage, or incident status fails closed;
- concurrent auto-top-up observers converge on one trigger reservation; the
  candidate excludes itself from pending funding, consume rechecks recovered
  balance/other funding, and settlement atomically grants exact principal/bonus
  lots while closing trigger/pending state;
- a funding-intent refund freezes `GrantedValueClawbackReservation`; partial-spent
  output limits the refundable amount, and cash return cannot commit unless the
  corresponding unspent principal/bonus lots become unspendable atomically;
- setup proof is never debit authority; revocation between setup dispatch,
  `client_dispatched` continuation, and terminal completion yields no usable
  receipt and retains mandate cleanup until authoritative absence/cancel/expiry;
- provider plans respect hard step/branch/byte/cleanup limits, closed effect
  classes and cardinalities (one mandate output, at most one debit/return, bounded
  aggregate holds); prepare/hold never settles, every next step proves the prior
  result, and cleanup remains possible after debit authority/tax/price/notice gates
  are revoked because it can only reduce exposure;
- terminal debit evidence names the exact accepted instrument, while terminal
  refund evidence names the exact original provider object and return destination;
- customer-hosted `mandate_setup`/hold/debit issuance has a proof-head/state CAS,
  one exact effect/amount/scope/expiry, `client_dispatched` claim retention, and
  authoritative cancel/read-back; a session that can hide hold+capture is refused;
- wallet rollup preserves virtual lot order, constituent remaining/source lineage,
  unique use, and refund/clawback results; its depth, arity, constituent count, and
  proof work are independently capped, max+1/nested overflow queues bounded
  compaction or refuses, and max+1 lines/source bytes/plan bytes never truncate;
  one canonical active index generation and range CAS prevent original+rollup or
  overlapping-rollup double reservation/use;
- `BillingDecisionProof` verifies the supplied predicate, proof head, row
  commitments/generations, transaction and outbox chain, while the report always
  leaves global non-omission `attested`; async publication detects only published
  rollback/equivocation and never gates collection;
- each canonical state transition produces exactly one ordered outbox evidence
  record and no uncommitted state can be published;
- a provider callback cannot originate or enlarge an attempt;
- an intent cannot settle twice across providers;
- all ledger transactions balance in one currency; and
- no external route/role/DTO can invoke the ledger writer; only the trusted core's
  purpose-typed validated transaction can commit entries/receipt/outbox; and
- evidence read and live refresh meet fixed status/size/timing/error policy,
  unauthorized or absent ids schedule no provider read, and residual timing
  leakage stays within the published conformance bound; and
- read-only components cannot reach provider-write capabilities.

Fuzz targets must cover canonical parsing/digesting, rating, policy selection,
tax-result decoding, authorization evaluation, receipt verification, provider
event normalization, and ledger transitions.

### Crash and concurrency tests

Fault injection covers every boundary before and after:

- intent seal,
- notice provider acceptance and receipt commit,
- notice invalidation against wallet commit, server dispatch, and
  `client_dispatched` issuance in both orderings,
- proof-edge append, stream-head update, engine application, and claim CAS,
- source chunk claim, seal barrier, service-exposure/prepaid-wallet reservation,
  exact close conversion, and surplus release,
- obligation-creating admission proof-head catch-up, revocation/window recheck,
  service-exposure/prepaid hold, and fact/base-window commit,
- authorization-lineage supersession and carried-exposure update,
- auto-top-up trigger acquire/recheck and atomic grant + trigger/pending close,
- subscription first-settlement ledger commit and schedule/service activation,
- subscription responsibility/schedule-generation activation CAS and post-cutoff
  old-payer settlement resolution,
- settlement claim,
- each purpose/step envelope append, consume, egress-journal fence, provider
  request, result evidence, and next-step authorization,
- customer-hosted continuation issuance CAS and provider completion,
- refund-capacity and granted-value-clawback reservation, provider return evidence,
  lot cancellation, ledger/outbox commit,
- wallet compaction generation lock, constituent/range fence, atomic index flip,
  allocation/refund/clawback/service reservation versus expiry/close race, and
  crash recovery,
- mandate-revocation engine cutoff, provider detach, and terminal receipt,
- responsibility-transfer dual proof/cutoff, deterministic two-head lock/apply,
  one-side backlog/revocation/crash, proof-deadline expiry, early/late activation,
  apply-versus-admission/period-seal, old-window seal, source partition, active-
  grant revocation, retained-old-claim fencing, two payer-private outbox views,
  and atomic new-payer activation,
- callback arrival,
- ledger commit, and
- receipt publication.

Tests race duplicate schedulers, callbacks, provider switches, ceiling changes,
authorization revocation, proof-head advance/application, tax-policy withdrawal,
and account/payer transfer.
No run may rely only on a provider's short idempotency-retention window.

The revocation race covers both orderings around the shared stream/claim and
active-to-dispatching locks,
stale or missing signed heads, sequence gaps, duplicate edge submissions,
edge-accepted but not-yet-engine-effective commands, crashes between capability
consume and provider response, delayed/stale grants after claim generation
changes, two distinct intents racing one cycle/frequency cap, and ledger commit
before or after outbox delivery. They also race source chunk/seal workers,
admission against accepted-but-not-yet-applied revocation/window close,
authorization N against delayed N-1
grants, duplicate auto-top-up observers/other pending funding, subscription usage
against first settlement, responsibility transfer against initial claim, server
dispatch, client dispatch, or already-collectible first settlement in both CAS
orders, concurrent refunds/clawbacks, and revocation between
prepare→hold/debit, hold→capture, and setup continuation→completion.

Crash recovery must preserve these atomic pairs: no service exposure without its
admitted fact, no prepaid fact without wallet capacity, no settled top-up without
granted lots and closed trigger, no returned cash with spendable clawback lots, no
active subscription window without first settlement, no provider step without its
egress fence, and no ledger/receipt state without its signed outbox record.

Claim-to-dispatch tests also withdraw or expire authorization, tax policy,
pricing/terms policy, build/signing key, adapter capability/readiness, notice or
evidence readiness, and the intent/capability itself. Every case must revoke or
refuse the still-`active` grant before provider mutation, even when the signed
capability has not reached its wall-clock expiry. Protective release/void/mandate-
revoke cleanup must remain executable from its exact retained source/plan/object
after those exposure-creating gates are withdrawn, while being unable to debit,
return, or create a mandate.

### Adapter conformance suite

Every payment adapter runs the same provider-neutral contract suite:

- capabilities are truthful and unsupported operations refuse locally;
- amount/currency/merchant binding is exact, and payer correlation uses either
  authoritative provider identity or an authenticated deterministic operation
  reference uniquely bound to the frozen local payer/attempt, as declared by the
  adapter;
- customer-action and callback states are normalized without guessing;
- signatures/authentication and duplicate events are enforced; callback-auth
  credential class/scope is truthful, public ingress holds only public-key or
  dedicated verification-only material, and shared mutation-scope verification
  uses only the bounded fixed verifier inside the exclusive enclave;
- setup/payment/mandate-revoke/void/refund step capabilities have separate
  signature domains and cannot be decoded or replayed as one another;
- the generated plan-step inventory equals the closed CHARGES/DESIGN effect enum,
  enforces one mandate output, at most one debit/return, aggregate hold ceilings,
  source-bound release, and every published step/branch/byte/depth maximum;
- provider writers accept only the matching exported opaque `*StepDispatchPermit`
  struct with unexported fields/constructor; zero, forged, copied, wrong-purpose,
  wrong-provider, wrong-step, and non-journaled values fail durable authentication
  before operation fields are exposed or an SDK call occurs;
  raw/deserialized `Authorized*StepEnvelope` values and ordinary DTOs cannot compile
  against a writer;
- each permit exposes exactly one closed frozen step and leads to at most one SDK
  mutation; hidden composite create/confirm/capture, skipped prior-step evidence,
  changed plan index/effect, or settlement on prepare/hold fails;
- mutation transports explicitly disable SDK/HTTP automatic retries and redirects
  and fence the actual request boundary; instrumented timeout, connection-reset,
  `429`, `5xx`, and redirect fixtures for Stripe and NewebPay observe exactly one
  outbound mutation request and then `submitted_unknown`/`execution_unknown`;
- the durable egress guard permits one local submission for a permit/claim/
  deterministic-operation identity and fences replay or an unused delayed permit
  after dispatch-lease expiry;
- the executor durably consumes a capability id before the write and binds
  returned evidence to capability id, attempt generation, operation, and raw
  provider evidence;
- ambiguous writes permit only bounded read-only same-operation reconciliation;
  mutation retransmission is unsupported and provider idempotency grants no retry;
- customer-hosted mandate/hold/debit capabilities use the same proof-head/state
  issuance fence, declare one exact consequence/scope/amount/duration/expiry, and
  retain `client_dispatched` until authoritative cancel/expiry/result; broader or
  combined provider sessions are unsupported;
- created/read-back provider objects prove `no_autonomous_future_debit`:
  subscriptions, auto-advance, smart retry, dunning, and delayed auto-capture are
  disabled, and missing disable/cancel/read-back support refuses readiness;
- a direct executor assertion cannot produce `succeeded`; the adapter advertises
  and tests `provider_signed`, `native_readonly_reconciler`, or the explicitly
  shared-TCB `attested_enclave_broker_readback` class without calling the latter
  an independent credential boundary;
- an engine-authorized refund candidate cannot exceed remaining source capacity
  after settled/active refunds and observed or conservatively reserved external
  source-return effects; pending/unknown effects retain capacity, externally
  imposed overflow is still appended and opens an incident/negative-recovery
  state, and release requires authoritative no-return proof;
- terminal debit evidence must name the accepted instrument, refund evidence the
  exact source object/return destination, and setup evidence the one accepted
  mandate output; same-payer substitutions fail;
- mandate-revoke and protective cleanup use their exact retained source/plan/
  object after debit authority or pricing/tax/notice gates are withdrawn, but can
  only revoke/release and never debit/return/create authority;
- trace nodes map back to exactly one attempt; and
- adapter reads cannot mutate provider state.

An adapter that claims separated reads also proves a provider-enforced read-only
credential, or an attested fixed-read broker inside that credential's exclusive
`ProviderCredentialEnclave` owner while the external reconciler remains credential-
free. A separate reader holding a broad merchant credential, or a narrow Go
interface over such a credential, does not pass this gate.

Provider-specific tests may add constraints but cannot weaken the shared suite.

---

## 6. Mutation testing

A passing test suite proves only that the implementation satisfied its tests.
Mutation testing breaks each public invariant deliberately and records which test
noticed.

Required mutations include:

| invariant deliberately broken | expected detector |
|---|---|
| admit `amount` or `price` on usage request | closed-vocabulary/source-shape test |
| allow mutable price fallback | price-policy property/golden test |
| execute price/module policy code with I/O/dynamic imports/unbounded work, exceed artifact/metric/tier/expression/input/output/fuel/memory limits, or scan every installed module for one fact | price/manifest bounded-interpreter and indexed-lookup test |
| treat `tax.status = unknown` as zero | tax fail-closed test |
| accept proprietary `provider_attested` tax as independently reproducible | tax public-artifact/verifier test |
| execute tax-rule code with I/O/dynamic imports/unbounded work, exceed artifact/input/output/node/depth/fuel/memory limits, or let core and verifier limits differ | tax artifact sandbox/boundedness conformance test |
| trust private-caller tax profile or substitute wrong-payer/later revision | tax-profile proof/issuer/binding test |
| post a positive tax correction directly to the ledger without a new exact authorized/noticed intent | positive-tax-correction authority test |
| skip notice delivery or wait | executor eligibility test |
| accept notifier role assertion without carrier proof/read-back | notice-evidence strength test |
| substitute notice content/destination/message/timestamp or replay evidence | notice binding/replay test |
| release/replace after notice invalidation loses the race to client_dispatched/server dispatch/wallet commit, or fail to clear readiness when invalidation wins | notice-invalidation point-of-no-return race test |
| widen authorization ceiling/currency | authorization binding test |
| substitute a different same-payer saved mandate/reference | saved-method receipt/identity binding test |
| mutate brand/type, masked suffix, expiry/revision, merchant/entity, or mandate scope behind the same provider token | saved-method immutability/read-back conformance test |
| substitute a private-relay or wrong provider/merchant/origin continuation URL | trusted-continuation origin/tuple test |
| bind continuation to the wrong setup/intent/attempt/operation or escalate one-use to reusable scope | trusted-continuation object/scope test |
| accept an expired/replayed continuation or a core-signed continuation outside the accepted tuple | trusted-continuation expiry/replay/acceptance test |
| sign a continuation from executor-reported fields when provider-signed/attested session read-back differs or is unavailable | trusted-continuation evidence-strength test |
| acquire claim from a stale or local-only proof high-watermark | proof-stream/claim race test |
| skip a payer sequence or acknowledge before durable append | proof-inbox linearizability test |
| assign a sequence to an invalid, oversized, bad-signature, or replayed envelope | proof-stream anti-jamming test |
| scan an unbounded proof backlog inside claim/consume or dispatch before heads match | proof high-watermark batch/flood test |
| bootstrap or recover a factor from private-relay identity alone, skip cooling/threshold, or reuse a revoked factor | factor enrollment/recovery authority test |
| dispatch/settle automatically with stale, missing, inconsistent, or incident-flagged revocation-path readiness | revocation availability/freshness gate test |
| execute from caller-supplied amount | package/API capability test |
| coerce setup/mandate-revoke/void/refund step capability into payment or another purpose | purpose/step-domain capability test |
| embed any forbidden effect class in an otherwise valid setup/payment/refund/void/mandate-revoke envelope, including a hold/debit/return in setup | exhaustive purpose/effect-matrix test |
| replay capability after crash before response commit | executor durable-consume test |
| let a writer accept `Authorized*StepEnvelope`/DTO, or accept zero/forged/copied/cross-purpose/cross-provider/cross-step/non-journaled `*StepDispatchPermit` | compile-time type + runtime egress-authentication tests |
| submit the same dispatch permit/egress identity twice or use a delayed permit after lease expiry | durable egress-guard crash/replay test |
| consume a delayed capability after its claim generation was revoked | grant-state/claim-generation CAS test |
| dispatch an exposure-increasing step after tax/policy/key/adapter/readiness withdrawal, or block source-bound protective cleanup on a withdrawn debit-only gate | purpose/effect-specific consume predicate test |
| release a claim while capability is dispatching/submitted_unknown/client_dispatched or a hold remains | capability fencing/ambiguity/cleanup test |
| let two intents each reserve the same remaining aggregate cap | authorization-exposure concurrency test |
| derive cap capacity from net ledger balance, or let refund/chargeback/reversal/write-off restore settled amount/count without explicit source-bound CapRecreditPolicy | gross-cap monotonicity/charge-refund-loop test |
| let concurrent refund intents reserve the same remaining source capacity | refund-capacity reservation concurrency test |
| re-rate or repost an obligation during collection, or let concurrent collection intents reserve the same receivable remainder | receivable-collection source-capacity test |
| omit/substitute a collection source/reservation or invent/re-tax an arbitrary collection principal | collection-equation golden/mutation test |
| combine period sources whose payer/commercial-identity/tax/currency/authority/funding/route-policy/instrument/window key differs, select an incompatible final route, or let one invalid group contaminate another | period-close compatibility-partition property/concurrency test |
| race a refund against a reversal, chargeback, or dispute credit on the same source | cross-effect source-capacity serialization test |
| release refund capacity while dispatch/refund outcome is pending or unknown | refund reservation/claim lifecycle test |
| subtract one stored-value lot as a rating credit and apply it as funding | credit-semantic unique-use/golden equation test |
| expire/roll up/reallocate/refund/claw back a service-reserved lot slice, admit deferred prepaid service from a non-preserving lot, or let nominal expiry create arrears/card fallback | wallet-expiry/reservation serialization property/crash test |
| omit/zero funding principal, treat bonus output as payment, or apply the service equation to credit purchase/auto-top-up | stored-value funding-principal golden/mutation test |
| release exposure while attempt is pending/unknown | reservation lifecycle test |
| allow second provider settlement | DB uniqueness + concurrency test |
| let callback create an attempt | webhook-origin property test |
| duplicate a broad callback/mutation secret into public ingress, misclassify its provider-enforced scope, bypass bounded raw-byte/header verification, or expose a read/write method from same-enclave VerifyCallback | callback-auth credential-topology conformance test |
| settle from executor assertion without provider proof/read-back | evidence-strength conformance test |
| let a read path import/invoke writer | architecture/source test |
| publish evidence without its canonical state transaction | outbox atomicity test |
| omit/substitute/revoke/expire/wrong-role an attempt-bound executor or trusted-reader artifact/workload/credential attestation, or replace it with current Health | historical artifact-binding verification test |
| omit/substitute/revoke/expire/wrong-role a setup-executor/session-reader/mandate-reader artifact or use current Health as setup history | setup-receipt artifact-binding test |
| authorize evidence read from api-platform identity assertion | customer-read-proof tenant test |
| remove amount/currency provider reconciliation | adapter conformance test |
| unbalance one ledger entry | ledger property test |
| omit build/policy digest from receipt | canonical golden test |
| substitute commercial identity after source/tax, accept oversized/ambiguous MerchantBindingSet proof, select a final route incompatible with its commercial identity/funding, or let route selection alter tax/gross | commercial-identity/merchant-set/final-route binding test |
| activate authorization N without atomically superseding N-1, reset scope exposure on revision, or consume a delayed old grant | authorization-lineage carry/concurrency test |
| reuse a source leaf by changing payer/currency/policy/aggregate/partition, omit a leaf at the seal barrier, or reuse after settlement | source-allocation uniqueness/completeness test |
| admit service without applying/locking the authoritative current proof head, with an accepted-but-unapplied revocation, without max-tax/rounding gross hold, over the ceiling, without prepaid wallet capacity, or above an over-bound close | service-admission proof-head/exposure test |
| trust producer occurrence time to backdate after revocation/window close | source-clock/lateness/high-watermark test |
| let private RPC assert either transfer proof, accept equal payers/current-owner mismatch, sign different common digests/cutoffs or wrong audience view, omit/substitute the activation-failure disposition, reveal old financial detail in the new view, skip either current payer head, reverse lock order, partially apply one side, activate before cutoff/after deadline, resume billable old-payer service after failure without fresh authority, admit across a due unresolved cutoff, backdate a blocked fact, omit either authority, move a mandate/wallet/tax profile/notice destination/liability/receivable, preserve an old active grant, recreate an old ambiguous claim under the new payer, or admit new-payer service before its ceremony | billing-responsibility-transfer dual-stream/privacy/time-barrier/partition test |
| activate a subscription schedule/service authority before exact first settlement, or re-anchor without replacement proof | subscription activation race test |
| omit the current responsibility/schedule-generation lock from subscription_start initial claim or any hold/debit/client-dispatched consume, let a stale old-payer grant dispatch after transfer, activate service after post-cutoff old-payer settlement, or lose that cash instead of source-linking refund/credit/manual resolution | subscription responsibility-generation claim/dispatch/transfer race test |
| let duplicate observers create top-ups, include candidate in its own pending balance, skip consume-time recovery check, or grant without trigger close | auto-top-up reservation/atomicity test |
| return cash for credit purchase/top-up while matching granted/bonus lots remain spendable or after those lots were spent | granted-value clawback property/crash test |
| complete setup after revocation with a usable receipt, treat setup proof as debit authority, or release a client-dispatched setup without mandate cleanup proof | setup terminal-proof/revocation test |
| skip/fabricate prior-step evidence, settle on prepare/hold, hide two SDK mutations behind one permit, or exceed plan/cardinality/hold limits | finite-plan step conformance test |
| leave SDK/HTTP retries or redirects enabled, let one permit emit two outbound requests after timeout/reset/429/5xx/redirect, or treat provider idempotency as retransmission authority | outbound mutation single-shot transport test |
| issue a customer-hosted setup/hold/debit continuation without the state CAS, with multiple consequences, or release it without cancel/expiry proof | client-dispatched capability test |
| enable provider subscription/auto-advance/smart retry/dunning/delayed capture or claim readiness without disable/cancel/read-back | provider-autonomy fixture test |
| terminally debit a different same-payer instrument or refund a different source/destination | terminal instrument/source binding test |
| frame/overlay/autosubmit the verifier, forward a gesture, retain opener control, or substitute origin/release | trusted-display browser/release test |
| report a native verifier ready without its versioned platform profile and full app/deep-link/release/substitution/occlusion/gesture/update suite | native trusted-display readiness test |
| exceed line/source/bundle/lot/plan hard bounds, nest/expand rollups past depth/arity/constituent-work limits, truncate at max+1, change allocation/refund result after wallet rollup, expose old+new generations, or reserve an original lot and its rollup/sibling range concurrently | boundedness/rollup generation-CAS equivalence test |
| present attested BillingDecisionProof as global non-omission proof or let async log status authorize/delay collection | verifier assurance/state-transparency test |
| report aggregate verified when state transparency proves a conflicting signed history | verifier aggregate-verdict test |
| accept proof/authority/policy/admission/seal/transfer/setup/capability/notice/settlement timing after rollback, excessive forward step/skew, stale/disagreeing sources, or an uncertainty interval crossing its cutoff | global time-readiness/cutoff test |
| expose foreign aggregate payout/batch totals or membership in a customer trace | cross-tenant provider-trace privacy test |
| vary evidence/live-refresh status, size, timing class, error shape, or provider-read scheduling for guessed foreign ids | read-oracle conformance test |
| expose a generic balanced-entry ledger route/DTO/IAM permission outside the trusted core transaction | ledger ownership architecture test |

The mutation report is created only after running the pass. Equivalent mutants
and survivors are recorded rather than hidden. CI verdicts are judged by process
exit status, not by grepping output for successful packages.

---

## 7. Static architecture checks

CI must mechanically enforce:

- provider SDK imports and raw provider-mutation HTTP hosts exist only in adapter/
  enclave packages and generated egress allow-lists;
- provider-write interfaces are injected only into the isolated executor
  deployment; its setup/payment/mandate-revoke/void/refund step ports remain
  purpose-separated;
- generated secret-to-workload/IAM/KMS and network-egress inventories prove each
  actual mutation credential has one exclusive `ProviderCredentialEnclave` owner,
  no second workload/backup/admin job can read it, and mutation endpoints are
  denied outside that enclave; broader provider-enforced scope/blast radius is
  attested and checked against merchant policy;
- callback-auth keys appear in the same secret/workload inventory; public ingress
  may own only public-key/dedicated verification-only material, while any shared
  mutation-scope secret has one enclave owner and a generated fixed-verifier-only
  route with strict raw request/header/time bounds and no provider read/write edge;
- planner, read, usage-ingress, notifier, and reconciler binaries cannot compile
  against write ports;
- every provider mutation method/raw endpoint is in a generated allow-list mapped
  to one closed plan effect in [`CHARGES.md`](CHARGES.md), one purpose/step writer,
  and at most one SDK/HTTP mutation; composite hidden calls fail;
- generated adapter configuration and transport tests require zero mutation
  retries, disabled redirects, and one permit-aware outbound-request fence;
  uninspectable retrying middleware or proxy configuration fails readiness;
- a generated route/IAM inventory proves the account API is Lambda-invoke-only,
  the public health integration cannot dispatch RPC actions, and each provider
  webhook is a separate authenticated ingress;
- generated transfer-action schemas prove proposal accepts only account, old/new
  payer ids, and closed policy selection, while apply accepts only transfer id;
  neither private action can carry a payer proof or proof-head override;
- generated `subscription_start` claim/envelope/consume predicates carry and
  lock the accepted responsibility/schedule generation for every adverse or
  customer-collectible transition, using the same row as transfer activation;
- a generated role-to-action matrix proves dispatch metering can invoke only
  `RecordUsage` through a separate function/IAM resource or an authenticated
  action-bound capability; generic Lambda invocation plus a caller-selected
  action fails the gate;
- documentation adjacency checks reject Customer/Browser-to-engine,
  Module-to-engine, callback-to-executor, notifier-to-ledger, and
  read/usage-to-settlement edges unless an explicit deployed exception exists;
- public request structs contain no forbidden monetary/authority fields;
- generated DB role/table/procedure grants, KMS/signing-key ownership, migration
  roles, and operator/admin paths prove only trusted billing-core procedures can
  mutate/sign proof heads, authorization, notice, source/exposure, claim, wallet,
  ledger, receipt, and outbox state; negative tests attempt each direct write;
- no external route/queue/role can call the ledger writer or construct its
  purpose-typed validated transition;
- generated inventories cover every public edge, private action, evidence
  ingress, and purpose/step-typed executor operation; setup/payment/mandate-
  revoke/void/refund use
  distinct roles, audiences, schemas, and signature domains;
- generated type checks prove provider writers accept only matching exported
  opaque `*StepDispatchPermit` values with mandatory durable authentication,
  never `Authorized*StepEnvelope`/ordinary DTOs, and the
  durable egress guard journals one submission per permit/claim/operation;
- consent-edge credentials can only append through the payer-stream procedure;
  evidence-edge credentials can call only the proof-verifying, replay-consuming
  `ReadEvidence` procedure and have no table/list/raw-outbox read; neither role
  can invoke the account dispatcher or provider writes;
- execution, reconciliation, and notice evidence roles are action-bound and
  cannot submit one another's envelopes;
- notice readiness proves carrier-signature verification or an attested
  credential-separated authoritative read path; notifier authentication alone
  fails the gate;
- adapter fixtures prove every created/read-back object disables provider-native
  subscription, auto-advance, smart retry, dunning, and delayed capture, and that
  customer-hosted capability issuance/cleanup is fenced; mutation transports also
  prove one outbound request under timeout/reset/429/5xx/redirect with no hidden
  retransmission;
- hard line/source/bundle/lot/plan limits are generated from one versioned schema
  and equal across core, adapter, verifier, docs, and `Capabilities`;
- the charge-kind enum and documentation catalog are exhaustive and equal;
- all shipped binaries stamp commit/artifact identity; and
- provider-funded receipts bind the historical executor/adapter
  artifact/workload/credential attestation and checkpoint, and any attested
  notice/reconciliation reader is bound the same way; current `Health` cannot
  substitute for per-effect evidence; and
- production readiness cannot pass while any legacy money path is reachable.

The route/IAM inventory is derived from entrypoint and infrastructure source,
not from diagram labels. This specifically prevents a nominal status read,
usage ingest, or infrastructure synchronization job from triggering auto top-up
or any other payment effect, and prevents public source from being confused with
a public runtime endpoint.

---

## 8. Transparency and release gate

No external transparency service sits in the synchronous payment path. The
signed transactional outbox attests engine ordering, while the optional
asynchronous payer-isolated `BillingStateTransparencyLog` can later reveal a
published rollback, equivocation, or split view. It does not prove an independently
witnessed pre-execution timestamp or hidden-row non-omission, and a delayed
checkpoint never authorizes or delays collection. Reports preserve
`state_assurance: attested` and the separate transparency status.

Release remains manual while this architecture is introduced. Production
promotion requires:

1. reviewed source and target-state document consistency;
2. signed build provenance;
3. all tests, fuzz smoke, adapter conformance, and recorded mutation pass;
4. shadow-intent reconciliation with no unexplained monetary differences;
5. public runtime identity, independently verifiable disclosure, customer-proof
   verifier, pinned trust-root/rotation evidence, independent evidence-edge
   availability, and receipt verifier availability;
6. route/IAM inventory and documentation adjacency checks passing;
7. customer authorization/notice/tax readiness; and
8. `legacyMoneyPaths: 0` with legacy provider credentials revoked.

No automatic merge or deployment is implied by a green unit-test run.

---

## 9. Known limits

Verification can prove which canonical bytes the independent consent client
bound into a customer proof, and which notice bytes the carrier reported at the
configured destination in the policy's terminal delivered state and when. It
cannot prove a person read an email or understood a rendered disclosure.

Public source plus build provenance makes tampering detectable; it cannot protect
against a fully compromised deployment, signing keys, database, notification
provider, and payment-provider account acting together. The threat model in
[`THREAT-MODEL.md`](THREAT-MODEL.md) states the remaining trust assumptions.

Provider payout/balance visibility differs by rail and merchant contract. A
trace marks unsupported evidence explicitly and never treats it as proof of
absence.
