# How customers can verify the billing engine

Public source is useful only when a customer can connect three things:

1. the rules in this repository,
2. the artifact that proposed and executed a charge, and
3. the evidence used for that particular charge.

> **Status: target contract.** None of §2, §3, §4 or §5 ships on `main` today.
> The verifier, the charge bundle, public build identity, policy digests, the
> transparency record and the intent-only capability gate are all unbuilt.
> Sentences using "must" or "will" are acceptance targets, not claims about
> current behavior. Current defects are enumerated in exactly one place:
> [`SECURITY.md`](../SECURITY.md#known-current-gaps).

Sections §1–§5 address a customer verifying one charge. Sections §6–§8 address
CI at the release gate. Section §9 states what verification cannot prove.

## 1. Evidence levels

| level | question answered | required evidence |
|---|---|---|
| source | what is this revision designed to permit? | public code, docs, schemas, tests |
| build | what code produced the artifact? | Git SHA, signed build provenance, artifact digest |
| deployment | what artifact and policies are running? | engine-signed `Health`/`Capabilities` evidence |
| intent | how was this total derived and authorized? | charge bundle plus its digest |
| runtime state | what serialized transition did the billing core report? | signed `BillingDecisionProof` (unbuilt); non-omission stays `state_assurance: attested` |
| provider | what did an external rail report happened? | normalized, verified provider evidence |
| ledger | what monetary transition did MirrorStack commit? | balanced append-only transaction plus correction chain |

`state_assurance: attested` names the billing engine as the attester. No
independent witness signs it.

No lower level substitutes for a higher one. A public repository does not prove
which binary charged a card. A provider's `paid` status does not prove why the
amount was allowed.

## 2. Customer-visible runtime identity

`billing-engine` is a private runtime service. In production `cmd/account-api`
starts as a Lambda invoke handler (`cmd/account-api/main.go:857`), and the local
HTTP path is gated on `X-MS-Internal-Secret` (`cmd/account-api/main.go:652`).
Its dispatcher exposes no `Health` or `Capabilities` action today; the only
local health route is `/__health` (`cmd/account-api/main.go:647`). Both target
actions must produce engine-signed evidence that `api-platform` can relay
publicly but cannot alter.

`Health` must return the same fields on healthy and unhealthy responses. Those
are the Git commit or the literal `unknown`, the artifact digest, and the build
provenance identity. It must also return the binary role (`planner`, `notifier`,
`executor`, `reconciler`), the receipt and bundle schema version, and a
deployment environment identifier holding no secret. An executor whose build
identity is `unknown` must refuse to execute.

`Capabilities` must return, as signed fields a verifier can pin:

- the active terms, price-book, tax, notice and routing-policy digests; the
  currency and scale registry revision; and each adapter's version,
  capabilities, transport configuration and last conformance revision;
- every numeric hard limit the verifier must reuse: intent lines and bytes,
  source-proof and bundle bytes, wallet lots, and rollup depth and arity;
- the remaining numeric limits: provider-plan steps and bytes, proof-apply
  batches, and `MerchantBindingSet` (unbuilt) members, proof bytes, depth and
  hash operations;
- the price-book, module-manifest and tax-rule interpreter identities and their
  limits, which must equal the public verifier's;
- readiness for the notifier, the customer-facing relay, the independent consent
  and proof verifier, the executor, the evidence edge and the receipt verifier;
- per-platform trusted-display profiles, where a native profile stays
  `unsupported` until its OS-specific suite passes;
- provider evidence strength, and how reconciliation reads are enforced. The two
  permitted forms are a provider-native read-only credential, or a
  credential-free reconciler calling a fixed-read broker inside the credential's
  exclusive owning enclave — INV-007 in [`DESIGN.md`](DESIGN.md#inv-007);
- the callback-auth credential class and scope, its numeric request, header and
  time limits, and the verifier artifact and owner;
- the callback replay policy, and whether verification runs in public ingress or
  inside that owning enclave; and
- the minimum notice policy, and the `TimeReadinessPolicy` (unbuilt) identity,
  source, maximum uncertainty, skew, forward step and readiness; and
- the audit-only asynchronous transparency status and checkpoint, plus every
  reachable legacy money-moving path with a count.

The production intent-only claim requires `legacyMoneyPaths: 0`. A stronger new
surface beside one weaker legacy route is not a stronger deployment. Build
identity and non-sensitive capability fields must reach customers as signed
engine evidence through the public control plane. A private support request is
not a substitute. Exposing the account RPC is not an acceptable route.

### Verification trust root and signing profiles

The verifier must pin a billing verification root independently of any runtime
response. That fingerprint must ship in this repository, in signed verifier
releases, and in a separately operated transparency channel — a different
mechanism from the state transparency log. No `api-platform`, consent-edge or
evidence-edge response may introduce a new root. Every signed disclosure,
capability statement, receipt and charge bundle must carry an algorithm
identifier, key id, issuer, audience, environment, schema, signature domain,
payload digest, validity interval and transparency checkpoint. A key valid for
`billing-capabilities/v1` therefore cannot sign `customer-acceptance/v1`. Leaf
rotation must be cross-signed and committed before use. Revocation and emergency
rotation must be root-signed, append-only events carrying an effective cutoff.
The verifier must reject an unknown algorithm, a substituted root, a revoked key
used after its cutoff, a missing chain, a stale checkpoint, or an audience
mismatch. Automatic execution must stay disabled while key identity,
transparency anchoring or trust-root distribution is `unknown`.

### Customer proof and evidence-edge contracts

- The engine must accept an acceptance receipt only when it names an
  engine-issued disclosure digest. It must record that receipt on a gap-free
  monotonic payer stream only the engine appends to. The receipt must be durable,
  with the head updated, before authority counts as established.
- The settlement-claim transaction must prove it locked the authoritative
  current head, consumed every sequence through it, and ran revocation and gate
  checks before the claim compare-and-swap — INV-013 in
  [`DESIGN.md`](DESIGN.md#inv-013). A local high-watermark without an
  authenticated current head is not valid.
- `CustomerReadProof` (unbuilt) must bind the enrolled factor to payer, account,
  one object or one size-limited collection, audience, nonce, expiry, replay
  identity and key version. The edge must verify it against the billing-owned
  factor mapping, never an `api-platform` identity assertion.
- The edge must return one published status shape, padded size class, timing
  bucket and rate limit for authorized, absent and unauthorized requests alike.
  It serves signed, customer-encrypted, checkpointed outbox records that it
  cannot itself create or edit.

## 3. Canonical charge bundle

This section owns the charge-bundle field contract. Other documents link here
rather than restating it.

The bundle has one versioned canonical encoding. It fixes field names and order,
integer and decimal representation, Unicode normalization, timestamps and time
zones, and absent versus explicit zero or null. It also fixes the ordering of
lines, sources, evidence and ledger entries, and the digest and signature
domains. Invalid Unicode, duplicate map keys, unknown critical fields,
out-of-range integers, unsupported schema versions and non-canonical encodings
must all be refused. The digest binds canonical bytes, not a lossy parser's
reading of them.

Customer exports must replace sensitive source fields with domain, payer, object
and field-bound hiding commitments using unique random nonces. Raw deterministic
hashes of addresses, tax ids or other low-entropy personal data are forbidden.
Only the owning payer can obtain the encrypted opening, and nonces are never
reused. Key rotation must preserve historical openings. Corrected evidence must
create a new append-only commitment, and redaction must be covered by the
digest.

Every element below is required unless the "when" column names a condition.

| # | element | contents | when |
|---|---|---|---|
| 1 | intent | canonical bytes and digest of the `ChargeIntent` (unbuilt) | always |
| 2 | commercial identity | the `CommercialIdentityBinding` (unbuilt) used by source, tax and wallet evaluation | always |
| 3 | merchant of record | composite `MerchantOfRecordBinding` (unbuilt), its `MerchantBindingSet` (unbuilt) membership proof, and the settlement route | always |
| 4 | source authority | one tagged form: service leaf and window allocation root with signed local checkpoint transition evidence; one-time replay identity; auto-top-up trigger; or receivable capacity | always |
| 5 | source ids | source event and aggregate ids, or privacy-preserving hashes of them | always |
| 6 | rating | every rating source commitment, module billing-manifest version, interpreter and limit revision, formula, integer scale, rounding step, subtotal, rating credit, tax and total | always |
| 7 | policy digests | terms, price book, tax, notice, time readiness, rail routing, autonomy and execution plan, plus observed time uncertainty wherever used | always |
| 8 | tax | status `final`, `not_applicable` or `unknown`, its `verificationClass` value, and the evidence behind it — state machine in [`DESIGN.md`](DESIGN.md#7--tax-and-what-it-refuses-to-guess) | always |
| 9 | funding | frozen `FundingPlan` (unbuilt), every credit-lot and authorization-scope exposure reservation, gross obligation, wallet application, provider remainder, wallet active-index generation and range proof, gross-monotonic cap and window arithmetic, and the funding result | always |
| 10 | ceilings | ceilings as evaluated at decision time, authorization scope and lineage head, carried exposure, and consume-time validity | always |
| 11 | authority branch | one tagged debit `AuthorityEvidence` (unbuilt) branch and no other — `debit_customer_present` with intent acceptance, proof and current one-time-or-standing authorization, or `standing_automatic` with authorization acceptance, proof, terminal notice receipt, completed wait and full revocation-path readiness receipt. Either branch binds payer sequence, head and cutoff; factor and verifier revision; authorization scope and lineage; carried exposure; and the dispatch-time revocation result | always |
| 12 | decision proof | signed `BillingDecisionProof` (unbuilt): closed key and predicate schema, authenticated payer proof head, before and after row commitments and generations, transaction, build and policy identities, and the matching outbox record; assurance is `attested` by the engine | always |
| 13 | build identity | engine Git commit, artifact digest, receipt schema version and build provenance | always |
| 14 | ledger | balanced ledger transaction ids and entries, plus the outbox checkpoint | always |
| 15 | service accrual | `ServiceAccrualExposure` (unbuilt) arithmetic converting the reserved upper bound to the settled line; a deferred prepaid lot additionally binds its expiry-preservation rule, reserved time, service window and scheduled close, nominal expiry, range, amount, generation, and consume or release evidence | a service accrual funded it |
| 16 | subscription | accepted `SubscriptionOffer` (unbuilt) and its activation-gated schedule receipt: cadence, time zone, anchor, first-period and recognition rules, schedule generation, atomic first-settlement activation compare-and-swap | subscription charge |
| 17 | responsibility transfer | shared transfer commitment, two audience-specific disclosure and proof digests, both payer-stream heads and applied cutoffs, effective cutoff and deadlines, generation compare-and-swap, source and exposure partition, retained old claims, and the non-transfer of mandates, wallet, tax and notice state; the new-payer view carries no old-payer private financial detail | payer changed |
| 18 | auto top-up | trigger reservation and epoch, creation balance and other-pending-funding snapshot, owning intent, consume-time recheck snapshot, result and time, and the atomic credit and bonus grant with trigger and pending-funding close | `auto_topup` |
| 19 | provider plan | autonomy policy and finite `ProviderExecutionPlan` (unbuilt); every step envelope, consume, opaque permit, egress identity and effect class; zero-retry, no-redirect transport configuration and its single outbound-request fence evidence | provider execution occurred |
| 20 | provider evidence | enclave, executor, adapter, workload and credential attestations each naming its attester; `PaymentAttempt` (unbuilt) transitions; the actual debit instrument binding; and normalized provider evidence with explicit evidence strength and TCB class | provider execution occurred |
| 21 | read-back or callback | reader, broker or callback-verifier artifact; its workload, credential class and scope; numeric request limits in force; replay result; evidence class; checkpoint; verification location; and attester | that path supplied evidence |
| 22 | corrections | correction, refund and dispute links | any exist |

A current runtime `Health` response can never stand in for these historical
bindings. Unknown, revoked, expired, wrong-role or substituted artifacts fail
verification.

### Sibling receipts with their own schemas

Refund, mandate-revocation and payment-method-setup receipts (all unbuilt) use
distinct schemas and distinct signature domains. None can satisfy debit
authority, a debit funding plan, or a collection notice.

| receipt | binds |
|---|---|
| refund | the immutable refund intent; source charge, settlement, ledger and provider commitments; typed return authority; line and tax reversal; wallet-lot restorations and provider-return remainder; refund-capacity reservations; any finite provider plan with per-step evidence; the actual source object and return destination; the `BillingDecisionProof`; balanced return entries; the outbox checkpoint |
| refund of `credit_purchase` or `auto_topup` | additionally the `GrantedValueClawbackReservation` (unbuilt) and atomic cancellation of the matching unspent principal and bonus lots. Cash return that leaves spendable output is invalid |
| mandate revocation | the customer proof and cutoff; the setup receipt and method identity; the immediate engine cutoff; the finite revoke steps; the provider evidence and status; separate engine and provider timestamps |
| payment-method setup | the setup disclosure and digest; provider merchant setup binding; acceptance receipt and its proof commitment; payer sequence, head and cutoff; factor and verifier revision; dispatch and completion proof head and revocation state; the no-debit execution plan; every step consume, permit and egress identity; enclave, executor, adapter and credential attestations; and either core-verifiable provider-signed setup evidence or the trusted reader attestations used for read-back |

## 4. Offline verifier

The public command will be:

```bash
billing-verify verify charge-bundle.json
```

It must check, without contacting MirrorStack or a payment provider:

1. canonical encoding and bundle digest;
2. build, source and policy references;
3. signed source-allocation transition evidence, leaf and window subject
   binding, and the conversion from reserved bound to settled line;
4. module billing-manifest and version binding;
5. price selection and effective windows;
6. integer arithmetic, tiers, credits, tax and rounding, reproduced from the
   content-addressed public tax-rule artifact;
7. equality of line, subtotal, tax and total;
8. equality of merchant of record, authorization scope and lineage, carried
   exposure, currency, cadence, ceilings, method, autonomy policy and window;
9. the tagged authority branch, per §3 row 11;
10. funding identity, `gross = wallet allocation + provider remainder`, with lot
    selection and rollup equivalence inside the limits published in
    `Capabilities`, credit compatibility and uniqueness, and caps;
11. subscription offer and schedule fields with atomic first-settlement
    activation, or the auto-top-up trigger, recheck, grant and close;
12. a finite provider plan with closed effect classes and cardinalities, and one
    envelope, consume, permit, egress and evidence chain per step;
13. the actual debit instrument or refund destination, with at most one debit or
    return;
14. the `BillingDecisionProof` (unbuilt) closed predicate and key schema, proof
    head, before and after commitments and generations, and transaction, build,
    policy and outbox binding; and
15. per-currency ledger balance, correction, refund and mandate-revocation
    structure, and every published hard line, byte, lot and plan limit.

The verifier must reject a missing or swapped customer-present/standing
authority branch. A setup bundle instead requires `setup_customer_present`,
which must be rejected inside a debit bundle. An unverifiable setup receipt
cannot support standing authority.

The result is structured:

```text
verdict: verified | invalid | unsupported
state_assurance: attested
state_transparency: verified | pending | unsupported | invalid
historical_provider_evidence: not_applicable | verified | invalid | unsupported
live_provider_status: not_requested | verified | pending | unsupported | invalid
```

`verdict: verified` means the supplied contract, arithmetic, signatures and
transition chain passed. It never upgrades `state_assurance` beyond the engine's
own attestation. An unknown required schema or policy is `unsupported`, and
missing async publication is `pending` or `unsupported` without changing the
offline arithmetic verdict. A conflicting published history sets
`state_transparency: invalid` and the aggregate `verdict: invalid`; a report must
never describe a known conflicting history as verified. A terminal
provider-funded charge receipt requires its historical debit evidence, so
missing or invalid evidence makes the aggregate verdict `invalid` and an unknown
evidence schema makes it `unsupported`. A `pending` or `unsupported` live status
belongs to the optional later provider refresh and must not weaken already
verified historical settlement evidence. Optional online mode may refresh
provider evidence through customer-authorized read-only APIs, but provider
reachability must never affect an offline verdict.

## 5. Public golden vectors

Golden vectors are public evidence, not internal fixtures, and they cross
package and repository boundaries. At minimum they must pin:

- canonical bytes and digests for intents and receipts;
- each charge kind and tax status;
- each currency settlement exponent and its boundary rounding;
- standing and one-time authorization evaluation, with scope-lineage
  supersession;
- notice bytes, carrier evidence, destination binding and wait calculation;
- ledger entries and correction chains; and
- normalized Stripe and NewebPay fixtures.

Changing a golden digest requires a schema or policy version change and an
explicit migration decision. Regenerating constants to make tests green is not a
fix.

## 6. Testing posture

The current suite is 114 `_test.go` files totalling 46,679 lines, run by
`.github/workflows/ci.yml` as `go test -race -count=1 ./...` plus an integration
pass under `-tags=integration`. It covers the shipped engine, not the target
model in [`DESIGN.md`](DESIGN.md#3--what-must-be-true-before-any-money-moves). Test policy for the
target model belongs beside the tests that implement it, and its backlog belongs
in a tracking issue on this repository rather than in a customer document.

Two process rules hold for any mutation or fuzz pass added later. The report is
created only by running the pass, with survivors and equivalent mutants recorded
rather than hidden. CI verdicts are judged by process exit status, never by
grepping output for the names of packages that passed.

## 7. Static architecture checks

These checks constrain code that exists, so they are the first part of this
document that can be made real. None of them runs today:
`.github/workflows/ci.yml` runs the migrations, `go vet`, `go build` and
`go test`, and nothing else.

CI must mechanically enforce:

- **Provider SDK confinement.** Provider SDK imports and raw provider-mutation
  HTTP hosts may appear only in adapter and enclave packages and the generated
  egress allow-list. Today 9 non-test Go files import
  `github.com/stripe/stripe-go`, among them `internal/shared/stripe/client.go`,
  `internal/account/webhook/router.go` and
  `internal/account/autotopup/executor.go`.
- **Write-port isolation.** Provider-write interfaces may be injected only into
  the isolated executor deployment. Planner, read, usage-ingress, notifier and
  reconciler binaries must not compile against write ports.
- **One enclave owner per mutation credential.** Generated secret-to-workload,
  IAM, KMS and egress inventories must prove each mutation credential has one
  exclusive owning enclave. They must also prove no second workload, backup or
  admin job can read it, and that mutation endpoints are denied outside it.
  INV-007 in [`DESIGN.md`](DESIGN.md#inv-007) is the rule.
- **Route and IAM inventory.** A generated inventory must prove the account API
  is Lambda-invoke-only and that the public health integration cannot dispatch
  RPC actions. It must also prove each provider webhook is a separate
  authenticated ingress, and that dispatch metering can reach only `RecordUsage`
  through a separate function and IAM resource. Today
  `cmd/account-api/main.go:857` starts the Lambda invoke handler; the local HTTP
  router is gated on `X-MS-Internal-Secret` (`cmd/account-api/main.go:652`); and
  `RecordUsage` sits behind the separate `X-MS-Meter-Secret` header
  (`internal/shared/auth/internal_secret.go:47`, `cmd/account-api/main.go:776`);
  and the webhooks are separate binaries (`cmd/account-webhook`,
  `cmd/account-webhook-eventbridge`). The inventory must be derived from
  entrypoint and infrastructure source, never from diagram labels. It is what
  stops a nominal status read, a usage ingest or an infrastructure sync job from
  triggering auto top-up or any other payment effect. It reaches customers
  through the signed `Health` evidence in §2, and it is defined nowhere else in
  this repository.
- **Public request struct shape.** Public request structs must carry no monetary
  or authority fields. No caller-supplied amount may reach the executor.
- **Database authority.** Generated DB role, table and procedure grants, KMS and
  signing-key ownership, migration roles and operator paths must prove one
  thing. Only trusted billing-core procedures may mutate or sign proof heads,
  authorization, notice, source and exposure, claim, wallet, ledger, receipt and
  outbox state. Negative tests must attempt each direct write, and no external
  route, queue or role may reach the ledger writer. The grant baseline in force
  today is `migrations/billing/024_billing_svc_grants.up.sql`.
- **One allow-listed mutation per effect.** Every provider mutation method and
  raw endpoint must map to one closed plan effect from
  [`DESIGN.md`](DESIGN.md#6--what-you-can-be-charged-for). Each must also
  map to one purpose and step writer, and to at most one SDK or HTTP call.
  Adapter transport tests must require zero mutation retries and no redirects.
- **Build stamping.** Every shipped binary must stamp its commit and artifact
  identity. Today `.github/workflows/publish.yml:46` builds with
  `-ldflags="-s -w"` and stamps neither.
- **No reachable legacy money path.** Production readiness must not pass while
  any legacy money path is reachable.

## 8. Transparency and release gate

No external transparency service sits in the synchronous payment path. The
signed transactional outbox records engine ordering. The optional asynchronous,
payer-isolated state transparency log can later reveal a published rollback,
equivocation or split view. It does not prove an independently witnessed
pre-execution timestamp, and it does not prove that no row was hidden. A delayed
checkpoint must never authorize or delay collection. Reports keep
`state_assurance: attested` and the transparency status as separate fields.

Release stays manual while this architecture is introduced. Production promotion
requires:

1. reviewed source and target-state document consistency;
2. signed build provenance;
3. the full `go test` suite green, per §6, with no result inferred from log text;
4. shadow-intent reconciliation with no unexplained monetary differences;
5. public runtime identity, independently verifiable disclosure, a customer
   proof verifier, pinned trust-root and rotation evidence, an independent
   evidence edge, and a receipt verifier;
6. every §7 check passing, the route and IAM inventory included;
7. customer authorization, notice and tax readiness; and
8. `legacyMoneyPaths: 0`, with legacy provider credentials revoked.

No automatic merge or deployment is implied by a green unit-test run.

## 9. Known limits

Verification can prove which canonical bytes the independent consent client
bound into a customer proof. It can prove which notice bytes the carrier
reported at the configured destination, in the policy's terminal delivered
state, and when. It cannot prove a person read an email or understood a rendered
disclosure.

Public source plus build provenance makes tampering detectable. It cannot
protect against a fully compromised deployment, signing keys, database,
notification provider and payment-provider account acting together. The
remaining trust assumptions are stated in
[`SECURITY.md`](../SECURITY.md#adversary-model).

Provider payout and balance visibility differs by rail and by merchant contract.
A trace must mark unsupported evidence explicitly and must never treat it as
proof of absence.
