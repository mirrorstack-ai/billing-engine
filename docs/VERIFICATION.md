# Checking a charge instead of trusting one

Every other document here asks you to believe something. This one hands you a
procedure. The question it serves is narrow and yours: **this line on my bill —
where did the number come from, which code produced it, and who said I agreed?**

Most of the machinery below is a target contract rather than shipped behavior.
Rather than warn you once at the top, each step says at that step whether you
can run it today. Where you cannot, it starts 🔴.

The trust boundary, layout and money flows are in [`../README.md`](../README.md).
The rules are in [`DESIGN.md`](DESIGN.md), linked by invariant, never restated.

---

## 1 · What you can check, and how strong each check is

A verification result is worth what its weakest input is worth, so settle first
which question a given piece of evidence answers. There are seven, and none of
them substitutes for another.

| level | the question it answers | what you need |
|---|---|---|
| source | what is this revision designed to permit? | this repository, its docs, schemas and tests |
| build | which code became the running artifact? | Git commit, build provenance, artifact digest |
| deployment | which artifact and policies are running now? | engine-signed `Health` and `Capabilities` evidence |
| intent | how was this total derived and authorized? | the charge bundle of §3, and its digest |
| runtime state | what transition did the billing core report? | a signed `BillingDecisionProof` (unbuilt) |
| provider | what did an external rail report happened? | normalized, verified provider evidence |
| ledger | what monetary transition did MirrorStack commit? | a balanced append-only transaction and its corrections |

Reading public source does not tell you which binary charged your card, and a
provider's `paid` status does not tell you why that amount was allowed. Both are
the same confusion of levels.

The runtime-state level is the weakest, and the report says so out loud.
`state_assurance: attested` names the engine as the attester of its own
transition. [INV-012](DESIGN.md#inv-012) makes that identity externally visible,
not externally guaranteed, and §6 says what that costs you.

---

## 2 · Which build answered you

You cannot tie a charge to a revision of this repository until the running
service says which revision it is. Today it will not. The dispatcher's action
table has no `Health` and no `Capabilities` case (`cmd/account-api/main.go:88`),
and the one unauthenticated route returns a fixed body carrying no identity
(`cmd/account-api/main.go:647`).

`Health` must return the same identity fields whether the service is healthy or
not. A binary that hides its build while it is sick hides it when that matters
most. Those fields are the Git commit or the literal `unknown`, the artifact
digest, the build provenance identity and the binary role. They also carry the
receipt and bundle schema version, and an environment name holding no secret. An
executor whose build identity reads `unknown` must refuse to execute.

`Capabilities` must return, as signed fields a verifier can pin, everything the
offline check of §4 has to reuse:

- the active terms, price-book, tax, notice and routing-policy digests, the
  currency and scale registry revision, and each adapter's version, transport
  configuration and last conformance revision;
- every numeric limit — intent lines and bytes, source proof and bundle bytes,
  wallet lots, rollup depth and arity, provider-plan steps and bytes, and
  proof-apply batch size;
- the price-book, module-manifest, tax-rule and notice interpreter identities,
  which must equal the public verifier's, since a different interpreter is a
  different answer;
- readiness for the notifier, executor, evidence edge, receipt verifier, consent
  verifier and each display profile, `unsupported` until its own suite passes;
  and
- provider evidence strength, the callback credential class and replay policy,
  which reconciliation read is in force ([INV-007](DESIGN.md#inv-007)), and
  every reachable legacy money-moving path with a count.

The intent-only claim requires `legacyMoneyPaths: 0`. A strong new surface
beside one weak legacy route is not a strong deployment. It is the weak route
with better documentation.

**Pin the root, not the response.** A verifier that learns its trust root from
the service it is checking has checked nothing. That root must ship in this
repository, in signed releases, and in a separately operated channel, and no
relayed response may introduce a new one. Every signed statement must carry an
algorithm, key id, issuer, audience, environment, schema, signature domain,
payload digest, validity interval and checkpoint. A key valid for
`billing-capabilities/v1` therefore cannot sign `customer-acceptance/v1`.

---

<a id="3-canonical-charge-bundle"></a>

## 3 · The charge bundle, field by field

This is the only place the bundle contract is written down. It lived in four
places once, the copies drifted, and every other document now links here.

The bundle has one versioned canonical encoding. It fixes field names and order,
integer and decimal form, Unicode normalization, timestamps and zones, and
absent versus explicit zero or null. It also fixes the order of lines, sources,
evidence and ledger entries, and the digest and signature domains. Invalid
Unicode, duplicate
keys, unknown critical fields, out-of-range integers and unsupported schema
versions are refused. The digest binds canonical bytes, never a lossy parser's
reading of them.

Exports replace sensitive source fields with domain, payer, object and
field-bound hiding commitments under unique random nonces. A raw hash of an
address or a tax id is one lookup table away from the value, so it is forbidden.
Only the owning payer obtains the opening, rotation preserves historical
openings, and the redaction is covered by the digest.

Every element is required unless the "when" column names a condition. Every type
marked (unbuilt) returns nothing from `git grep` on `origin/main` today.

| # | element | what it binds | when |
|---|---|---|---|
| 1 | intent | canonical bytes and digest of the sealed `ChargeIntent` (unbuilt) | always |
| 2 | commercial identity | the `CommercialIdentityBinding` (unbuilt) that source, tax and wallet evaluation used | always |
| 3 | merchant of record | `MerchantOfRecordBinding` (unbuilt), its `MerchantBindingSet` (unbuilt) membership proof, and the settlement route | always |
| 4 | source authority | one tagged form: service leaf with window allocation root and signed checkpoint transition, one-time replay identity, auto-top-up trigger, or receivable capacity | always |
| 5 | source ids | source event and aggregate ids, or privacy-preserving hashes of them | always |
| 6 | rating | rating source commitments, module billing-manifest version, interpreter and limit revision, formula, integer scale, rounding step, subtotal, rating credit, tax, total | always |
| 7 | policy digests | terms, price book, tax, notice, time readiness, rail routing, autonomy and execution plan, plus the observed time uncertainty wherever it was used | always |
| 8 | tax | status `final`, `not_applicable` or `unknown`, its verification class, and the evidence behind it — [DESIGN §7](DESIGN.md#7--tax-and-what-it-refuses-to-guess) | always |
| 9 | funding | frozen `FundingPlan` (unbuilt), credit-lot and exposure reservations, gross obligation, wallet application, provider remainder, wallet generation and range proof, caps | always |
| 10 | ceilings | ceilings as evaluated at decision time, authorization scope and lineage head, carried exposure, and consume-time validity | always |
| 11 | authority branch | one tagged `AuthorityEvidence` (unbuilt) debit branch and no second one — see the note below | always |
| 12 | decision proof | signed `BillingDecisionProof` (unbuilt): payer proof head, before and after commitments and generations, transaction, build and policy identity, outbox record | always |
| 13 | build identity | engine Git commit, artifact digest, receipt schema version, build provenance | always |
| 14 | ledger | balanced ledger transaction ids and entries, plus the outbox checkpoint | always |
| 15 | service accrual | `ServiceAccrualExposure` (unbuilt) arithmetic from reserved bound to settled line; a deferred prepaid lot adds its expiry rule, window, generation and release evidence | a service accrual funded it |
| 16 | subscription | accepted `SubscriptionOffer` (unbuilt) and its activation-gated schedule receipt: cadence, zone, anchor, recognition rules, generation, first-settlement compare-and-swap | subscription charge |
| 17 | responsibility transfer | shared transfer commitment, two audience-specific disclosure digests, both payer heads and cutoffs, generation CAS, source and exposure partition, retained old claims | payer changed |
| 18 | auto top-up | trigger reservation and epoch, creation snapshot, owning intent, consume-time recheck, result and time, and the atomic credit and bonus grant with trigger close | `auto_topup` |
| 19 | provider plan | autonomy policy and finite `ProviderExecutionPlan` (unbuilt), with per-step envelope, consume, opaque permit, egress identity, effect class, and zero-retry transport | provider execution occurred |
| 20 | provider evidence | enclave, executor, adapter, workload and credential attestations each naming its attester, `PaymentAttempt` (unbuilt) transitions, the debit instrument, evidence strength | provider execution occurred |
| 21 | read-back or callback | reader, broker or verifier artifact, its workload, credential class and scope, the request limits in force, replay result, evidence class, checkpoint, attester | that path supplied evidence |
| 22 | corrections | correction, refund and dispute links | any exist |

**Row 11 has two forms and never both.** `debit_customer_present` carries the
intent acceptance, its proof, and a current one-time or standing authorization.
`standing_automatic` carries the authorization acceptance, its proof, a terminal
notice receipt, the completed wait, and a revocation-readiness receipt. Either
binds payer sequence, head and cutoff, factor and verifier revision, scope,
lineage, exposure, and the revocation result at dispatch —
[INV-013](DESIGN.md#inv-013).

**Row 17 is two views, not one.** The new payer's copy carries no private
financial detail of the old payer, and the transfer moves no mandate, wallet,
tax or notice state.

A current `Health` response never stands in for a historical binding. Unknown,
revoked, expired, wrong-role or substituted artifacts fail.

**Sibling receipts are not charge bundles.** Refund, mandate-revocation and
payment-method-setup receipts (all unbuilt) have their own schemas and signature
domains. None can satisfy debit authority, a debit funding plan, or a collection
notice. That matters most for the setup receipt, the one an attacker would pass
off as permission to charge. What each requires is one table, in
[DESIGN §6](DESIGN.md#6--what-you-can-be-charged-for), not repeated here.

---

## 4 · Checking a charge yourself, offline

🔴 **You cannot run this today.** There is no verifier in the tree: `cmd/` on
`origin/main` holds seven binaries and none of them is one
(`.github/workflows/publish.yml:35-41` builds six of them). The command below is
the target shape, and §5 is the part of this file you could turn on first.

```bash
billing-verify verify charge-bundle.json
```

It must reach a verdict without contacting MirrorStack or any payment provider.
That constraint is the whole point: a safety property you can check only by
trusting our staging environment is one you cannot check. It must confirm:

1. canonical encoding, bundle digest, and the build, source and policy
   references;
2. the signed source-allocation transition, its leaf and window binding, and the
   conversion from reserved bound to settled line;
3. module billing-manifest version, price selection and effective windows;
4. integer arithmetic, tiers, credits, tax and rounding, reproduced from the
   public content-addressed tax artifact, and that line, subtotal, tax and total
   agree;
5. that merchant of record, scope, lineage, carried exposure, currency, cadence,
   ceilings, method, autonomy policy and window all agree;
6. the tagged authority branch of §3 row 11, and the subscription schedule or
   auto-top-up trigger, recheck, grant and close behind it;
7. funding identity, `gross = wallet allocation + provider remainder`, and lot
   selection and rollup inside the limits `Capabilities` published;
8. a finite provider plan with closed effect classes, one envelope, consume,
   permit, egress and evidence chain per step, and at most one debit or return
   against the named instrument; and
9. the `BillingDecisionProof` (unbuilt) predicate, heads, generations and outbox
   binding, then per-currency ledger balance and correction structure.

A missing or swapped customer-present branch fails. A setup bundle requires
`setup_customer_present`, which is rejected inside a debit bundle, and an
unverifiable setup receipt cannot support standing authority.

The result is five fields, not a word:

```text
verdict:                      verified | invalid | unsupported
state_assurance:              attested
state_transparency:           verified | pending | unsupported | invalid
historical_provider_evidence: not_applicable | verified | invalid | unsupported
live_provider_status:         not_requested | verified | pending | unsupported | invalid
```

`verdict: verified` means the supplied contract, arithmetic, signatures and
transition chain held. It never upgrades `state_assurance`, because arithmetic
cannot witness a hidden row. An unknown schema is `unsupported` and a missing
async publication is `pending`, neither of which moves the arithmetic. A
conflicting published history sets `state_transparency: invalid` and the whole
verdict with it. A provider-funded charge needs its historical debit evidence,
so missing evidence is `invalid`. The optional online refresh moves
`live_provider_status` alone, and reachability never moves an offline verdict.

**Golden vectors are public evidence, not fixtures.** They must pin canonical
bytes and digests for intents and receipts, each charge kind and tax status, and
each currency exponent with its boundary rounding. They must also pin standing
and one-time authorization with scope supersession, notice bytes and wait
calculation, ledger and correction chains, and normalized provider fixtures.
Changing one takes a version change, and regenerating a constant to pass a test
is not a fix.

---

<a id="7-static-architecture-checks"></a>

## 5 · What CI enforces against this tree

Everything above constrains code that does not exist yet. This section
constrains code that does, which makes it the honest place to start.

🔴 None of it runs today. `.github/workflows/ci.yml` applies the migrations,
then runs `go vet ./...` (`:79`), `go build ./...` (`:82`) and `go test`
(`:92`, `:100`). That is the whole gate.

**Provider SDK confinement.** Provider SDK imports and raw provider-mutation
hosts may appear only in adapter and enclave packages, and in the generated
egress allow-list. Ten non-test files import `github.com/stripe/stripe-go`
today, among them `internal/shared/stripe/client.go`,
`internal/account/webhook/router.go` and
`internal/account/autotopup/executor.go`.

**Write-port isolation.** Provider-write interfaces may be injected only into
the isolated executor deployment. The planner, read, usage-ingress, notifier and
reconciler binaries must not compile against a write port at all.

**One enclave owner per mutation credential.** Generated secret-to-workload,
IAM, key and egress inventories must show that each mutation credential has one
exclusive owning enclave. They must also show that no backup or admin job can
read it, and that mutation endpoints are denied outside it —
[INV-007](DESIGN.md#inv-007).

**Route and IAM inventory.** This check is customer-visible, through the signed
`Health` evidence of §2, and it is defined nowhere else here. A generated
inventory must prove the account API is Lambda-invoke-only, and that the public
health integration cannot dispatch an RPC action. It must prove each provider
webhook is a separate authenticated ingress, and that dispatch metering reaches
`RecordUsage` and nothing else. It must be derived from entrypoint and
infrastructure source, never from a diagram label. It is what stops a status
read or a usage ingest from triggering a payment.

The facts it has to reproduce hold today. `lambda.Start` runs the invoke handler
(`cmd/account-api/main.go:857`), the internal-secret group wraps the local
routes (`:652`), and `RecordUsage` alone sits behind `X-MS-Meter-Secret`
(`:776-777`, `internal/shared/auth/internal_secret.go:47`). The webhook
receivers are separate binaries (`cmd/account-webhook`,
`cmd/account-webhook-eventbridge`).

**No monetary or authority field on a public request struct.** No
caller-supplied amount may reach the executor. 🔴 This one fails today.
`GrantCreditsRequest` carries `AmountMicros` and a caller-asserted `Actor`
(`internal/account/billing/types.go:434-435`), reached through the
`GrantCredits` action (`cmd/account-api/main.go:187`). The metering request is
the shape to aim at, carrying a metric and a value and no money
(`internal/account/usage/types.go:71`).

**Database authority.** Generated role, table and procedure grants, key
ownership, migration roles and operator paths must show one thing. Only trusted
billing-core procedures mutate or sign proof heads, authorization, notice,
source, claim, wallet, ledger, receipt and outbox state. A negative test must
attempt each direct write. 🔴 The baseline in force is far looser:
`migrations/billing/024_billing_svc_grants.up.sql` grants `billing_svc` blanket
`SELECT, INSERT, UPDATE, DELETE` on every table in `ms_billing`.

**One allow-listed mutation per effect.** Every provider mutation method and raw
endpoint must map to one closed effect from
[DESIGN §6](DESIGN.md#6--what-you-can-be-charged-for). Each must also map to one
purpose and step writer, and to at most one SDK or HTTP call. Adapter transport
tests must require zero mutation retries and no redirects.

**Build stamping.** Every shipped binary must stamp its commit and artifact
identity, or §2 has nothing to report. 🔴 The build stamps neither today:
`.github/workflows/publish.yml:46` passes only `-ldflags="-s -w"`.

**No reachable legacy money path.** Readiness must not pass while one is
reachable, which is the `legacyMoneyPaths: 0` field of §2.

A green test run implies no merge and no deployment. The conditions that must
hold before production intent execution is enabled are listed once, in
[DESIGN §11](DESIGN.md#11--getting-from-here-to-there), and release stays manual
until they do.

---


## 6 · What none of this proves

🔴 **A receipt proves what the engine was told, not that you agreed.** This is
the largest limit here, and it is not a detail. `api-platform` holds your
session and relays your acceptance, and the engine treats the subject id it is
handed as opaque (`internal/account/billing/service.go:105-111`). A compromised
or buggy caller can assert an acceptance you never made, and nothing here
disproves it. [INV-006](DESIGN.md#inv-006) states that as a trust assumption
rather than a control. What survives is
reproducibility: the disclosure, its digest and the receipt stay readable, so a
fabricated acceptance is something you can point at afterwards. That is
detection, not prevention.

🔴 **Almost nothing in §2, §3 and §4 exists.** The offline verifier is unbuilt,
and so are the signed identity objects it would check. `ChargeIntent`,
`BillingDecisionProof`, `AuthorityEvidence` and `FundingPlan` return nothing
from `git grep` on `origin/main`. Until they ship, a customer holding a bill has
this repository and §5, and no procedure.

**`attested` is not `witnessed`.** The engine signs its own transition. The
optional transparency log can later reveal a rollback or a split view. It cannot
prove no row was hidden, and a delayed checkpoint must never delay or authorize
a collection.

**Delivered is not read.** Verification can prove which bytes a consent client
bound into your proof, and which bytes a carrier reported delivered, and when.
It cannot prove a person read them or understood them.

**Provider evidence differs by rail.** Payout and balance visibility depends on
the rail and the merchant contract. A trace must mark unsupported evidence as
unsupported, never as proof that nothing happened.

**A total compromise defeats all of it.** Public source plus build provenance
makes tampering detectable, not impossible. A deployment, its signing keys, its
database, its notifier and its provider account acting together can produce a
consistent lie. The trust assumptions are enumerated in
[`../SECURITY.md`](../SECURITY.md#adversary-model) and the current defects in
its [gap register](../SECURITY.md#known-current-gaps). This file keeps no second
copy of either.
