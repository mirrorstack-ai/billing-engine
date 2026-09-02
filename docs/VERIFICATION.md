# Checking a charge instead of trusting one

Every other document here asks you to believe something. This one is meant to
hand you a procedure. The question it serves is narrow and yours: **this line on
my bill — where did the number come from, which code produced it, and who said I
agreed?**

Read the next section before anything else. It says which parts of this file you
can act on from a laptop, which parts need an account, and which parts nobody
outside MirrorStack can run at all. The short version: the checks over *this
source tree* are real and you can run them in about five minutes. The checks that
would tie **a specific charge on your bill** to a specific build are not
available to you, and several of them are not built.

The trust boundary, layout and money flows are in [`../README.md`](../README.md).
The rules are in [`DESIGN.md`](DESIGN.md), linked by invariant, never restated.

---

## Before you start: what you can actually reach

Three tiers of access appear in this file. Every step below is tagged with the
one it needs, and 🔴 marks a step nobody outside MirrorStack can perform.

| tier | what it is | who has it |
|---|---|---|
| **A — public** | this repository, Go 1.26, Docker | anyone, right now |
| **B — account** | a MirrorStack account and the bills it shows you | you, as a customer |
| **C — 🔴 production** | `lambda:InvokeFunction` on the account API, or the internal RPC secret | MirrorStack staff only |

There is no tier between B and C. The account API has exactly one
unauthenticated surface in production and it carries no evidence: an API Gateway
health probe answered by a fixed `{"status":"ok"}` body
(`cmd/account-api/main.go:879-884`). Every other action — including
`Capabilities`, the one that reports which build is running — is behind either
the IAM grant on `lambda.Invoke` (`cmd/account-api/main.go:864-870`) or the
`X-MS-Internal-Secret` header on the local HTTP router
(`:663`, `internal/shared/auth/internal_secret.go:35`). So a customer cannot ask
the running service what it is.

### Tier A: four things you can check in five minutes

Clone the repository at the commit you want to reason about, then:

```bash
go vet ./... && go build ./...
go test -race -count=1 ./internal/architecture/ ./internal/shared/signing/ ./internal/intent/
```

No database, no Docker, no credentials. That suite is the enforcement described
in §5 — the mutation inventory, the legacy-path count, the SDK reach bound, the
sealed-document digest. If it fails on a commit, that commit's claims in §5 are
false and you have proven it yourself.

```bash
go test -tags=integration -race -count=1 ./internal/architecture/
```

Adds the checks that need a real Postgres 17 (they boot their own container
through testcontainers and **skip silently when Docker is unreachable** —
`internal/shared/testutil/db.go`, and CI sets `REQUIRE_DOCKER=1` for exactly that
reason, `.github/workflows/ci.yml:104`). These cover the database seal and the
read-only ops role.

```bash
go list -deps ./cmd/intent-executor | grep billing-engine/internal/provider
go list -deps ./cmd/account-api     | grep billing-engine/internal/provider
```

The first prints a package; the second prints nothing. That is the claim "the
executor is the only binary that links a provider adapter", checked against the
compiler's own view of the dependency graph rather than against a comment. Run it
for all ten binaries in `cmd/` if you want the whole answer.

```bash
printf 'd6905a8cd8099ea30eabadee9bc638fdd554e5868b48aa2a9c3daf2fd43e38e4' \
  | xxd -r -p | shasum -a 256 | cut -c1-32
# 7cd37ff8ba25c9d79445918a5eab5d17
```

That is the pinned trust root of §2, verified without running any of our code:
the key id is the first 128 bits of the SHA-256 of the public key bytes
(`internal/shared/signing/signing.go:253`), and both values are literals in
`internal/shared/signing/trustroot.go:132-138`.

### Tier B: what an account gets you

Your bill, through the customer surfaces `api-platform` renders. It does not get
you a signed statement, a charge bundle, a policy revision id, or a way to ask
which build produced the number. §6 lists what that costs you.

### 🔴 Tier C: what the rest of this file needs

§2's `Capabilities` read, and anything that inspects a deployment rather than a
source tree. If you are reading this to decide whether to build on MirrorStack,
treat §2 as a description of what we owe you and have not yet delivered, not as
something you can go and check.

---

## 1 · What you can check, and how strong each check is

A verification result is worth what its weakest input is worth, so settle first
which question a given piece of evidence answers. There are seven, and none of
them substitutes for another.

| level | the question it answers | what you need | reachable today |
|---|---|---|---|
| source | what is this revision designed to permit? | this repository, its docs, schemas and tests | **A** — yes |
| build | which code became the running artifact? | Git commit, build provenance, artifact digest | **A** for the stamping mechanism; 🔴 **C** to read it off a deployment |
| deployment | which artifact and policies are running now? | engine-signed `Health` and `Capabilities` evidence | 🔴 **C**, and unsigned even there |
| intent | how was this total derived and authorized? | the charge bundle of §3, and its digest | 🔴 no — the bundle is not produced |
| runtime state | what transition did the billing core report? | a signed `BillingDecisionProof` | 🔴 no — the type does not exist |
| provider | what did an external rail report happened? | normalized, verified provider evidence | 🔴 no — one provider client serves reads and writes |
| ledger | what monetary transition did MirrorStack commit? | a balanced append-only transaction and its corrections | 🔴 no customer read path |

Reading public source does not tell you which binary charged your card, and a
provider's `paid` status does not tell you why that amount was allowed. Both are
the same confusion of levels.

The runtime-state level is the weakest, and the report is designed to say so out
loud. `state_assurance: attested` names the engine as the attester of its own
transition. [INV-012](DESIGN.md#inv-012) makes that identity externally visible,
not externally guaranteed, and §6 says what that costs you.

---

## 2 · Which build answered you

You cannot tie a charge to a revision of this repository until the running
service says which revision it is. Two of the three pieces now exist. The third —
your ability to ask — does not.

**The build is stamped.** `.github/workflows/publish.yml:41-42` sets a commit and
environment stamp and `:57` adds the artifact name and binary role, for all eight
published binaries. The values land on unexported variables in
`internal/shared/buildinfo`, whose default is the literal `unknown`, and
`Identified()` requires both commit and artifact to be something else. An
unstamped `go build` therefore reports itself unidentified rather than plausible.

**An executor whose build identity reads `unknown` refuses to execute.**
`cmd/intent-executor/main.go:162-172` is a three-condition readiness function,
called before anything else at `:45`; the second condition is
`!caps.Build.Identified`. See §5 for the other two.

**`Capabilities` exists, and you cannot call it.** It is an action on the
dispatcher (`cmd/account-api/main.go:99`) and a route on the local HTTP router
(`:669`), and it returns `capabilities.Current()` — build identity, the legacy
money-path count, and two booleans derived from them
(`internal/account/capabilities/capabilities.go`). Both transports are
authenticated. There is no public read.

**`Health` is not a dispatcher action, and the production probe carries no
identity.** The handler that returns build identity unconditionally, healthy or
not, is `cmd/account-api/main.go:812`, registered at `:658` — on the router that
`main` builds only in local HTTP mode, which it selects only when the Lambda
environment is absent (`:899-905`). In production the sole unauthenticated
response is the fixed body at `:879-884`. A binary that hides its build while it
is sick hides it when that matters most; today it hides it always.

What a `Health` response must eventually carry: the Git commit or the literal
`unknown`, the artifact digest, the build provenance identity and the binary
role, plus the receipt and bundle schema version and an environment name holding
no secret.

What `Capabilities` must eventually carry, as signed fields a verifier can pin —
none of which it returns today, and the package says so about itself:

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

The last of those is the only one built. `legacyMoneyPaths` is
`internal/account/capabilities/capabilities.go:52`, and it reads **3**. The
intent-only claim requires 0. A strong new surface beside one weak legacy route
is not a strong deployment. It is the weak route with better documentation.

### Pin the root, not the response — and the root ships here

A verifier that learns its trust root from the service it is checking has checked
nothing. That root must ship in this repository, in signed releases, and in a
separately operated channel, and no relayed response may introduce a new one.

**This part is done, and you can check it without running our code.**
`internal/shared/signing/trustroot.go:132-138` pins one real key for the
`billing-evidence/v1` domain. Four properties make the pin worth something, and
each is a few lines you can read:

- **The id is derived from the key, not asserted beside it.** `NewTrustRoot`
  recomputes `KeyID(pub)` and refuses an entry whose id does not match
  (`internal/shared/signing/trustroot.go:61-64`), so a wrong pin fails at construction rather than as a
  confusing bad-signature error at the first verification. The shell command in
  the access section is that same derivation, run by you.
- **Domain is part of the lookup key, not a field compared afterwards.** The map
  is keyed on `rootKey{domain, id}` (`internal/shared/signing/trustroot.go:25-28`, looked up at `:82`),
  so a key valid for `billing-capabilities/v1` cannot verify a
  `customer-acceptance/v1` statement even on an id collision.
- **The slice is unexported.** No package in the tree can append to it at
  runtime. Adding a key is an edit to that file with a diff attached.
- **Pinning the public half is not provisioning the private one.** The file says
  so itself. Verification is armed by the pin; signing is armed only where seed
  material is present, and its absence is deliberately not an error.

Every signed statement must carry an algorithm, key id, issuer, audience,
environment, schema, signature domain, payload digest, validity interval and
checkpoint — the `Statement` shape in `internal/shared/signing/signing.go`.

🔴 **You will not receive one.** Two binaries construct a signer and an evidence
recorder — `cmd/intent-executor/main.go:77` and `cmd/billing-cycle/main.go:459`,
the latter only when `BILLING_CYCLE_INTENT_CUTOVER` arms the proposer
(`:426-435`). What they write goes to `ms_billing.evidence_records`, and that
table has exactly one writer (`internal/intent/store/evidence.go:56`) and **no
reader anywhere in the tree** outside tests and a privilege probe
(`cmd/intent-shadow/grants.go:97`). A pinned root with nothing to verify is a
control that is ready before the thing it controls. That is the correct order,
and it is not yet a facility you can use.

---

<a id="3-canonical-charge-bundle"></a>

## 3 · The charge bundle, field by field

This is the only place the bundle contract is written down. It lived in four
places once, the copies drifted, and every other document now links here.

🔴 **No bundle is produced today, and none is served to a customer.** What
follows is the contract a bundle must satisfy, not a description of a document
you can obtain. Read it as the specification §4's verifier would be written
against.

The bundle has one versioned canonical encoding. It fixes field names and order,
integer and decimal form, Unicode normalization, timestamps and zones, and
absent versus explicit zero or null. It also fixes the order of lines, sources,
evidence and ledger entries, and the digest and signature domains. Invalid
Unicode, duplicate keys, unknown critical fields, out-of-range integers and
unsupported schema versions are refused. The digest binds canonical bytes, never
a lossy parser's reading of them.

Exports replace sensitive source fields with domain, payer, object and
field-bound hiding commitments under unique random nonces. A raw hash of an
address or a tax id is one lookup table away from the value, so it is forbidden.
Only the owning payer obtains the opening, rotation preserves historical
openings, and the redaction is covered by the digest.

Every element is required unless the "when" column names a condition. **(unbuilt)**
marks a type that has no declaration in this tree at this commit — check any of
them with `grep -rn "type MerchantOfRecordBinding " --include='*.go' .`

| # | element | what it binds | when |
|---|---|---|---|
| 1 | intent | canonical bytes and digest of the sealed `ChargeIntent` — `internal/intent/chargeintent.go:254` | always |
| 2 | commercial identity | the `CommercialIdentityBinding` (unbuilt) that source, tax and wallet evaluation used | always |
| 3 | merchant of record | `MerchantOfRecordBinding` (unbuilt), its `MerchantBindingSet` (unbuilt) membership proof, and the settlement route | always |
| 4 | source authority | one tagged form: service leaf with window allocation root and signed checkpoint transition, one-time replay identity, auto-top-up trigger, or receivable capacity | always |
| 5 | source ids | source event and aggregate ids, or privacy-preserving hashes of them | always |
| 6 | rating | rating source commitments, module billing-manifest version, interpreter and limit revision, formula, integer scale, rounding step, subtotal, rating credit, tax, total | always |
| 7 | policy digests | terms, price book, tax, notice, time readiness, rail routing, autonomy and execution plan, plus the observed time uncertainty wherever it was used | always |
| 8 | tax | status `final`, `not_applicable` or `unknown`, its verification class, and the evidence behind it — [DESIGN §7](DESIGN.md#7--tax-and-what-it-refuses-to-guess) | always |
| 9 | funding | frozen `FundingPlan` — `internal/intent/predicate/state.go:120`, four fields — plus credit-lot and exposure reservations, gross obligation, wallet application, provider remainder, wallet generation and range proof, caps, all still unbuilt | always |
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

Rows 1 and 9 name types that exist; every other `(unbuilt)` marker is a type with
no declaration. The predicate treats that honestly rather than skipping the
clause: `internal/intent/predicate/clause.go:57` opens the group of clauses "whose
supporting records are unbuilt", and each refuses. The evidence they would read
arrives as `predicate.UnbuiltEvidence`
(`internal/intent/predicate/state.go:232-247`), fourteen booleans, and the only
production constructor passes the zero value (`cmd/intent-executor/main.go:190`).

🔴 **Row 6 is not populated as specified, and this is the gap that matters most
to a module developer.** `intent.Line` carries meter, module, module version,
quantity and unit price (`internal/intent/chargeintent.go:33-41`), and the store
persists all five (`internal/intent/store/store.go:179`). But the only production
producer collapses them: `internal/intent/proposer/proposer.go:267` calls
`NewLine(description, sourceRef, "1", 1, amountMicros)` — the meter becomes
free-text prose, the module version becomes the literal `"1"`, quantity becomes 1
and the whole derived amount becomes the unit price. The comment there explains
why (it is the only decomposition of a single figure that satisfies
`amount = quantity × price`), and the consequence is unchanged: the digest
attests to one opaque number. The rater that does fill all five fields
(`internal/intent/rater.go:184`) has one caller, `cmd/intent-shadow`, which by
construction cannot move money.

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

🔴 **You cannot run this today, at any tier.** There is no verifier in the tree.
`cmd/` holds ten binaries — `account-api`, `account-webhook`,
`account-webhook-eventbridge`, `billing-cycle`, `infra-egress-sync`,
`infra-ssr-compute-sync`, `intent-executor`, `intent-shadow`,
`pm-default-backfill`, `signing-keygen` — and
`.github/workflows/publish.yml:43-51` publishes eight of them. None is a
verifier, there is no `testdata` directory anywhere in the repository, and there
are no golden vectors. The command below is the target shape, and §5 is the part
of this file that is real.

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

The one thing here that *is* enforced today is the charge vocabulary the bundle
would carry. `internal/intent/catalog.go:59-67` closes the catalog at seven
kinds — `platform_base`, `module_usage`, `tax`, `subscription_start`,
`credit_purchase`, `auto_topup`, `collect_receivable` — and a kind absent from
that map cannot be sealed.

---

<a id="7-static-architecture-checks"></a>

## 5 · What CI enforces against this tree

This section constrains code that exists, which makes it the part of this file
you can act on. Everything here is **tier A**: clone, `go test`, disagree.

`.github/workflows/ci.yml` applies and rolls back every migration, then runs
`go vet ./...` (`:79`), `go build ./...` (`:82`), `go test -race -count=1 ./...`
(`:92`) and `REQUIRE_DOCKER=1 go test -tags=integration ...` (`:104`).
`internal/architecture` is an ordinary package of twenty-seven files, so those
last two commands run every check below that has one. `REQUIRE_DOCKER` matters:
without it a runner that lost Docker prints `ok` for a suite that started no
container.

| check | status | where |
|---|---|---|
| provider SDK confinement | **enforced, with named debt** | `internal/architecture/sdk_confinement.go` |
| one allow-listed mutation per effect | **enforced** | `internal/architecture/allowlist.go`, `internal/architecture/surface.go` |
| legacy money-path count is true | **enforced** | `internal/architecture/surface_test.go:135` |
| the execution predicate has one caller | **enforced** | `internal/architecture/predicate_single_caller_test.go` |
| build stamping | **enforced by the workflow** | `.github/workflows/publish.yml:41-42,57` |
| database seal on sealed columns | **enforced (needs Docker)** | `internal/architecture/sealed_columns_integration_test.go` |
| read-only role holds no write privilege | **enforced (needs Docker)** | `internal/architecture/billing_ro_grants_integration_test.go` |
| no caller-supplied money field | 🔴 **countdown, not a wall** | `internal/architecture/request_fields_allowlist.go` |
| write-port isolation | 🔴 **one binary only** | `internal/architecture/shadow_isolation_test.go` |
| one enclave owner per mutation credential | 🔴 **not built** | — |
| route and IAM inventory | 🔴 **not built** | — |
| no reachable legacy money path | 🔴 **not met — three remain** | `internal/account/capabilities/capabilities.go:52` |

**Provider SDK confinement.** Provider SDK imports and raw provider-mutation
hosts may appear only in adapter and enclave packages, and in the generated
egress allow-list. `ScanProviderSDKImporters`
(`internal/architecture/sdk_confinement.go:25`) walks `internal` and `cmd`, and
the test refuses both an unlisted importer and a listed one that no longer
imports — so the inventory cannot drift in either direction. Ten non-test files
import `github.com/stripe/stripe-go`. Two are the adapter, three are callback
transports, two are reusable test support, and **three are named debt**:
`internal/account/autotopup/executor.go`,
`internal/account/creditpurchase/executor.go` and
`cmd/pm-default-backfill/main.go` each construct provider requests outside the
adapter. `TestProviderSDKDebtOnlyShrinks` makes that count a ratchet.

**One allow-listed mutation per effect.** Every provider mutation method maps to
one closed effect (`internal/architecture/surface.go`, `EffectMutate` /
`EffectCollect`), and every call site maps to one allow-list entry with the
reason it exists (`internal/architecture/allowlist.go`). A call site not listed
fails the build; an entry with no matching call site also fails. Adapter transport requires zero
mutation retries, and that is a constant rather than a policy:
`internal/shared/stripe/client.go:71`, `const maxNetworkRetries int64 = 0`.

**The legacy count is measured, not asserted.**
`internal/architecture/surface_test.go:135` re-derives the number of collecting
call sites from an AST scan and fails if it disagrees with
`capabilities.LegacyMoneyPaths`. This is the one place a self-report is turned
into a checked claim, and it is why the number in §2 is worth reading. The three
exclusions from the count are earned rather than asserted — the intent adapter is
excluded only because it is reachable solely through the executor, which is the
predicate's single caller, and `TestExecutionPredicateHasAtMostOneCaller`
enforces that independently.

**Write-port isolation.** Provider-write interfaces may be injected only into the
isolated executor deployment; the planner, read, usage-ingress, notifier and
reconciler binaries must not compile against a write port at all. 🔴 **One
binary has this check.** `internal/architecture/shadow_isolation_test.go` runs `go list -deps` over
`cmd/intent-shadow` and fails if it links the SDK, the adapter surface, a
provider adapter, the executor, the auto-top-up executor or the legacy cycle.
Nothing equivalent guards the others, and running the same command yourself shows
why it matters: `cmd/account-api` and `cmd/billing-cycle` both link
`internal/shared/stripe`.

**One enclave owner per mutation credential.** Generated secret-to-workload, IAM,
key and egress inventories must show that each mutation credential has one
exclusive owning enclave, that no backup or admin job can read it, and that
mutation endpoints are denied outside it — [INV-007](DESIGN.md#inv-007). 🔴 **No
such inventory exists in this repository.**

**Route and IAM inventory.** A generated inventory must prove the account API is
Lambda-invoke-only, that the public health integration cannot dispatch an RPC
action, that each provider webhook is a separate authenticated ingress, and that
dispatch metering reaches `RecordUsage` and nothing else. It must be derived from
entrypoint and infrastructure source, never from a diagram label. It is what
stops a status read or a usage ingest from triggering a payment. 🔴 **Not built.**

The facts it would have to reproduce hold today, and you can read them:
`lambda.Start` runs the invoke handler (`cmd/account-api/main.go:900`), the
health probe returns before `dispatch` is ever reached (`:879-884`), the
internal-secret group wraps the local RPC routes (`:663`), and `RecordUsage`
alone sits behind `X-MS-Meter-Secret` (`:793-794`,
`internal/shared/auth/internal_secret.go:47`) — both middlewares returning 503
rather than opening the route when their secret is unset (`:59`). Provider
ingress lives in separate binaries: Stripe now arrives only on an EventBridge
partner bus consumed by `cmd/account-webhook-eventbridge`, and
`cmd/account-webhook` is a deliberately empty HTTP ingress kept for local PSPs
that cannot publish to an AWS partner bus (`cmd/account-webhook/main.go:1-33`).

**No monetary or authority field on a public request struct.** No caller-supplied
amount may reach the executor. 🔴 **This is a countdown, not a wall, and it is
red.** `internal/architecture/request_fields_allowlist.go` enumerates every
caller-supplied money or authority field with a verdict: `ceiling` for a number
that can only reduce what is chargeable, `pending-migration` for one that lets
the caller decide something the engine must derive. Seven fields carry
`pending-migration`, including `GrantCreditsRequest.AmountMicros` and its
caller-asserted `Actor` (`internal/account/billing/types.go:465-466`, reached
through `cmd/account-api/main.go:201`) and
`StartCreditPurchaseRequest.AmountMicros`. `TestCallerSuppliedMoneyDebtOnlyShrinks`
prevents the count rising. The metering request is the shape to aim at, carrying
a metric and a value and no money (`internal/account/usage/types.go:72`, with
`Metric` and `Value` at `:90-91`).

**Database authority.** Only trusted billing-core procedures should mutate or
sign proof heads, authorization, notice, source, claim, wallet, ledger, receipt
and outbox state, with a negative test attempting each direct write. Two pieces
of that exist and one does not.

*Built — the sealed document cannot be edited, and the check runs twice.* In Go,
`intent.Rehydrate` (`internal/intent/chargeintent.go:786`) re-seals every stored
field, recomputes the digest and refuses on mismatch with `ErrDigestMismatch`
(`:766`) — so a restored backup, a replicated row, a migration that rewrote a
column in passing, and a deliberate edit by someone holding the database
credential all fail identically. In the database,
`migrations/billing/063_seal_every_sealed_column.up.sql:102-125` freezes the
**whole row minus a named mutable set** (`state`, `state_changed_at`,
`reserved_micros`) rather than an enumerated tuple of frozen columns, so a column
added tomorrow is frozen tomorrow.

*Built — a genuinely read-only operator role, and the story of why that took
three migrations.* Migration 058 held the right grants but was wrapped in
`IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro')`, nothing
created the role, so in production it took the `ELSE` branch, raised a notice,
exited 0 and was recorded as applied having granted nothing. Migration 064's
`REVOKE` of `evidence_records` was gated identically and also did nothing. A
migration runs once, so 068 re-issues both, in an order that is load-bearing
(grant then revoke), and **raises instead of skipping** if the role is missing.
Four integration tests check the result, including that a table created after the
grants is still readable and that the role holds no write privilege.

🔴 *Not built — the service role is still broad.*
`migrations/billing/024_billing_svc_grants.up.sql:20-22` grants `billing_svc`
blanket `SELECT, INSERT, UPDATE, DELETE` on every table in `ms_billing`, plus
default privileges for future tables. There is no "only trusted procedures
mutate" property, and the seal trigger above is what stands in for it.

🔴 *And `billing_ro` is not a facility for you.* It is an operator identity. It
bounds what a MirrorStack engineer can read — notably not `evidence_records` —
which is a control on us. It grants no customer any read at all.

**Build stamping.** Every shipped binary stamps its commit, artifact, role and
environment, or §2 has nothing to report — `.github/workflows/publish.yml:41-42` and `:57`, for all
eight published binaries.

**No reachable legacy money path.** Readiness must not pass while one is
reachable, which is the `legacyMoneyPaths: 0` field of §2. 🔴 **It is 3.** The
executor's own readiness check refuses to start because of it
(`cmd/intent-executor/main.go:169`), which is the correct behaviour: the
remaining paths and the executor must not both be able to settle the same
obligation. What remains is not a collector — two are crash-recovery paths that
finish a charge a legacy run already put in front of the provider, and the third
is a scan false positive on billing's own `PayInvoice`, which now proposes a
receivable. They drain rather than being deleted, and
`scripts/legacy-drop-preconditions.sql` is what measures when that is done.

### The three refusals in front of the collector

The executor is the only binary in the tree that links a provider adapter, and it
refuses to start on any of three independent conditions
(`cmd/intent-executor/main.go:162-172`, called at `:45`, each with its own named
error at `:147-149`):

1. `INTENT_EXECUTOR_ENABLED` is unset;
2. the build is not stamped, so a charge could not be tied to a revision;
3. `capabilities.LegacyMoneyPaths != 0` — today it is 3, so **this refusal fires
   unconditionally on every deployment**.

Behind that, a fourth thing holds. Even if the binary started, `environment()`
(`:183-192`) returns every gate false and hands the predicate an empty
`UnbuiltEvidence`, so every clause whose supporting record does not exist
refuses. Returning true for a gate whose evidence is unbuilt is the
declared-but-not-implemented failure this repository exists to expose, and the
one place it would do real damage is the function that decides whether money
moves.

None of this has been exercised against real money. That is the honest reading of
a collector that has never been permitted to start.

A green test run implies no merge and no deployment. The conditions that must
hold before production intent execution is enabled are listed once, in
[DESIGN §11](DESIGN.md#11--getting-from-here-to-there), and release stays manual
until they do.

---

## 6 · What none of this proves

Read this section first if you are deciding whether to build a business on this
platform. It is the shortest honest answer to "can I check that I am being
charged correctly?", and that answer is **not yet**.

🔴 **You cannot obtain a single artifact that explains a charge.** There is no
verifier binary, no golden vectors, no charge bundle, and no read path to
`ms_billing.evidence_records` from any binary in `cmd/`. What you have is this
repository, the checks in §5 that you can run against it, and a bill. Between the
bill and the source there is nothing you can hold.

🔴 **You cannot ask which build charged you.** §2 explains the mechanism and why
the answer is authenticated at both transports. In production the only
unauthenticated response is a fixed `{"status":"ok"}`.

🔴 **A charge line does not name your meter, your module or your version.** The
sealed line has fields for all of them and the production proposer fills none:
`internal/intent/proposer/proposer.go:267`. The digest is real, and what it
attests to is one number and a description string.

🔴 **No price rule is published — and the predicate refuses because of it.**
Every leg seals its terms, price-book, notice, tax and routing revisions as the
literal `unpublished/pending-decision-12` (fifteen non-test constants, among them
`internal/account/cycle/domain_charges.go:57-62`,
`internal/account/autotopup/executor.go:1135-1140` and
`internal/account/creditpurchase/types.go:44-45`). There is no price book, no
effective-dated policy, and no revision id you could pin a charge against.

The one good thing to say about that is a guard, not a fix.
`ClausePolicyPublished` reads
`len(intent.UnpublishedRevisions(s.Intent)) == 0`
(`internal/intent/predicate/predicate.go:203`), and
`UnpublishedRevisions` checks all five sealed ids
(`internal/intent/revision.go:56-80`), so an intent carrying the placeholder
cannot be executed. Until 2026-08-30 those ids went into the canonical digest
unexamined and no clause objected — a charge bundle could attest to the
placeholder and an authorization minted with the same placeholder satisfied every
equality check, so the fiction was self-consistent rather than self-refuting. The
gate exists now. It fails closed, which means: not that pricing is verifiable,
but that nothing can collect until it is.

⚠️ **Price resolution has an immutable path and a mutable fallback.**
`LookupMetricVersionPrice` reads a per-(module, metric, version) snapshot that is
insert-only with no update path, so a later re-price cannot retroactively change
an earlier version's price (`internal/account/db/queries/rollup.sql:332-334`).
When no snapshot exists the Go caller falls back to `LookupMetricPrice`, which
reads `metric_definitions` — a row whose only writer is an upsert that overwrites
`unit_price_micros` in place with no effective date and no history
(`internal/account/db/queries/usage.sql:33-47`). Which path a given charge took
is not visible to you. How often the fallback is taken is a fact about the
production database, not about this source tree, and this file does not claim it
either way.

🔴 **If you write modules, you cannot check what you are owed.**
`cycle.SettleDevelopers` (`internal/account/cycle/service.go:594`) is the only
caller of the settlement writer and has no non-test caller —
`cmd/billing-cycle/main.go` calls `RollupPeriod` (`:638`, `:713`) and
`RunBillingCycle` (`:669`) and never this. Even if it ran, its cost input is
`const infraMicros = 0` (`:626`). The take rates are Go constants — 15% published,
30% private (`internal/account/cycle/types.go:71-78`) — not a published,
effective-dated, accepted policy with a revision id, and a visibility lookup that
returns nothing defaults to the higher 30% take (`:616-624`). That default is
documented and deliberate; it is also in the platform's favour, and you have no
way to see which rate was applied.

🔴 **A receipt proves what the engine was told, not that you agreed.**
`api-platform` holds your session and relays your acceptance, and the engine
treats the subject id it is handed as opaque
(`internal/account/billing/service.go:112-114`). A compromised or buggy caller
can assert an acceptance you never made, and nothing here disproves it.
`migrations/billing/065_authorization_acceptances.up.sql:21-26` concedes this in
the schema itself. [INV-006](DESIGN.md#inv-006) states it as a trust assumption
rather than a control. What 065 *did* remove is the ability to authorize
recurring automatic collection with a string nobody issued: the acceptance now
carries a nonce, an audience and a replay identity, where previously any
non-empty value satisfied the gate. What survives is reproducibility — the
disclosure, its digest and the receipt stay readable, so a fabricated acceptance
is something you can point at afterwards. That is detection, not prevention.

**`attested` is not `witnessed`.** The engine signs its own transition. The
optional transparency log can later reveal a rollback or a split view. It cannot
prove no row was hidden, and a delayed checkpoint must never delay or authorize a
collection.

**Delivered is not read.** Verification can prove which bytes a consent client
bound into your proof, and which bytes a carrier reported delivered, and when. It
cannot prove a person read them or understood them.

**Provider evidence differs by rail.** Payout and balance visibility depends on
the rail and the merchant contract. A trace must mark unsupported evidence as
unsupported, never as proof that nothing happened. And there is no independent
reader: this tree has one provider client, and its own comment says so —
"splitting read and write backends is the provider-port work of
[DESIGN §9](DESIGN.md#9--where-the-provider-credential-lives); until then one
setting covers both" (`internal/shared/stripe/client.go:65-71`). Evidence
read back through the same credential that wrote cannot rise above executor
assertion.

**A total compromise defeats all of it.** Public source plus build provenance
makes tampering detectable, not impossible. A deployment, its signing keys, its
database, its notifier and its provider account acting together can produce a
consistent lie. The trust assumptions are enumerated in
[`../SECURITY.md`](../SECURITY.md#adversary-model) and the current defects in its
[gap register](../SECURITY.md#known-current-gaps). This file keeps no second copy
of either.

### What would change the answer

Three things, in the order that would help most:

1. **An unauthenticated, signed `Capabilities` and `Health`.** The build
   identity and the legacy-path count are built; the public read is not, and
   neither is the rest of the surface §2 specifies. Until a customer can ask,
   no external party can establish which code answered them, and every level
   below "source" in §1 stays closed.
2. **Sealed lines that carry the meter, module, version and quantity they
   already have fields for**, against a published price book with a revision id.
   Without it a verifier would confirm the arithmetic of a number whose
   derivation is not in the document.
3. **A `billing-verify` binary with committed golden vectors, and a per-payer
   read of your own evidence records.** The writer, the signer and the pinned
   trust root exist; the reader and the tool do not.

Until then: build here to learn the platform. Do not yet build here on the
assumption that you can audit what you are charged or what you are paid.
