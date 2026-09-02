# Security

This repository is public so that one question with real money behind it can be
settled by reading code and checking a receipt, rather than by trusting a
support reply:

> **Can this path collect money you never agreed to?**

Answering it that way only works if the file is honest about where the reading
stops. Both places it stops go before anything else.

> 🔴 **The intent rail is built and cannot execute.** Every billing leg now
> only *proposes* a sealed `ChargeIntent`; `cmd/intent-executor` is the only
> thing that can collect one, and it refuses to start while
> `capabilities.LegacyMoneyPaths` is non-zero — it is 3
> ([`internal/account/capabilities/capabilities.go`](internal/account/capabilities/capabilities.go),
> `readiness` in [`cmd/intent-executor/main.go`](cmd/intent-executor/main.go)).
> Every gate its `environment()` reports is `false` except build identity, so
> the execution predicate refuses every intent. Nothing has ever been collected
> through this rail. For a
> researcher that has one consequence: a requirement in
> [`docs/DESIGN.md`](docs/DESIGN.md) whose supporting record is unbuilt is not
> yet a claim, so breaking one is not yet a finding.
> [§1](#1--reporting-something-you-found) says what is.

> 🔴 **Public source cannot restrain an unrestricted merchant credential.** A
> replaced executor can ask the provider to move money outside this code, and no
> Go interface reaches it. [§5](#5--what-we-assume-and-what-breaks-when-the-assumption-is-wrong)
> states that limit and the controls that make it narrow or detectable.

A third limit of that kind, which this file fails to name, is itself worth
reporting.

---

## 1 · Reporting something you found

**Email `security@mirrorstack.ai`.** That is the only channel. Private
vulnerability reporting is **not** enabled on this repository, so GitHub offers
you no "Report a vulnerability" button — you can confirm that yourself on the
repository's Security tab. Please do not open a public issue, and do not put
customer billing data into an issue, a discussion, or a pull request.

Send the source revision, the file or action, the rule you believe is
bypassable, and the shortest sequence that shows it. Say whether real money
moved. A failing test, a fuzz input, or a mutation that survives the suite is a
good report on its own.

Never send card numbers, payment credentials, notice tokens, webhook secrets,
provider signatures, or unredacted customer records. Intent ids and provider
references are correlation data, so redact them unless the investigation needs
them.

We acknowledge within three working days and give an assessment within ten. If
we disagree with you we will say why, in enough detail that you can argue back.
**There is no bounty programme.** We would rather say that plainly than imply
one.

### What counts as a finding

Anything that breaks a claim this repository makes and has marked as built.
The rules live in [`docs/DESIGN.md`](docs/DESIGN.md) and are not repeated here.
The list below judges eligibility, nothing more.

- a provider write from anywhere but the one permitted writer
  ([INV-007](docs/DESIGN.md#inv-007)), or one sealed intent settling twice
  ([INV-008](docs/DESIGN.md#inv-008));
- an amount changing after disclosure without a replacement intent
  ([INV-003](docs/DESIGN.md#inv-003)), or a caller field becoming authoritative
  for amount, currency, tax, lines, or eligibility
  ([INV-001](docs/DESIGN.md#inv-001), [INV-004](docs/DESIGN.md#inv-004));
- collection before the notice window, after a cancellation, or above an
  accepted cap ([INV-005](docs/DESIGN.md#inv-005),
  [§10](docs/DESIGN.md#10--what-you-can-stop-and-what-you-cannot));
- a charge kind sealed that the closed catalog does not list — it holds seven
  ([`internal/intent/catalog.go`](internal/intent/catalog.go)), and a kind
  invented at a call site is the defect that list exists to stop;
- a callback, invoice, or dashboard state accepted as ledger truth
  ([INV-009](docs/DESIGN.md#inv-009)), or customer evidence that only works
  while the private relay cooperates ([INV-014](docs/DESIGN.md#inv-014));
- one consumed permit emitting a second outbound request, or a second attempt
  out of `execution_unknown` without proof the first collected nothing
  ([§5](docs/DESIGN.md#5--paying-and-what-happens-when-the-answer-never-comes));
- an unresolved tax result treated as zero tax
  ([§7](docs/DESIGN.md#7--tax-and-what-it-refuses-to-guess));
- a read or reconciliation path handed a provider client that also exposes
  writes, or any exposure of credentials or cross-tenant evidence; and
- **a sentence in this repository that overstates what the code guarantees.**

The last one is not filler. In a repository built to be read, a confident false
sentence can do the harm of a code defect. We treat it as the same class of
defect — including the sentences in this file. A row of
[§2](#known-current-gaps) that no longer matches the tree is reportable under
this bullet even though restating a row that *does* match is not.

### What is not a finding

- Restating a row of [§2](#known-current-gaps) that still holds. A *new* way to
  exploit one of those rows is still worth sending.
- A `docs/DESIGN.md` requirement that this file marks unbuilt or refusing.
- Disagreement with a published price or tax policy the engine applied
  reproducibly. Charging while tax is unresolved, or applying a different
  policy, is in scope.
- MirrorStack's private UI or tenancy logic alone. A private-caller defect that
  makes *this engine* exceed an intent or a tenant boundary is in scope, and it
  is the boundary this repository exists to hold.
- Provider outages, card declines, or a provider's documented settlement
  decision, unless this engine mishandles the result.
- Compromise of your device or account, our cloud account, a signing root, a
  tax authority, or a merchant credential. [§5](#5--what-we-assume-and-what-breaks-when-the-assumption-is-wrong)
  names each as an assumption, and a path here that widens one needlessly is
  still in scope.
- Scanner output with no demonstrated impact.

Do not probe production with real charges. Reproduce against test adapters and
fixtures, and write to us first if an issue can only be shown in production.

### 🔴 INV-006 is a trust assumption, and its existence is not a finding

[INV-006](docs/DESIGN.md#inv-006) says every debit carries customer authority.
The engine cannot enforce that today. `api-platform` holds your session, and the
engine treats the subject id it is handed as opaque — `Service.Ensure` in
[`internal/account/billing/service.go`](internal/account/billing/service.go)
says so, and `BillingAuthorization`'s own doc comment in
[`internal/intent/authorization.go`](internal/intent/authorization.go) records
the same limit at the type that would carry the permission. So `api-platform`
can assert an acceptance that never happened, and nothing here can disprove it.

What survives is reproducibility. The engine-signed disclosure, its digest, and
the recorded evidence must stay readable, so that a fabricated acceptance is
something you can point at afterwards. That is detection, not prevention.

Reasoning about what a hostile caller does with that gap is in scope, and it is
among the more interesting things you could send us. Reporting that the gap
exists is not, because it is written here and at
[INV-006](docs/DESIGN.md#inv-006).

---

<a id="known-current-gaps"></a>
## 2 · Known current gaps

This register was re-derived against commit `82378fc` by reading the tree, not
by editing the previous revision. It is the only list of current defects in this
repository; no other file keeps a second one. The rows are not accepted final
behavior.

| Current gap | Why it matters |
|---|---|
| Three call sites can still move money without an intent, so the intent executor refuses to start. Two finish a charge a pre-cutover run had already placed at the provider (`recoverModuleOverageCharge`, `recoverDomainCharge`); the third is a scanner over-match on billing's own `Service.PayInvoice`, which proposes and reaches no provider. All three are named with their reason in [`internal/architecture/allowlist.go`](internal/architecture/allowlist.go), pinned against an AST scan of the tree. | Two rails that could each settle the same obligation must not both be live, which is why the executor refuses rather than running beside them. Until the count reaches zero, no charge is governed by the intent rules. |
| The legacy provider client interface still combines customer administration, invoice writes, payment writes, and provider reads in one type (`Client` in [`internal/shared/stripe/types.go`](internal/shared/stripe/types.go)). | A read or reconciliation consumer cannot be proven harmless from its interface alone. The intent path takes a four-method `IntentClient` instead; the legacy surface is what the rest of the tree still holds. |
| One provider transport configuration serves reads and writes alike, and nothing counts or fences the outbound request that actually left before a path reports itself ready. | The transport is at least stated rather than inherited — retries are set to zero and redirects are refused ([`internal/shared/stripe/client.go`](internal/shared/stripe/client.go)) — but a shared configuration means a write inherits whatever a read needs, and an unfenced request cannot be reconciled against what the code believes it sent. |
| A large automatic charge has no pre-charge notice. The only large-charge surface is a boolean stamped on the invoice mirror after the money moved (`IsLargeAutoCollect`, [`internal/account/collection/collection.go`](internal/account/collection/collection.go)). | It describes money that already moved; it is not notice or authorization. The notice evidence the intent rail requires (`NoticeReceipt`) has a schema and a store method nothing calls, so the predicate refuses on that clause rather than anything being delivered. |
| App budgets are alert-only ([`internal/account/budget/service.go`](internal/account/budget/service.go) records threshold crossings and changes no charging behaviour), and the separate spend ceiling is applied in the cycle charge path alone, not as a universal bound over every charge category. | A displayed budget must not be mistaken for a hard authorization cap. |
| Ordinary status and ingest reads still reach the automatic top-up trigger. The trigger no longer collects: the leg proposes a sealed intent, and a deployment whose proposer is not armed refuses outright (`ErrProposerUnarmed`, [`internal/account/autotopup/executor.go`](internal/account/autotopup/executor.go)). | What remains is the capability reach, not a charge. A component that only reads must not be wired to one that can charge, because the property then depends on the collector staying deleted rather than on the seam. |
| Per-version module metric prices have immutable snapshots, but pricing is not yet one complete, effective-dated, customer-disclosed policy. The reconciliation rater prices only from `ms_billing.metric_version_prices` ([`internal/intent/shadow/source.go`](internal/intent/shadow/source.go)) and its price key carries meter, module and module version with no model dimension ([`internal/intent/pricebook.go`](internal/intent/pricebook.go)), so usage that varies by model cannot be re-rated from the catalog. | Reproducibility of some usage rows does not prove authorization of the final total, and the reconciliation gate that would prove it cannot be met for every row. |
| The provider-callback consumer still constructs the top-up and credit-purchase executors with a provider client, and so holds provider-mutating capability ([`cmd/account-webhook-eventbridge/main.go`](cmd/account-webhook-eventbridge/main.go)). Neither executor can charge — both propose — but the reconciliation they perform can still void and discard invoices at the provider. | A callback path is not read-only merely because its input was authenticated. Until the credentials and the writer links are removed from it, callback reconciliation cannot be called capability-safe. |
| A credit-wallet draw settles inside this engine's own ledger and produces no provider record ([`internal/account/cycle/overage.go`](internal/account/cycle/overage.go)). | Value the customer already paid for is consumed on a path with no external artifact to reconcile against, and no sealed intent bounding it. |
| In production the metering role and the ordinary billing role share one invocation target. The separation that exists on the local HTTP surface — a distinct credential for usage submission — is not reproduced in the deployed configuration. | A credential issued for submitting facts is not confined to submitting facts. Only a separate function or an authenticated role-to-action check makes the metering seam real; describing it as dedicated today would be false. |
| Tax is not implemented as a frozen, versioned decision. The execution predicate has a tax clause and it refuses: it requires both a resolved sealed determination and an independent reproduction, and the executor reports the reproduction as false ([`internal/intent/predicate/predicate.go`](internal/intent/predicate/predicate.go), `environment` in [`cmd/intent-executor/main.go`](cmd/intent-executor/main.go)). | The engine cannot yet claim either a correct tax charge or a justified zero-tax result. It refuses rather than guessing, which is the right direction and not a substitute. |
| The public health route does not identify the deployed build. The local HTTP `/__health` route returns the linker-stamped commit, artifact, role and environment; the production API Gateway path answers a static `{"status":"ok"}` before the dispatcher runs ([`cmd/account-api/main.go`](cmd/account-api/main.go)). | Public source cannot verify a private deployment when the route a customer can reach cannot name the code that produced their charge. The `Capabilities` action does report build identity, but it sits behind the authenticated RPC surface. |
| A provider-neutral intent ledger exists (`ms_billing.charge_intents` and the sealed rail, tax, funding-split and group columns in [`migrations/billing/`](migrations/billing/)), but exactly one adapter is implemented and nothing has settled through it. | A settlement claim that has never run is untested. Adding a second provider before one has executed would mean designing cross-provider retry and double-settlement behaviour against no evidence. |
| A customer-visible infrastructure line carries a 1.2x markup that its own displayed unit price does not include. | `infraMarkupNum = 12` sets charge = cost x 12/10 ([`internal/account/cycle/types.go`](internal/account/cycle/types.go)). The displayed `UnitPriceMicros` is pre-markup cost while `ChargedMicros` already includes the markup, and the type's own comment says the shown unit price x quantity therefore does not equal the charge ([`internal/account/usage/types.go`](internal/account/usage/types.go)). Quantity x displayed unit price does not reconcile to the charge, on a line the customer is shown. |
| The marked-up infrastructure total reaches customers through the live read path, not an internal cost report. | `RecordInfraUsage` ([`internal/account/usage/infra.go`](internal/account/usage/infra.go)) feeds the `AppInfraBill` and `AppModuleInfraBill` reads ([`internal/account/usage/bill.go`](internal/account/usage/bill.go)), which `GetAppBill` and `GetAccountBill` serve ([`cmd/account-api/main.go`](cmd/account-api/main.go)) as `infra_total_micros`, `infra_lines`, and `module_infra_lines`. Any claim that infrastructure is internal-only cost is false about this build. |
| The billing-period anchor is stamped from a provider event rather than a customer authorization. | The first `payment_method.attached` event freezes `accounts.activated_at` ([`internal/account/webhook/handlers.go`](internal/account/webhook/handlers.go)). First bind wins, and the stamp is best-effort: an error is logged and the attach continues. So the day every later cycle closes and charges can be set, or missed, by provider event ordering. |
| One boundary collection covers the closed period's usage arrears and the next period's forward fees. | The intent rail seals them as two intents of different kinds, because one intent carries one kind and a single intent would let whichever kind it named authorize the other (`splitBoundary`, [`internal/account/cycle/boundary_charges.go`](internal/account/cycle/boundary_charges.go)). They still settle as one grouped collection with one rounding, so a customer cannot refuse the forward half on its own. |

**Closed since the previous revision of this register, and checkable in the
tree.** `ChargeIntent` and `BillingAuthorization` exist as first-class sealed
types with unexported fields ([`internal/intent/`](internal/intent/)); the
charge catalog is closed at seven kinds and a kind outside it cannot be sealed;
every collecting leg was replaced by a proposal, taking the count of money paths
outside the intent boundary from eleven to three; the Stripe HTTPS receiver no
longer verifies or routes anything, because Stripe now arrives on an
EventBridge partner bus consumed by a separate binary; the provider transport
sets retries to zero and refuses redirects; published binaries are stamped with
their commit and artifact and an unidentified build refuses to execute; and the
evidence trust root pins one real ed25519 public key whose id is derived from
its own bytes, in an unexported slice no package can append to at runtime
([`internal/shared/signing/trustroot.go`](internal/shared/signing/trustroot.go)).
Pinning the public half is not provisioning the private one, and none of this
means an intent has ever been executed.

**Re-verification.** This register is re-derived by hand. No job re-checks it:
there is no `schedule:` trigger in any workflow under
[`.github/workflows/`](.github/workflows/), which you can confirm in a grep. A
row here that no longer matches the tree is a finding under
[§1](#1--reporting-something-you-found), and reporting one is useful.

The current code contains worthwhile operational controls: integer money,
idempotent usage ingestion, immutable per-version price snapshots, frozen retry
amounts, provider idempotency keys, and crash-recovery markers. Those reduce
calculation drift and accidental duplicate collection. They do **not** by
themselves prove that a customer was notified of and authorized the exact
charge.

---

<a id="adversary-model"></a>
## 3 · Who this defends your money against

Every billing guarantee is a guarantee against somebody in particular. Naming
them turns "we use idempotency keys" into something you can check, and it shows
which of your worries this code does not answer at all.

### The adversary is MirrorStack

Not an unauthenticated browser. **Us.**

Nothing customer-facing reaches this engine; README owns that boundary and its
citations. An outside attacker has to get through `api-platform` or our cloud
account first, and at that point this engine is not where your problem lives.
The threat that justifies publishing the source is the other one:

> You have a card on file. What stops the half of MirrorStack you cannot read —
> buggy, compromised, or deliberately hostile — from charging it?

So the modelled adversary is **the private caller**. Assume it invokes every
action its credential reaches, and that it submits, omits, duplicates, reorders,
delays, and replays requests while choosing every identifier in them. Assume it
lies about what its UI displayed, withholds your cancellation, and has read
every test here.

That framing has one consequence worth saying flatly:

> **A check the private caller can satisfy with a statement about itself is not
> a control.** It is a claim.

A field reading `customer_approved: true`, `notice_sent: true`, or
`amount: 10200` must grant no authority whatever. Each of those facts has to be
derived here, or observed across a boundary the caller cannot write.

The argument also rests on the deployment keeping keys apart. The private caller
must hold none of the database, sealing, notice, tax-signing, merchant, or
executor credentials. One credential holding several erases every boundary
below.

### What the caller still influences

The caller is the source of usage and lifecycle facts, and always will be. The
engine must check that an event names a declared metric, belongs to an installed
module and app, falls inside a period, deduplicates, and prices under a frozen
public rule. That stops a caller handing us money directly.

It does not prove you consumed the service. A caller that fabricates plausible
usage can still raise a proposed total. Disclosure equality, a notice window,
caps, exportable source evidence, and cancellation each narrow the damage. None
of them makes private metering an independent witness, and we do not call it
one. That claim needs a separately trusted meter, argued metric by metric.

### Adapters, callbacks, and provider ambiguity

Stripe and NewebPay are payment adapters, not billing authorities. Providers
disagree about what an invoice, an authorization, a capture, a void, and a
callback mean, and the core must never adopt the weakest one's semantics as a
platform rule.

Executor return values and provider callbacks are untrusted input. They may be
stale, duplicated, reordered, or dishonest. They may wake reconciliation, and
they must never declare an intent settled. Settlement needs provider-signed
evidence the core verifies itself, or a read-back through a credential the
provider restricts to reads. Comparing fields inside a normalized response
proves nothing against an adapter that fabricated all of them.

Notice delivery follows the same rule. A signature from our own notifier proves
which component spoke, not that a carrier delivered the disclosed bytes to the
enrolled destination. `NoticeReceipt` has a table, a type, and a store method that
would write one, and nothing in this tree calls it — so the predicate refuses on
that clause for every intent. When something does call it, it must rest on
carrier-signed proof, or a read-back binding content digest, destination,
message id, terminal status, and delivered time. Every nonterminal status must
fail closed.

---

## 4 · What each party can do

This is the division of authority the design requires, and it is not all in
force. Where the current tree departs from it, the departure is a row of
[§2](#known-current-gaps) — read the two together.

| | you | private caller | billing core | executor | provider |
|---|---|---|---|---|---|
| submit usage and lifecycle facts | indirectly | ✅ | validates, retains evidence | — | — |
| choose a charge amount | only by accepting a stated cap | 🔴 **no** | ✅ **derives and freezes** | — | — |
| establish an authorization | ✅ **yours to give** | relay only, cannot mint proof | verifies and seals | — | may attach a method |
| decide an intent is executable | cancel or allow | 🔴 **no** | ✅ from durable gates | consumes a permit | — |
| reach a provider write | customer-present flows only | 🔴 **no** | no | ✅ **only here** | performs it |
| declare the ledger settled | — | — | ✅ only on verified evidence | reports observations | 🔴 **no** |
| stop an unexecuted intent | ✅ **yours** | may ask, never override | cancels or expires | must obey | — |
| explain a completed charge | inspect the evidence | proxy only | ✅ produces the receipt | submits evidence | supporting artifact |

Four rows carry the weight: the authorization, the executability decision, the
provider write, and the settlement declaration. They must stay separate in code,
credentials, deployment roles, and tests. Collapsing any two collapses the
argument.

Three rows read stronger than the tree currently is:

- **"cannot mint proof"** is what [INV-006](docs/DESIGN.md#inv-006) requires and
  what [§1](#1--reporting-something-you-found) says is unenforced.
- **"billing core: no"** on reaching a provider write is the target. Two
  crash-recovery call sites in the billing core still reach one — the first row
  of [§2](#known-current-gaps).
- **"produces the receipt"** names an artifact that does not exist yet. Nothing
  in this tree emits a completed-charge receipt, because nothing has completed a
  charge through the intent rail.

### Three substitutes that would look like controls

- **An opaque id, or a private-UI URL, is not consent.** Neither shows you
  terms, so neither can stand in for an acceptance.
- **A local `last_consumed` value is not a cancellation control.** Without the
  authoritative current head it cannot see a revocation that landed elsewhere.
- **A customer-facing `api-platform` route is necessary and not sufficient.**
  Being reachable by a browser says nothing about who authored the bytes.

### The double-spend that the typing rule would prevent

Rating and tax credits reduce what you owe. Stored-value wallet lots do not
reduce it — they fund it. Every source must be typed `rating_credit` or
`stored_value`, and never both.

Without that typing, a hostile caller subtracts one lot from the obligation and
then spends the same lot again as funding. The lot pays twice, and you are
charged for value the platform already gave you. A unique-use constraint across
both domains is the mechanism, in
[`docs/DESIGN.md` §3](docs/DESIGN.md#3--what-must-be-true-before-any-money-moves).

**That typing does not exist in this tree.** Neither term appears in the schema
or the Go source. What does exist is narrower: an intent seals its funding split
into two integers — the wallet allocation and the remainder due at the rail —
and the execution predicate refuses unless they sum to the sealed gross. That
bounds one intent. It is not the cross-domain uniqueness the rule asks for, and
no intent has executed.

---

## 5 · What we assume, and what breaks when the assumption is wrong

An honest threat model is mostly a list of assumptions. Each of these names what
an attacker gets when it turns out to be false.

**We assume the deployed artifact is the public artifact.** Public source proves
nothing about a binary you cannot identify. Part of this shipped: published
binaries are stamped at link time with commit, artifact, role and environment;
the `Capabilities` action reports that identity together with the count of money
paths outside the intent boundary; and the executor refuses to run when any
field reads `unknown`. What has not shipped is the half that makes it evidence —
the stamp is not attested to a root you pin outside every runtime relay, and the
one route a customer can reach names nothing. *If it is wrong:* you cannot check
that this code is the code holding the payment credentials, and no test here
closes it.

**We assume billing-owned storage and keys are isolated.** The private caller
must have no direct write access to intent, authorization, notice, execution, or
receipt rows, and must not hold their sealing keys. Provider keys and tax
credentials come from a secrets manager, and never appear in source, logs,
intents, or receipts. *If it is wrong:* the state machine is advisory, because
the caller can edit the rows or mint the envelopes. Database operators stay
powerful even when it holds. Append-only constraints, audit export, and backups
are the controls, while `BillingDecisionProof` — a type this tree does not
declare — and a transparency log would detect a rollback afterwards and gate no
payment.

**We assume the acceptance ceremony means what it says.** An authorization is
only as strong as the presentation you saw and the proof you gave. A bearer
credential the private caller can mint defends nothing against that caller, and
a signature over an opaque digest does not prove which terms you read. *If it is
wrong:* the authorization shows only that some credential answered. Your device
and the verifier's update channel are inside the trusted base unless your
enrolled factor has its own secure display.

**We assume the notice carrier tells the truth.** Where a carrier signs, the
core must verify that signature. Otherwise the carrier and the reader are
trusted for content digest, destination, message id, terminal status, and
delivered time, so that reader must be credential-separated and attested. *If it
is wrong:* the engine cannot prove delivery, and unknown evidence strength must
disable automatic execution rather than downgrade quietly. Today it has no
delivery evidence at all, and the predicate refuses on that clause.

**We assume payment providers enforce their own authenticated operations.** The
core must verify what it asked for and what the provider later reports, under
credentials separated by adapter and environment. *If it is wrong:* a provider
or card network can charge a different amount while falsifying every
authoritative read. Reconciliation, merchant statements, and your dispute rights
are then the only external controls.

> 🔴 **This is where the residual risk sits.** A deliberately replaced executor
> holding an unrestricted merchant credential can exceed its Go interface,
> because that credential answers to the provider and not to this code.
> Credential scope, artifact attestation, narrow deployment roles, public
> adapter source, and provider audit logs make the bypass narrow or detectable.
> They do not make it impossible, and nothing here should be read as saying they
> do. Where a provider offers an amount-bound or per-operation token, its
> adapter must use it.

**We assume the published pricing and tax authorities are legitimate.** The
engine will prove it applied one immutable rule. It cannot judge whether the
business was entitled to publish that price, or whether a tax rule is legally
right. *If it is wrong:* the engine reproducibly applies an illegitimate rule,
and produces faithful evidence that it did so.

**We assume clocks are trusted only inside declared roles.** Notice windows,
expiries, price windows, service cutoffs, and consume transitions all rest on
time, so every money-authoritative transition must use a billing-owned monotonic
source disciplined by an authenticated wall clock. Readiness must go false on a
forward jump, a rollback, source disagreement, or stale synchronization.
Recovery may move a transition later, and must never credit elapsed notice time
or pre-expiry authority. *If it is wrong:* whoever controls time controls
expiry, so an uncertain ordering has to fail closed with no new debt. The
executor reports its time readiness as false today, and refuses on it.

---

## 6 · What this design does not claim

These are the limits of the architecture in [`docs/DESIGN.md`](docs/DESIGN.md),
written down so nobody infers more from it. Even fully built, it will not:

- prove that a person read a delivered notice;
- prove that private metering describes real consumption, unless a metric has an
  independently documented evidence source;
- force the private platform to show you a billing page, forward a receipt,
  propose an intent, or keep serving you;
- stop you accepting terms you later regret — it proves which terms constrained
  the charge, and keeps the cancellation and cap evidence;
- survive any of the compromises §5 assumes away, or make a provider, bank, tax
  authority, or notice carrier available;
- treat provider acceptance, invoice finalization, callback receipt, or
  dashboard status as ledger settlement; or
- hide a refund, dispute, reversal, credit, or adjustment. Each is a new
  reason-coded ledger event linked to the receipt it corrects.

How any of it gets checked — evidence levels, the charge bundle you recompute
offline, and the static architecture checks — belongs to
[`docs/VERIFICATION.md`](docs/VERIFICATION.md), not here.
