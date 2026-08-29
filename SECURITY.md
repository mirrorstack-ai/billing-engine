# Security

This repository is public so that one question with real money behind it can be
settled by reading code and checking a receipt, rather than by trusting a
support reply:

> **Can this path collect money you never agreed to?**

Answering it that way only works if the file is honest about where the reading
stops. Both places it stops go before anything else.

> 🔴 **The design is proposed, and `main` does not implement it** — see
> [README's status section](README.md#status-before-anything-else). For a
> researcher that has one consequence. A requirement in
> [`docs/DESIGN.md`](docs/DESIGN.md) is not yet a claim, so breaking one is not
> yet a finding. [§1](#1--reporting-something-you-found) says what is.

> 🔴 **Public source cannot restrain an unrestricted merchant credential.** A
> replaced executor can ask the provider to move money outside this code, and no
> Go interface reaches it. [§5](#5--what-we-assume-and-what-breaks-when-the-assumption-is-wrong)
> states that limit and the controls that make it narrow or detectable.

A third limit of that kind, which this file fails to name, is itself worth
reporting.

---

## 1 · Reporting something you found

**Email `security@mirrorstack.ai`.** Please do not open a public issue, and do
not put customer billing data into an issue, a discussion, or a pull request.

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
defect. That rule has already blocked work on this branch. It is what turned the
undisclosed infrastructure markup from a pricing detail into a release blocker —
rows 15 and 16 of [§2](#known-current-gaps).

### What is not a finding

- Restating a row of [§2](#known-current-gaps). A *new* way to exploit one of
  those rows is still worth sending.
- A `docs/DESIGN.md` requirement that the status block above marks
  unimplemented.
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
engine treats the subject id it is handed as opaque
(`internal/account/billing/service.go:105-109`). So `api-platform` can assert an
acceptance that never happened, and nothing here can disprove it.

What survives is reproducibility. The engine-signed disclosure, its digest, and
the recorded receipt must stay readable, so that a fabricated acceptance is
something you can point at afterwards. That is detection, not prevention.

Reasoning about what a hostile caller does with that gap is in scope, and it is
among the more interesting things you could send us. Reporting that the gap
exists is not, because it is written here and at
[INV-006](docs/DESIGN.md#inv-006).

---

<a id="known-current-gaps"></a>
## 2 · Known current gaps

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
| A customer-visible infrastructure line carries a 1.2x markup that its own displayed unit price does not include. The remedy is decided — fold the line into a published base price ([`docs/DESIGN.md` decision 15](docs/DESIGN.md#12--what-we-have-not-decided)) — and has not shipped, so this row stands. | `infraMarkupNum = 12` sets charge = cost x 12/10 ([`internal/account/cycle/types.go:59-60`](internal/account/cycle/types.go)). Displayed `UnitPriceMicros` is pre-markup COGS while `ChargedMicros` already includes the markup ([`internal/account/usage/types.go:446-448`](internal/account/usage/types.go)). Quantity x displayed unit price therefore does not reconcile to the charge, on a line the customer is shown. |
| The marked-up infrastructure total reaches customers through the live read path, not an internal cost report. | `RecordInfraUsage` ([`internal/account/usage/infra.go:326`](internal/account/usage/infra.go)) feeds `AppInfraBill` and `AppModuleInfraBill` ([`internal/account/usage/bill.go:500-530`](internal/account/usage/bill.go)), which `GetAppBill` and `GetAccountBill` serve ([`cmd/account-api/main.go:690`](cmd/account-api/main.go) and `:696`) as `infra_total_micros`, `infra_lines`, and `module_infra_lines`. Any claim that infrastructure is internal-only cost is false about this build. |
| The billing-period anchor is stamped from a provider webhook event rather than a customer authorization. | `StampAccountActivated` writes `accounts.activated_at` on the first `payment_method.attached` event ([`internal/account/webhook/handlers.go:131-135`](internal/account/webhook/handlers.go)). The stamp is best-effort: an error is logged and the attach continues, so the day every later cycle closes and charges can be set, or missed, by provider event ordering. |
| `StartCreditPurchase` finalizes an auto-advance invoice before the browser holds its client secret. | The purchase drives a finalize with `AutoAdvance: true` ([`internal/shared/stripe/client.go:454-456`](internal/shared/stripe/client.go)), which that file calls the only money-moving step, from `finalizeDraft` ([`internal/account/creditpurchase/executor.go:271`](internal/account/creditpurchase/executor.go)). The client secret is returned to the caller only afterwards ([`internal/account/billing/credit.go:624-635`](internal/account/billing/credit.go)). Stripe may charge the default card before the customer's browser has anything to confirm. |
| Four ordinary read and ingest paths can reach the auto-top-up executor. | `GetServiceStatus` calls the credit gate ([`internal/account/billing/service.go:465-476`](internal/account/billing/service.go)), `GetCreditStanding` calls `GetServiceStatus` ([`internal/account/billing/credit.go:48`](internal/account/billing/credit.go)), and usage ingress plus infra sync call `EvaluateCreditUsage` ([`internal/account/usage/service.go:216`](internal/account/usage/service.go), [`internal/account/usage/infra.go:435`](internal/account/usage/infra.go)). All four converge on `maybeTriggerAutoTopUp` ([`internal/account/credit/coordinator.go:316`](internal/account/credit/coordinator.go) and `:578`). A status read is not capability-safe when it can move money. |
| One boundary invoice carries the closed period's usage arrears and the new period's advance charges, and the split is not shown to the customer. | Charging both at a boundary is intended: the total is arrears plus advance base plus overage plus domains in a single Stripe invoice ([`internal/account/cycle/charge.go:296-299`](internal/account/cycle/charge.go) and `:592-594`). The gap is presentation and authority, not the combining. The customer is not shown which part was consumed and which is billed forward, and one collection decision covers two periods, so a refusal of the forward part cannot be expressed separately. |

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
enrolled destination. `NoticeReceipt` (unbuilt) must rest on carrier-signed
proof, or a read-back binding content digest, destination, message id, terminal
status, and delivered time. Every nonterminal status must fail closed.

---

## 4 · What each party can do

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
argument. One row is also weaker than it reads — "cannot mint proof" is what
[INV-006](docs/DESIGN.md#inv-006) requires, and §1 says why it is unenforced.

### Three substitutes that would look like controls

- **An opaque id, or a private-UI URL, is not consent.** Neither shows you
  terms, so neither can stand in for an acceptance.
- **A local `last_consumed` value is not a cancellation control.** Without the
  authoritative current head it cannot see a revocation that landed elsewhere.
- **A customer-facing `api-platform` route is necessary and not sufficient.**
  Being reachable by a browser says nothing about who authored the bytes.

### The double-spend that the typing rule prevents

Rating and tax credits reduce what you owe. Stored-value wallet lots do not
reduce it — they fund it. Every source must be typed `rating_credit` or
`stored_value`, and never both.

Without that typing, a hostile caller subtracts one lot from the obligation and
then spends the same lot again as funding. The lot pays twice, and you are
charged for value the platform already gave you. A unique-use constraint across
both domains is the mechanism, in
[`docs/DESIGN.md` §3](docs/DESIGN.md#3--what-must-be-true-before-any-money-moves).

---

## 5 · What we assume, and what breaks when the assumption is wrong

An honest threat model is mostly a list of assumptions. Each of these names what
an attacker gets when it turns out to be false.

**We assume the deployed artifact is the public artifact.** Public source proves
nothing about a binary you cannot identify. Health, `Capabilities` evidence
(unbuilt), and every receipt must name the source commit, artifact digest,
schema revision, and policy digests. That naming must be attested to a root you
pin outside every runtime relay. *If it is wrong:* you cannot check that this
code is the code holding the payment credentials, and no test here closes it.

**We assume billing-owned storage and keys are isolated.** The private caller
must have no direct write access to intent, authorization, notice, execution, or
receipt rows, and must not hold their sealing keys. Provider keys, webhook
secrets, and tax credentials come from a secrets manager, and never appear in
source, logs, intents, or receipts. *If it is wrong:* the state machine is
advisory, because the caller can edit the rows or mint the envelopes. Database
operators stay powerful even when it holds. Append-only constraints, audit
export, and backups are the controls, while `BillingDecisionProof` (unbuilt) and
a transparency log detect a rollback afterwards and gate no payment.

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
disable automatic execution rather than downgrade quietly.

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
expiry, so an uncertain ordering has to fail closed with no new debt.

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
