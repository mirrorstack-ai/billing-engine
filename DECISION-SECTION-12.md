# Section 12 — the sixteen decisions that hold the executor shut

Companion to [`docs/DESIGN.md` §12](docs/DESIGN.md) (DESIGN.md:1896-2020),
[`EXECUTOR-GATE-PLAN.md`](EXECUTOR-GATE-PLAN.md) and
[`DECISION-12-POLICY-REVISIONS.md`](DECISION-12-POLICY-REVISIONS.md).

---

## How to use this document

**The sequence per item is: owner answers → ADR → published policy revision → the
gate can open.** Nothing shorter works. Deciding an item in conversation moves
no gate; `ClausePolicyPublished` (internal/intent/predicate/predicate.go:203)
reads five sealed revision names (internal/intent/revision.go:56-80) and refuses
while any carries the `pending-decision` marker. All three shipped proposers seal
`"unpublished/pending-decision-12"` today (internal/account/cycle/domain_charges.go:57-62).
Publication is what flips the clause, not agreement.

**Until an item is published, its gate stays shut and no engineering removes it.**
Every gate field in `executor.Environment` and `predicate.UnbuiltEvidence` is
hardcoded `false` at cmd/intent-executor/main.go:115-123, deliberately — the
comment at :108-114 says setting one true without the record behind it is the
declared-but-not-implemented failure `SECURITY.md` exists to expose. That comment
is correct and should not be edited to unblock anything.

**Nothing here may be reconstructed from what the code currently does.**
DESIGN.md:2012-2014. Three concrete traps already in the tree: the 24-hour
`notice_lead_seconds` backfill (migrations/billing/056_authorization_frequency_ceiling.up.sql:73),
the `chargeCurrency = "usd"` constant (internal/account/cycle/types.go:195-198),
and the wallet consumption order whose own SQL comment calls it
"the owner-decided consumption order" with no accepted revision behind it
(internal/account/db/queries/credit_wallet.sql:265, 294-303). Each looks like an
answer. None is one.

**Items an engineer must NOT decide — OWNER-ONLY: 3, 4, 5, 6, 7, 8, 9, 10, 12,
13, 14, 15, and the numeric values in 1.** These are tax determinations
(5, 6, 7, 8, 10), a legal-entity and merchant-of-record choice (4), consumer
protection and stored-value characterization (1, 9, 13), pricing (12), finance
and revenue recognition (14), and a liability-transfer rule (15). An engineer
choosing any of them is inventing policy, which is exactly what §12 forbids.

**Items where engineering has a recommendation and should give one: 2, 11, 16,
and the *split* in 1.** Those are argued below rather than deferred.

**The measurement everything rests on.** In production: `charge_intents = 0`,
`billing_authorizations = 0`, `credit_ledger = 0`. The engine has never sealed
or settled an intent. Every "this is free today" claim below is true because of
that and stops being true at the first sealed production intent, after which any
change to the sealed document is a migration of live rows past the INV-003
reject-sealed-update trigger (migrations/billing/054_intent_core.up.sql:97-121).
The owner's 2026-08-31 decision to build all 17 gates as designed means the
*build* is no longer waiting on §12. *Collection* is.

---

## Summary

| # | Item | The decision, in one line | Gates (§12) | Owner-only |
|---|---|---|---|---|
| 1 | Notice and standing authority | How long before money moves you must already have been told, and how much one "yes" may cover | G1, G2 | Yes (values) — the *split* is engineering's |
| 2 | Budget stop semantics | Whether a spending cap switches off service, collection, both, or nothing | G1 | **No** |
| 3 | Change policy | Whether a customer who said yes stays bound after we move a price, a tax rule or the terms | G1, G3 | Yes |
| 4 | Merchant of record | Which legal company sells, per market and rail, and who carries the tax liability | G1, G3, G4 | Yes |
| 5 | Registrations and treatment | Where we are registered, whose money we may collect automatically, what a B2B tax id does | G3 | Yes |
| 6 | Tax classification, display, rounding | Which charge kinds are taxable, inclusive or exclusive, and where rounding happens | G2, G3 | Yes |
| 7 | Location evidence | Which facts prove where a customer is, and for how long | G3 | Yes |
| 8 | Rate source and verifiability | Which rate data we license, and whether we may republish it for customers to check | G3 | Yes |
| 9 | Adverse outcomes and value return | Whether money ever goes back to the card, or only to a balance | G2, G3, G4 | Yes |
| 10 | Invoicing duties, Taiwan, NewebPay | Whether Taiwan is a market this engine may collect in at all | G1, G3 | Yes |
| 11 | Currency | Whether the engine's money is ever denominated in more than one currency | G1, G2, G4 | **No** |
| 12 | Which kinds exist, and their timing | Whether the bill carries four priced service lines or fewer, and when each lands | G1, G2 | Yes |
| 13 | Credit, wallet, developer settlement | Whether a credit balance is the customer's money or an entitlement we may expire | G2, G4 | Yes |
| 14 | Ledger and evidence policy | Which accounts exist, when money becomes revenue, and what evidence may leave | G1, G3, G4 | Yes |
| 15 | Responsibility transfer | When the payer changes, who owes for service already consumed | G1 | Yes (infra half **settled**) |
| 16 | Consent authority, verifiable reads | Whether to fund an identity product, or accept in writing that we cannot | G1, G4 | **No** |

Gate legend (DESIGN.md:1902-1907): **G1** production execution · **G2** catalog
acceptance · **G3** production collection · **G4** ledger cutover.

---

## 1 · Notice and standing authority

### What is being decided
Two separable questions bundled as one. (a) How long before money moves the
customer must already have been told, at which address, and what proof counts
that the bytes arrived. (b) How much a single "yes" may cover, at what frequency,
for how long, before we must ask again.

### Gates it unblocks
`notice_policy` is one of the five names `intent.UnpublishedRevisions` checks
(internal/intent/revision.go:64). Publishing it removes exactly one of five from
`ClausePolicyPublished`'s refusal (predicate.go:203) — terms, price book, tax and
routing still refuse. The seal today is
`proposedNoticePolicy = "unpublished/pending-decision-12"`
(internal/account/cycle/domain_charges.go:59).

This applies in **both** authority modes. Customer-present exempts the notice
from being *delivered* (predicate.go:106, :112), never the notice policy from
being *published*. So item 1 is on the critical path for every intent, not only
standing ones.

Four clauses already exist and already refuse:
- `ClauseNoticeDelivered` → `noticeTerminallyDelivered` (predicate.go:102-109,
  344-363). "Delivered" is today the hardcoded allow-list
  `deliveredStatuses = {"delivered","relayed"}` (predicate.go:371-374).
  DESIGN.md:719-723 forbids exactly this: accepted terminal statuses and the
  minimum lead time are published by `Capabilities` and "must never be hidden
  deployment constants." `capabilities.Report`
  (internal/account/capabilities/capabilities.go:39-57) publishes no notice field.
- `ClauseNoticeWaitElapsed` (predicate.go:111-140) measures against
  `BillingAuthorization.NoticeLeadTime()` — per-authorization. Migration 056
  backfills `notice_lead_seconds = 86400`
  (migrations/billing/056_authorization_frequency_ceiling.up.sql:73). That is a
  24-hour policy constant reconstructed in a migration. It is inert only because
  `billing_authorizations = 0`.
- `ClauseAuthorityEvidence` standing branch (predicate.go:329-333).
- `ClauseAuthorizationValid` / `ClauseWithinCeilings` (predicate.go:91-97,
  142-156). Ceiling *fields* are built — per-charge, per-cycle, frequency,
  trigger, top-up amount, lead time (internal/intent/authorization.go:66-77;
  migration 056:19-20, 45-46, 60-70, 85-100), with refusal codes
  `over_per_charge_ceiling` / `over_period_ceiling` / `over_frequency_ceiling`
  (authorization.go:319-321). Nothing decides their *values*: `intent.Authorize`
  accepts any non-blank positive figure.

Honest negative: item 1 touches **zero** of the 14 `UnbuiltEvidence` fields
(internal/intent/predicate/state.go:188-203).

Necessary but not sufficient: nothing writes a `NoticeReceipt`
(`store.RecordNotice`, internal/intent/store/authorization.go:145-160, has no
non-test caller), nothing mints an authorization (`intent.Authorize` has no
production call site), and `notice_receipts` has no destination column
(migrations/billing/054_intent_core.up.sql:225-243) — so "which contacts receive
it" and DESIGN.md:729's re-notice-on-contact-change rule have nowhere to be
recorded. The decision creates that schema work.

### Options
**A — Split it.** Publish a notice policy revision now (minimum lead duration,
accepted terminal statuses, destination class, retry/backoff, bounce handling);
defer standing authority. One of five names comes off every refusal and the two
hidden constants get a published home. G2 is not reached and G1 does not move.
Forward-only: a published revision is immutable, so supersession is the only
later edit.

**B — Customer-present only for v1.** `authorityMode` (executor.go:314-319) never
returns standing, so the two notice clauses pass vacuously and the notifier,
carrier-evidence reader and destination schema all defer. It does **not** skip
publishing the notice policy. And it kills the recurring product: subscriptions,
cycle charges, auto-top-up and receivable collection all require standing
(DESIGN.md:1230-1231), so the intent rail could never take those over and legacy
would keep them forever. This is the option that quietly makes the cutover
impossible.

**C — Decide both halves now, conservatively.** Multi-day minimum lead, verified
billing contact with carrier terminal-delivery evidence, published retry
schedule, standing authority with a fixed expiry and a renewal ceremony,
customer-set per-charge and per-cycle ceilings, named frequency ceiling and
top-up amount. Unblocks the most. Real operational cost: every cycle charge waits
the lead time *after delivery*, and a bouncing address blocks collection
indefinitely, because INV-005 is fail-closed by design (DESIGN.md:254-258).

**D — Defer entirely.** `notice_policy` stays a placeholder, `ClausePolicyPublished`
refuses every intent, and legacy keeps collecting under no notice policy at all.
Not a neutral hold — it extends the weaker path.

### Recommendation
**OWNER-ONLY for the numbers.** The ceilings, cadence, expiry and renewal of a
standing authorization, including auto-top-up amount and frequency, are
consumer-protection and commercial judgements, and in TW they interact with item
10's e-invoice duties. An engineer must not pick how much a customer may
pre-authorize, how long a "yes" lasts, or how often we may automatically take
money. Migration 056's 86400 is what picking one looks like when nobody decided.

**Take option A — split the item and publish the notice policy on its own.** Two
engineering conditions come with it: (1) the accepted terminal-status set must
move out of predicate.go:372-373 into the published revision, and the minimum lead
duration must appear in `capabilities.Report` — DESIGN.md:719-723 requires both
and neither happens today; (2) the ADR must say which governs when a published
policy minimum and a per-authorization lead time disagree, because
`ClauseNoticeWaitElapsed` reads only the authorization's.

**Reject option B.** Its cost is invisible from the predicate: it looks like it
removes two clauses; what it removes is the recurring business.

### Cost to defer
Blocked: `ClausePolicyPublished` for every intent in both authority modes. Item 1
is one of eleven items EXECUTOR-GATE-PLAN.md:409-415 names as the minimum set for
one honest settlement.

More expensive with time, three ways:
1. Every day of deferral is a day the legacy rail collects with no notice control
   at all (`LegacyMoneyPaths = 11`, internal/account/capabilities/capabilities.go:37).
2. Once shadow intents are sealed under `unpublished/pending-decision-12`,
   publishing the real policy means every stored intent attests to a superseded
   revision, and INV-003 forbids editing a sealed intent. Supersession is the only
   route, at one new intent per stored intent — linear in how many accumulate.
3. The 86400 backfill is inert only while `billing_authorizations = 0`. The moment
   anything mints a standing authorization, a 24-hour lead time nobody accepted
   becomes de-facto policy and is sealed into digests.

Not more expensive: the notifier, carrier-evidence reader and destination schema
are LARGE work that can proceed against a *proposed* policy shape.

---

## 2 · Budget stop semantics

### What is being decided
Whether the spending cap a customer sets is allowed to switch anything off — and
if so, which: the service they are consuming, the money we take from them, or
both.

### Gates it unblocks
G1 only, and §12's stated reason is "the control vocabulary cannot otherwise name
a consequence" (DESIGN.md:1914-1916). Item 2 is one of **four** locks on
`proposedTermsRevision` (with 3, 9, 13) — one string at
internal/account/cycle/domain_charges.go:57, checked at revision.go:62, refused at
predicate.go:203, feeding `Environment.PolicyDigestsMatch`
(internal/intent/executor/executor.go:116-118; false at cmd/intent-executor/main.go:119).
Settling 3, 9 and 13 without 2 buys nothing, and vice versa.

Touches **zero** `UnbuiltEvidence` fields and adds no clause to the 29 in
`AllClauses` (internal/intent/predicate/clause.go:95-125). That absence is §12's
complaint, and it is literal — there are two consequence vocabularies in this tree
and a budget appears in neither:
- `intent.Refusal` (internal/intent/authorization.go:303-335): three ceiling codes,
  no budget code.
- `eligibility.Reason` (internal/account/eligibility/eligibility.go:85-104):
  NO_USABLE_CARD, FIRST_CHARGE_FAILED, TOO_MANY_FAILURES, UNPAID_INVOICES,
  OUT_OF_CREDITS — no BUDGET_EXCEEDED. This is the vocabulary the shipped
  serving-block path already carries to api-platform.

Measured state today: `budget.EvaluateAppBudget`
(internal/account/budget/service.go:193-200) records crossings and returns them.
Its only production caller (internal/account/usage/service.go:349-354) is
best-effort by contract — a budget error must not fail the usage ingest — so the
event is recorded regardless. `maxPercentUsed` (service.go:260) exists precisely
because spend is expected to exceed the cap. DESIGN.md:1844-1851 states this as a
known gap; DESIGN.md:150-152 lists it among the four substitutions the rebuild
exists to undo.

**The asymmetry that should drive the decision.** "Pause billable service" already
has a shipped transport: internal/account/standing/notifier.go:54 POSTs a blocked
verdict to api-platform's `/internal/apps/serving-block`, which rewrites app-stage
manifests so the edge gates serving without a per-request billing call, driven by
`GetServiceStatus` (internal/account/billing/service.go:409-435). Adding a budget
consequence there is a new `Reason` plus a signal, not a new mechanism.
"Block collection" has **no** transport: no clause reads a budget, so it is a new
line in the §4 conjunction.

Honest frame: DESIGN.md:1848-1851 says that while budgets stay alert-only, the
only ceiling on liability is the per-fact `ServiceAccrualExposure` reservation —
which is unbuilt (DESIGN.md:585-611). Whichever way item 2 goes, that reservation
is the load-bearing control, not the budget.

### Options
**A — Budget stops billable service at admission; collection of what already
accrued is unaffected.** Rides the shipped path: one new `eligibility.Reason`, one
signal, one trigger from `EvaluateAppBudget`. Accrued money still collects, which
matches DESIGN.md:1831-1836 ("later revocation stops future accrual. It does not
erase an accrued receivable"). Customer-visible consequence is an outage on their
app — the sharpest thing we can do to them — needing its own notice (items 1 and 3).
Caveat: the ingest hook is best-effort today, so a stop built on it fails **open**.

**B — Budget blocks collection; service continues.** Manufactures what
DESIGN.md:604-608 forbids: "Deferring the ceiling check to close turns a prepaid
wallet into an unauthorized credit line: the service is already rendered and the
money already spent." The receivable grows behind a cap the customer believes is
protecting them. This is the worst option, not a neutral alternative.

**C — Both.** A's plumbing plus B's clause, plus a policy for what the
accrued-but-uncollectible amount becomes — item 9's territory. Also collides with
gross-monotonic exposure (DESIGN.md:1815-1826): capacity does not return by
refund, so it needs its own accepted re-credit story.

**D — Keep it alert-only and say so.** Publish a terms revision stating a budget
is a notification control with no stop consequence. A real decision, not a
non-decision: it clears item 2's share of the terms revision and makes shipped
behaviour honest. Cost: the product ships a cap that does not cap,
`ServiceAccrualExposure` becomes the only bound and it is unbuilt, and the
published terms would contradict DESIGN.md:150-152.

### Recommendation
**A**, with alert-only kept as today's default until it can be armed. This is a
recommendation rather than OWNER-ONLY because §12 routes item 2 to G1, not to G3's
"accountable tax, legal and finance owners" — the substance is how our own control
behaves. The money half (what an accrued-but-uncollectible balance becomes) is
item 9's and is not mine.

Why: (1) A is the only option with a shipped transport, so it costs one vocabulary
entry rather than a new mechanism; (2) DESIGN.md:604-611 says the ceiling must
bind at service admission, not at collection — A is exactly that; (3) B is
explicitly forbidden by that passage; (4) D is cheap and available but publishes
terms saying the cap does not cap, which the owner should choose deliberately
rather than inherit from the code.

**One precondition, not optional:** internal/account/usage/service.go:349-354 makes
the budget hook best-effort by contract. A stop built on a best-effort hook fails
open, which is a control in name only. If A is chosen, budget evaluation must
become blocking for the stop decision, and the refusal must be the customer-facing
consequence, not a log line.

### Cost to defer
Blocked: the terms revision, and through it `ClausePolicyPublished` — but item 2 is
one of four locks on that one string, so deferring it alone does not move the gate
while 3, 9 and 13 are open.

The sharper cost is not the gate. Today nothing bounds a customer's liability while
service runs: the cap does not cap, and `ServiceAccrualExposure` is unbuilt. That
exposure is live on the **legacy** rail now and does not wait for the cutover.

More expensive: alert-only semantics are shipped and customer-visible. The longer a
cap that does not cap sits in front of customers, the more a later hard stop becomes
a behaviour change requiring notice (items 1 and 3). Deciding A now, before anyone
relies on current semantics, is materially cheaper than deciding A in a year. The
trap worth naming: option D gets *cheaper* the longer it waits, because it is what
the code already does — and DESIGN.md:150-152 says that is the defect.

No schema cost either way. `budget_alerts` exists, no intent references it, and
`charge_intents = 0`.

---

## 3 · Change policy

### What is being decided
Whether a customer who already said yes stays bound after we move a price, a
module manifest version, a tax rule or the terms — and if not, what notice and
what re-consent it takes to bind them again.

### Gates it unblocks
**G1.** `PolicyDigestsMatch` (cmd/intent-executor/main.go:119) via
`ClausePolicyPublished` (predicate.go:185-203). Item 3 owns two of the five checked
names — `terms_revision` and `price_book_revision` — so even a fully built policy
registry cannot pass this clause until item 3 publishes both. The placeholders are
internal/account/cycle/domain_charges.go:57-62 and a second, **divergent** copy at
internal/account/autotopup/executor.go:1391-1394 (autotopup says `unpublished/` for
tax where cycle says `not-applicable/` — two constants for one policy that already
disagree).

Half of this item is **already answered in code, in the strictest direction, and
nobody decided it.** `BillingAuthorization.Permits`
(internal/intent/authorization.go:387-474) refuses on a price-book move
(:421-423, `RefusalPriceBookMoved`), a terms move (:427-429), a notice-policy move
(:430-432). The shipped default is "every change requires renewed authorization."
The asymmetry: `tax_rule_revision` and `routing_policy` are compared **nowhere** in
`Permits` — a tax rule change silently keeps a standing authorization alive today,
which is precisely the half §12 names.

`ClauseNoticeDelivered` / `ClauseNoticeWaitElapsed` (predicate.go:102-140) measure
against a single `NoticeLeadTime()` (authorization.go:154, :536-538). Item 1 owns
the standing-authority lead time; item 3 owns the price-change lead time; the type
cannot hold two answers.

`Unbuilt.FundingMatchesAccepted` (state.go:196, clause.go:70) needs item 3 to say
which acceptance is "the accepted" one after a revision moves.

**G3.** `NoticeReceipt.RevocationPathFresh` (state.go:83-86; column at
migrations/billing/054_intent_core.up.sql:240) is the only cancellation hook in the
predicate, returned as the last term of `noticeTerminallyDelivered`
(predicate.go:362). "The customer's route to cancel was verified working" is
unwritable until cancellation terms exist.

**Already settled, do not re-decide:** DESIGN.md:1277-1280 fixes the mechanism for
module price changes — a new immutable manifest and price revision with future
effect, plus notice and acceptance. DESIGN.md:1285-1287 settles that a rail change
needs a replacement intent, never a mutation. Out of scope: cancellation-proration
arithmetic (items 6 and 12), refunds and disputes (item 9).

### Options
**A — Strict re-authorization.** Ratify the shipped default and extend it to tax and
routing: any revision move under a standing authorization refuses; the customer
re-accepts before the next collection. No charge is ever collected under a revision
the customer did not see. Cost: one statutory tax-rate change halts standing
collection for every payer on the old revision, and re-consent conversion becomes
the revenue bottleneck. Requires a re-acceptance flow in api-platform **before** any
price may change.

**B — Two classes: deal vs mechanics.** Terms and price book require renewed
acceptance; notice policy, tax rule and routing require delivered notice at the
accepted lead time but keep the authorization alive. Statutory changes never halt
collection. Cost: the class must be **sealed**, not inferred — a canonical-encoding
change (internal/intent/canonical.go), i.e. a v5 supersession on top of the v2/v3/v4
batch. And the customer's accepted disclosure no longer matches the charge's tax
basis, so §4's "the split the customer saw is the split that settles" weakens to
"the price the customer saw is the price that settles."

**C — Grandfather by pinned revision, with a published sunset.** An authorization
keeps the price book and module manifest version it was accepted under until a
published sunset; new revisions bind at the first boundary after sunset plus lead
time. The only option that gives a truthful answer to "will my price change?".
Cost: the rater must price N concurrent price books and N module manifest versions
permanently, plus a per-payer effective-revision resolver. Same shape as the
unresolved half of item 15 (DESIGN.md:1973-1975) — one answer must govern both, or
two grandfather policies ship.

**D — Term price lock.** Bind price and terms for the authorization's term; changes
take effect at renewal, using the expiry that already exists (authorization.go:409).
The mid-term question stops arising. Cost: term-bounded authorizations everywhere,
and we absorb any mid-term tax rate change — which for Taiwan business tax may not
be lawful to absorb (item 10).

### Recommendation
**OWNER-ONLY.**

Two things are safe to do now with no policy input and should not wait:
(a) close the `Permits` asymmetry so `tax_rule_revision` and `routing_policy` are
compared like the other three (authorization.go:421-432) — fail-closed in both
directions, so a later decision to relax is a visible edit rather than a gap;
(b) collapse the two divergent placeholder blocks (domain_charges.go:57-62 vs
autotopup/executor.go:1391-1394), which currently make different claims about tax.
Neither reconstructs a policy.

### Cost to defer
Blocked: G1 and G3, unconditionally. All 17 executor gates can be built to
completion and the rail will still move zero money. Item 3 is not on the
engineering critical path; it is on the collection critical path.

More expensive at three thresholds, two of them open now:
1. Every authorization minted before publication is minted under an unpublished
   terms revision, and the code already enforces that a terms move voids an
   authorization (authorization.go:427-429). Publishing the first real terms
   revision invalidates every authorization in flight and forces a re-acceptance
   campaign sized by how long this waited. `billing_authorizations = 0` today —
   this is the cheapest it will ever be.
2. Grandfathering is retroactive-hostile. You cannot promise afterwards that a
   customer was grandfathered through a cycle you already billed at the new price.
   Every cycle legacy bills while this is open has its grandfather status decided
   by default — including the infra-markup line item 15 says is still on the bill.
3. Option B's per-revision class is a digest-shape change: one migration and golden
   digests today, a live-document migration past the INV-003 trigger after the
   first sealed production intent.

---

## 4 · Merchant of record

### What is being decided
Name the legal company that sells to the customer in each market and on each rail,
and the company that carries the tax liability for what it sells — so the receipt,
the settlement route and the tax determination all bind to one accountable seller.

### Gates it unblocks
**G1, directly.** `ClauseMerchantOfRecord` (clause.go:66, evaluated at
predicate.go:223-224) reads `UnbuiltEvidence.MerchantOfRecord` (state.go:192),
hardcoded false by the empty literal `Unbuilt: predicate.UnbuiltEvidence{}` at
cmd/intent-executor/main.go:122. It is a non-provider clause, so the six-clause
`providerRemainder == 0` exemption (clause.go:83-90) does not touch it.

**G3, serially.** MoR is strictly upstream of two more gates:
MerchantOfRecord → CommercialIdentity → TaxIndependentlyReproducible
(EXECUTOR-GATE-PLAN.md:358, restated as build order D3 at :564-566, "Not
parallel"). So it also holds shut `ClauseCommercialIdentity` (predicate.go:221-222
→ state.go:191) and the third conjunct of `ClauseTaxFinal` (predicate.go:172-183),
which reads `Environment.TaxIndependentlyReproducible` (executor.go:122-124, false
at main.go:121). DESIGN.md:1920-1922 states the coupling.

**G1/G4, the authorization record.** `BillingAuthorization` carries the
`MerchantBindingSet` as a control field, not description (DESIGN.md:438). §4
requires "the final MerchantOfRecordBinding has an accepted
membership/compatibility proof matching notice, funding, and rail"
(DESIGN.md:784) — membership is meaningless against an undefined set.

**G4.** DESIGN.md:413 fixes the order: legal seller, then settlement route, and
route selection must not change price, tax or gross obligation. Reconciliation
verifies every provider relationship by merchant account (DESIGN.md:1643-1644).

Second-order and measurable: a multi-entity answer changes intent **shape**. Cycle
intents partition on equality of payer, commercial identity, tax profile, currency
(DESIGN.md:866) — N entities means N intents per cycle per payer. And
`Capabilities` must publish the `MerchantBindingSet` byte cap and row count
(DESIGN.md:1412), which does not exist in
internal/account/capabilities/capabilities.go today.

### Options
**A — One entity, one market, one rail** (the entity holding the single
`STRIPE_SECRET_KEY`, 8 non-test call sites, EXECUTOR-GATE-PLAN.md:476-484). The
binding set has one member, the compatibility proof is near-trivial, no per-entity
partitioning, and Taiwan/TWD/NewebPay stays formally unsupported — which keeps
items 10 and 11 off the G1 critical path. Cheapest route to a true
`ClauseMerchantOfRecord`. Cost: any customer outside that market is unsupported for
automatic collection, not merely untaxed.

**B — Two entities, split by market/rail** (TW entity for TWD/NewebPay, non-TW for
USD/Stripe). The binding set becomes a genuine set, so the compatibility proof
becomes real engineering; intents partition per entity; a second merchant account
per entity with its own credential multiplies the enclave/credential-exclusivity
problem (EXECUTOR-GATE-PLAN.md:476-484); and it drags items 10 and 11 onto the same
critical path. DESIGN.md:1503-1506 forbids claiming any NewebPay behaviour before
the merchant agreement exists, so this option has a procurement lead time in front
of its engineering.

**C — A third-party merchant of record / reseller.** The tax determination becomes
the reseller's, and §7 will not accept a determination it cannot reproduce: a
proprietary or vendor-attested result is recorded `provider_attested`, disclosed as
unsupported for independent verification, and leaves the state `unknown`, which
cannot execute (DESIGN.md:1358-1367, 1477-1489). This likely makes
`TaxIndependentlyReproducible` structurally unreachable — it buys G1 a seller and
buys G3 nothing.

**D — Defer.** Not neutral: legacy keeps collecting in hardcoded USD
(internal/account/cycle/types.go:195-198) and every proposed intent keeps sealing
Jurisdiction `"not-applicable"` under `not-applicable/pending-decision-12`
(domain_charges.go:60, :420-428; internal/account/cycle/overage.go:843-851).

### Recommendation
**OWNER-ONLY.** Naming the legal seller and the entity carrying tax liability is
not an engineering call under any framing.

### Cost to defer
Blocked: G1, G3 and G4 in full — `ClauseMerchantOfRecord`, and behind it
`ClauseCommercialIdentity` and `ClauseTaxFinal`, so no intent executes for a reason
no engineering can remove. Also the `Capabilities` expansion (DESIGN.md:1412) and
the entire tax subsystem downstream.

**This is the head of the longest serial chain in the plan**
(EXECUTOR-GATE-PLAN.md:358, :564-566), so its latency is not absorbed by parallel
work: every week of deferral is a week added to the end date.

More expensive, with a hard cliff: adding the seller entity / `MerchantBindingSet`
to the authorization record and to whatever the intent freezes about it is, right
now, one migration plus golden-digest churn. After the first sealed production
intent it is a migration of live sealed documents past the INV-003 trigger
(054_intent_core.up.sql:97-121).

Accruing, not merely blocked: while this is open, legacy issues USD invoices whose
intent twin asserts Jurisdiction `"not-applicable"` with no rule artifact behind
the claim, and nothing in migrations/billing stores a seller entity, customer
country or tax id. If the answer names a TW entity with business-tax duties
(DESIGN.md:1499-1503), the population of invoices issued under the wrong assumption
grows monotonically with every cycle. Whether that is an exposure is an
accountant's judgement — which is itself a reason to decide early rather than size
it later.

---

## 5 · Registrations and treatment

### What is being decided
Which tax registrations the seller actually holds, which customer jurisdictions are
therefore supported for automatic collection, and what a validated tax id, an
exemption certificate or a reverse-charge case does to the amount — so the engine
has an allowlist to refuse against instead of collecting from anyone, anywhere, at
zero.

### Gates it unblocks
**G3.** This decision is the *content* of two of the four parts that decide whether
a `TaxPolicyRevision` can execute at all: "The supported jurisdictions and required
location evidence. And the behavior when evidence is unavailable"
(DESIGN.md:1396-1400). No revision can be published without it.

Customer-side records: `ClauseCommercialIdentity` (clause.go:65, predicate.go:221-222)
reads `UnbuiltEvidence.CommercialIdentity` (state.go:191), false via main.go:122.
Its supporting records are the `CommercialIdentityBinding` and the versioned
`TaxProfileReceipt` (DESIGN.md:1379-1388); this decision defines their vocabulary —
B2B vs B2C class, what counts as a validated tax id, what an exemption reference is.
Ordering constraint: MoR (item 4) is upstream, so item 5 alone does not flip this
clause (EXECUTOR-GATE-PLAN.md:358).

Then `ClauseTaxFinal` (predicate.go:172-183) requires
`Environment.TaxIndependentlyReproducible` (executor.go:122-124; false at
main.go:121). Reproduction needs the rule table keyed by "jurisdiction, charge-kind
classification and customer class" (DESIGN.md:1402-1408) — customer class *is* the
B2B/B2C/exempt/reverse-charge answer.

Verifier artifact: the required public golden vectors must cover "exemption,
reverse charge" by name (DESIGN.md:1508-1511). They cannot be written without this,
and the offline verifier does not exist in any form (VERIFICATION.md:165-171:
"There is no verifier in the tree").

Cross-repo: the engine may only receive an engine-issued envelope relayed unchanged
and may not establish that an address or registration is yours or valid
(DESIGN.md:1373-1382). So the enrollment surface lives in api-platform +
web-account (EXECUTOR-GATE-PLAN.md:470-473), with its own lead time. **Measured:**
nothing in migrations/billing stores a country, address, tax id or exemption today,
and no `automatic_tax` reference exists anywhere in internal/shared/stripe or
internal/provider.

Would also close a live hole: nothing reads `Tax().Jurisdiction` — an intent could
seal "Atlantis" and no clause objects (EXECUTOR-GATE-PLAN.md:311-315). A
supported-jurisdiction allowlist is exactly the independently-readable half
`ClauseCommercialIdentity` is missing.

### Options
**A — Home jurisdiction only**, an allowlist of exactly one, everything else
`unknown`. Smallest publishable revision; no tax-id validation source, no exemption
workflow, no reverse-charge rows at v1; a non-domestic customer is refused rather
than silently zero-rated, which DESIGN.md:1352-1357 already requires. Cost: the
automatic-collection footprint is one market.

**B — Home plus an explicit allowlist of registered foreign jurisdictions**, with
B2B reverse charge and validated tax ids. Largest option: a tax-id validation source
per jurisdiction, an exemption-document workflow, the full golden-vector set, and it
collides with item 8 — a rate source without redistribution rights makes that
jurisdiction unsupported and must not fall back to an attestation
(DESIGN.md:1389-1392). The more jurisdictions, the more likely the only available
feed cannot legally be republished to the verifier.

**C — A published "no tax applies" rule.** Not free, and the option most likely to
be underestimated. DESIGN.md:1349-1351 requires `not_applicable` to be reproduced
from an immutable public rule and its inputs; "we don't charge tax" is a claim that
needs an artifact. Still requires a published, effective-dated, digest-pinned
revision, an evaluator and golden vectors — just a very small table. It is also,
word for word, what the shipped code already asserts **without** the artifact
(domain_charges.go:60, :427).

**D — Fail-closed holding pattern.** Publish the supported-jurisdiction set as
explicitly **empty** and make the placeholder structurally refuse. No collection,
but the refusal becomes a stated control with a named reason instead of a fiction
inside the canonical digest. Needs no policy answer and can be built today. Pair
with EXECUTOR-GATE-PLAN.md:316-322: change `not-applicable/pending-decision-12` to
`unpublished/` at domain_charges.go:60 and overage.go, since `not-applicable` is a
substantive claim the engine has no authority to make.

### Recommendation
**OWNER-ONLY.**

### Cost to defer
Blocked: G3 entirely. No `TaxPolicyRevision` can be published, so the tax evaluator,
the golden vectors, the offline verifier binary and `ClauseTaxFinal` are all
unstartable — four of the 17 subsystems. `ClauseCommercialIdentity` stays false, and
with it the api-platform + web-account enrollment surface, which is cross-repo work
with independent scheduling latency: deferring the decision defers the *start* of
that work.

Partially more expensive, and it matters which part. The customer-side tables can be
added later at ordinary cost. But **what a determination freezes** cannot:
DESIGN.md:1427-1428 requires the `CommercialIdentityBinding` and its proof, the
location evidence and issuer, and the `TaxProfileReceipt` revision and proof-stream
head to be frozen *into* the determination, which is inside the canonical digest
(054_intent_core.up.sql:47-50, extended by 060_intent_tax_verification.up.sql:27-39).
Same cliff as item 4.

Compounding, and worth naming: an empty supported-jurisdiction list is trivially
consistent, so a later reviewer can mistake "nothing to check" for "checked."

---

## 6 · Tax classification, display and rounding

### What is being decided
Which of the nine catalog charge kinds MirrorStack owes tax on and under what
classification, and the three arithmetic conventions — inclusive or exclusive price,
per-line or per-invoice rounding, and the order credits eat a line before tax — that
turn that answer into a number a customer can recompute.

### Gates it unblocks
**G2.** §6 requires every sealed line to carry "the taxable classification and tax
allocation" as one of its six authority fields (DESIGN.md:1267-1273).
internal/intent/chargeintent.go:23-29 shows `Line` carrying five factors and no
classification; internal/intent/catalog.go:34 shows `KindTax` as a bare name. The
catalog is closed as a list of names and open as a list of tax treatments; G2 cannot
close on the names alone.

**G3**, two clauses, both currently unsatisfiable:
1. `ClauseTaxFinal` (predicate.go:172-183; clause.go:50) ANDs `Tax().Resolved` with
   `SealedState.TaxIndependentlyReproducible` (state.go:153), hardcoded false at
   main.go:121 — and nothing in the tree could set it: `TaxResolver` is an interface
   with **zero** implementations (internal/intent/rater.go:43-44, called at :200).
2. `ClausePolicyPublished` (predicate.go:203) requires no unpublished revision, and
   `tax_rule_revision` is one of the five (revision.go:65). Item 6 is one of six §12
   items gating that id (EXECUTOR-GATE-PLAN.md:335).

No per-leg escape: `ClauseTaxFinal` is absent from `providerClauses`
(clause.go:83-90), so even a wallet-only intent is refused on it.

**Why the answer changes code, not only data — three measured shape constraints:**
- `Seal` derives `total = subtotal + tax.AmountMicros` (chargeintent.go:451-455).
  That is exclusive display hardcoded, with no slot for the `rounding` or
  `eligibleRatingTaxCredits` terms §6's own obligation formula requires
  (DESIGN.md:1180-1187). An inclusive answer, or a rounding line, changes the sealed
  document and therefore the canonical digest version.
- `TaxResolver.Determine(payer, currency, subtotalMicros, at)` is handed **one**
  subtotal (rater.go:43-44). A per-kind classification cannot be expressed through
  it; if classification varies by kind, both this interface and `Line` change.
- `centsFromMicros` (internal/provider/stripeadapter/adapter.go:205-217) rounds
  micros to cents half-up at the wire with `10_000` hardcoded — a two-decimal
  assumption already wrong for TWD. DESIGN.md:1443-1444 forbids "a second rounding
  point on the provider's side." Item 6 decides whether that line is a violation or
  whether `Seal` must guarantee the total is already an exact multiple of the
  settlement minor unit.

### Options
**A — One classification, one jurisdiction, exclusive display, per-line rounding at
the settlement minor unit, credits allocated to lines before tax in a published
order.** Smallest artifact satisfying §7's table shape (DESIGN.md:1403-1409); one
row set; `ClauseTaxFinal` becomes reachable in that jurisdiction and every other
location stays `unknown`. Still needs a rounding term in `Seal` and a canonical
digest bump, but no classification field on `Line`.

**B — Full per-kind classification table across the nine kinds**, both inclusive and
exclusive treatments, invoice-level rounding. The doc's intended end state and the
only option that survives a second market. Costs a classification field on `Line`, a
per-line `TaxResolver`, a `rounding` kind in the catalog, a canonical digest bump,
and the complete golden-vector set before any intent seals.

**C — Publish "no tax arises, anywhere" as an accepted immutable artifact.** Cheapest
route to a published `tax_rule_revision`, and it makes the `TaxNotApplicable` class
(chargeintent.go:103-114) an honest sealed claim instead of a placeholder. But it is
a substantive assertion that no jurisdiction has a collection duty, and
DESIGN.md:1497-1504 says Taiwan business-tax and e-invoice duties are exactly what is
undecided. Only the owner knows whether the MoR entity is registered nowhere. If it
is wrong, it is wrong retroactively on every charge sealed under it.

**D — Defer.**

### Recommendation
**OWNER-ONLY.** A tax-treatment determination per product line, a display convention
with consumer-pricing-law consequences, and a credit-allocation policy with
revenue-recognition consequences. Picking any of the four would be inventing the
policy DESIGN.md:2011-2014 forbids reconstructing "from current constants, code
comments, or the shape of today's Stripe-shaped schema."

Buildable meanwhile, needing no answer: the J1/J2/J3 tightenings at
EXECUTOR-GATE-PLAN.md:306-322 — make `ClauseTaxFinal` read `Jurisdiction` and
`RuleRevision` instead of the unfalsifiable `Resolved` flag; change the
`not-applicable/` placeholder to `unpublished/` at domain_charges.go:60 so the sealed
digest stops carrying a tax claim; add a closed classification enum on `Line` whose
zero value refuses to seal, the idiom `TaxVerificationClass` already uses
(chargeintent.go:90-91).

### Cost to defer
Blocked: G2 entirely, and G3's tax leg entirely. `TaxIndependentlyReproducible` can
never be set, so `ClauseTaxFinal` refuses every intent of every kind — no wallet-only
shortcut. Downstream: the public golden vectors and the offline verifier;
EXECUTOR-GATE-PLAN.md:515-521 warns that building executor-side tax reproduction
before the verifier risks creating the second rating implementation INV-002 forbids.

Two things get more expensive, and they are unrelated:
1. **Retroactive tax exposure — the one that compounds.** Legacy keeps collecting and
   applies no tax at all: no `automatic_tax`, no tax rate and no tax line anywhere in
   the shipped Stripe path. If item 6 eventually classifies any shipped kind as
   taxable in a market where the entity has a duty, the tax is owed on gross already
   collected and practically un-rebillable, so it comes out of margin. That grows with
   revenue, not with engineering time.
2. **Digest cost — zero, and only while it stays zero.** `charge_intents = 0`, so no
   sealed document yet carries the placeholder. Once shadow sealing writes rows,
   changing classification, display or rounding stops being a data change and becomes
   a canonical version bump plus a re-seal of history.

---

## 7 · Location evidence

### What is being decided
Which facts about a customer count as proof of where they are for tax purposes, and
how long each stays good before it must be re-established. The conflict half is
already settled: conflicting required evidence yields `unknown` (DESIGN.md:1435).

### Gates it unblocks
**G3 only**, through two clauses plus the artifact both read.
1. `UnbuiltEvidence.CommercialIdentity` (state.go:191) via `ClauseCommercialIdentity`
   (predicate.go:221; clause.go:64). §4 requires "CommercialIdentityBinding matches
   tax, source, and wallet state" (DESIGN.md:784), and §7 says a final determination
   freezes "the location evidence and issuer, and the `TaxProfileReceipt` revision and
   proof-stream head" (DESIGN.md:1427-1429). The location evidence is literally the
   content of that clause.
2. `TaxIndependentlyReproducible` (state.go:153) via `ClauseTaxFinal`
   (predicate.go:172-183): conflicting or missing required evidence yields `unknown`
   (DESIGN.md:1435, 1385-1387) and `unknown` cannot execute (DESIGN.md:1355-1356).
3. `PolicyDigestsMatch` / `ClausePolicyPublished` (predicate.go:203): required
   location evidence is one of the four parts of a `TaxPolicyRevision`
   (DESIGN.md:1396-1400). **So items 6 and 7 do not queue independently — one artifact
   needs both.**

All three are hardcoded false at cmd/intent-executor/main.go:119-122.

Ordering: MerchantOfRecord → CommercialIdentity → TaxIndependentlyReproducible, "not
parallel" (EXECUTOR-GATE-PLAN.md:564-566). Item 7 is strictly downstream of item 4 and
strictly upstream of the tax clause.

**Measured absence:** there is no location, address, country or evidence field anywhere
under internal/intent. `TaxDetermination` (chargeintent.go:53-80) carries
`Jurisdiction` as a bare string no clause reads, so an intent can seal "Atlantis"
today; the shipped domain leg seals "not-applicable" (domain_charges.go:420-427).

Cross-repo consequence unique to this item in the 6/7 pair: api-platform may relay the
evidence only unchanged and may not establish that it is yours or valid
(DESIGN.md:1371-1376); enrollment uses an engine-issued envelope recorded on the payer
stream (DESIGN.md:1377-1382). This decision creates api-platform work.

### Options
**A — One accepted evidence type**: the payer's self-declared billing country, captured
once at enrollment, no expiry. Cheapest, no per-jurisdiction table. But §7 already
calls an unproven address `unverified`, which yields `unknown` (DESIGN.md:1385-1387),
so this cannot produce a `final` determination without amending §7 itself. Named
explicitly because it looks like the cheap answer and is actually a doc change plus a
decision.

**B — Two non-contradictory pieces from a closed list published inside the artifact**
(e.g. billing address plus payment-instrument country), network-derived signals
admissible only as a tiebreaker and never sufficient alone, a published maximum age
per evidence type, conflict → `unknown`, re-enrollment on expiry or change. Fits §7
with no doc amendment and matches what most VAT/GST regimes require. Costs a
`TaxProfileReceipt`, an engine-issued enrollment envelope, a payer-stream write and
api-platform relay work. The only option that makes `CommercialIdentity` settable from
real evidence rather than a flag flip.

**C — Scope to the MoR entity's own market only.** One supported jurisdiction, one
evidence rule; every customer whose evidence points elsewhere is `unsupported` →
`unknown` → no automatic collection on the intent rail. Smallest honest artifact, and
it makes a first cutover leg reachable. But it leaves most customers collectible only
on legacy, contradicting §11's intent-only condition `legacyMoneyPaths: 0`
(DESIGN.md:1882-1888). A step, not a destination.

**D — Defer.**

### Recommendation
**OWNER-ONLY.** Whether a self-declared country, an instrument BIN country or a
network address is acceptable proof of place of supply is a tax-compliance
determination, and the maximum age of each is a risk judgement about audit defence.

The engine-side half is settled and needs nothing from the owner: conflict → `unknown`
(DESIGN.md:1435); a profile changed after acceptance is `unverified` → `unknown`
(DESIGN.md:1385-1387); evidence is bound at enrollment on the payer stream, not read
per-intent from the relay (DESIGN.md:1377-1382). So the owner's question is narrow and
can go to tax counsel verbatim: **name the accepted evidence types per supported
jurisdiction, and a maximum age for each.**

### Cost to defer
Blocked: G3, through `CommercialIdentity` and transitively
`TaxIndependentlyReproducible`. Neither can be set from anything that exists. It also
blocks item 6's artifact, because required location evidence is part of the same
`TaxPolicyRevision` — **deciding 6 without 7 publishes nothing.**

Does it get more expensive? On the engine side, almost not at all — the unusual one.
Nothing has been built on a wrong assumption, so there is no location field to migrate
and no digest contamination to unwind.

What does accrue is customer-facing. Every customer who signs up while this is open is
one whose location evidence was never captured at enrollment under an engine-issued
envelope, and DESIGN.md:1377-1387 does not permit back-filling it from whatever
api-platform happens to hold — an address the engine did not enroll is an unproven
address, which is `unverified`, which is `unknown`. Collecting it later is a
re-enrollment interaction with the entire installed base, sized by the customer count
on the day the decision lands.

Second-order: item 7 sits behind item 4 in a strict chain, so deferring item 4 defers
this one by the same amount. They cannot be run in parallel to recover the time.

---

## 8 · Rate source and verifiability

### What is being decided
Which named body of tax rate-and-rule data MirrorStack licenses for each market it
intends to collect in, and whether that licence permits republishing it to customers
as a pinned public artifact they can evaluate themselves — because a source that
cannot be redistributed makes its jurisdiction unsupported for automatic collection no
matter how authoritative it is.

### Gates it unblocks
§12 files this under G3 (DESIGN.md:1935). **In code it is broader.**

Primary field — `Environment.TaxIndependentlyReproducible`: declared at
internal/intent/predicate/state.go:143-153 (an `Environment` bool, not an
`UnbuiltEvidence` field), hardcoded false at cmd/intent-executor/main.go:121, consumed
by `ClauseTaxFinal` (predicate.go:181-183). `ClauseTaxFinal` is **not** in
`providerClauses` (clause.go:83-90), so the only exemption in `Evaluate`
(predicate.go:44-51, `providerRemainder == 0`) does not reach it. It refuses every
intent including a wallet-only one. **So although the doc says G3, this field is a hard
G1 blocker too.**

Second field — `PolicyDigestsMatch` (main.go:119) via `ClausePolicyPublished`
(predicate.go:203), which also requires no unpublished revision; `tax_rule_revision` is
entry four (revision.go:65). Item 8 is one of six §12 items gating that single string.
Today: `not-applicable/pending-decision-12` (domain_charges.go:60, used at :423 and by
overage.go:846) and `unpublished/pending-decision-12`
(internal/account/autotopup/executor.go:1394, used at :1349).

No `UnbuiltEvidence` field maps to item 8.

**Already built, waiting on this answer:** the sealed carrier exists.
`TaxDetermination.Verification` (chargeintent.go:79), the closed set at
chargeintent.go:117-131, `Seal` refusing the zero value at chargeintent.go:392, stored
under a CHECK at migrations/billing/060_intent_tax_verification.up.sql:27-39. Item 8
decides what may ever legitimately be written as `independently_reproducible`.

**Open engineering question this answer must settle alongside:** `ClauseTaxFinal` reads
the executor's `Environment` bool, not the sealed class. An intent sealed
`provider_attested` (chargeintent.go:102) and one sealed `independently_reproducible`
are treated identically as long as the executor asserts the bool. §7 says a proprietary
result must leave the determination unknown (DESIGN.md:1483-1489), so whichever option
is chosen, the clause should derive from the **sealed class** rather than take the
environment's word.

### Options
**A — License a redistributable source per market and publish the artifact plus golden
vectors** (the §7 target). The artifact must be typed declarative data, effective-dated,
lookup plus integer arithmetic, never a program or WASM (DESIGN.md:1401-1410); public
means retrievable without an operator, provider or vendor secret and usable by the
offline verifier (DESIGN.md:1416-1418); golden vectors must cover inclusive/exclusive,
zero, exemption, reverse charge, compound components, both rounding points, credits,
refunds, an unsupported jurisdiction, a conflict and an outage (DESIGN.md:1508-1510).
The only path on which `independently_reproducible` can be written truthfully. Requires
a licence review per market — procurement calendar, not a ticket
(EXECUTOR-GATE-PLAN.md:502-509) — plus the evaluator and the offline verifier binary
that exists nowhere (VERIFICATION.md:166-168, 185-186).

**B — One market first**, Taiwan only, every other jurisdiction published as unsupported.
Smallest legal surface, one rate table, one golden-vector set. Non-TW customers stay
`unknown` — a revenue decision, not an engineering one. Not independent of item 10:
Taiwan business tax, e-invoice issuance, numbering, retention and correction are
themselves undecided (DESIGN.md:1500-1502), so B unblocks G3 only when 8 and 10 land
together.

**C — Buy a commercial calculator** (Stripe Tax, Avalara, Vertex) as the rate source.
§7 has **already settled** that this cannot open the gate: DESIGN.md:1483-1489 records a
proprietary result as `provider_attested`, discloses it as unsupported for independent
verification, leaves the determination `unknown`, and states that choosing a different
vendor cannot weaken this. C buys estimates, internal ops and a dashboard; it moves
`TaxIndependentlyReproducible` not at all. A real option only as an explicit decision
*not* to open G3 through the intent rail while legacy keeps collecting.

**D — Publish a non-applicability rule.** Decide that the current charge kinds are, in
the supported markets, outside the tax base, and publish that as a reproducible
effective-dated artifact with its inputs and reasoning. This is what the code already
asserts without authority (domain_charges.go:423-427, overage.go:846-850,
autotopup/executor.go:1349-1353). By far the cheapest path to G3 — one row per
(jurisdiction, charge kind), no vendor, no feed. But it is a tax-liability judgement
about MirrorStack's own exposure, and DESIGN.md:2013-2015 forbids reconstructing it from
today's constants, which is exactly what adopting D *from the code's current behaviour*
would be.

### Recommendation
**OWNER-ONLY.**

### Cost to defer
Blocked: G3 entirely, and in practice all execution — `TaxIndependentlyReproducible`
stays the literal `false` at main.go:121 and `ClauseTaxFinal` has no exemption, so two
of the 29 clauses refuse every intent, wallet-only included. Two of the 17 unbuilt
subsystems sit behind this answer.

More expensive, on a clock that is not collection:
1. The digest cost for the verification **class** is already paid (canonical v2 /
   migration 060 landed while `charge_intents = 0`). But the frozen tax **inputs** §7
   requires — line basis, credit allocation, rate components, rounding steps, evidence
   commitments (DESIGN.md:1433-1443) — are **not** in canonical v2. Whatever item 8
   decides will name some of them. One migration today; a migration of live sealed
   documents past the INV-003 trigger after the first sealed row.
2. **That window closes on proposal, not on collection.** Arming
   `BILLING_CYCLE_INTENT_CUTOVER="propose-do-not-collect"`
   (cmd/billing-cycle/main.go:387-388) makes the cut-over legs seal and persist —
   internal/intent/proposer/proposer.go:133 calls `store.SaveIntent`. The cheap-migration
   window is open only while that flag is unarmed in production. Arming the shadow rail
   before item 8 is answered starts the expensive clock without moving any money.
3. Procurement is serial and cannot be parallelised. Building the evaluator against a
   guessed artifact shape is not a safe hedge: INV-002 (DESIGN.md:135) requires one
   rating model shared by preview, settlement and the verifier, so a wrong shape means
   rewriting the evaluator and the verifier together.

Honestly stated: legacy keeps collecting throughout, so deferring item 8 costs no
revenue today. That is also why it can sit indefinitely without anything failing loudly.

Buildable while it waits, no policy input: (a) make `ClauseTaxFinal` derive from the
sealed `TaxVerificationClass` instead of trusting the `Environment` bool; (b) build the
policy revision registry and leave it **empty**, so `PolicyDigestsMatch` is false for a
structural reason rather than because someone typed `false`; (c) close the drift where
two proposers seal `not-applicable/` while the third says `unpublished/` — both refuse
today, but `not-applicable/` asserts a tax verdict the engine has no authority to assert.

---

## 9 · Adverse outcomes and value return

### What is being decided
Whether value ever returns to the customer's original payment rail at all (a cash
refund) or only to a MirrorStack balance, and by what authority each of the eight
backwards-money events — cancellation, refund, partial refund, dispute, chargeback, bad
debt, write-off, negative balance — is initiated, plus the two thresholds that decide
when the engine declines to move money at all.

### Gates it unblocks
§12 assigns G2, G3 and G4 (DESIGN.md:1937-1940).

**G1 (not claimed by the doc, but real in code):** `ClausePolicyPublished`
(predicate.go:185, 203) checks `terms_revision` (revision.go:62), which is
`"unpublished/pending-decision-12"` on every intent this tree proposes
(domain_charges.go:57, :404). Item 9 is one of **four** locks on the terms revision
(with 2, 3, 13). Settling item 9 alone publishes nothing.

**G2:** §6's obligation formula subtracts `eligibleRatingTaxCredits`
(DESIGN.md:1180) and §6:1194-1198 lists four bill-reducing kinds
(`promotional_credit`, `adjustment_credit`, `tax_credit`, `rounding`). **None exists:**
`catalog` holds exactly nine positive kinds (catalog.go:58-68) and `Line` carries only
meter/module/quantity/unit price (chargeintent.go:23-29). `Seal` refuses any negative
line outright (`ErrNegativeLine`, chargeintent.go:328, enforced :349) and
`ErrCreditExceedsTotal` forbids crediting past zero (:319, enforced :465-468).
§6:1203-1205 says a negative total becomes wallet credit, a refund intent or carried
credit and calls that "a product choice (§12 item 9)." **Measured consequence: a
negative total is not merely undecided, it is unrepresentable, and the engine's current
behaviour is a fourth, undocumented option — quarantine at seal.**

**G3:** the adapter capability §5 requires includes "authorize/capture, void and refund
support" (DESIGN.md:1010). No adapter capability type exists in the tree, and
EXECUTOR-GATE-PLAN.md:273 scopes `RailCapabilities` to currencies and exponents only —
so `UnbuiltEvidence.RailSupportsPlan` (state.go:197) has an unscoped conjunct. §7:1466-1472
requires partial refunds to preserve the jurisdiction's allocation and rounding, and
§7:1508-1511 requires public golden vectors covering credits and refunds.

**G4:** four of §8's ten permitted ledger families (DESIGN.md:1565-1576) are refund,
reversal/void, dispute/chargeback and write-off, each naming a required source that is
finance policy. `LedgerTransaction` is unbuilt (DESIGN.md:430) and no ledger writer
exists.

Two further fields carry an open branch until this settles:
- `UnbuiltEvidence.ExposureReservation` (state.go:195) — DESIGN.md:537-541 and
  1822-1826 make exposure gross and monotonic, and re-crediting cap capacity requires a
  separately accepted `CapRecreditPolicy`. Whether a refund, chargeback or write-off
  restores headroom **is** this decision. Build the reservation subsystem first and you
  build it around a hole.
- `UnbuiltEvidence.CreditLotsReserved` (state.go:194) — refunding a settled
  `credit_purchase` or `auto_topup` requires a `GrantedValueClawbackReservation`
  freezing the unspent lots in the same transaction (DESIGN.md:1256-1260, 578-582).
  Unbuilt, zero code.

🔴 **The most load-bearing finding: none of this is inside the executor gate plan's
scope.** `AllClauses` is 29 initial-execution clauses for a positive charge;
`UnbuiltEvidence` has 14 fields and not one is refund-side. §4's refund branch is a
*different* transition — `AuthorizeNextProviderStep`, where "a refund `return` requires
current typed refund authority, source and refund capacity, and any granted-value
clawback — not debit authority, and not notice" (DESIGN.md:834-835), under the rule at
:845-847 that a control protecting you must not depend on the same gates as a control
charging you. `RefundIntent`, `RefundPlan` and `RefundCapacityReservation` are unbuilt
(DESIGN.md:422). EXECUTOR-GATE-PLAN.md contains "refund", "dispute", "chargeback" and
"write-off" **zero times**, and its H3 freezes a single 4-step plan of purpose
`payment` (line 283). **So the LARGE 26 / MEDIUM 4 / SMALL 2 tally does not include the
return rail: an 18th subsystem, a second predicate, and the `return` effect class
(DESIGN.md:1157, purpose matrix :1166) sit outside it.**

### Options
**A — Balance-only return.** Every adverse outcome becomes wallet credit; cash never
goes back to the rail through the engine. `RefundIntent`/`RefundPlan`/
`RefundCapacityReservation`, the `return` effect class and the purpose=refund matrix row
drop off the G1/G3 critical path, and the cap-reset attack of DESIGN.md:555-582 largely
closes because no cash leaves. But stored value becomes the company's liability vehicle,
dragging item 13's legal characterization onto the critical path, and consumer-law refund
rights may make "no cash back" unenforceable — a legal question. §6:1254-1256 still
requires the receipt to say value returned only to a balance.

**B — Full cash-return rail as designed.** Highest fidelity, and the only option that
satisfies §4:834-835's separation (revoking authority must not disable a refund). Cost:
a second predicate plus roughly four new tables and the granted-value clawback, none of
it inside the plan's 32 scoped items. Plausibly the single largest unscoped item
remaining.

**C — Publish a narrow terms revision now, defer the thresholds.** Decide only the
representation question §6:1205 asks — negative total → wallet credit — plus "cash
refunds are manual, operator-initiated and recorded out of rail", "no automatic
write-off; every write-off names a human actor and a reason", "no minimum-collection
suppression; small balances carry". A complete, defensible policy, not a placeholder
reconstructed from constants, so it survives §12:2011-2014. Clears item 9's share of
`terms_revision` without building the return rail. Cost: manual refunds still cannot be
written to the ledger, so reconciliation stays a spreadsheet and every manual refund is
an unreproducible entry in a system whose premise is reproduction.

**D — Defer entirely.** `terms_revision` stays a placeholder, `ClausePolicyPublished`
refuses forever, G2/G3/G4 stay shut by construction. Meanwhile legacy keeps collecting
under an implicit adverse-outcome policy nobody accepted: "D1e — no refunds" appears
only in code comments (internal/account/cycle/proration.go:66,
internal/account/usage/bill.go:95, internal/account/cycle/store.go:414). A customer has
never been shown that rule and never agreed to it.

### Recommendation
**OWNER-ONLY.**

### Cost to defer
Blocked: G2 outright (§6's four bill-reducing kinds cannot be added and the catalog
cannot be accepted as closed while §6:1205 defers to this item); G3 and G4 outright (the
refund/tax-allocation golden vectors cannot be authored; four of §8's ten ledger families
have no source rule); G1 indirectly via `terms_revision`. Two already-scoped subsystems —
`ExposureReservation` and `CreditLotsReserved` — can be started but each is built around
an undecided branch and will need rework.

More expensive with time:
1. **Cheap now for exactly the reason EXECUTOR-GATE-PLAN.md (D) gives.** `charge_intents = 0`.
   Adding a credit-line or return-effect representation to the canonical encoding today
   costs one migration and golden-digest churn. The return rail touches `Line`, the
   `Draft`, the digest and the catalog — squarely a Tier-0-shaped change, not currently
   in Tier 0 because the plan never scoped it.
2. **The plan's sequencing gets it wrong-ordered otherwise.** Building exposure and
   credit-lot reservations before this settles retrofits `CapRecreditPolicy` and the
   clawback into two subsystems rather than designing them in — and DESIGN.md:1826 is
   explicit that recredit "must never be inferred from a net ledger balance," which is
   exactly the shortcut a retrofit invites.
3. **Legacy accrues an undisclosed liability every day**, collecting under the unwritten
   no-refund rule. Every month adds customers whose refund entitlement was never
   disclosed and whose treatment, once a policy is published, either applies
   retroactively (a real cash cost) or is grandfathered (an item 3 decision).
4. **The wallet schema already carries an unbacked refund vocabulary.**
   `ms_billing.credit_ledger.type` permits `'refund'` and `'adjustment'` and `status`
   permits `'refunded'` (migrations/billing/048_credit_wallet.up.sql:37-44), and **nothing
   in the tree ever writes any of the three** — every INSERT hardcodes `'usage_draw'` or
   `'purchase'` (internal/account/db/queries/credit_wallet.sql:313-330, 520-535). A schema
   that says refunds exist, with no writer and no policy, is a live invitation to an
   operator writing one by hand.

Not blocked by this decision: deleting the fictional `'refund'`/`'refunded'` vocabulary
from migration 048, or adding a guard that no writer may produce it.

---

## 10 · Invoicing duties, Taiwan and NewebPay

### What is being decided
Whether Taiwan is a market this engine may collect in at all — and if it is, which legal
entity issues the government e-invoice and carries the business-tax liability there, and
whether NewebPay is contracted as the second rail. Numbering, retention, correction
duties and "which NewebPay products" are consequences of that one answer, not separate
questions.

### Gates it unblocks
**G1:** `ClauseTaxFinal` → `TaxIndependentlyReproducible` (state.go:153;
predicate.go:172-183; false at main.go:121). `ClausePolicyPublished` →
`PolicyDigestsMatch` (main.go:119) plus `UnpublishedRevisions` checking
`tax_rule_revision`. `ClauseMerchantOfRecord` → `Unbuilt.MerchantOfRecord` (state.go:192;
predicate.go:223-224); EXECUTOR-GATE-PLAN.md:336 maps "MerchantBindingSet + MoR ←
§12 items 4, 10, 11". `ClauseCommercialIdentity` → `Unbuilt.CommercialIdentity`
(state.go:191). `ClauseRailSupportsPlan` → `Unbuilt.RailSupportsPlan` (clause.go:71;
state.go:197; predicate.go:233-254): `ls internal/provider/` returns only
`stripeadapter`, and DESIGN.md:1647-1651 forbids claiming any NewebPay feature before the
merchant agreement and official integration spec exist. **The rail half of G1 is blocked
by a contract, not by code.**

**G3:** the tax policy revision itself. EXECUTOR-GATE-PLAN.md:400 maps it to "§12 items
4, 5, 6, 7, 8 (+10 for a Taiwan entity)"; DESIGN.md:1499-1503 makes Taiwan e-invoice
issuance a precondition of collecting there.

**Settled, do not re-decide:** DESIGN.md:1492-1496 fixes that the tax determination is
provider-neutral and frozen before either adapter runs, and that a NewebPay flow receives
the frozen presentation data. DESIGN.md:1502-1503 fixes that the resulting invoice
identity binds into the receipt like any other frozen input. **The shape of the answer is
decided; only the answer is not.**

**Live state, not hypothetical:** zero Taiwan / e-invoice / NewebPay code exists anywhere
(a grep over `*.go` and `*.sql` for `einvoice`, `發票`, `統一編號`, `newebpay` returns
nothing). Two of the three shipped proposers seal `not-applicable/pending-decision-12`
(domain_charges.go:60, shared by overage.go) with `Jurisdiction: "not-applicable"` and
`Verification: intent.TaxNotApplicable` (overage.go:843-851, domain_charges.go:422);
autotopup already says `unpublished/` (autotopup/executor.go:1394). DESIGN.md:1349-1351
defines `not_applicable` as "an immutable public rule and its inputs reproduce why no tax
applies" — so that class, sealed under an unpublished revision, claims a reproduction that
has not happened. If the answer is a Taiwan entity, "not-applicable" is not merely
unpublished, it is **wrong for that market, inside the canonical digest** a customer's
bundle attests to.

### Options
**A — Taiwan out of scope for the intent cutover.** Sell only from the existing non-TW
entity; no TWD, no NewebPay, no e-invoice. Item 10 collapses into item 4 and stops gating
the tax revision. `ClauseRailSupportsPlan` needs Stripe only. The NewebPay adapter, its
independent conformance suite (§11 step 7, DESIGN.md:1870-1873) and an invoice-issuance
subsystem all leave the plan. Prerequisite: someone must first **measure** whether legacy
collection already reaches Taiwan customers — if it does, this option describes the target
and not the present, and the exposure is a legacy-rail question a cutover decision does not
touch.

**B — Taiwan entity, e-invoice delegated to a licensed value-added centre (加值中心) /
turnkey issuer;** the engine seals the issuer-returned invoice identity into the receipt.
Numbering, retention and correction become contract terms of the issuer, and the engine's
job shrinks to binding one identity plus a correction linkage — exactly the shape
DESIGN.md:1502-1503 already requires. Still needs the entity, the business-tax
registration, a redistributable rule artifact (item 8), and a second published tax
revision.

**C — Taiwan entity, e-invoice issued in-house against the MOF platform.** The engine
acquires number-track allocation, issuance, void/allowance and retention duties — an 18th
subsystem, and the only one whose failure mode is regulatory rather than billing. Hard to
justify for a rail that has never sealed an intent.

**D — Contract NewebPay first, decide the invoice duty later.** DESIGN.md:1647-1651
forbids implementing any NewebPay feature before the merchant agreement and official spec,
so this only moves the calendar on the rail half; the invoice/tax half still holds G3 shut.
It produces a rail that can collect in a market where the engine cannot issue the required
invoice — the worse of the two orderings.

### Recommendation
**OWNER-ONLY.**

### Cost to defer
Blocked: the tax policy revision, therefore `ClauseTaxFinal` and `ClausePolicyPublished`,
therefore **every intent — including a USD, non-Taiwan one**, because the three shipped
proposers seal one shared set of placeholder revisions (domain_charges.go:57-62). Item 10
blocks the first honest settlement even for a customer with no Taiwan connection, unless
option A is taken and written down as an accepted ADR. Also blocked:
`ClauseMerchantOfRecord`, `ClauseCommercialIdentity`, the merchant binding set, and the
NewebPay adapter plus second-rail conformance suite — where **no engineering can start**.

More expensive: if Taiwan is in scope at all, an invoice-identity field is a
canonical-encoding change. Free-ish today (`charge_intents = 0`); a migration of live
sealed documents past the INV-003 trigger after the first sealed intent. The field belongs
in the Tier 0 canonical bump, which is free exactly once.

The `not-applicable/pending-decision-12` claim is sealed inside the digest today by two of
three proposers. Correcting it to `unpublished/` (domain_charges.go:60) needs no policy
answer, buys no gate, and only narrows what a future `true` could permit — the one piece
an engineer can and should do while the owner decides.

Not accruing: deferral creates no new exposure on the intent rail; nothing has ever
settled through it. The exposure question, if there is one, is on the legacy rail and is a
measurement nobody has taken.

---

## 11 · Currency

### What is being decided
Whether the engine's money is ever denominated in more than one currency — because today
every amount is a micro-**dollar** by column semantics rather than by a currency field,
and the wallet the settled funding model now draws from has no currency at all.

### Gates it unblocks
DESIGN.md:1945-1947 assigns G1, G2 and G4.

**G1:** `ClauseAuthorizationValid` → `BillingAuthorization.Permits`, which refuses
`RefusalWrongCurrency` when the intent's currency differs from the authorization's
(authorization.go:317, :415-417). A real check, vacuous today: both sides come from one
constant, `chargeCurrency = "usd"` (internal/account/cycle/types.go:195-198).
`ClausePolicyPublished` → the price book revision, which cannot be constructed without a
currency (`ErrPriceBookCurrencyMissing`, internal/intent/pricebook.go:59, :73-74).
`ClauseRailSupportsPlan` → `Unbuilt.RailSupportsPlan` (state.go:197;
predicate.go:233-254). The clause is literally named
`rail_supports_currency_and_frozen_plan` (clause.go:71) and **nothing behind that name
reads a currency.** DESIGN.md:1005-1007 requires each adapter to publish supported
currencies and the settlement-unit exponent; nothing publishes one. Both conversion sites
hardcode exponent 2: `centsFromMicros` (stripeadapter/adapter.go:205-217) and
`microsPerCent = 10_000` (internal/account/cycle/money.go:145-151). adapter.go:122
lowercases and passes through whatever currency string it was handed, so a non-2-decimal
currency is mis-scaled 100× on a path that already reaches Stripe. (Stripe also treats TWD
as a special case — amounts must be evenly divisible by 100 despite not being
zero-decimal; confirm against current Stripe docs before any TWD price book is published.)

**G2 — the "is FX a customer line" half.** DESIGN.md:1043-1046 states FX "is not in the
closed effect vocabulary" and that a currency change produces a new same-currency-priced
intent; DESIGN.md:1333-1335 puts FX on the internal-cost side unless §6 lists a customer
line. **The document has already answered this in the negative.** What is undecided is
only whether to reopen §6, which is a pricing decision.

**G4:** DESIGN.md:1556-1558 — "Each transaction balances in one named currency. Changing
currency requires a new same-currency-priced intent under a published price-book
revision." The chart of accounts (item 14) cannot be laid out without the currency set.

**The collision the funding decision just created:** the settled model makes a wallet draw
a negative line item on a Stripe invoice (FUNDING-SPLIT-DECISION.md:1-20), and
`credit_ledger` has **no currency column at all**
(migrations/billing/048_credit_wallet.up.sql:33-75), while
internal/account/creditledger/settlement.go:181-183 hard-refuses anything but `usd`. A
second price-book currency therefore needs either a per-currency wallet or an implicit
conversion at the credit line — and the implicit one is exactly what DESIGN.md:1043-1046
and :1556-1558 forbid. This coupling did not exist before the funding model was settled.

**Scale:** 32 distinct `*_micros` column names across 25 migration files, and 62 in-tree
annotations reading "micro-dollar (1e-6 USD)". Multi-currency is not a column addition; it
re-defines the unit of every money column.

### Options
**A — One currency, published as a policy:** exactly one currency per price-book revision,
one per ledger transaction, and the engine supports exactly one until an ADR adds a second.
Item 11 stops gating the price book revision immediately and stops being coupled to items 4
and 10. `RefusalWrongCurrency` stays honest and stays vacuous. A currency+exponent registry
still has to exist — with one entry — because `ClauseRailSupportsPlan` is named for it.
Trap: publishing "USD" *because the code says usd* is precisely the reconstruction
DESIGN.md:2012-2014 forbids; the ADR must state the commercial reason.

**B — Two currencies, TWD priced independently** — its own price-book revision with its own
numbers, not a conversion. Satisfies "no implicit FX" cleanly. Costs a per-currency wallet
(or a published rule that credit is not spendable across price books), a
currency+settlement-exponent registry with the Stripe special case, a second set of golden
tax vectors, and re-defining `*_micros` from micro-dollars to micros-of-the-intent's-currency
across 25 migrations. Strictly downstream of items 4 and 10.

**C — Two currencies, TWD converted from a USD price at charge time.** Implicit
cross-currency under another name; the rate becomes an unpublished input to a sealed digest.
Reject on the document (DESIGN.md:1041-1046 and §7's reproducibility requirement), not on
taste.

**D — Offer FX as a customer line.** Reopens §6's closed catalog and reverses
DESIGN.md:1333-1335. Worth doing only if the commercial answer is that the customer carries
the spread.

### Recommendation
**Adopt A for the first cutover, with two conditions; B only if item 10 puts Taiwan in
scope.**

1. The ADR must state the **commercial** reason for one currency. It must not cite
   `chargeCurrency = "usd"` as its authority — DESIGN.md:2012-2014 forbids reconstructing a
   revision from current constants, and DECISION-12-POLICY-REVISIONS.md:29-38 already
   withdrew that shortcut once.
2. Build the currency + settlement-exponent registry anyway, with one entry, and make
   `centsFromMicros` consult it (stripeadapter/adapter.go:205-217). This needs no policy
   answer, flips no gate, and only adds refusals — but it removes a latent 100× mis-scale and
   gives `ClauseRailSupportsPlan` the readable half its name promises. Add the currency
   column to `credit_ledger` in the same change **while the table is empty**.
3. C must be refused on §8 whatever the commercial answer. Say so in the ADR so it does not
   come back as an implementation shortcut.

**Not mine to decide, and I am not deciding it:** which currencies MirrorStack sells in, and
whether the customer or MirrorStack carries the FX spread (option D). Both are commercial. My
recommendation is about sequencing and representation.

### Cost to defer
Blocked: the price book revision, therefore `ClausePolicyPublished`, therefore every intent
— the same terminal consequence as item 10. `ClauseRailSupportsPlan` cannot get a truthful
half: the adapter capability record DESIGN.md:1005-1007 requires cannot be written without
knowing which currencies to declare. The chart of accounts (item 14), therefore G4.

**This is the item that actually compounds:**
1. The wallet. `credit_ledger` has no currency column and holds 0 rows. Adding the column is
   free today. After the wallet holds balances it is a backfill that must assert a currency
   for stored value whose legal characterization is itself undecided (item 13).
2. Every month of single-currency assumption adds more `*_micros` columns documented as
   micro-dollars — 32 distinct names across 25 files already.
3. The digest is fine either way: currency is already sealed and digested
   (chargeintent.go:420, :579), so unlike item 10 this is not a Tier 0 race.

**Already latent, and deferral does not fix it:** the exponent-2 assumption is unreachable
only because one constant is `usd`. Nothing in the type system stops it —
`executor.Debit.Currency` is a plain string handed straight through at adapter.go:122. The
first non-USD value that reaches that function is a 100× error on a live Stripe path, and it
will arrive as an implementation detail of whoever adds the second currency, not as a
decision anyone reviewed. Fixing it costs nothing and does not wait on this decision.

---

## 12 · Which kinds exist, and their timing

### What is being decided
Whether a MirrorStack bill carries four separately-priced service lines or fewer, and
whether a kind hits the customer the moment a grace timer expires or only at a period
boundary — which is the same thing as deciding whether `module_capacity` and
`custom_domain` survive as charge kinds at all, and whether the three mid-period sweeps
that mint money today survive with them.

### Gates it unblocks
**G2 — the catalog.** DESIGN.md:1135 and :1136 condition two of the five service rows on
product policy *in the row text itself* ("if product policy keeps it"). The Go enum does not
carry that condition: catalog.go:29 (`KindModuleCapacity`) and :32 (`KindCustomDomain`) are
unconditional members of the closed set at catalog.go:58-68, enforced by `KindInCatalog`
(:71) and `Seal`. **The shipped enum is up to two entries wider than the accepted
vocabulary.** DESIGN.md:1140-1143 is explicit that the numbers behind them are product
decisions and that "Today's compiled constants must not enter the target rater until
published as immutable, future-effective revisions." Those constants:
internal/account/usage/bill.go:40 (BaseFee $20), :46 (ProBaseFee $50, a placeholder), :52
(IncludedModules 5), :58 (ModuleBlockSize 5), :70 (ModuleBlockFee $5), :96
(ModuleOverageFee $1, derived), :101 (DomainFee $2), :118 (GraceDays 3).

**G1 — via exactly one `Environment` field and no `Unbuilt` field.**
`Environment.PolicyDigestsMatch` (executor.go:118; false at main.go:119; passed at
executor.go:235) feeds `ClausePolicyPublished` (clause.go:51; predicate.go:185-203), whose
`UnpublishedRevisions` checks `price_book_revision` (revision.go:63); `RevisionPublished`
(revision.go:38-47) refuses anything carrying the `pending-decision` marker (:29). Both
shipped proposers seal `proposedPriceBookRevision = "unpublished/pending-decision-12"`
(domain_charges.go:58, used at overage.go:833 and domain_charges.go:406) — **the placeholder
literally names this item.** Necessary, not sufficient: the same clause also waits on items 1,
2/3/9/13 and 4-8/10.

No mapping to `UnbuiltEvidence`. The nearest contact is indirect:
EXECUTOR-GATE-PLAN.md:242-244 records that `custom_domain` proration is one of the non-usage
legs whose source is a single durable row, making that slice of `Unbuilt.SourceAllocation`
(state.go:193) buildable today — deleting the kind deletes that slice, and changing the
proration coverage window changes the `SourceRef` identity the proposer seals
(internal/intent/proposer/proposer.go:119-128).

🔴 **Honest negative: the timing half is enforced by nothing in the predicate.** All 29
clauses can be satisfied by an intent that landed the wrong way. DESIGN.md:1135 says
`module_capacity` lands "the cycle; never an immediate sweep", and today's shipped behaviour
contradicts that row — `SweepModuleOverage` (internal/account/cycle/overage.go:617) charges
each timer mid-period on a per-timer Stripe invoice (overage.go:6-31) and is one of only
three intent proposers in the tree (:824, proposing `intent.KindModuleCapacity` at :826).
`SweepDomainCharges` (domain_charges.go:226) does the same for `KindCustomDomain` (:397), and
`SweepCreationProrations` (internal/account/cycle/proration.go:1165) for the creation base.
All three fire on every `cmd/billing-cycle` run (main.go:125-127, 479-481). **Whatever this
item decides about timing has to be enforced by which proposer exists, not by a gate.**

### Options
**A — Keep all four service kinds separately priced.** Publish a price-book revision covering
base, module block and domain as a fresh product decision (which may land on today's numbers
but must not be reconstructed from them). catalog.go needs no change, both shipped proposers
survive, and G2's catalog half can close. But it is the widest surface to keep publishing
forever — base × plan × currency, block price, block size, included pool, domain fee — and
item 11 then owes a TWD book for every one.

**B — Collapse to one published base price.** Delete `module_capacity` and `custom_domain`
from catalog.go, fold both into `platform_base`. **This is the identical shape item 15 has
already settled for infrastructure** (DESIGN.md:1977-1983). It deletes two of the three intent
proposers and two of the three mid-period sweeps, and shrinks the price book to one number per
plan per currency. Cost: a repricing event for every existing account, carrying the four
unresolved execution questions item 15 already enumerates (DESIGN.md:1984-1991). Forfeits any
ability to price a heavy-module or many-domain account differently.

**C — Keep the kinds, move all of them to the cycle boundary.** No mid-period collection:
sweeps accrue facts only, and one consolidated periodic charge carries base + capacity +
domains, which is what `RunBillingCycle` already does for arrears + advance base + advance
overage on one invoice (internal/account/cycle/charge.go:20-52). Matches DESIGN.md:1135 and
removes the per-timer / per-domain invoice fan-out (an account installing six modules and
three domains mid-period can mint ten separate charges today). Changes who pays what:
`GraceDays` stops being a delay-before-charging and becomes an eligibility rule on facts, and
short-lived resources may fall out of the bill entirely unless the accrual rule says otherwise.

**D — Split the item: settle the kind SET now, defer the price and tier numbers to the
price-book publication.** §6's vocabulary closes and catalog.go stops being wider than the
accepted list, so G2's catalog half moves. G1 does not move. Converts one blocked decision
into one closed decision plus one that is later and smaller, and is the only option producing
a gate movement without a repricing event.

Buildable under any of these, without the answer: the Tier 0 canonical-v2 work
(EXECUTOR-GATE-PLAN.md:88-118) and the empty policy-revision registry (:133-146). Both only add
refusals.

### Recommendation
**OWNER-ONLY.**

### Cost to defer
**Nothing stops.** Legacy still collects, and `BILLING_CYCLE_INTENT_CUTOVER`'s only armed value
is the literal `"propose-do-not-collect"` (cmd/billing-cycle/main.go:387-389). That makes this
item cheaper to defer than items 4-8, which block collection outright.

Blocked: G2 entirely — §6 is not a closed vocabulary until this settles, and two of its five
service rows are conditional on it. G1 stays blocked on `PolicyDigestsMatch` through the
unpublished price book, but item 12 alone would not open it. Also §11 step 4's shadow
reconciliation, for a different reason (SHADOW-PRICING-GAP.md — `metric_version_prices = 0`, so
`shadow.Source.PriceBookFor` at internal/intent/shadow/source.go:161-196 prices nothing).

More expensive, in order of severity:
1. **It converges with item 15's unexecuted migration.** Item 15's infra half is settled in
   direction and its migration has not shipped (DESIGN.md:1992-1994). If item 12 later chooses
   option B, that is the same repricing of the same base price with the same notice obligation.
   Deciding 12 *after* 15's migration ships means paying the customer-notice and grandfathering
   cost twice. **This is the one cost already accruing.**
2. **Free today, a sealed-document migration tomorrow.** `charge_intents = 0`, so deleting a
   kind or changing the coverage window costs a code edit and golden-digest churn. After the
   first sealed intent, `price_book_revision` and `kind` are both inside the INV-003 rejection
   tuple (054_intent_core.up.sql:100-101), so correcting one is a supersede
   (`supersedes_digest`, 054:67), not an update.
3. **Carrying cost on the sweeps.** Options B and C delete or gut both service-kind proposers.
   Any further idempotency, crash-recovery or straddle work on them is work on the half of the
   catalog the design marks conditional.
4. **Proration policy is a joint dependency.** DESIGN.md:1276-1279 requires the proration
   policy to fix start and end instants, anchored-period behaviour, denominator, grace and
   cancellation treatment, and the rounding point — under items 6 **and** 12. So deferring 12
   partially blocks 6, which is one of five items gating the tax revision and therefore G3.

---

## 13 · Credit, wallet and developer settlement

### What is being decided
Whether a MirrorStack credit balance is the customer's money held on their behalf
(refundable, non-expiring, a balance-sheet liability) or a non-refundable service entitlement
the platform may expire — and, as a separate half bundled into the same item, what share of a
module's metered income the platform keeps and when a developer's accrued cut becomes an
actual payable.

### Gates it unblocks
§12 states G2 + G4 (DESIGN.md:1952-1955). That is understated — it also holds G1 transitively.

**G2:** §6's deferred-prepaid rule is written as a condition on a document that does not
exist: "A lot may back a deferred prepaid reservation under one condition. Your accepted lot
terms must preserve its reserved slice past nominal expiry" (DESIGN.md:497-503, restated
:1216-1219). No accepted lot terms ⇒ no lot may back a deferred reservation ⇒ the §6 funding
table entry for `credit_purchase` (DESIGN.md:1229, which requires the disclosure to name
"restrictions, expiry, refund terms") cannot be filled in.

**G1, transitively:** item 13 is one of the four inputs to `proposedTermsRevision`
(domain_charges.go:57), refused at predicate.go:203 with the list at revision.go:56-80.

**G4:** `credit_ledger` is the customer-liability journal
(migrations/billing/048_credit_wallet.up.sql:33-92) and `developer_settlements` the
developer-liability journal (migrations/billing/013_developer_settlements.up.sql:54-98). Item
14's chart of accounts and recognition timing cannot be written until stored value is
characterized and until `platform_take` is known to be gross or net revenue.

Concrete fields, all false at cmd/intent-executor/main.go:119-122:
- `Unbuilt.CreditLotsReserved` (state.go:194; clause `credit_lots_compatible_available_and_reserved`,
  clause.go:68; read as a bare bool at predicate.go:229). "Compatible" and "available" **are**
  the lot-terms decision.
- `Unbuilt.SourceAllocation` (state.go:193; clause.go:67; predicate.go:227).
- `Unbuilt.FundingMatchesAccepted` (state.go:196; clause.go:70; predicate.go:233).
  EXECUTOR-GATE-PLAN.md:340-345 splits it: the split and caps conjuncts are buildable now, the
  credit-policy conjunct waits on this item.
- `Environment.PolicyDigestsMatch` via `ClausePolicyPublished`.

One structural consequence: predicate.go:43-51 skips the six `providerClauses` only when
`Funding.ProviderRemainderMicros == 0`, i.e. a wallet-only intent. That escape needs a wallet
allocator, which needs accepted lot terms. **So item 13 also decides whether the cheapest path
through the predicate (EXECUTOR-GATE-PLAN.md:378-384) ever has a production instance.**

**Already settled, do not re-open:** (a) §6's catalog names `credit_purchase` and `auto_topup`
as their own intent kinds with their own authority (DESIGN.md:1223-1233), enforced at
catalog.go:47, :50, :58-68 — DECISION-12-POLICY-REVISIONS.md's older claim that "§6 names no
funding kind" is stale as of the closed catalog; (b) `walletFunding = 0` for both stored-value
kinds is settled and enforced at seal (chargeintent.go:480-483, `walletFundingForbidden` at
:505-511) — a wallet cannot refill itself; (c) the owner settled the draw mechanics 2026-08-31
(FUNDING-SPLIT-DECISION.md:3-30); (d) the split is sealed into the canonical document (v3;
chargeintent.go:198, :427, provider remainder derived at :472). **None of those decide the
terms of the lot being drawn from, which is what item 13 is.**

### Options
**A — Non-refundable, expiring service entitlement.** Cheapest liability treatment: deferred
revenue with breakage, no refund machinery. But it collides with DESIGN.md:497-503 — a lot
whose terms do not preserve the reserved slice past expiry "may fund only a same-transaction
wallet settlement completed while it is eligible", and "Admission without the preservation
proof is refused, with no service and no debt." So prepaid customers could never buy anything
whose service window outlives the lot. It is also a schema and terms change, not a
codification of today: 048:73-75 CHECKs that only `grant` rows may carry `expires_at`, so
purchased and top-up credit cannot expire today. Largest item-4/item-10 legal surface.

**B — Refundable customer prepayment** (customer money held; refundable to the original rail;
purchases never expire). Closest to the live schema (048:73-75) and to the settled credit-note
model. Cost: a refund path that exists nowhere — the Stripe adapter has no Refund method at
all — and `GrantedValueClawbackReservation`, which DESIGN.md:579 and :1258-1261 require before
cash may go back while granted value is still spendable, has **zero occurrences in the tree**.
Creates a real reported liability item 14 must carry, and raises the e-money/stored-value
question for a TW entity.

**C — Two lot classes with different terms:** expiring, non-refundable promotional `grant`
lots and non-expiring, refundable purchased lots, with a published spend order (grants first).
**This is what the code already does** — `WalletSpendableLots` orders expiring grants (0) →
non-expiring grants/preallocation/refund/adjustment (1) → purchases and top-ups (2), then
expiry, created_at, id (internal/account/db/queries/credit_wallet.sql:294-303), and 048:73-75
already restricts expiry to grants. Cheapest to implement, and it matches
DESIGN.md:490-492. Two honest costs: (i) that SQL comment calls the ordering "the
owner-decided consumption order" (credit_wallet.sql:265) and **no accepted revision recording
that decision exists**, so publishing it is either writing down a real prior decision or
exactly the reconstruction DESIGN.md:2012-2014 forbids — somebody must say which; (ii) the
terms still have to be disclosed at purchase (DESIGN.md:1229), which no surface does.

**D — The developer half, orthogonal and separately answerable:** keep accrual-only
(visibility-derived 15% published / 30% private, status `accrued`, `developer_id` NULL —
013_developer_settlements.up.sql:14-46, internal/account/cycle/types.go:71-74) versus publish a
take rate with a reserve and a payout schedule. Today this is entirely theoretical:
`SettleDevelopers` (internal/account/cycle/service.go:585) has no caller outside its own
package and no RPC route, so `developer_settlements` is a table nothing writes and the accrued
developer liability is exactly zero. "Platform keeps everything" is still the actual state,
which migration 013:6-9 says the table exists to end. Explicitly deferring ("no third-party
developer program before date X") is a legitimate and free answer; publishing a rate creates a
payable, and then reserve, refund treatment and payout timing all become mandatory before the
first module developer signs.

### Recommendation
**OWNER-ONLY.** The legal characterization of stored value, refundability, expiry, the credit
exposure ceiling, and the developer take rate / reserve / payout timing are legal, tax,
consumer-protection and finance judgements. Choosing between A, B and C is choosing whether
MirrorStack holds customer funds; choosing D sets a third party's compensation.

The safe holding pattern, costing no policy answer: (1) **do not build the wallet allocator** —
every intent keeps sealing wallet=0 / provider=total, which is honest and keeps the six-clause
provider escape unreachable; (2) keep `CreditLotsReserved` / `SourceAllocation` /
`FundingMatchesAccepted` false; (3) decide now, while it is free, whether the accepted lot
terms need a **per-lot terms digest sealed into the intent**, because that is a canonical-schema
bump and internal/intent/canonical.go:22-25 says the window closes on the first sealed
production intent; (4) either wire a caller for `SettleDevelopers` or delete it — a
revenue-share ledger nobody writes is a declared-but-unimplemented contract of exactly the shape
`SECURITY.md` exists to expose.

### Cost to defer
Blocked: G2 outright; G1 transitively via `terms_revision`; G4 via item 14. Three of the 29
clauses and the wallet-only predicate path stay unreachable. Note item 13 is **not** on the
minimum set for one honest settlement unless the first intent is stored-value
(EXECUTOR-GATE-PLAN.md:413-415) — deferring it does not by itself hold up a card-funded first
cutover, provided the terms revision it feeds is published without needing its stored-value
clauses.

More expensive, four measurable ways:
1. **Hard deadline, currently free.** If the accepted terms require a lot-class or
   credit-policy digest inside the sealed document, that is a canonical bump.
   `charge_intents = 0` and `billing_authorizations = 0` (SHADOW-PRICING-GAP.md:17), and
   canonical.go:22-25 states the window in as many words. The funding split already spent one
   such window (v3); this would be the next.
2. **`credit_ledger` = 0 rows today.** Every lot sold before the terms are published is sold
   under no published terms, into an append-only journal. Re-characterizing already-sold stored
   value is a customer-terms change and a possible refund event, not a migration.
3. **The exposure half is already live and unowned.** `accounts.credit_limit_micros` defaults
   to $25 (migrations/billing/016_account_collection.up.sql:66-67) and
   internal/account/collection/collection.go:306-341 carries a trust ramp to a $5,000
   auto-earned ceiling whose own comment says "CONSERVATIVE placeholders — the exact ramp is
   FINANCE-OWNED" with a TODO(finance). It has no caller today (:359-361), so deciding it is
   still free; the day anything wires it, unsecured customer exposure begins accruing under
   numbers no accountable owner signed.
4. **The developer half** costs nothing while nothing writes `developer_settlements` — but if a
   developer agreement is signed before the take rate is published, the accrual for
   already-rolled-up periods must be reconstructed from `usage_aggregates` under a rate chosen
   after the fact. There is also a standing correctness debt independent of the decision:
   `infra_micros` is hardcoded 0 (service.go:617) and `developer_id` is always NULL (013:67-70),
   so even the accrual formula in the migration header cannot currently be evaluated as written.

---

## 14 · Ledger and evidence policy

### What is being decided
Which monetary accounts exist and which one each posting touches, at what instant collected
money stops being a liability and becomes revenue, and what evidence may leave the engine — to
whom, and for how long before it must be destroyed.

### Gates it unblocks
Named G1 + G3 + G4 (DESIGN.md:1959-1960). G4 is the substance; G1 and G3 are consequences.

**G4 — the cutover object cannot even be designed.**
migrations/billing/054_intent_core.up.sql creates six tables and **none of them is a ledger**.
The only journal in the tree is `ms_billing.credit_ledger`
(migrations/billing/048_credit_wallet.up.sql:33): single-entry, account-scoped, eight `type`
values, and **no currency column at all** — while DESIGN.md:1556 requires "Each transaction
balances in one named currency" and :1539-1540 requires signed entries balancing to zero. The
eight types in 048 and the ten families-with-required-source at DESIGN.md:1560-1573 are two
unrelated vocabularies. **The chart of accounts is the map between them, so nobody can write
the migration.**

**G4, second half — recognition timing decides whether 048 already breaks INV-011.**
`credit_ledger.balance_after_micros` (048:39) is a derived balance stored on the posted row, and
DESIGN.md:1544-1545 says derived balance rows "are never the audit source." 048's header and
migrations/billing/057_credit_ledger_proposed_status.up.sql both depend on an in-place `status`
transition on an existing row, which DESIGN.md:1542 forbids for money. Whether that is a
violation or merely pre-posting bookkeeping **is** the recognition question.

**G1/G3 through the predicate, by field:**
- `Unbuilt.SourceAllocation` (state.go:193; predicate.go:225-226; clause.go:67) and
  `Unbuilt.CreditLotsReserved` (state.go:194; predicate.go:227-228).
  FUNDING-SPLIT-DECISION.md settles the *shape* — a wallet draw is a negative invoice line, not
  a parallel rail — and explicitly leaves this half open: "a credit applied to an invoice must
  be reserved against a real lot… those are about the LEDGER, not about a rail." Item 13 owns
  the lot's terms; item 14 owns which account the reservation and the application post to.
- `Unbuilt.ProofsApplied` (state.go:190; predicate.go:219-220), `ClauseNoPriorSettlement` and
  `ClauseClaimAvailable` (predicate.go:208-214) all sit inside the claim transaction
  DESIGN.md:1550-1551 requires to commit "reservations, claim state, balanced entries, receipt
  and evidence outbox together." "Balanced entries" is the chart of accounts. **Measured today:
  internal/intent/store/store.go:243-247 is a single `pool.Exec` with `ON CONFLICT DO NOTHING`
  — no transaction at all, so there is nothing for balanced entries to join.**
- No charge bundle can be produced either way: VERIFICATION.md:131 row 14 marks the ledger
  element ("balanced ledger transaction ids and entries, plus the outbox checkpoint") required
  **always**.

**Retention / export / access — partly shipped without the policy:** ops read access shipped as
a credential (migrations/billing/058_billing_ro_grants.up.sql) — that is who may read
internally, not what a customer's export may contain. The export *constraints* are written
(VERIFICATION.md:107-111; DESIGN.md:1631-1637) but the per-provider exportable list, the
retention period, and what deletion means are not.
migrations/billing/052_org_deletion_finalizations.up.sql:1-7 **already retains all financial
history indefinitely on org deletion, by fiat** — a retention decision effectively taken without
the ADR.

### Options
**A — Split the item, accounting half first.** One ADR for chart of accounts + recognition
timing; a second, later, for exportable provider evidence + retention/deletion/access. Unblocks
the G4 schema and the balanced-entry half of the claim-transaction rewrite (EXECUTOR-GATE-PLAN
leg D) — among the largest items in the plan — while the deferred half only gates the evidence
edge (DESIGN.md:1674+, INV-014), already blocked behind item 16 whatever this decides. **Risk:
the chart of accounts cannot be finalised without knowing whether a credit lot is a customer
liability, which is item 13's characterization. A must be sequenced after 13, not before.**

**B — One ledger-and-evidence ADR, all three parts.** Internally consistent, one acceptance
ceremony, and recognition timing and retention are both the accountant's anyway. Cost: couples
the fast half to the slow half — nothing in G4 moves until the privacy/retention half is
answered.

**C — Derive the chart of accounts from the entity's existing books**, and generate the ten §8
families from those accounts rather than inventing a billing-specific set. Cheapest to accept,
audit-consistent by construction. Risk: books never designed for stored value, deferred revenue
or multi-currency will not have the accounts, and adopting them anyway is exactly the
reconstruction DESIGN.md:2012-2014 forbids.

**D — Status quo.** `credit_ledger` stays the only journal, no `LedgerTransaction` can be
created, G4 stays shut, and the accrual below starts the day the first credit lot is sold.

### Recommendation
**OWNER-ONLY.**

### Cost to defer
Blocked: all of G4; the balanced-entries half of the claim-transaction rewrite
(store.go:243-247); `SourceAllocation` and `CreditLotsReserved` for anything wallet-funded;
**every** charge bundle, because VERIFICATION.md:131 marks the ledger element "always"; and the
customer trace/export surface.

**Right now, no — and that is the finding.** `charge_intents = 0`,
`billing_authorizations = 0`, `credit_ledger = 0` (verified 2026-08-31). There is no monetary
history to restate, so the chart of accounts can still be chosen freely. **The curve turns the
day the first credit lot is sold or the first wallet draw posts.** From that instant every
collection lands in a single-entry, currency-less journal whose rows carry a stored derived
balance and an in-place status transition, and the eventual double-entry cutover must either
restate those rows into accounts nobody had chosen yet, or declare a date before which the
ledger is not restatable. **Selling stored value before this ADR is what makes it expensive;
the calendar alone does not.**

Engineering-authorable meanwhile, needing none of this decision: build the ledger's **shape** —
append-only, signed entries summing to zero, an explicit currency column, correction-by-new-
transaction — with the family/account enum left **absent** rather than defaulted. Same
"absent, not defaulted" discipline EXECUTOR-GATE-PLAN leg E applies to `permit_identity` and
`enclave_scope`. A defaulted account value would be precisely the forbidden reconstruction.

---

## 15 · Responsibility transfer

### The infrastructure half is SETTLED
DESIGN.md:1965-1967: the infrastructure line and its 12/10 markup are **folded into a published
base price, not disclosed and not left as a separate customer line.** That is the direction
INV-010 already describes; the target does not change, only the migration does. **Do not
re-decide the direction.** What remains, and only this (DESIGN.md:1971-1979):

- the new published base price, and whether one price replaces the flat per-app base or sits
  beside it;
- whether existing accounts are re-priced at their next boundary or grandfathered, and for how
  long;
- the notice each affected customer must receive before the first boundary that charges the new
  price;
- whether the infra metrics keep being measured for internal margin analysis after they stop
  being rated — §6 already says they may, as internal cost only, so **this fourth bullet needs
  no ADR.**

⚠️ **Doc defect to fix while deciding:** DESIGN.md:1331-1332 still says "Whether to disclose the
markup, fold it into a published base price, or delete the line is an open product decision
(§12 item 15)", contradicting DESIGN.md:1321-1322 ten lines above and :1965 below. A reader
landing in §6 will conclude the direction is still open when it is not. That is an edit, not a
decision.

### What is being decided (the open half)
When the party who pays for an organization changes, at what instant the new payer starts owing
and what becomes of service already consumed but not yet billed.

### Gates it unblocks
Named G1 only (DESIGN.md:1963).

🔴 **The most important mapping fact is a negative one: there is no `Environment` or
`UnbuiltEvidence` field for this at all.** DESIGN.md:802 requires "for `subscription_start`, the
accepted responsibility/schedule generation is locked with claim acquisition and equals the
current account generation", and :841 extends it to every adverse or customer-collectible
transaction. clause.go:95-125 enumerates all 29 clauses and none names responsibility or
generation; `grep -rn generation internal/intent/` returns nothing outside tests. **So this is a
§4 conjunct with no gate** — unlike items 4 and 13, which at least own a field to be set. Item 15
needs an 18th clause plus a new evidence field, and that work is **not** in the LARGE 26 /
MEDIUM 4 / SMALL 2 tally.

Where it touches existing fields:
- `Unbuilt.SourceAllocation` (state.go:193; predicate.go:225-226; clause.go:67). DESIGN.md:416
  makes the transfer a source-partition rule — "accrued obligations stay with the old payer and
  facts cannot backdate across it" — and VERIFICATION.md:134 row 17 requires "source and exposure
  partition, retained old claims." The cutoff instant is what the partition is keyed on, so for
  any payer who has ever changed, `SourceAllocation` has no rule to evaluate.
- `Unbuilt.CommercialIdentity` (state.go:191; predicate.go:221-222). DESIGN.md:1386-1388: a
  transfer "never moves a tax profile or commercial identity — the new payer enrolls its own
  receipt first." Nothing today distinguishes a fresh payer from a transferred one.
- The sealed payer is immutable per intent (chargeintent.go:144, :625; payer columns at
  054_intent_core.up.sql:29-30, frozen by the trigger at :100-106). Honest — but it means an
  intent sealed before a cutoff and executed after it charges the old payer with no clause asking
  whether the cutoff permitted it.

🔴 **And the legacy rail already transfers payers in production, with no cutoff object:**
- internal/account/cycle/org.go:354 `RevokeSponsorship` — the sponsor withdraws, the org drops to
  unbilled, frozen attribution never rewrites, roster rows keep their `account_id`. **This half
  already matches DESIGN.md:416.**
- internal/account/cycle/org.go:449 `attachOrgBilling` → `RepointOrgNullAccountEvents`
  (internal/account/db/queries/org.sql:171-192) does the opposite. It UPDATEs previously
  unattributed usage onto the newly designated account and moves the billing clock forward:
  `billable_at = GREATEST(COALESCE(occurred_at, recorded_at), window_start)` (:174-177),
  `recorded_at = GREATEST(recorded_at, window_start)` (:186),
  `occurrence_policy = 'first_funded'` (:178-183). The query comment names it "decision 1":
  "backfilled events bill in the first period that closes after designation." **A new payer is
  billed for service consumed before they were the payer.** DESIGN.md:416 as written carries no
  exception for it. Item 15's ADR either writes that exception down or removes the behaviour.
- migrations/billing/052_org_deletion_finalizations.up.sql:22-38 already builds a one-case
  transfer-cutoff object (`org_deletion_retired_sponsorships`, kept as immutable history so a
  worker cannot silently fall back to the customer account). **The general policy is being paid
  for ad hoc, one case at a time.**

### Options
**1 — Hard cutoff both ways** (the literal DESIGN.md:416 rule). Every fact accrues to whoever was
the responsible payer at its occurrence instant; a new payer owes nothing before their cutoff;
usage consumed while an org had no payer is billed to nobody. Consequence: org.sql:171-192 must
stop repointing pre-cutoff events — a behaviour change on a live rail — and MirrorStack absorbs
pre-designation usage. Cleanest predicate: one cutoff, no exception.

**2 — Hard cutoff payer→payer, absorption unfunded→payer** — writes down what already ships. A
transfer between two real payers never moves accrued obligations; an org that had no payer, whose
usage was never anyone's obligation, has it absorbed by the first payer to designate, clamped to
the current window. No live behaviour change, and "decision 1" is promoted from a query comment
to accepted policy. New work: the absorption must be **disclosed**, since §6's per-line source
rule (DESIGN.md:1263-1270) requires the line to state when the obligation accrued, and
`first_funded` is currently invisible to the customer.

**3 — No absorption**: refuse billable service to an org with no payer. Strongest and simplest
ledger; but it changes the product and the metering ingest path would need a payer check it does
not have, against a hard INV-013 constraint that ingest stays lock-free at ≥50 facts/sec/payer
(DESIGN.md:603). A product decision, not a cleanup.

**4 — Explicit transfer with voluntary assumption**: a new payer may assume named accrued
obligations through a separate authorized intent, old payer credited, new payer charged. The only
option that lets an acquirer take over a bill **without** liability reassignment, which
DESIGN.md:1962-1963 forbids. Most work: a typed `BillingResponsibilityTransfer` object, the
two-view receipt of VERIFICATION.md:134 + :149, and the ledger families from item 14 — so it
cannot ship before that ADR.

**Infra-migration sub-choice** (direction settled, execution not): (a) one new base price
**replacing** the flat per-app base, everyone re-priced at their next boundary — the only shape
that lets the `infra_*` wire fields be deleted outright; (b) new base **beside** the old, existing
accounts grandfathered indefinitely — internal/account/usage/bill.go keeps serving
`infra_total_micros` with its broken arithmetic forever for that cohort; (c) grandfather with a
published expiry, then converge.

### Recommendation
**OWNER-ONLY.**

Engineering-authorable meanwhile: add the missing clause as a **fail-closed** one — refuse any
intent whose payer's responsibility generation is unknown. It only narrows what a future `true`
can permit, and it closes a §4 conjunct the predicate silently drops today, the same repair
`ClausePolicyPublished` received at predicate.go:203. Separately, fix the DESIGN.md:1331-1332
contradiction before anyone reads §6 and concludes the infra direction is still open.

### Cost to defer
Blocked: G1 — but "blocked" understates it. The responsibility-generation conjunct has **no
clause**, so the predicate does not refuse on it; it simply never asks. Also
`Unbuilt.SourceAllocation` and `Unbuilt.CommercialIdentity` for any payer who has ever changed,
and VERIFICATION.md:134 row 17 — no bundle can be produced for a charge where the payer changed.

Two independent clocks:
1. **Every org that designates a payer today runs `RepointOrgNullAccountEvents` and invoices a
   payer for consumption that predates them.** The row rewrite is largely reversible
   (`occurred_at` untouched, original `recorded_at` survives in `repointed_from`, org.sql:184-185)
   — so this is not data loss. The cost is commercial: those periods have already closed and been
   collected. If the ADR lands on option 1, the remedy for every org designated in the interim is
   a credit or refund, not a recomputation, and that population grows with every designation.
2. **Until the base-price migration ships, every bill keeps carrying the broken arithmetic**
   DESIGN.md:1318-1320 names: internal/account/usage/types.go:316 and :325-327 say it in the
   code's own words — `UnitPriceMicros` is "raw COGS (pre-markup)" while `ChargedMicros` is
   "qty × price × 12/10" — so quantity × displayed price ≠ charged, on the customer's face.
   Markup constants at internal/account/cycle/types.go:61-62. Every cycle that closes is another
   cohort that can check the arithmetic and find it wrong, and another cohort that must be
   re-priced or credited rather than simply moved.

---

## 16 · Consent authority, and reads you can verify yourself

### Declining the cost is the CURRENT POSITION. State what it costs.
DESIGN.md:2005-2010, verbatim in substance: acceptance rests on a receipt `api-platform` relays,
so **INV-006 in its stronger form is a trust assumption with after-the-fact reproduction, not
independent verification. INV-014 has the same dependency, because `CustomerReadProof` (unbuilt)
binds an enrolled factor that does not exist. One answer governs both.**

This item is therefore not "open" in the sense of unanswered — the position exists. What is
missing is that the position has never been **accepted as an ADR**, so the gate it names stays
shut anyway. §12's closing sentence already writes the consequence down: the engine can be made
to derive every number, disclose it before collecting, and reproduce it afterwards; **the party
that tells it you agreed is still MirrorStack's own private half.**

### What is being decided
Whether MirrorStack funds a separate identity product so the billing engine can independently
verify that you agreed to a charge and that you may read its evidence — or accepts, in writing,
that `api-platform` is the only party that can say so and that a fabricated consent is
detectable only after the fact.

### Gates it unblocks
G1 and G4 (DESIGN.md:2010).

**Structurally unlike every other §12 item:** item 16 maps to **no** field in
`predicate.UnbuiltEvidence` (state.go:188-203 — 14 bools, none about consent) and none in
`executor.Environment`. It flips nothing, because **the clause it governs is already built**:
`ClauseAuthorityEvidence` (clause.go:45; dispatched at predicate.go:99-100; implemented at
predicate.go:290-337). What item 16 decides is not whether to write that clause but whether the
clause as written **counts** as satisfying INV-006 (DESIGN.md:271-289) — whether a relayed
receipt is a control or a recorded trust assumption.

**G1, mechanically:** `ClauseAuthorityEvidence` sits in `AllClauses` (clause.go:106) and is
absent from `providerClauses` (clause.go:83-90), so it has **no exemption** — it runs for
wallet-only intents too. No intent of any shape executes while it is unanswered, however many
other §12 items land.

**G4, mechanically:** `LedgerTransaction`/`ChargeReceipt` carry INV-014 (DESIGN.md:430) and
`BillingDecisionProof` reports `state_assurance: attested` (DESIGN.md:419). The ledger's
customer-facing evidence inherits the same relay dependency.

**Four things measured:**
1. **The customer-present gate is unreachable, not merely unexercised.** executor.go:225-241
   builds `predicate.SealedState` and omits the `Acceptance` field entirely; `grep -rn
   'Acceptance:' --include=*.go` outside tests returns nothing. So `s.Acceptance` is always the
   zero `AcceptanceReceipt{}` and `authorityEvidenceBinds` returns false at its first check
   (predicate.go:296-298) for every one-time intent.
2. **The standing gate's whole consent proof is a non-empty string.** predicate.go:332-333 passes
   on `s.Authorization.AcceptanceDigest() != ""`; `Authorize` validates only non-emptiness
   (authorization.go:270-272). Nothing compares it to a disclosure the customer was shown.
3. **Nothing mints an authorization at all.** `store.SaveAuthorization`
   (internal/intent/store/authorization.go:24) has zero non-test callers — consistent with
   `billing_authorizations = 0`.
4. **The INV-014 half does not exist in any form.** A grep for
   `ReadEvidence|TracePayment|outbox|CustomerReadProof|CustomerProofStream` over `*.go` and
   `*.sql` returns zero hits: no evidence outbox, no evidence edge, no trace API. And
   `capabilities.Report` (internal/account/capabilities/capabilities.go:38-58) carries four
   fields — none of the trusted-computing-base roots DESIGN.md:2001-2003 requires it to publish.

### Options
**A — Write down the decline.** Accept the current position as an ADR that names INV-006 and
INV-014 as trust assumptions with after-the-fact reproduction, and add a machine-readable field
to `capabilities.Report` — e.g. `consent_authority: "relayed"` — so the assumption is pinnable by
a verifier and not only prose in a design doc. Item 16 stops blocking G1 and G4 at roughly SMALL
cost. The strongest sentence the product may ever say about consent becomes "we can show you what
we were told and you can point at a fabrication afterwards" — permanently.

**B — Fund the identity product** as specified at DESIGN.md:1987-2003: `AccountAuthorityCredential`
under a pinned public identity root, enrollment by proof of possession only, a customer-held
verifier at an independently distributed top-level origin with a reproducible signed release and
`frame-ancestors 'none'`, a non-programmatic approval gesture, an offline recovery authority with a
published cooling interval, and the roots published in `Capabilities`. INV-006 and INV-014 become
real controls and the document's closing sentence changes. Cost: a **second product** with its own
operational root and its own release and recovery ceremonies — not a line item on the LARGE-26
plan but a peer of it. The doc already dropped it as a mechanism and kept it only as a costed
option.

**C — Build the independence that needs no enrolled factor.** Three pieces, none requiring an
identity root: (1) the billing-owned transactional evidence outbox — DESIGN.md:395-397 explicitly
says it "is worth building first anyway: it makes your evidence a durable side effect of the money
moving"; (2) bind `AcceptanceDigest` to an engine-issued, engine-signed disclosure digest instead
of accepting any non-empty string (authorization.go:270-272), and populate
`SealedState.Acceptance` in the executor from a stored engine-issued challenge so the
customer-present branch (predicate.go:292-317) is reachable and actually checks something;
(3) publish the trust boundary in `Capabilities` as in A. Does **not** make the engine able to
distrust `api-platform` — a relayed receipt is still relayed — but it converts "reproducible in
principle" into "reproducible in practice", and every piece is a strict prerequisite for B.
MEDIUM, no second product.

**D — Split the answer**: accept the relay for consent, fund only the read half. **Rejected on
the doc's own ground** — DESIGN.md:2008-2010: `CustomerReadProof` "binds an enrolled factor that
does not exist" and "One answer governs both." The enrolment ceremony *is* the expensive half;
funding only the read path still buys the credential, the root and the recovery authority, and
saves only the approval-gesture UI.

### Recommendation
**A + C together, and explicitly not D.**

Accept the decline as an ADR (A) — that is what actually unblocks G1 and G4 on this item; per
EXECUTOR-GATE-PLAN.md:415-416 and 567-570, silence is not a third option, because item 16 blocks
G1 whichever way it is answered. Then take C's three builds so the recorded assumption is honest
rather than nominal: an assumption you cannot reproduce in practice is not a weaker control, it is
no control. Today the customer-present branch cannot pass at all and the standing branch passes on
a non-empty string — **that is not "detection instead of prevention", it is neither.**

Two halves are not mine and I am not deciding them:
- **Funding option B is a resourcing and commercial call.** The engineering trigger to name for
  the owner is concrete: if a market, an enterprise contract or a payments regulator ever requires
  consent proof that MirrorStack itself cannot forge, B becomes mandatory and A stops being
  available. Nothing in the code can tell you whether that trigger is near.
- **Whether "our own service can assert your acceptance and you can only detect it afterwards" is
  disclosable to customers in a given market** — Taiwan and any EU-facing sale in particular — is a
  legal review. The ADR should be drafted and then reviewed, not published on my judgement.

On the threat-model question proper, which **is** an engineering call: A is defensible today
because `api-platform` already holds the session and the engine already treats the subject id as
opaque (internal/account/billing/service.go:105-111, cited at DESIGN.md:43-48 and 283-285). An
identity product bolted onto an unchanged session boundary would move the trust, not remove it. B
is only worth its price alongside the full enrolment ceremony — **a partial B is worse than A,
because it looks like independence and is not.**

### Cost to defer
Blocked: G1 and G4, and uniquely, **they stay blocked no matter what else lands.**
`ClauseAuthorityEvidence` runs in every evaluation with no exemption, so settling all eleven other
critical-path items and building all 17 subsystems still executes zero intents while item 16 is
silent. It is also one of the only §12 items whose clause is already written, so **no engineering
can be substituted for the decision.**

Three ways it gets more expensive:
1. **It is the only §12 item that re-costs backward.** Items 12 and 15 re-price forward at a
   boundary; item 1 re-notices forward. Item 16, answered as B later, requires **enrolling the
   installed base retroactively** and re-obtaining consent for authorizations already relied on.
   That cost scales with customers acquired between now and the answer, and with money already
   collected under the relayed receipt.
2. **The cheapest moment to tighten `AcceptanceDigest` is now, and it is measurably now.**
   `SaveAuthorization` has zero non-test callers and `billing_authorizations = 0`, so tightening
   authorization.go:270-272 from "non-empty" to "equals an engine-issued disclosure digest" costs
   one validation change, zero migration, and invalidates zero rows. The first production
   authorization minted under the loose rule converts that into a backfill of grants whose consent
   proof cannot be reproduced — exactly the grants that would be litigated.
3. **The outbox is cheapest before the ledger cuts over.** DESIGN.md:395-397 says to build it first
   regardless of the answer. After G4, adding a transactional evidence outbox means backfilling
   customer evidence for money that already moved through a ledger that did not emit it —
   reconstruction, which DESIGN.md:2012-2014 forbids elsewhere.

**What does not get more expensive: option B's own build cost is flat.** Deferring the *decision*
compounds; deferring the *product* does not. That asymmetry is the argument for writing the ADR
now even if the answer is "no."

---

## Answer order — highest leverage first

### By gate count (§12's own assignment)

| Gates | Items |
|---|---|
| **3** | 4 (G1/G3/G4) · 9 (G2/G3/G4) · 11 (G1/G2/G4) · 14 (G1/G3/G4) |
| **2** | 1 (G1/G2) · 3 (G1/G3) · 6 (G2/G3) · 10 (G1/G3) · 12 (G1/G2) · 13 (G2/G4) · 16 (G1/G4) |
| **1** | 2 (G1) · 5 (G3) · 7 (G3) · 8 (G3) · 15 (G1) |

Gate count alone is the wrong order, for three reasons the code makes plain, so the working order
below corrects for them: several items **share one artifact** (settling three of four buys
nothing), several are **strictly serial** (parallel work cannot recover their latency), and two
single-gate items (8, 16) actually refuse **every** intent because their clause has no exemption.

### The working order

**Tier 0 — answer first; each blocks everything, including itself.**
1. **Item 16.** `ClauseAuthorityEvidence` has no exemption and is already written. Every other
   answer combined still executes zero intents while this is silent. The answer may be "we decline
   the cost" — but it must be an accepted ADR, and A+C is recommended above. Lowest cost of any
   item on this list; highest blocking power.
2. **Item 4.** Head of the longest serial chain (MoR → CommercialIdentity →
   TaxIndependentlyReproducible). Three gates, and its latency is not absorbed by parallel work.
   Every week of deferral is a week on the end date.
3. **Item 10.** Decides whether the tax chain is one market or two, and whether the second rail
   exists. Option A (Taiwan out of scope for the cutover) collapses it into item 4 and takes items
   11-B and the NewebPay subsystem off the plan. Answering 10 before 5-8 is what makes 5-8 small.

**Tier 1 — the four policy revisions. Each needs its whole group; a partial group publishes
nothing.**
4. **`tax_rule_revision` ← items 4, 5, 6, 7, 8, 10.** Six items, one string. After 4 and 10 are
   answered, take **8** next (redistribution rights are procurement, i.e. calendar time in front of
   the evaluator and verifier), then **7** and **6** *together* — required location evidence and
   classification are parts of the same artifact, so deciding 6 without 7 publishes nothing —
   then **5**.
5. **`terms_revision` ← items 2, 3, 9, 13.** Take **2** first (recommendation ready, and it is the
   only one of the four an engineer may argue), then **9** (the largest unscoped surface: the whole
   return rail sits outside the LARGE 26 / MEDIUM 4 / SMALL 2 tally), then **3**, then **13**.
6. **`price_book_revision` ← items 11, 12.** **11** first: recommendation ready (one currency,
   published as a commercial decision, plus the currency+exponent registry built now while
   `credit_ledger` is empty). Then **12**, and note its convergence with item 15's unshipped
   base-price migration — decide 12 *before* that migration ships or pay the customer-notice and
   grandfathering cost twice.
7. **`notice_policy` ← item 1.** The smallest artifact and the only revision gated by a single
   item. Split it per the recommendation and publish the notice half now; the standing-authority
   numbers can follow.

**Tier 2 — cheap now, expensive after the first sale.**
8. **Item 14.** Nothing to restate today (`credit_ledger = 0`). The chart of accounts is still
   free to choose. It turns expensive the day the first credit lot is sold or the first wallet draw
   posts — not on the calendar. Must be sequenced **after 13**.
9. **Item 13.** Free while `credit_ledger = 0` and `billing_authorizations = 0`. Its hard deadline
   is the canonical-schema window, not a revenue date.
10. **Item 15.** The infrastructure half is settled; only the migration's execution and the
    payer-cutoff rule remain. It is the one item whose deferral is already invoicing customers under
    an undecided rule (`RepointOrgNullAccountEvents`) and shipping a bill whose arithmetic does not
    add up.

**Two deadlines that are not on anyone's calendar and should be:**
- **The first sealed production intent** ends the free window for every digest-shape change: items
  3 (option B), 5, 6, 8, 9, 10, 12, 13. Arming `BILLING_CYCLE_INTENT_CUTOVER="propose-do-not-collect"`
  starts that clock **without moving any money** — proposers seal and persist
  (internal/intent/proposer/proposer.go:133).
- **The first minted authorization and the first sold credit lot** end the free windows for items 1
  (the 86400 lead time), 3 (re-acceptance campaign), 13 (lot terms) and 16 (`AcceptanceDigest`).

Both are currently at zero. That is the only reason this list is cheap.

---

## The goal is MIGRATION, not just a working executor

Stated by the owner 2026-08-31: *"change all exist billing account to new
billing services and drop all legency path."*

That is a stronger target than "the executor can settle an intent", and it adds
a constraint none of the sixteen items above states directly.

### `billing_authorizations` is 0

Measured in production via the ops Lambda on 2026-08-31. **Not one existing
account has a standing authorization.**

INV-006 requires one for every charge — the intent seals an `AuthorizationID`
(`internal/intent/chargeintent.go`), and `BillingAuthorization.Permits` is what
the predicate consults. So migrating an account is not a data copy. It is
**minting an authorization per account**, and an authorization is something a
customer *accepts*: `AcceptanceDigest` binds it to the disclosure bytes they
were shown (`internal/intent/authorization.go`).

**An authorization cannot be synthesised on a customer's behalf.** Doing so
would make INV-006 a statement about our own records rather than about the
customer's decision — the exact failure §12 item 16 already describes for
acceptance relayed by `api-platform`, one step worse.

### So the migration is gated on items 1 and 3, specifically

- **Item 1** fixes what a standing authorization *contains*: ceilings, cadence,
  expiry, renewal. Without it there is no shape to mint.
- **Item 3** decides whether an existing customer's acceptance **carries over**
  to a new authorization, or whether every account must re-accept. That single
  answer decides whether the migration is a background job or a customer-facing
  campaign across every account.

Nothing else in the list changes that answer, and no engineering substitutes
for it.

### What the migration actually has to move

Measured in production, 2026-08-31:

| | | |
|---|---|---|
| `accounts` | 6 | each needs an authorization that does not exist |
| `apps` | 8 | |
| `billing_authorizations` | **0** | the whole gap |
| `charge_intents` | 0 | nothing to migrate; the free-window is still open |
| `usage_events` | 38,326 | **version-blind, so unratable by the new engine** |
| `usage_aggregates` | 12 | 6 carry a `model`, needing tier-2 pricing |
| `invoices` | 15 | |
| open invoices | **0** | nothing in flight to strand at cutover |
| `periods_invoiced_without_aggregates` | **0** | the legacy rollup is correct |

Two of these matter more than the rest.

**Zero open invoices is the good news.** The cutover strands nothing. The seven
legacy-drop preconditions are all clear today for that reason, and re-running
them in the same window as the deletion is what `LEGACY-DROP-PLAN.md` requires.

**38,326 version-blind usage events is the constraint.** The owner ruled
legacy-shape pricing compatibility out of scope (2026-08-31, as a legacy
concern), so the new engine will never rate that history. The migration
therefore moves *accounts and their authority forward*; it does not re-rate the
past. Anything requiring the new engine to reproduce a historical charge —
including DESIGN §11's shadow reconciliation — is out of reach by that same
decision, and should stop being described as pending.

### The order this forces

1. **Item 3 first, then item 1.** Whether acceptance carries decides the shape
   of everything downstream. If it does not carry, the campaign is the long pole
   and every other decision can proceed in parallel with it.
2. **Then the rest of the answer order below.**
3. **Then mint authorizations**, one per account, under the published revisions.
4. **Then arm a leg**, and only then does the first sealed intent appear —
   which is the moment the free digest-shape window closes for items 3B, 5, 6,
   8, 9, 10, 12 and 13.
5. **Then re-run the preconditions and drop**, in the same deployment window.

🔴 **Step 3 is the irreversible one, not step 5.** The first minted
authorization ends the free window for items 1, 3, 13 and 16. Dropping legacy
afterwards is mechanical by comparison — the preconditions are already clear and
the collectors are already doing their job correctly.

---

## ✅ ANSWERED BY THE OWNER — 2026-08-31

### Item 16 — consent authority: **C**

Build the independence that needs no enrolled factor:

1. the billing-owned transactional evidence outbox (DESIGN.md:395-397 — "worth
   building first anyway");
2. bind `AcceptanceDigest` to an **engine-issued, engine-signed** disclosure
   digest instead of accepting any non-empty string
   (`internal/intent/authorization.go:270-272`), and populate
   `SealedState.Acceptance` in the executor from a stored engine-issued
   challenge so the customer-present branch (`predicate.go:292-317`) is
   reachable and actually checks something;
3. publish the trust boundary in `Capabilities`.

A relayed receipt stays relayed. What changes is that "reproducible in
principle" becomes "reproducible in practice", and every piece is a strict
prerequisite for B if it is ever funded.

### Item 10 — Taiwan: **out of scope for this cutover**

Owner: *"not in this scope, implement in future."* The NewebPay adapter, its
independent conformance suite (§11 step 7) and any invoice-issuance subsystem
leave the plan.

⚠️ **One measurement is still owed before this is safe to call settled:**
whether legacy collection *already* reaches Taiwan customers. If it does, this
decision describes the target and not the present, and the exposure is a
legacy-rail question the cutover does not touch.

### Items 3 and 4 — answered, and they change the SHAPE of the model

The owner's answers were not the single global choices §12's framing assumed.

**Item 3 — change policy.** Not one answer:
- **the platform runs option B** (two classes: terms and price book need
  renewed acceptance; notice policy, tax rule and routing need delivered notice
  but keep the authorization alive);
- **a distributor may run A or B** for its own customers.

**Item 4 — merchant of record and rail.** Also not one answer:
- **the platform sells in USD only, through Stripe**;
- **a distributor may select currency**;
- **routing rule:** inside Stripe's supported area go direct to Stripe;
  otherwise a local provider (NewebPay, TapPay, …) — *"but need to built to
  support it"*.

## 🔴 What those two answers imply: policy is PER-DISTRIBUTOR, and the model has no distributor

This is the largest consequence of this decision round, and it is not in §12.

**`distributor` exists in exactly one place in the billing engine:** as a value
of `credit_ledger.actor`
(`migrations/billing/048_credit_wallet.up.sql:47`, `CHECK (actor IN ('self',
'distributor', 'system'))`). There is no distributor entity, no distributor
identity on an account, and `BillingAuthorization` has no notion of one — it
carries `Scope`, `Subject`, `Currency`, `Kinds`, ceilings, `Provider`,
`MandateReference` and the four policy revisions, and nothing that says *whose
policy* those revisions are.

So both answers require a dimension that does not exist:

| answer | needs |
|---|---|
| distributor may run change policy A or B | the class rule to be **resolvable per distributor**, and — per item 3 option B's own cost note — **sealed, not inferred**, so a canonical supersession |
| distributor may select currency | `chargeCurrency` is a **constant** `"usd"` (`internal/account/cycle/types.go:195-198`) — one of the three traps this document already names |
| routing: Stripe if supported, else local | `ClauseRailSupportsPlan` compares the sealed rail to `Authorization.Provider()` — a single accepted rail per authorization. A *routing rule* that picks among rails needs the published routing policy (§12 item 3's routing half) and a per-distributor permitted-rail set |
| platform USD/Stripe, distributor otherwise | `ClauseMerchantOfRecord` must resolve the seller **from the distributor**, not from a global constant — which is item 4's binding set becoming genuinely plural after all |

**Consequence for item 4's costing above:** the answer is closer to option **B**
(plural entities, plural rails) than to option **A**, even though Taiwan is out
of scope. Option A's cheapness came entirely from the binding set having one
member. A distributor-selectable currency and a rail-routing rule make it
plural on day one, and items 10 and 11 come back onto the critical path the
moment a distributor selects a currency the platform does not sell in.

Taiwan being out of scope defers the *implementation* of a local provider. It
does not defer the *seam*, because the routing rule is what the owner asked
for.

### The next question this raises, which is the owner's

**Is a distributor a billing principal, or a reseller?** The two readings have
different answers to "whose customer is it" and therefore to merchant of
record:

- **A distributor is a channel.** MirrorStack remains merchant of record for
  every sale; the distributor's currency and policy choices are presentation
  and packaging over MirrorStack's own terms. Item 4 stays close to option A.
- **A distributor is a reseller.** The distributor is merchant of record for
  its customers, owns their tax liability, and MirrorStack sells to the
  distributor rather than to the end customer. That is item 4 option **C**,
  whose costed consequence above is that `TaxIndependentlyReproducible` may
  become structurally unreachable for those sales.

Nothing in the tree answers this, and it decides how much of §12 items 4, 5, 6,
10 and 11 MirrorStack owns versus delegates.
