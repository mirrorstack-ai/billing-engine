# Every monetary effect billing-engine may create

This is the exhaustive customer and provider effect catalog for the target
intent-only engine.

> **Status: proposed, not implemented.** Current `main` has fragmented,
> Stripe-shaped charge paths and additional mutable pricing inputs. This catalog
> is the desired closed vocabulary. The engine cannot claim conformance until the
> code enum, public schema, generated mutation inventory, and this file are
> machine-checked to contain the same set.

The boundary is:

> **If a positive customer charge kind is not listed here, billing-engine cannot
> propose or collect it.**

A private caller, module, payment adapter, webhook, tax provider, or operator
cannot create a new kind with free text.

---

## 1. Customer bill lines

The customer-facing positive vocabulary is intentionally small:

| kind | purpose | authoritative quantity | authoritative rate | normal timing |
|---|---|---|---|---|
| `platform_base` | published MirrorStack platform access for an app/account period | eligible app/account-period facts | immutable platform price-book revision | recurring cycle; prorated only by published rule |
| `module_usage` | one installed module's declared metered usage | immutable usage facts aggregated by the manifest's declared rule | immutable module-version billing manifest + effective price revision | recurring cycle |
| `module_capacity` | published charge for installed-module capacity/allowance above the included tier, if retained by product policy | versioned installation/timer facts | immutable platform price-book revision | recurring cycle; no hidden immediate sweep |
| `custom_domain` | published domain feature charge, if retained by product policy | immutable domain activation/active-window facts | immutable platform price-book revision | recurring cycle; activation proration by published rule |
| `tax` | final tax determined on enumerated taxable lines | frozen taxable basis and customer tax evidence | immutable tax-policy revision + versioned determination | before intent notice/seal |

The exact prices, included module allowance, block/tier shape, grace windows, and
domain policy are product decisions. Today's compiled constants are discovered
behavior, not accepted future policy. They cannot enter the target rater until
published as immutable, future-effective revisions.

### No infrastructure line

There is no `infrastructure`, `compute`, `egress`, `model_cost`, provider cost,
or hidden markup kind in the customer vocabulary.

MirrorStack may measure those values for internal operations, publisher
settlement, or margin analysis. That data is physically outside the customer
rater. Platform infrastructure is recovered through a disclosed base price;
module-specific cost is recovered through the module price the customer
accepted. Internal cost movement cannot increase a customer intent.

### No payment-provider fee line

Stripe, NewebPay, card-network, settlement, payout, FX, or adapter fees are
internal costs unless a future accepted ADR adds one exact customer kind with a
published rule and renewed authorization. An adapter cannot append its own fee.

---

## 2. Negative and zero-value lines

These lines may reduce or explain a bill but cannot be used to hide a positive
charge:

| kind | source | rule |
|---|---|---|
| `credit_applied` | immutable wallet/credit source lots owned by the payer | exact lot allocation; never exceeds eligible positive lines or available settled credit unless an accepted credit policy permits bounded exposure |
| `promotional_credit` | typed grant with issuer, authorization, reason, and terms | applied only to permitted line kinds/windows; expiration and refundability are disclosed |
| `adjustment_credit` | reviewed correction linked to a prior intent/ledger entry | append-only; never edits the original charge |
| `tax_credit` | final replacement/refund tax determination | references original tax line and rule/evidence |
| `rounding` | canonical settlement conversion | one documented rounding step; bounded below one settlement minor unit and never free-form |

A zero-valued line may be shown for explanation, such as a final tax determination
of zero. Zero and unknown are different: unresolved tax, price, quantity, or
credit provenance prevents sealing.

Negative invoice totals are not silently sent to a payment provider. Product and
finance must choose whether they become wallet credit, refund intent, or carried
credit; the receipt states which.

---

## 3. Funding and collection intents

These are monetary effects but not extra service lines on a recurring bill.

### `credit_purchase`

A customer-triggered one-time purchase of MirrorStack credit. The engine-served
page displays the exact currency, amount, credit received, restrictions, expiry,
refund terms, payment rail, and intent digest before the customer submits.

The payment adapter receives that exact authorized total. Credit is granted only
after verified provider evidence and the balanced ledger settlement commit.

### `auto_topup`

An opt-in standing funding intent with its own authorization. It binds:

- balance trigger,
- exact top-up amount/currency or bounded deterministic rule,
- selected provider and payment mandate,
- per-attempt, frequency, and period ceilings,
- exact notice channel and lead time,
- effective time, expiry, and revocation behavior, and
- treatment when top-up is pending or fails.

General billing authorization does not enable auto top-up. A balance read,
status read, usage ingest, infrastructure sync, or provider callback cannot
synchronously collect money. They may append a trigger fact; only the intent
lifecycle and isolated executor can act.

### `collect_receivable`

Settlement of one already-sealed service intent. Manual pay is customer-triggered
one-time authorization against the exact receipt/intent. Automatic pay consumes
a valid standing authorization after notice and waiting.

Paying an existing invoice/receivable does not re-rate it and cannot add lines.

---

## 4. Refunds, voids, disputes, and corrections

| effect | required authority and source | provider consequence |
|---|---|---|
| `void` | known unsettled attempt + intent/operation ownership + typed reason | cancels only the verified provider object if the adapter supports it |
| `refund` | settled attempt + linked refund intent + allowed amount/currency | provider refund through executor; never exceeds remaining refundable amount |
| `partial_refund` | as refund plus exact line/tax allocation | only when adapter capability and accepted policy support it |
| `reversal` | known erroneous local ledger transaction | append-only local reversal; provider operation handled separately and linked |
| `dispute` / `chargeback` | authenticated provider evidence | records provider cash state; never rewrites original intent |
| `write_off` | reviewed finance policy and actor | changes receivable treatment; no provider debit |

Credits and refunds are not interchangeable. A customer receipt states whether
value returned to the original payment rail or only to a MirrorStack balance.

Provider callbacks may confirm these effects but cannot originate an unlinked
refund or debit.

---

## 5. Non-customer financial effects

### Developer/module settlement

Module developer earnings are a separate liability/settlement domain derived
from settled eligible module lines and an immutable distributor/publisher policy.
They do not change the customer total after sealing.

The settlement receipt identifies the originating customer line commitments
without exposing customer data to the developer. Publisher/private take rates,
eligibility, reserve, refund, dispute, and payout timing require a published
policy revision.

A developer payout adapter is not a customer collection adapter and has separate
credentials and execution authorization.

### Tax remittance and provider payout

Tax liability/remittance and provider settlement/payout are accounting/cash
effects connected by the ledger and provider trace. They never become additional
customer lines merely because a provider reports a fee or net payout.

---

## 6. Line provenance contract

Every line in a sealed intent contains:

- closed `kind` enum and schema version,
- customer-readable label and consequence code,
- app/module/domain subject where applicable,
- immutable source fact/aggregate ids or commitments,
- meter/quantity, declared scale, and aggregation rule,
- price-book/module-manifest/tax-policy id and digest,
- effective window,
- exact formula inputs and integer/rational operations,
- pre-round and final amount with named currency,
- taxable classification and tax allocation,
- credit/adjustment links, and
- explanation of when the obligation accrues and when collection may occur.

Descriptions are presentation. The enum, sources, policy digests, and arithmetic
are authority.

### Module provenance

A `module_usage` line must name the exact installed module billing-manifest
version. A module may emit constrained usage facts for metrics in that manifest.
It cannot send a price, change aggregation for already-recorded facts, or bill an
undeclared metric.

Publishing a new module price creates a new immutable manifest/price revision
with future effect and required customer notice/acceptance. Missing version
pricing does not fall back to a mutable module catalog.

---

## 7. Timing and proration

Proration is a formula attached to a documented line kind, not a separate hidden
charge path.

For each prorated kind, the price policy fixes:

- inclusive/exclusive start and end instants,
- billing-zone and anchored period behavior,
- day/second denominator,
- grace and cancellation treatment,
- exact integer/rational calculation and rounding point, and
- whether it joins the next consolidated cycle or requires a separate one-time
  intent.

The preferred target is one consolidated cycle intent. Creating an app,
installing a module, or activating a domain should update the read-only estimate
and future facts, not silently finalize an immediate provider invoice.

---

## 8. Payment-rail relationship

`ChargeIntent` freezes the total before a provider is selected/executed. A
provider adapter may transform only representation required by its API:

- named currency to its declared settlement-unit integer,
- engine operation id to an opaque provider reference,
- customer-action/callback state, and
- supported provider metadata that binds the original intent.

It cannot re-rate, add tax, add a fee, change currency, select another payer, or
split an amount in a way that changes the customer's obligation.

Stripe and NewebPay attempts map back to the same effect catalog and receipt
schema. Unsupported provider capabilities fail before any external mutation.

---

## 9. Current implementation inventory to retire or wrap

At proposal time, current `main` includes distinct provider or wallet effects
for:

- monthly boundary collection,
- app creation/proration,
- module capacity/overage grace,
- custom-domain activation and recurrence,
- manual unpaid-invoice payment,
- manual credit purchase,
- auto top-up,
- wallet usage/grant/preallocation/adjustment records,
- payment-method setup/default/detach,
- organization sponsorship/payer changes, and
- developer settlement accrual.

Some contain strong recovery/freeze controls worth preserving. None may remain a
parallel route around `ChargeIntent` and the isolated executor.

The highest-priority current gap is that nominal reads and usage/infra paths can
reach auto top-up. In the target, those components cannot compile against a
payment-write interface and hold no provider write credential.

Legacy in-flight attempts are finished or quarantined under their captured
legacy semantics. The migration must not fabricate a notice, authorization,
tax, or policy digest that never existed.

---

## 10. Machine-enforced exhaustiveness

CI compares:

1. the domain `ChargeKind` and financial-effect enums,
2. canonical receipt schema variants,
3. the generated inventory of provider/wallet mutation sites,
4. adapter conformance fixtures, and
5. this catalog's machine-readable index.

A new enum without documentation or a provider mutation without a mapped effect
fails the build. Free-text line creation is not part of any public or internal
API.

---

## 11. Product decisions still required

Before accepting this catalog, decide:

- whether `module_capacity` and `custom_domain` remain separately chargeable;
- exact base, module, and domain price/tier policies;
- proration/grace and consolidation timing;
- credit expiry, refundability, exposure, and allocation order;
- auto-top-up amount/frequency/notice rules;
- developer settlement/take/refund policy;
- negative-total and minimum-collection treatment; and
- supported currencies and any explicit FX customer line/policy.

Until then, the vocabulary is proposed and exact values are intentionally not
copied from mutable constants.
