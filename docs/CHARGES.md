# Every monetary effect billing-engine may create or record

This is the exhaustive customer-collection, wallet, and accounting-effect
catalog for the target intent-only engine. Its purpose-separated provider-write
vocabulary is limited to customer setup, payment, mandate revocation, void, and
refund. Developer payouts and tax remittance are outside the billing-engine
command surface; this engine may only
record their independently verified accounting evidence.

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

Every provider plan step also uses one closed effect class:

| effect class | allowed consequence |
|---|---|
| `non_adverse_prepare` | creates only a non-collectible prerequisite; no hold, debit, reusable mandate, or provider-autonomous future path |
| `mandate_setup` | creates only the exact accepted reusable mandate scope under setup proof; never holds or debits |
| `funds_hold` | places one exact disclosed hold for the accepted amount/duration; it is adverse and retains claim/exposure until capture or verified release |
| `debit` | collects the one exact provider remainder; at most one per charge plan |
| `return` | returns the one exact provider-refund remainder; at most one per refund plan |
| `release` | releases/voids one known hold, collectible continuation, or unsettled object; cannot collect or return new cash |

Setup, payment, mandate-revoke, void, and refund plans enumerate every server
mutation as its own step. A composite adapter call, read-only reconciliation
disguised as a mutation step, or provider-native autonomous schedule is outside
the vocabulary and fails conformance. A setup plan creates at most one exact
`mandate_setup` output; a charge/refund plan has at most one exact `debit`/
`return`; holds stay within the accepted aggregate count/amount/duration; and
every `release` is bound to a known prior collectible object.

The machine-checked purpose/effect matrix is exhaustive:

| purpose | allowed mutation effects |
|---|---|
| `setup` | `non_adverse_prepare`, `mandate_setup`, source-bound `release` cleanup |
| `payment` | `non_adverse_prepare`, exact disclosed `funds_hold`, exact sealed `debit`, source-bound `release` cleanup |
| `refund` | `non_adverse_prepare`, exact source-linked `return`, source-bound `release` cleanup |
| `void` | source-bound `release` only |
| `mandate_revoke` | source-bound `release` only |

Setup never performs even a temporary verification hold. A rail that requires one
must use a separately disclosed and authorized payment/hold intent. A forbidden
pair is rejected before disclosure, envelope persistence, consume, and adapter
invocation; a customer-hosted actor cannot widen the table.

---

## 1. Customer bill lines

The customer-facing charge-line vocabulary is intentionally small. In the
equations below, `positiveServiceLines` means only the positive non-tax service
lines; `tax` is added exactly once as its own line:

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

### Stored-value wallet funding is not a negative bill line

`promotional_credit`, `adjustment_credit`, and `tax_credit` are rating/tax lines
that reduce the obligation under their published rules. A settled stored-value
wallet lot is different: it is a funding source allocated by `FundingPlan` after
the obligation is calculated. It does not reduce taxable basis, appear as a
second negative line, or change `grossObligation`.

The canonical equations are kind-specific so a stored-value purchase cannot
accidentally have zero principal:

```text
serviceGrossObligation = positiveServiceLines - eligibleRatingTaxCredits + tax + rounding
fundingGrossObligation = cashPurchasePrincipal + tax + rounding
collectionGrossObligation = sourceRemainingCollectibleReserved
grossObligation = serviceGrossObligation OR fundingGrossObligation OR collectionGrossObligation, selected by intent kind
grossObligation = walletFunding + providerRemainder
```

`credit_purchase` and `auto_topup` use `fundingGrossObligation`; their positive
`cashPurchasePrincipal`, exact `creditGranted`, any explicit `bonusCredit`, unit/
currency, restrictions, and expiry are all digest- and receipt-bound. Bonus output
never reduces the cash principal.

`collect_receivable` uses `collectionGrossObligation`. Its digest binds the source
intent/receipt/ledger references, original obligation, prior collections, applied
credits/write-offs, exact remaining collectible amount, and unique source-capacity
reservation. It does not reapply tax or create an arbitrary collection principal.

Every credit/grant kind declares exactly one semantic class: `rating_credit` or
`stored_value`. The same source id/lot cannot participate in both equations.
Customer displays may show “wallet applied,” but it is settlement allocation, not
an invoice discount.

Deferred prepaid service may reserve only a stored-value slice whose accepted lot
terms preserve that reservation through terminal consume/release for the bound
service window, even if nominal expiry passes. Nominal expiry blocks new
allocations but cannot retire the reserved slice. A lot without that preservation
rule may fund only an immediate same-transaction settlement while eligible; it
cannot admit deferred prepaid service. Expiry, close, compaction, refund, and
clawback serialize on the same lot generation/range fence, so expiry can retire
only unreserved value and can never turn prepaid service into debt or card
fallback.

---

## 3. Funding and collection intents

These are monetary effects but not extra service lines on a recurring bill.

### `subscription_start`

The first SaaS-period purchase is an exact customer-present intent sourced by an
accepted immutable `SubscriptionOffer`, `pending_first_settlement` schedule, and
one-time acceptance/replay identity. It may contain the published first-period
`platform_base` and only applicable `module_usage`, `module_capacity`, or
`custom_domain` kinds from the closed §1 catalog that the offer explicitly
enumerates, each under its frozen policy revision, plus tax and rounding. No
free-form or unlisted add-on kind is valid. It posts
no pre-settlement receivable and grants no service/accrual authority. Wallet
settlement first requires the same accepted responsibility/schedule generation or
refuses/cancels pre-adverse. Exact provider settlement is always recorded, but
activates `SubscriptionScheduleReceipt`, the first window/anchor, and bound service
authority only when that generation CAS succeeds. Already-dispatched old-payer cash
settling after a transfer enters source-linked refund/credit/manual resolution and
opens no service. Refusal, pending/unknown, revocation, or crash leaves the schedule
pending and admits no billable usage. Later periods use the ordinary service/cycle
source-allocation rules, never a provider-native subscription.

### `credit_purchase`

A customer-triggered one-time purchase of MirrorStack credit. `api-platform`
relays an engine-signed canonical disclosure containing the exact currency,
amount, credit received, restrictions, expiry, refund terms, payment rail, and
intent digest. An independently verifiable consent client or origin renders
those fields before the customer submits a proof the private relay cannot mint.

The payment adapter receives that exact authorized total. Credit is granted only
after verified provider evidence and the balanced ledger settlement commit.
Neither existing stored value nor a rating/tax credit may fund a credit purchase:
`walletFunding = 0` and `providerRemainder = grossObligation`. Any bonus credit is
an explicit disclosed output under the accepted package/promotion revision, never
a way to recursively buy more value with wallet value.

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

Auto top-up creates stored value and therefore cannot consume existing stored
value or rating/tax credits: `walletFunding = 0` and
`providerRemainder = grossObligation`. Threshold evaluation and trigger-epoch
reservation are one transaction. The consume transaction rechecks canonical
balance/pending funding; if the threshold recovered before dispatch, it cancels
and releases rather than performing an unnecessary top-up.

### `collect_receivable`

Settlement of one already-sealed service intent. Manual pay is customer-triggered
one-time authorization against the exact receipt/intent. Automatic pay consumes
a valid standing authorization after notice and waiting.

Paying an existing invoice/receivable does not re-rate it and cannot add lines.
The engine creates a linked `collect_receivable` intent for only the remaining
amount, freezes a new `FundingPlan`, and atomically reserves source collection
capacity. It posts no second obligation. Customer-present payment requires fresh
exact proof; standing collection requires current authority plus terminal notice
evidence and wait. Pending/unknown retains the reservation, and concurrent
collection intents cannot spend the same remaining receivable.

---

## 4. Refunds, voids, disputes, and corrections

| effect | required authority and source | provider consequence |
|---|---|---|
| `payment_method_setup` | exact setup acceptance + `ProviderMerchantSetupBinding` + no-debit finite plan | creates only the accepted reusable scope; cannot debit |
| `mandate_revoke` | engine-effective customer revocation + exact setup receipt/method identity + finite revoke plan | detaches/revokes only that mandate; engine use is already cut off even while provider status is pending |
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

Refunding a settled `credit_purchase` or `auto_topup` additionally requires a
`GrantedValueClawbackReservation`. In the same source-capacity transaction it
freezes the exact unspent granted-principal lots and any bonus lots required by
the accepted refund policy. A pending/unknown provider return keeps those lots
unspendable. Verified cash return atomically cancels the reserved outputs and
records the provider return; authoritative no-return proof releases them. Cash
cannot be returned while the corresponding granted value remains spendable, and
already consumed value cannot be silently refunded as if unused.

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

Billing-engine accrues the payable liability and records independently verified
payout evidence, but it has no developer-payout credential or execution port.
A future payout service must publish and review its own intent, authorization,
non-coercible `AuthorizedPayout`, credential, reconciliation, and receipt contract
before it can move money; these billing documents do not authorize that service.

### Tax remittance and provider payout

Tax liability/remittance and provider settlement/payout are accounting/cash
effects connected by the ledger and provider trace. Billing-engine records their
verified evidence but cannot initiate them. They never become additional customer
lines merely because a provider reports a fee or net payout.

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

The preferred target is one consolidated cycle intent per compatible group:
payer, exact commercial seller/tax identity, tax profile, currency,
service/collection authority, funding mode/policy, accepted settlement-route
policy/instrument class, and window must all match. After tax and wallet
allocation, the group selects one compatible exact settlement route and composite
merchant binding; otherwise it splits or refuses. Creating an app, installing a
module, or activating a domain should update the read-only estimate and future
facts, not silently finalize an immediate provider invoice.

---

## 8. Payment-rail relationship

Before sealing and disclosure, the engine freezes the total, `FundingPlan`,
selected rail, merchant-account policy, and routing-policy digest. A later rail
change creates a replacement intent with a new digest and eligibility decision,
plus either fresh exact customer-present disclosure/proof or standing notice and
delivery-relative wait, as applicable. The chosen provider adapter may transform only the
representation required by its API:

- named currency to its declared settlement-unit integer,
- engine operation id to an opaque provider reference,
- customer-action/callback state, and
- supported provider metadata that binds the original intent.

It cannot re-rate, add tax, add a fee, change currency, select another payer, or
split an amount in a way that changes the customer's obligation.
The settlement integer is the sealed `providerRemainder`, never
`grossObligation` or wallet funding.

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
3. the generated inventory of billing-engine provider/wallet mutation sites,
4. adapter conformance fixtures, and
5. this catalog's machine-readable index.

A new enum without documentation or a billing-engine provider mutation without a
mapped effect fails the build. External payout/remittance input is evidence-only
and must not expose a writer in this repository. Free-text line creation is not
part of any public or internal API.

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
