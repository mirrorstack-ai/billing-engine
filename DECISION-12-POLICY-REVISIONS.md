# The four placeholder revisions — what you are actually deciding

Every intent this engine proposes today seals four policy identifiers.
All four are placeholders (`internal/account/cycle/domain_charges.go:55-62`):

```go
proposedTermsRevision     = "unpublished/pending-decision-12"
proposedPriceBookRevision = "unpublished/pending-decision-12"
proposedNoticePolicy      = "unpublished/pending-decision-12"
proposedTaxRuleRevision   = "not-applicable/pending-decision-12"
```

These are not cosmetic. They go **inside the canonical digest**, and the digest
is what a customer's bundle attests to. A sealed intent says, in effect:
*"this amount was derived under terms revision X, price book Y, notice policy Z,
tax rules W."* Right now all four sentences are blank cheques.

## What each one is waiting on

| constant | §12 items that gate it | gates held shut |
|---|---|---|
| `proposedNoticePolicy` | **1** — what counts as delivered notice, which contacts, lead time, retry schedule | G1, G2 |
| `proposedTermsRevision` | **3** change policy · **9** refund/dispute/chargeback/write-off · **13** credit expiry + legal characterization of stored value · **2** budget-stop semantics | G1, G3, G4 |
| `proposedPriceBookRevision` | **12** which kinds exist + price/tier policy · **11** currency + TWD price books · **15** the infra-markup migration into a published base price | G1, G2, G4 |
| `proposedTaxRuleRevision` | **4** merchant of record · **5** registrations · **6** classification/rounding · **7** location evidence · **8** rate source · **10** Taiwan/NewebPay invoicing duties | G3 |

## Why my earlier recommendation was wrong

I offered you "mint v1 frozen from today's behaviour". §12's closing paragraph
forbids exactly that:

> Until each of these is accepted as an immutable policy revision and an ADR,
> they stay named decisions. **They must not be reconstructed from current
> constants, code comments, or the shape of today's Stripe-shaped schema.**

Minting `2026-08-30/v1` from what the code does today *is* reconstruction from
current constants. It would produce a digest that looks settled and is not.
Withdraw that option.

## The one that is not a placeholder

Three of the four say `unpublished/` — honest, if inert. The fourth says
**`not-applicable/`**. That is not a deferral, it is a substantive claim: *tax
does not apply to this charge.* For a Taiwan entity billing in TWD, §12 item 10
says business-tax and e-invoice duties are exactly what has **not** been
decided. So the one field that asserts something is the one with the least
authority to assert it.

If nothing else changes, this should change: `not-applicable/` → `unpublished/`.
A refusal to guess is honest; a claim of non-applicability is not.

## The real options

**A. Fail-closed marker (recommended, pending verification).**
Make the `unpublished/` prefix structurally uncollectable: a predicate clause
that refuses any intent whose revisions are unpublished. Then the placeholder
stops being a fiction and becomes a *control* — the engine can propose and
store and reconcile all day, and physically cannot collect until you publish
real revisions. This is what §12's G1 ("production execution fails closed until
each item is settled") already asks for.
Cost: one clause, one migration-free change. Does not need your policy answers.

**B. Publish real revisions.**
Requires settling §12 items 1, 3, 9, 11, 12, 13, 15 (and 4–8, 10 for tax) as
accepted ADRs. That is finance + legal + product work, not engineering. It is
the only path that lets money actually move.

**C. Status quo.**
Placeholders stay, nothing refuses them, and the day someone flips
`INTENT_EXECUTOR_ENABLED` the engine collects under an invented revision.

A and B are not alternatives — **A is the safe holding pattern that makes B
unhurried.** C is the one to avoid.

---

## Verified 2026-08-30: the placeholder is a silent fiction, not a fail-closed marker

I assumed something refused these. Nothing does. Measured, not reasoned:

| what I expected to refuse it | what it actually does | evidence |
|---|---|---|
| `ClausePolicyPublished` — named `policy_published_effective_and_digest_matching` | `return s.PolicyDigestsMatch` — a caller-supplied bool. Never reads the intent. | `internal/intent/predicate/predicate.go:165-166`, field at `state.go:130` |
| `ClauseTaxFinal` | reads `Tax().Resolved` + a caller bool. Never reads `RuleRevision`. | `predicate.go:161-163` |
| any tax clause | **zero** clauses read `taxRuleRevision`. Its only non-test reads are the digest and the INSERT. | `chargeintent.go:321`, `store/store.go:83,148` |
| `Seal` | non-blank check only. No format, prefix, or registry. | `chargeintent.go:210-215` |
| `Authorize` | accepts ANY non-blank string | `authorization.go:160-168` |

The consequence: an authorization minted with the *same* placeholder string
satisfies every equality check in `Permits`. The four values are sealed into the
canonical digest, so a customer's bundle would attest to
`unpublished/pending-decision-12` — and nothing in the engine would object.

An intent sealed today *is* refused, but only by `BuildIdentified`,
`PolicyDigestsMatch`, `TimeReady`, `TaxIndependentlyReproducible` and a missing
authorization row. **Every one of those flips to permit without anyone touching
the placeholder strings.** They are unrelated guards that happen to be shut.

### This makes option A necessary, not merely prudent

Option A is no longer "add a nice safety clause". It is **filling in a clause
that already exists and lies about what it checks**. `ClausePolicyPublished`
should read the intent's four revisions and refuse any that is unpublished.
That is the contract its own name states.

Building five more legs before this lands would multiply the fiction by five.

---

## The three legs that cannot be built, and what unblocks each

Verified 2026-08-30. These are blocked on decisions, not on effort. None of them
is a matter of someone finding time.

### Credit purchase and auto-top-up — blocked by §12 item 12

> 🔴 **CORRECTION, 2026-08-31.** The paragraph below said "§6's charge catalog
> is closed and names no funding kind, so neither can legally seal an
> `intent.Draft` today." **That is false, and it was false when written.**
>
> `internal/intent/catalog.go:36-54` carries a section headed *"funding and
> collection: not service lines, per §6"* and names four kinds:
> `KindSubscriptionStart`, `KindCreditPurchase`, `KindAutoTopUp` and
> `KindCollectReceivable`. The auto-top-up leg **already seals**
> `KindAutoTopUp` in production code
> (`internal/account/autotopup/executor.go:1329`).
>
> So the catalog is not the blocker for either leg, and no kind needs
> inventing. Auto-top-up is routed. What actually blocks CREDIT PURCHASE is
> the disclosure binding — see the next correction — and the schema paragraph
> below is also stale: `charge_intents.kind` has been CHECKed against the
> closed set since the catalog landed, so sealing an invented kind no longer
> compiles *or* stores.

Both are **stored-value funding**, not charges for consumption. §6's charge
catalog is closed and names no funding kind, so neither can legally seal an
`intent.Draft` today.

The schema would not stop you: `ms_billing.charge_intents.kind` is a bare
`TEXT NOT NULL` with no CHECK. Sealing `kind: "credit.purchase"` would compile,
insert, and digest cleanly. That is exactly why it must not be done — inventing
a kind is inventing the catalog, and §12 item 12 is the decision that says which
kinds exist.

**What unblocks them:** a decision on whether buying stored value is a charge
kind in the §6 catalog, or a different document class the intent vocabulary does
not cover. That is item 12 plus item 13 (the legal characterization of stored
value), not an engineering task.

Credit purchase carries a second, independent blocker: it is **not terminal**.
The cycle legs propose and stop, and nothing downstream needs the money to have
moved. Credit purchase's RPC response must carry a Stripe client secret that
only exists *after* the finalize (`creditpurchase/executor.go:271`), so a
proposing version returns a response the customer's browser cannot use. Cutting
it over changes a synchronous customer-facing contract, which the two shipped
legs never had to do.

Auto-top-up carries its own: it is wired into **six binaries** by a duplicated
inline closure, so the seam has six installation sites rather than one
constructor.

### Unpaid retry — blocked by INV-001 itself

It re-pays an invoice that **already exists at Stripe**
(`billing/unpaid.go:223`). `stripeadapter.Collect` always creates a new
draft + line + finalize + pay, so the adapter cannot express it.

Adding a `CollectExistingInvoice` method would be easy and wrong. The amount on
a pre-existing invoice comes from the provider, not from our derivation — so an
intent wrapping it would attest to a figure the engine did not derive. That is
the exact inversion INV-001 exists to prevent, and the reason this design says
the engine derives every financial field.

**What unblocks it:** a decision on what an unpaid retry *is* in intent terms.
The honest options look like: supersede the original intent and settle the
successor (INV-003 already describes supersession), or classify retry as an
execution concern of the original intent rather than a new charge. Both are
design work in §5, not adapter work.

### What this means for the count

`legacy_money_paths` does not move for any of this. The constant's own comment
is explicit: it "reaches zero when the last of them is deleted — not when an
intent surface is added beside them." It stayed at **11** across both shipped
cutovers. The count falls only on deletion, and deletion is gated on production
state by `scripts/legacy-drop-preconditions.sql`.

**So the critical path to "drop the legacy things" runs through production
access, not through more legs.** That is what the read-only ops Lambda is for.

---

## A related gap the placeholder finding uncovered: `Seal` forces a tax claim

`Seal` refuses an unresolved tax determination outright:

```go
if !draft.Tax.Resolved {
    return ChargeIntent{}, ErrTaxUnresolved       // chargeintent.go:219
}
```

So a proposer *cannot* seal an intent that says "tax has not been determined".
The cycle legs therefore seal this (`domain_charges.go:403`):

```go
Tax: intent.TaxDetermination{
    Resolved:     true,
    Jurisdiction: "not-applicable",
    RuleRevision: proposedTaxRuleRevision,
}   // AmountMicros omitted, so zero
```

The code comment calls this "the honest option". It is the honest option
*available*, but the digest ends up attesting that a determination **was made**
and came to **zero** — for a Taiwan entity whose business-tax duties §12 item 10
says are undecided.

That is the shape INV-004 exists to forbid: an unknown input must quarantine,
never default to zero. Here the type system forces the default, because
`Resolved` is a two-state bool with no room for "not determined".

**Not exploitable today** — as of `c5dc1fd` the predicate refuses to collect
under an unpublished rule revision, so the zero never reaches a customer. But
the sealed document still says something untrue, and superseding those intents
later is more work than not creating them.

**The fix is a design change, not a one-liner:** `TaxDetermination` needs a
third state (determined / not-applicable / **not-determined**), which touches
`Seal`, the canonical digest, the `charge_intents` schema and `ClauseTaxFinal`.
Whether "not determined" may ever be sealed at all is itself a §12 question —
it is the difference between an intent that cannot be collected and an intent
that cannot be written down. **I have not changed this.** It needs your call.


---

## 🔴 What actually blocks credit purchase, and a live divergence found looking

Recorded 2026-08-31, verified in code.

### The disclosure it needs has no producer and no consumer

§6 requires a credit purchase to rest on "your acceptance of engine-signed
disclosure bytes naming currency, amount, credit received, restrictions,
expiry, refund terms, rail and intent digest". `internal/intent/disclosure.go`
implements exactly that type — and **nothing outside its own unit test
references it**. `ChargeIntent` carries no disclosure digest field, so an
intent cannot even name the document it was accepted under.

That is §12 item 16 option C, piece 2 — bind acceptance to an engine-issued,
engine-signed disclosure — which is unbuilt. Credit purchase is blocked on
that piece and on nothing else in the catalog.

### The amount charged and the credit granted already differ

`internal/account/creditledger/settlement.go:184` requires the customer to have
paid `microsToCentsRoundHalfUp(row.AmountMicros)` — the **rounded cents**.
Fourteen lines later, `:198` credits `balance + row.AmountMicros` — the **raw
micros**.

Nothing requires `amount_micros` to be a whole-cent multiple:
`internal/account/billing/credit.go` range-checks only 5,000,000 to
5,000,000,000, and migration `048`'s CHECKs are a non-zero test and a sign
test. So a purchase of 5,004,999 micros charges $5.00 and credits 5,004,999 —
the customer receives 4,999 micros they did not pay for, per purchase.

It is sub-cent and it is in the customer's favour, so it is not urgent. It
matters here because **a cutover cannot seal both numbers**: `proposer.Charge`
expresses one amount, and `intent.Disclosure` is explicit that the amount paid
and the credit received "are NOT the same number — a promotion, a bonus tier
or a fee makes them differ, and collapsing them would hide exactly the term a
customer most needs to see."

So routing this leg requires the intent to carry the credit granted as well as
the amount charged. That is another canonical field, and it is free only while
`charge_intents = 0`.
