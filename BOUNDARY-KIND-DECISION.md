# One invoice, four charge kinds — the decision that blocks two legs

**Status: open. Owner's call. Nothing else I can build resolves it.**

Found 2026-08-31 while routing `internal/account/cycle/charge.go`. I wrote a
draft that sealed the boundary charge under a `KindUsageCycle` that does not
exist, checked the catalog, and reverted it. Inventing a kind is exactly what
§6 forbids, so this is written down instead.

---

## The tension, in one paragraph

Two legacy legs each issue **one** Stripe invoice made of components that map
to **different** kinds in §6's closed catalog. A `ChargeIntent` carries
**one** `Kind`, and migration `054`'s own header says why that matters:

> "it selects which rule of a standing authorization applies: a caller that
> chose it could pick the permission its charge happens to fit."

So collapsing the components into one intent is not a naming convenience — it
is the authorization control being weakened. But splitting into several
intents means several invoices and several card charges, because
`stripeadapter.Collect` runs draft → item → finalize → pay **per intent**.

## The two legs, which are not the same shape

### `cycle/charge.go` — the period boundary

| component | §6 kind |
|---|---|
| usage arrears (closed period) | `module_usage` |
| advance base fee | `platform_base` |
| advance module overage | `module_capacity` |
| advance custom domains | `custom_domain` |

Rounds **once, on the net**, at `charge.go:595`:
`centsFromMicros(arrears + base + overage + domains − walletDraw)`.

🔴 So splitting it into four intents rounds four times, and the totals do not
agree. Only `arrears` and the wallet draw are sub-cent-fractional (the three
fee components are exact whole-cent multiples), so the divergence is small —
but it is real, and "a cutover must seal exactly what a collection takes" is
the rule this repository has already been bitten by.

### `cycle/proration.go` — combined proration

| component | §6 kind |
|---|---|
| app base fee (prorated) | `platform_base` |
| module overage (prorated), × timers | `module_capacity` |

Rounds **per component**, then sums (`proration.go:751-753`,
`combinedProrationTotalCents`), and already writes **separate Stripe invoice
items** per component.

✅ So splitting proration into intents **reproduces its total exactly** — each
intent seals a component whose cents were already rounded independently. Its
only cost is invoice count, not arithmetic.

That distinction is the useful part: **the arithmetic problem is
boundary-only; the invoice-count problem is both.**

---

## The options

### A — one intent per kind, one invoice per intent

- ✅ Simplest. No executor or adapter change. §6 untouched.
- ✅ **Exact** for proration.
- ❌ Boundary total diverges from legacy by a cent or two.
- ❌ The customer's statement changes: four card charges where there was one,
  two where there was one. That is the most visible thing in this document.

### B — one intent per kind, all collected onto ONE invoice  ← my recommendation

The executor collects a **group** of intents that belong to one billing event:
evaluate each predicate independently, refuse the whole group if any is
refused, claim each separately (so INV-008 is untouched), then one draft, one
item per intent, one finalize, one pay, with the cents apportioned from a
single rounding — the mechanism already merged in #154 for lines.

- ✅ Preserves §6's kind-per-intent **and** the customer's statement **and**
  the single rounding.
- ✅ Needs no DESIGN change and publishes no new rule.
- ❌ A real executor/adapter change: a group identity, group refusal
  semantics, and partial-failure handling (all claimed, all unresolved — the
  same shape as today's single-intent ambiguity, multiplied).
- ✅ The group identity is **resolved** — see below. It needs neither a
  canonical supersession nor a fragile string convention.

### C — add a recurring-bill kind to §6

- ✅ One intent, one invoice, one rounding, no executor change.
- ❌ A DESIGN change, and §6 is emphatic: a kind "arrives by being written
  here first, under a published rule you accepted." It also re-opens the
  question the kind exists to answer — which authorization rule applies to a
  charge that is four things at once.

### D — leave both legs on legacy

- ✅ Zero risk today. Everything else routes.
- ❌ These are the **largest** charges in the system. `legacyMoneyPaths`
  cannot reach 0 without them, and `legacyMoneyPaths: 0` is what
  `cmd/intent-executor` requires before it will start at all — so D means the
  intent rail never runs in production.

---

## What is already true regardless of the choice

- `proposer.Charge` carries lines and a wallet allocation (#154), so a single
  intent can express a multi-component charge whenever it is allowed to.
- `splitCents` apportions a single rounded total across lines by largest
  remainder, so items sum to the sealed charge by construction. Under **B**
  the same mechanism applies across intents.
- The executor has a work loop (#155), so a routed intent can actually be
  executed.
- The payer resolves (#153), so a routed intent can actually be collected.

## What I need

**Pick A, B, C or D.** That is the whole of it — the group-identity
sub-question below is answered and needs nothing from you.

---

# The group identity, resolved

I asked for a steer on this and then talked myself out of needing one. Writing
down why, so the answer is checkable rather than assumed.

## The question

Under B, several intents settle onto one invoice. Something durable has to say
which intents belong together, because the executor discovers work from the
store rather than from the leg that proposed it. Two options were on the table
and both looked bad:

- **derive it from the shared source reference** (`run:<id>#arrears`,
  `run:<id>#base`, …) — fragile, because a grouping inferred from a string
  convention breaks the first time somebody changes the convention, silently
  and in the direction of splitting one charge into several;
- **seal a group id into the intent** — a canonical supersession, free only
  while `charge_intents = 0` and expensive forever after.

## The answer: neither, because grouping is not part of the document

A `ChargeIntent` is what the customer owes and under what terms. **Which other
charges happen to share its invoice is not one of those things.** Two
customers owed identical amounts under identical terms hold identical
documents whether their charges were invoiced together or separately — and if
that were false, the digest would be attesting to something the customer never
agreed to.

So grouping is an **execution concern**, and it belongs where the other
execution concerns already live: beside `intent_settlement_claims`, not inside
the seal.

That is a side table — `intent_digest` to `group_id` — written when a leg
proposes. Concretely it means:

- **no canonical supersession**, so the change is not on the
  `charge_intents = 0` clock and can be revised later at ordinary cost;
- **no string convention** to break, because the grouping is stated rather
  than inferred;
- **the seal is unaffected**, so an intent's digest is identical whether it is
  invoiced alone or with three others — which is the property that makes the
  decision reversible;
- and a group that is never executed strands nothing: the rows are inert, and
  `PendingExecution` already refuses to hand out a claimed or terminal intent.

## What is left to build under B

The adapter half is merged (#160): `CollectGroup` settles a set of intents onto
one invoice, apportioned from one rounding, keyed on the sorted set of digests.

What follows once B is confirmed is the side table, the store read that
assembles a group, and the executor path that claims every intent in a group
before collecting and records every outcome after — all-or-nothing, with an
ambiguous pay leaving every claim in the group retained.

None of that needs a further decision from you.


---

# A second leg that cannot route, for a different reason

`internal/account/billing/unpaid.go` — the unpaid-invoice retry — was listed
as one of the four remaining legs. It is not blocked on a decision. It is
blocked on **not being a charge**.

## It hands the provider no integer

`internal/shared/stripe/client.go:599` is:

```go
params := &stripego.InvoicePayParams{}
```

No amount, no payment method, no idempotency key — deliberately, and the
comment says why: Stripe replays a saved response on an identical key for
~24h, *declines included*, so a deterministic key would replay the original
decline after the customer fixed their card, which is the exact retry this RPC
exists for.

So a retry is **not a new charge**. It re-attempts settlement of an obligation
the provider already holds, and the amount lives at Stripe. There is no
integer to seal, and sealing one would either duplicate Stripe's figure or
drift from it.

## Routing it through the adapter would create a SECOND obligation

`stripeadapter.Collect` creates a draft, adds items, finalizes and pays — a
**new** invoice. It has no operation that pays an existing one. So a
receivable intent collected the ordinary way would leave the original invoice
open *and* raise a second one for the same money.

## §6 already names the right model, and it does not fit legacy invoices

`KindCollectReceivable` is exactly this case: "retries an amount already owed,
under a one-time authorization against the sealed receipt or a standing one
after notice." And `ChargeIntent.CollectRemainderOf` implements it.

But `CollectRemainderOf` requires a **sealed source intent** — it refuses on
`!c.Sealed()` and binds `collects` to that intent's digest. The 15 invoices in
production came from the legacy legs and have no source intent, so there is
nothing for a receivable to collect the remainder *of*.

## The conclusion

🔴 **Unpaid retry is not a leg to route. It is a leg that becomes reachable
only after the others are routed** — once the intent rail itself raises
invoices, a failed one has a sealed source document and a receivable can name
it. Until then there is nothing to seal against.

It also needs an adapter operation that **pays an existing provider invoice**
rather than creating one, which does not exist today and is not the same seam
as `Collect`.

No decision is needed on this one. It should simply stop being counted among
the legs that can be cut over now, and `REMAINING-LEGS-PLAN.md`'s tally should
say three, not four.
