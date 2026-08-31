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
- ❓ The group identity needs deciding. The components already share a source
  reference (`run:<id>#arrears`, `run:<id>#base`, …), so it could be derived —
  but deriving a group from a string prefix is fragile, and sealing a group id
  is another canonical supersession.

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

**Pick A, B, C or D.** If B, I also need a steer on group identity: derive it
from the shared source reference, or seal it (another canonical supersession,
which is free while `charge_intents = 0` and expensive afterwards).
