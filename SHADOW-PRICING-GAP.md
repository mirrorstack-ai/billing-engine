# Why the shadow reconciliation cannot price production

Measured 2026-08-31 against production, via the intent-shadow ops Lambda.

## The state

`{"action":"shadow"}` refuses to run:

    the price catalog is empty; a shadow run against it would quarantine
    every period and report a clean sheet of nothing

`{"action":"census"}` says production is **not** empty:

    usage_events           38326      invoices                15
    usage_aggregates          12      accounts                 6
    billing_periods            6      apps                     8
    billing_runs               1      credit_ledger            0
    metric_definitions        25      charge_intents           0
    metric_version_prices      0      billing_authorizations   0

So DESIGN §11's gate is **UNMET, not vacuous**. History exists and the rater
cannot price it.

🔴 The earlier reading — "production has no billing activity" — was inferred
from the seven legacy-drop preconditions all returning zero. That inference was
wrong. **The preconditions measure IN-FLIGHT state, never history.** Zero open
invoices is entirely consistent with 15 invoices in other statuses.

## Why: the rater reads one catalog, the legacy path reads three

`cycle.Store.MetricPriceMicros` (`internal/account/cycle/store.go:102-120`)
resolves in three tiers:

| tier | source | when |
|---|---|---|
| 1 | `metric_version_prices` | `module_version != ''` — a hit wins outright |
| 2 | `metric_model_prices` | `model != ''`, falling back to the catalog row |
| 3 | `metric_definitions` | no version snapshot and no model |

`shadow.Source.PriceBookFor` (`internal/intent/shadow/source.go:161-196`) reads
**tier 1 only**, and tier 1 has zero rows. Every fact goes unpriced, every
period quarantines, and the guard — correctly — refuses rather than reporting a
clean sheet of nothing.

## Three separate defects, of increasing depth

**1. `PriceBookFor` reads one tier.** A query change.

**2. `intent.PriceKey` has no `Model` dimension.** It is
`{Meter, Module, ModuleVersion}` (`internal/intent/pricebook.go:18-22`), so
tier-2 per-model prices (migration 018's AI model prices) **cannot be expressed
in the intent price book at all**. If production's usage priced through tier 2,
this is a schema change to a core intent primitive, not a query fix.

**3. `FactsFor` does not read `model`.** It selects
`metric, module_id, module_version, billable_quantity`
(`source.go:113-117`). So even with a `Model` dimension on `PriceKey`, the
facts would not carry the value to populate it. Two changes, not one.

## 🔴 The decision that is NOT mine

Facts carry a **non-empty** `module_version`. A tier-3 catalog price keyed
`{meter, module, ""}` therefore never matches an exact lookup. Reproducing the
legacy resolution requires **fallback semantics** in
`PriceBookRevision.UnitPriceMicros`: try the exact key, then the version-blind
one.

That is a change to how the engine derives a financial field, and it weakens
the property the price book exists to provide:

- **INV-001** — the engine derives every financial field.
- **INV-002** — one derivation.

A book whose lookup falls back has *two* resolution paths for the same meter,
and which one applied is not visible in the key. The immutability argument in
migration 044 — that a later version's re-price must never retroactively
re-bill earlier usage — is exactly what tier 1 exists to guarantee, and a
fallback to the MUTABLE `metric_definitions` catalog reintroduces the
anachronism for any usage tier 1 cannot cover.

There is also a real question whether pricing 2026 usage from today's mutable
catalog produces differences that are *findings* or merely *noise*. §11 says
"reconcile until every difference is explained. Never tune the rater to hide an
unexplained difference" — it does not say what to do when the rater cannot
reach the historical price at all.

**So this needs an owner decision, not an implementation.** The options:

| option | cost | what it gives up |
|---|---|---|
| A. Fallback lookup in `UnitPriceMicros` | small | INV-002's single derivation; anachronistic prices for tier-2/3 usage |
| B. Backfill `metric_version_prices` from history | medium | nothing in the model, but invents snapshots that were never taken |
| C. Reconcile only tier-1 usage, quarantine the rest explicitly | small | coverage — §11 closes over a subset, and that must be stated |
| D. Add `Model` to `PriceKey` + `FactsFor`, then A or C | large | touches the sealed digest's key space |

## ✅ MEASURED 2026-08-31 — the counts came back, and two of this file's claims were WRONG

    metric_model_prices             26     events_without_account            50
    aggregates_with_module_version   0     events_inside_a_rolled_period   6937
    aggregates_with_model            6     periods_with_aggregates            2
    aggregates_priced_nonzero       10

**🔴 Correction 1 — the fallback is NOT needed, and INV-002 need not be
weakened.** This file claimed facts carry a non-empty `module_version`, so a
tier-3 catalog price keyed `{meter, module, ""}` would never match an exact
lookup. **Wrong.** `aggregates_with_module_version` is **0** — no aggregate
carries one. `FactsFor` therefore emits `PriceKey{meter, module, ""}` for every
row, and a tier-3 catalog entry keyed the same way matches EXACTLY.

So **option A is unnecessary**. `UnitPriceMicros` keeps single-path resolution,
and INV-002 survives intact. What was framed as the hard decision does not
arise.

**🔴 Correction 2 — tier 2 is REQUIRED, not contingent.**
`aggregates_with_model` is **6 of 12**. Half the priced usage resolved through
`metric_model_prices`, which `intent.PriceKey` cannot express. **Option D is
mandatory for that half**, not an option to be avoided if the count came back
zero. It came back non-zero.

Tier 1 is dead weight: it has never priced anything, because no aggregate
carries a version. That is consistent with no module version ever having been
published.

**🔴 Correction 3 — and this one reverses Correction 1's conclusion.**

Correction 1 said a tier-3 entry keyed `{meter, module, ""}` would match
exactly, so no fallback was needed. The MATCHING claim is true. The
CONSTRUCTION claim is false: `NewPriceBookRevision` rejects any key with an
empty `ModuleVersion` —

    ErrPriceKeyIncomplete = "intent: price book key is missing a
                             meter, module or version"

Verified by probe against the real type, not by reading:

    version-blind key -> err = intent: price book key is missing a
                               meter, module or version: mod@/m

So the key that would match **cannot be put in a price book at all**. That
validation is deliberate — the type's own doc says a book "with holes needs a
fallback, and a fallback is a second pricing implementation, the thing INV-002
exists to forbid" — and it requires a version because reproducible pricing is
the whole claim.

**The real shape of the gap:** production's usage is entirely version-blind
(`aggregates_with_module_version` = 0, because no module version has ever
published), and the intent price book requires a version on every key.
**The intent engine cannot rate production usage as designed** — not for want
of a query, but because the price book's completeness invariant is
incompatible with version-blind usage.

Closing §11 therefore needs BOTH:

1. **Relax the version requirement** so version-blind usage can be priced at
   all — this weakens "complete and effective-dated", the property the book
   exists to provide; and
2. **Add `Model` to `PriceKey`** and carry it in `FactsFor` — mandatory,
   because 6 of 12 aggregates resolved through tier 2.

Both touch the invariant, so this is an owner decision after all. My instinct
to ask was right; my stated REASON for asking was wrong twice before landing
here. The safe option remains:

- **C** — rate only version-stamped usage and quarantine the rest explicitly.
  Today that reconciles **nothing**, which is honest and closes §11 over an
  empty subset rather than by weakening a guarantee.

Nothing here is safe to decide by inference. This file has now been wrong
three times, each time because a claim was reasoned rather than measured.

## What the four new census counts decided

`metric_model_prices`, `aggregates_with_module_version`,
`aggregates_with_model`, `aggregates_priced_nonzero`.

- If `aggregates_with_model` is **0**, tier 2 is moot and option D is unneeded.
- If `aggregates_with_module_version` is **12** (all of them) and
  `metric_version_prices` is 0, then tier 1 was *attempted* and missed for
  every row — which is what makes B tempting and C honest.
- If `aggregates_priced_nonzero` is **0**, then nothing was ever actually
  priced and the 15 invoices came from somewhere other than usage — which
  would change the whole picture again.

Measure before choosing. Every inference in this file that was not measured has
been wrong once already.
