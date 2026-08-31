# The funding split belongs in the SEALED document

Adversarial check, 2026-08-31: **4 of 4 lenses refuted** the recommendation to make
funding an execution-time port. This file is the corrected answer.

> I told the owner funding was execution-time, citing DESIGN.md:1207. That line is an
> *arithmetic ordering* rule under the heading "The typing rule", not a lifecycle claim.
> Four passages state the phase and all say the opposite. My own EXECUTOR-GATE-PLAN.md
> (T0.4: "persist and digest the FundingPlan") was RIGHT; my correction to it was wrong.

---

**1. VERDICT**

The port does not stand. Do not add a `FundingAllocator` to the executor — seal the wallet/provider split into the charge intent document itself, and make `ClauseFundingPlanBalances` compare a persisted split against a persisted total.

---

**2. WHY THE PORT FAILS, AND WHAT REPLACES IT**

Two independent kills.

*(a) It sits on the wrong side of the seal.* The brief's constraint reads `docs/DESIGN.md:1207` as a lifecycle claim. It is not — that paragraph is headed "The typing rule" and its own next sentence is "It must not reduce the taxable basis, add a second negative line, or change `grossObligation`." It is an *arithmetic ordering* rule (compute the obligation, then pick the source) written against stored-value double-spend. Every passage that states the *phase* says the opposite of execution-time:

- `docs/DESIGN.md:205-206`, under the INV-001 "What it may never send" list: "**A funding split.** Which part comes from wallet and which from a card is a `FundingPlan` (unbuilt) the engine freezes **before you are shown anything**."
- `docs/DESIGN.md:470`: "Every intent freezes one provider-neutral `FundingPlan` **before disclosure**."
- `docs/DESIGN.md:1281`: "**Before an intent seals**, the engine freezes the total, the `FundingPlan`, the rail, the merchant-account policy and the routing-policy digest."
- `internal/intent/predicate/state.go:91-93` already encodes it: "The plan is frozen before disclosure, so the split the customer saw is the split that settles."

A port evaluated inside `Execute()` produces the split after sealing, after disclosure, after acceptance, after the notice wait. `executor.go:373`'s `Frozen: true` is then a *second* declared-but-unimplemented defect nested inside the first — a flag asserting a pre-disclosure freeze performed post-disclosure. The port preserves that lie and gives it an interface.

*(b) It does not remove the vacuity anyway.* The vacuity is not "one component computes and verifies." It is "the plan is a pure function of `sealed`, evaluated in the same call." With `fundingFor` as the default implementation behind the interface, `predicate.go:167` still compares `s.Funding.GrossMicros != s.Intent.TotalMicros()` where both sides trace to the same `sealed` loaded at `executor.go:197` and passed at `executor.go:220`. Injection changes who calls f, not that the clause checks `f(x).Gross == x.Total`. The proposal concedes it — "non-vacuous **the moment any real allocator exists**" — which is already true today without the port. `SECURITY.md` judges the tree as it stands; after the refactor it is 29 advertised, 28 verified, plus an interface asserting 29.

The `provenance` enum makes it worse, not better. `derived | allocated` is self-asserted by the component whose honesty is the question — identical in shape to `Frozen: true` today. Its refusal rule (`Provenance == Derived && Wallet != 0`) has a constant-false second conjunct, because `WalletAllocationMicros: 0` is a literal at `executor.go:375`. It is a vacuous guard added to repair a vacuous guard. This codebase has fixed exactly this shape twice, and never with a flag: `ClauseNoticeElapsed` (`predicate.go:120-141`) was hollow until it started checking the asserted instant against `s.Authorization.NoticeLeadTime()`; `ClauseInstrumentBinding` (`predicate.go:233-244`) still is hollow on its own and is rescued by `s.Authorization.InstrumentBound()`. In both, the fix was a **second, independently-readable half** — never a label.

**THE DESIGN**

Copy what tax already does. Tax is this identical problem solved in-tree with no port: computed elsewhere, carried as data on `proposer.Charge`, sealed into the digest (`chargeintent.go:475-478`), then checked by the predicate in two halves — the sealed flag plus `TaxIndependentlyReproducible` (`predicate.go:172-183`).

1. **Schema.** `migrations/billing/054_intent_core.up.sql:26-78` has `subtotal_micros` and `total_micros` and no funding column at all. New migration `061` (next free — 053 is a known gap, 055-060 are taken) adds `wallet_allocation_micros` and `provider_remainder_micros` to `ms_billing.charge_intents`, with `CHECK (>= 0)` on both and `CHECK (wallet_allocation_micros + provider_remainder_micros = total_micros)`. That is the balance arithmetic enforced by an evaluator that is not the Go process — the same move migration 060 made for the tax verification class.
2. **Document.** Add the two fields to `intent.Draft` (`chargeintent.go:142`), encode them in `computeDigest` (`chargeintent.go:456-493` — which today covers **no** funding at all, so the disclosure the customer accepts does not name the split), and bump `canonicalSchema` to `/v3` (`canonical.go:26`).
3. **Seal-time rule.** `Seal` refuses a draft where `wallet + provider != total`, and enforces §6's `walletFunding = 0` for `credit_purchase` / `auto_topup` **where the kind already lives**. Today that rule is a comment with no code under it (`executor.go:355-359`). Move `fundingFor`'s by-kind selection into the seal path: an unknown `ChargeKind` then refuses to *seal* rather than refusing to *execute* — strictly earlier and cheaper.
4. **Executor.** `fundingFor(sealed)` becomes a pure projection reading the two sealed integers off the loaded row. `Frozen: true` stops being a self-assertion and starts meaning "this came off a sealed, digest-verified document."
5. **The load-bearing fix.** `executor.go:260` hands the adapter `AmountMicros: sealed.TotalMicros()`. `docs/DESIGN.md:1284` requires the sealed `providerRemainder` — "never `grossObligation`, and never wallet funding." This is a live latent defect, invisible today only because the stub makes the two equal.

**THE EXACT FAILURE MODE THAT BECOMES REACHABLE**

Seal a `module_usage` intent, total 20,000,000 micros, split wallet 6,000,000 / provider 14,000,000. Call `Execute`. Assert the `Debit` handed to the `Collector` carries `AmountMicros: 14000000`.

Against the code as written that test fails: the adapter is handed 20,000,000, the wallet is drawn for 6,000,000, and the customer is charged **26,000,000 of value for a 20,000,000 obligation**. That is a customer double-charge, it is one line from live, and today no test in the tree can express it because `fundingFor` makes wallet always zero. The second reachable false is drift: a row written by proposer build B1 whose `provider_remainder` no longer sums to a `total_micros` written by B2 — `predicate.go:170` refuses, where today `x + 0 == x` for every int64.

**ONE MORE, INDEPENDENT OF ALL OF THIS.** `predicate.go:43-49` skips the six `providerClauses` when `ProviderRemainderMicros == 0`. The wallet-side clauses that must then carry the weight — `SourceAllocation`, `CreditLotsReserved`, `ExposureReservation`, `FundingMatchesAccepted` (`predicate.go:224-231`) — are all bare `s.Unbuilt.X` booleans supplied by the same composition root. So the funding value *selects which arm of the conjunction runs*, and the arm it can select into is guarded only by caller-asserted booleans. `fundingFor`'s hardcoded `Provider: sealed.TotalMicros()` is therefore not merely vacuous — it is load-bearing as a fail-closed default. Add a guard: while `Unbuilt.CreditLotsReserved` / `SourceAllocation` are false, a zero provider remainder must **refuse**, not skip. "Skip the provider arm" must never come to mean "skip to a wallet arm nobody built." One line, do it regardless of everything above.

---

**3. NOW vs. WHEN A REAL ALLOCATOR EXISTS**

NOW, before the first sealed intent:

- Migration 061 + `Draft` fields + `computeDigest` + `canonicalSchema` v3. `canonical.go:22-25` states the window in as many words: "Safe to change exactly now — production holds ZERO sealed intents (`charge_intents = 0`, measured 2026-08-31). After the first, this becomes a migration of live sealed documents past the INV-003 reject-sealed-update trigger." Commit `afed4a3` spent that window today on the tax verification class. It closes on the first seal.
- Move §6 kind-selection from `executor.go:348-370` into `Seal`. `fundingFor`'s own comment argues for doing it now: "The three formulas coincide TODAY... Writing the selection now, while they agree, is the only time it can be done without a migration of live documents."
- Fix `executor.go:260` to `funding.ProviderRemainderMicros`, plus the 6/14/20 test above.
- The zero-provider-remainder refusal guard.
- Every intent seals `(wallet: 0, provider: total)` until a wallet rail exists. That is honest — it is a real derived split that happens to be trivial, not a synthesized tautology — and `ClauseFundingPlanBalances` is non-vacuous from day one because the operands are separated by a durable write and by time.

WAITS:

- The wallet allocator itself, and lot reservation. When it lands, its home is a pre-disclosure funding step that owns lot reservations (`DESIGN.md:470-473`), and its output is data the **proposer** seals. The executor never authors it.
- If a port is still wanted then, the only admissible shape mirrors `Collector` and `TaxResolver`: it runs at proposal time, never inside `Execute`; its output is sealed and re-read so the predicate never consumes the return value directly; and it gets a `ClauseFundingIndependentlyReproducible` counterpart, exactly as `ClauseTaxFinal` admits `TaxResolver`'s figure only alongside a reproduction. Anything less would be the first money-returning port in this engine with no INV-002 counterweight — and `Collector`'s own doc (`executor.go:30-36`) refuses even a two-method port because "assembling is where an amount stops being the one that was sealed." A split *is* assembly.
- `Unbuilt.CreditLotsReserved` and `Unbuilt.SourceAllocation` stay exactly where they are and are not touched. They ask whether *this customer's lots are really held*; `ClauseFundingPlanBalances` asks whether *the sealed arithmetic balances*. The reservation is precisely what lets a split frozen at disclosure still be performable at execution — they are complements, not competitors.

---

**4. OWNER DECISIONS, NOT ENGINEERING ONES**

1. **Spend the canonical window now?** Sealing the split forces `canonicalSchema` v3. Doing it after the first sealed intent means migrating live sealed documents past the INV-003 reject-sealed-update trigger. This is a "spend the last cheap moment" call with a hard deadline the engineer cannot move. My recommendation is yes, but it is the owner's to make.
2. **Does the customer-facing disclosure name the split?** `DESIGN.md:206` says the split is frozen "before you are shown anything," and `ClauseFundingMatchesAccepted` implies the authorization must carry funding terms to compare against — `authorization.go:503-542` exposes scope, terms, provider, mandate and lead time, and no funding mode, split or cap. Whether "you will pay $6 from wallet and $14 on your card" is part of what the customer accepts is a product and legal statement about the disclosure, not an implementation detail.
3. **`internal/account/cycle/overage.go:408-418` already draws a wallet allocation entirely outside the engine**, flag-gated on `CreditBillingModeCredits`. That is a second derivation of the funding split, in the file `internal/architecture/predicate_single_caller.go` and `predicate/clause.go:8-14` both name as the counter-example this design is written against. Either it is an INV-002 violation to be killed at cutover, or it is a sanctioned pre-engine path with an agreed end date. Somebody must say which — the engineer cannot decide to delete a live credits-mode billing path.
4. **Is a wallet rail in scope for the first executor cutover at all?** If no, everything above still lands and the split is trivially `(0, total)` — cheap, honest, and the clause is real. If yes, sequencing the allocator against the cutover is a scope call.

Incidental: `internal/account/cycle/overage_cutover_wallet_test.go:29-30` cites "executor.go:321-325" for the hardcoded wallet zero; it is now `executor.go:372-377`. That test is the designated tripwire for a wallet rail landing, so its citation should track.
