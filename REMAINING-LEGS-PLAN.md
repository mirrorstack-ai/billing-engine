# The three remaining legs — what they actually need

I told the owner three times that these could not be built because §6's closed
catalog names no funding kind. **That was wrong.** §6's subsection *"Funding and
collection are not extra service lines"* names all three, each with its
authority. They are blocked on building that authority, which is scoped work —
not on a product decision.

This is what each needs, checked against the code rather than asserted.

---

## `auto_topup` — the most machinery already exists

§6 requires "its own standing authorization, binding the balance trigger, amount
rule, provider and mandate, per-attempt, frequency and period ceilings, notice
channel and lead time, effective time, expiry, revocation, and pending-or-failed
treatment."

**Already in `internal/intent/authorization.go`:**

| §6 requirement | field |
|---|---|
| per-attempt ceiling | `PerChargeCeiling` |
| period ceiling | `PeriodCeiling` |
| effective time / expiry | `EffectiveFrom`, `ExpiresAt` |
| revocation | `RevokedAt`, `Revoke()` |
| the kinds it permits | `Kinds []ChargeKind` |
| binding to the accepted disclosure | `AcceptanceDigest` |

**Missing, and each is a new field plus a `Permits` clause:** balance trigger,
amount rule, provider/mandate binding, **frequency** ceiling (distinct from the
period ceiling — "at most N attempts per window", not "at most X micros"),
notice channel and lead time, pending-or-failed treatment.

⚠️ `Permits` is a pure total function returning refusals together. Every new
bound must return its own named refusal, or a failure will report as a
neighbouring one. Follow `RefusalOverPerCharge` / `RefusalOverPeriod`.

🔴 **The collection-authority hazard is here, not in the ceilings.** SECURITY.md
records that four ordinary read and ingest paths can reach auto-top-up, and §6
says plainly: "Turning on general billing must never turn on auto top-up. A
balance read, a status read, a usage ingest, an infrastructure sync or a
provider callback must not collect money while you wait. Each may append a
trigger fact, and no more." The existing default-deny grant in
`internal/account/credit/coordinator.go` is the control; the cutover must not
widen it. `OutOfCredits` still holds a grant because removing it deadlocks — see
the countdown in `internal/architecture`.

**Wiring cost:** the executor is installed by a duplicated inline closure in
**six binaries**. One seam, six installation sites — unlike `cycle.Service`,
which has one constructor.

---

## `collect_receivable` — needs a LINK primitive, not supersede

§6: "one-time authorization against the sealed receipt, or a standing
authorization after notice and the wait", funded by "a linked intent for the
remaining amount only, under a new `FundingPlan` and a source-capacity
reservation."

`ChargeIntent` has `supersedes` (INV-003) — but **supersede is the wrong
relation**. Superseding replaces a document; a receivable *links* to one and
collects only its remainder. Both intents stay live, with a stated arithmetic
relation between them. Building this on `supersedes` would make the original
look replaced when it is still owed.

**What to build:** a distinct `collects` link, the remainder arithmetic
(`collectionGrossObligation = sourceRemainingCollectibleReserved`, §6), and the
source-capacity reservation so two receivables cannot both claim the same
remainder.

🔴 **This is also why the legacy unpaid-retry leg cannot simply be wrapped.** It
re-pays an invoice that already exists at the provider, so the amount comes from
the provider rather than our derivation — the inversion INV-001 exists to
prevent. The receivable intent must derive the remainder itself and reserve
against it; the provider object is evidence, not the source of the figure.

---

## `credit_purchase` — needs the disclosure mechanism

§6: "your acceptance of engine-signed disclosure bytes naming currency, amount,
credit received, restrictions, expiry, refund terms, rail and intent digest."
Funding: `walletFunding = 0; providerRemainder = grossObligation`.

`AcceptanceDigest` exists on the authorization and is documented as binding it
to "the disclosure the customer was shown" — but **the disclosure bytes
themselves are not built** in `internal/intent`. That is the gap: an engine-side
canonical encoding of exactly those nine fields, signed, whose digest is what
the customer accepts.

Two further blockers specific to this leg:

1. **It is not terminal.** The cycle legs propose and stop. This one's RPC
   response must carry a Stripe client secret that only exists *after* the
   finalize (`creditpurchase/executor.go:271`). A proposing version returns a
   response the browser cannot use — so cutting it over changes a synchronous
   customer-facing contract, which no shipped leg has had to do.
2. **The typing rule.** §6: a settled stored-value lot is a *funding source*,
   allocated after the obligation is calculated. It must not reduce the taxable
   basis or change `grossObligation`. Every credit kind declares exactly one
   semantic class, `rating_credit` or `stored_value`, and the same lot must
   never appear in both — the double-spend named in SECURITY.md's gap register.

---

## Order

`auto_topup` first — most machinery exists, and its authorization work is
reusable. Then `collect_receivable`, whose link primitive is self-contained.
`credit_purchase` last: it needs the disclosure mechanism *and* changes a
synchronous customer contract.

**None of them moves `legacy_money_paths`.** That falls only on deletion — see
`LEGACY-DROP-PLAN.md`.
