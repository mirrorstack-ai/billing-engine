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

### Cutover design, derived 2026-08-30 — start here

**Seam placement, settled.** The branch goes in `resume`
(`autotopup/executor.go:~386`) immediately after the `current.Status != "pending"`
check and **BEFORE `recoverOrCreateInvoice`**, gated on
`attempt.StripeInvoiceID == ""`.

- `recoverOrCreateInvoice` is what arms the provider. Past it an invoice
  exists at Stripe, and the intent path holds no write port to finalize or
  void one — so proposing beside it strands a live provider object for as
  long as the account exists.
- The attempt row is already a durable claim (`Trigger` creates it pending
  before anything reaches a provider), so proposing there still happens
  AFTER a claim and keeps the crash-recovery guard.
- `StripeInvoiceID != ""` is the direct refutation of "nothing has been
  collected for this attempt". The rail that started the charge finishes it.

This is the boundary leg's expensive lesson (it sealed a second obligation
over an invoice another process had collected) applied without repeating it.

**✅ STEP 1 IS DONE — measured 2026-08-30, and the answer is good.**

Every `credit_ledger` status comparison in the codebase is POSITIVE:
`= 'settled'` (wallet balance, source allocation, draw recovery, app bill),
`= 'pending'` and `= 'failed'` (auto-top-up resume and retry). There is **no**
`<>`, `!=` or `NOT IN` on ledger status anywhere — SQL, Go, or the migration's
partial indexes. The one `status <>` in the neighbourhood is
`run.status <> 'invoiced'` on **billing_runs**, a different table.

So adding `'proposed'` to `credit_ledger` is **additive and safe**: no existing
reader can accidentally include it, because every reader names the statuses it
wants.

That is the OPPOSITE of the boundary leg, where `status <> 'invoiced'` meant a
new value silently joined the "in flight" set and produced the org-deletion
deadlock a reviewer caught. The difference is entirely down to positive vs
negative filtering — see [[filters_that_were_noops_become_load_bearing]].

Two consequences:
- The resume/retry paths (`status = 'pending'` / `= 'failed'`) will SKIP a
  proposed row, which is exactly right: a proposed attempt must not be resumed
  by the legacy rail.
- The wallet balance (`= 'settled'`) ignores it, which is also right: a
  proposed top-up has granted no credit.

**What remains genuinely bigger than the cycle legs.** An auto-top-up attempt is a
`credit_ledger` row, and `autotopup/store.go` goes through **sqlc**
(`db.Queries`). So a `proposed` status is a change to the SHARED credit
ledger's status vocabulary, not to a leg-private table — every path that reads
`credit_ledger.status` is in the blast radius, and the change needs sqlc
regeneration ([[sqlc_stub_is_a_claim_not_a_schema]]: an invented column
type-checks and 42703s live).

**Do not start this at the end of a session.** Order:
1. Establish which paths read `credit_ledger.status` and what a new value does
   to each. This is the real work.
2. Migration adding the status, with the CHECK widened.
3. sqlc query + regeneration for the terminal marker.
4. The seam, using the placement above.
5. `microsToCentsRoundHalfUp` already exists in the package — use it, do not
   add a second rounding helper.

**Also settled:** the arming flag must be deliberate, like
`BILLING_CYCLE_INTENT_CUTOVER`. A proposing auto-top-up collects nothing, which
for THIS leg means wallets never refill and blocked accounts stay blocked — a
harder consequence than the cycle legs' revenue stop.

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

### 🔴 The receivable leg is DOWNSTREAM of the others — verified 2026-08-30

`billing/unpaid.go` retries an invoice identified by `target.StripeInvoiceID`
(`unpaid.go:222`) — a Stripe invoice the LEGACY rail created. There is no
intent behind it.

`collect_receivable` is defined by `CollectRemainderOf(source)`: it links to a
source intent and collects what is left of it. **You cannot collect the
remainder of an intent that does not exist.** Every unpaid invoice in the
system today predates the intent rail, so there is nothing for a receivable to
link to.

So the order is forced, and it is not a matter of effort:

1. other legs cut over AND **enabled**, so intents exist and settle;
2. an unpaid invoice arises from one of those intents;
3. THEN a receivable can link to it and collect the remainder.

**This ties clause 2's completion to the production-access question.** Enabling
any leg requires shadow reconciliation coming back explained (§11 puts it before
cutover), and shadow reconciliation requires reading production billing history.
So "cut over the unpaid-retry leg" is downstream of the read-only DB role
decision, exactly as the legacy drop is.

The primitives are built and proved (`collects` link, source-capacity
reservation, funding formula) — what is missing is a source intent to point
them at.

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
