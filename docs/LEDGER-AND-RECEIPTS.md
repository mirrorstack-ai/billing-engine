# Ledger, receipts, and provider cash-flow trace

This document defines the durable evidence behind the intent-only design in
[`DESIGN.md`](DESIGN.md).

> **Status: proposed, not implemented.** The current engine has useful frozen
> attempts, an invoice mirror, and an append-only credit ledger, but no single
> provider-neutral intent/attempt/ledger/receipt chain. Stripe invoice rows are
> currently carrying responsibilities that this design separates.

The central rule is:

> **Our ledger states the monetary obligation. Provider evidence proves what an
> external rail did. Neither silently rewrites the other.**

---

## 1. Four different records

| record | answers | may move money? |
|---|---|---|
| `ChargeIntent` | what exact effect was proposed and permitted? | no |
| `PaymentAttempt` | what did one provider operation try to do? | through the isolated executor only |
| `LedgerTransaction` | what monetary state did MirrorStack commit? | records the effect; does not call a provider |
| `ProviderEvidence` | what does the provider report happened? | read-only |

A provider invoice is not a `ChargeIntent`. A successful callback is not a
ledger entry. An internal ledger row does not prove the provider received cash.
A complete `ChargeReceipt` connects all four.

---

## 2. Append-only ledger contract

Every committed monetary transition has:

- a globally unique transaction id,
- a typed operation and schema version,
- payer/account and currency,
- intent and payment-attempt references where applicable,
- one or more source/reference ids,
- entries whose signed amounts balance to zero within one currency,
- exact creation and effective times,
- engine build and policy digests,
- a deterministic idempotency key, and
- a link to any reversal, refund, dispute, or correction chain.

Posted transactions are never updated or deleted to correct money. A correction
is a new transaction that explicitly references what it reverses or adjusts.
Derived balance/cache rows may be rebuilt and are never the audit source.

The chart of accounts, revenue-recognition time, tax-liability accounts, and
legal retention period are finance/accounting decisions. This document does not
infer them from today's table names. Whatever chart is accepted must retain the
balanced, append-only, per-currency invariants above.

### No implicit cross-currency entry

Each transaction balances in one named currency. Currency conversion is a
separate, disclosed operation referencing a versioned FX rule and the two linked
currency transactions. An adapter never converts an authorized total silently.

### No generic positive adjustment

Administrative tools may issue a typed customer credit or reverse a known
incorrect debit. They cannot post an arbitrary new customer debit. A positive
customer obligation follows the normal intent, notice, authorization, and
execution lifecycle.

---

## 3. Operational transaction families

The exact accounting entries remain finance-owned, but the engine must represent
these operational families explicitly:

| family | required source | customer consequence |
|---|---|---|
| receivable/obligation | one sealed intent | amount becomes due under the documented collection terms |
| external payment settlement | one successful attempt + verified provider evidence | cash collected; intent can settle once |
| wallet/credit purchase | customer-authorized purchase attempt | cash becomes customer credit/liability under accepted terms |
| wallet/credit application | exact intent + source credit lots | credit reduces that intent; source lots remain traceable |
| grant/goodwill credit | typed authorized issuer and reason | customer balance increases without external payment |
| refund | settled transaction + authorized refund intent | cash/credit returned; original history remains |
| reversal/void | known unsettled/incorrect operation | neutralizes a specific earlier record |
| dispute/chargeback | verified provider dispute evidence | disputed cash state recorded without rewriting settlement |
| tax adjustment | original tax determination + replacement rule/evidence | explicit correction; never silent mutation |
| write-off | accepted finance policy + actor/reason | receivable treatment changes; customer history remains visible |

Late usage does not reopen a settled intent. It produces a separately disclosed
adjustment intent or a credit, according to the accepted late-event policy.

---

## 4. Payment attempts

One intent may have multiple attempts over time, but a unique settlement claim
permits at most one successful debit across all providers.

Suggested attempt states are:

```text
created
  ├─ customer_action_required ─► submitted
  ├─ submitted
  └─ provider_pending
        ├─ succeeded
        ├─ failed
        ├─ canceled / voided
        └─ outcome_unknown
```

Transitions are appended as events. An `outcome_unknown` attempt blocks another
attempt until read-only provider reconciliation proves absence or an operator
records a reviewed resolution.

The attempt freezes:

- intent id and digest,
- provider, merchant account, and adapter version,
- payer and authorization/mandate reference,
- currency and provider minor-unit total,
- deterministic provider operation reference,
- provider capability version used to allow the operation, and
- creation/expiry times and selected customer-action flow.

Retries do not automatically switch provider. A rail switch creates a new
attempt only after the prior rail is proven not to have settled and the
authorization permits the new rail.

---

## 5. Provider evidence

Each adapter has a read-only `PaymentReader`. It may query or verify provider
objects but has no collection/refund credential in processes that only trace or
reconcile.

An evidence snapshot contains:

- provider and merchant-account identity,
- adapter and provider API/schema version,
- external object type and opaque id,
- normalized status, amount, currency, and payer binding,
- parent/child edges to related provider objects,
- provider event/callback identity when applicable,
- observation time and retrieval method,
- canonical payload digest, and
- verification result for signature/authenticity and expected intent fields.

Sensitive raw provider payloads are encrypted and access-controlled. Customer
exports contain the normalized evidence and stable hashes needed for validation,
not reusable credentials or unrelated personal data.

### Stripe trace

The Stripe adapter must be able to walk from a `PaymentAttempt` to the applicable
Stripe payment/invoice objects, their successful or failed payment evidence,
balance movement, payout/settlement evidence where the account exposes it,
refunds, and disputes. The exact object graph is adapter-versioned because Stripe
APIs evolve.

The trace answers both directions:

- given a MirrorStack receipt, which Stripe objects and cash movements support
  it? and
- given a Stripe object/event, which one intent, attempt, ledger transaction,
  and customer receipt own it?

Every relationship is verified by provider account, customer/payer binding,
amount, currency, deterministic operation reference, and stored metadata. A
matching text description is never sufficient.

### NewebPay trace

The NewebPay adapter will normalize the order/payment, authenticated server
callback, customer return, settlement/batch, refund, and reversal evidence that
the contracted APIs actually expose. This design intentionally makes no claim
about a NewebPay feature until the merchant agreement, official integration
specification, and adapter conformance tests establish it.

A return-page request alone never proves payment. The adapter's authenticated
server evidence and/or authoritative read API must reconcile the known attempt.

### Evidence is not authority to invent money

If a provider reports an amount, payer, currency, or status that disagrees with
the attempt, the engine records a reconciliation incident. It does not change
the intent or ledger to make the mismatch disappear.

---

## 6. Cash-flow trace API

`TracePayment` is customer-read-only and returns a normalized graph:

```text
BillingAuthorization
        │
ChargeIntent ── NoticeReceipt
        │
PaymentAttempt
        │
provider order/invoice
        │
payment transaction
   ┌────┼───────────┐
balance movement  refund/reversal  dispute
        │
payout/settlement batch (when provider exposes it)
        │
LedgerTransaction ── ChargeReceipt
```

Each node is labelled `recorded`, `provider_verified`, `pending`, `unsupported`,
or `mismatch`. Unsupported evidence is different from absent evidence.

The trace is safe to call repeatedly. It may append new observations but cannot
retry payment, finalize an invoice, issue a refund, trigger auto top-up, mutate a
budget, or change an intent. Read paths are incapable of provider writes by
interface and deployed credential.

This is a critical migration requirement: current `main` contains a status-read
path that can synchronously trigger auto top-up. The intent-only deployment may
not call itself read-only until query and reconciliation binaries cannot compile
against a payment-write port.

---

## 7. Charge receipt and verification bundle

A `ChargeReceipt` is created only after the relevant ledger transition commits.
It contains or references:

1. canonical `ChargeIntent` bytes and digest;
2. every rating source id/hash and module billing-manifest version;
3. exact formula, integer scale, rounding, subtotal, credit, tax, and total;
4. terms, price-book, tax, notice, and rail-routing policy digests;
5. `BillingAuthorization`, evaluated ceilings, and revocation check time;
6. exact notice content digest, channel, delivery evidence, and wait interval;
7. engine Git SHA, artifact digest, receipt schema, and build provenance;
8. `PaymentAttempt` transitions and normalized provider evidence;
9. balanced ledger transaction ids and entries; and
10. correction/refund/dispute links, if any.

The bundle has a canonical encoding and its own digest. The public verifier
recomputes the rating and structural invariants offline:

```text
billing-verify verify charge-bundle.json
```

Provider live status is optional verification evidence and requires network
access plus customer authority. Offline arithmetic and policy verification do
not.

---

## 8. Reconciliation rules

Reconciliation is continuous but non-authoritative:

1. authenticate a callback or query through the provider adapter;
2. resolve exactly one known attempt by deterministic reference;
3. verify merchant account, payer, currency, amount, and operation kind;
4. append the evidence snapshot;
5. compare it to attempt and ledger state;
6. append the one allowed state transition, or open an incident; and
7. never originate a new debit from an unmatched event.

Duplicate/reordered callbacks are absorbed by unique provider event ids and
monotone transition rules. A callback that arrives before local commit is held
for bounded reconciliation; it is not attached to a similar-looking customer.

When provider evidence proves money moved but local ledger commit failed, the
engine recovers the exact frozen attempt into the ledger. It never calls the
provider again to make local state easier.

---

## 9. Verification requirements

Before this design can be reported as deployed, tests must demonstrate:

- every ledger transaction balances per currency;
- a settled intent cannot settle through a second provider;
- an unknown provider outcome prevents retry until reconciled;
- provider callbacks cannot create attempts or change amounts;
- a read/trace/status call cannot reach any write interface;
- provider evidence mismatches create incidents rather than ledger mutation;
- corrections are linked append-only entries;
- receipt verification reproduces every cent/minor unit;
- sensitive provider fields are absent from exports; and
- Stripe and NewebPay adapters pass the same provider-neutral conformance suite,
  with unsupported capabilities reported explicitly.

Mutation testing must deliberately break the cross-provider uniqueness guard,
amount comparison, callback-origin rule, per-currency balance, and read/write
interface separation, and record which test kills each mutation.

---

## 10. Decisions still required

Finance, product, legal, and operations must settle these before ledger cutover:

- chart of accounts and revenue-recognition timing;
- merchant-of-record and tax-liability ownership;
- wallet/credit legal characterization and expiry/refund rules;
- dispute, chargeback, write-off, and negative-balance treatment;
- TWD and other currency price books and any FX policy;
- payout/settlement evidence customers may see for each provider; and
- retention, export, and deletion rules for provider evidence and personal data.

Until accepted, they remain explicit decisions—not behavior inferred from the
current Stripe-shaped schema.
