# How customers can verify the billing engine

Public source is useful only when a customer can connect three things:

1. the rules in this repository,
2. the exact artifact that proposed and executed a charge, and
3. the evidence used for that particular charge.

> **Status: target verification contract.** The current engine does not yet ship
> the verifier, canonical charge bundle, public build identity, policy digests,
> transparency record, or intent-only capability gate described here. Commands
> labelled **planned** are acceptance targets, not claims about current `main`.

---

## 1. Evidence levels

| level | question answered | required evidence |
|---|---|---|
| source | what is this revision designed to permit? | public code, docs, schemas, tests |
| build | what code produced the artifact? | Git SHA, reproducible/signed build provenance, artifact digest |
| deployment | what artifact and policies are running? | public `Health`/`Capabilities`, signed deployment statement |
| intent | how was this exact total derived and authorized? | canonical charge bundle + digest |
| provider | what did an external rail report happened? | normalized, verified provider evidence |
| ledger | what monetary transition did MirrorStack commit? | balanced append-only transaction + correction chain |

No lower level substitutes for a higher one. A public repository does not prove
which binary charged a card. A provider's `paid` status does not prove why the
amount was allowed.

---

## 2. Public runtime identity

`Health` returns on healthy and unhealthy responses:

- exact Git commit or the literal `unknown`,
- artifact/container digest,
- build provenance identity,
- binary role (`planner`, `notifier`, `executor`, `reconciler`, and so on),
- receipt/canonical-schema version, and
- deployment environment identifier that contains no secret.

An executor with unknown build identity cannot execute.

`Capabilities` returns:

- active terms, price-book, tax, notice, and routing-policy digests;
- supported currency/scale registry revision;
- each payment adapter's version and declared capabilities;
- notifier, authorization-route, executor, and verifier readiness;
- minimum notice policy in force;
- transparency-log checkpoint; and
- every reachable legacy money-moving path, with a required count.

The production intent-only claim requires `legacyMoneyPaths: 0`. A stronger new
surface beside one weaker legacy route is not a stronger deployment.

Build identity and non-sensitive capability fields must be available directly to
customers/developers, not only through a private support request.

---

## 3. Canonical charge bundle

The bundle described in
[`LEDGER-AND-RECEIPTS.md`](LEDGER-AND-RECEIPTS.md) has one versioned canonical
encoding. Canonicalization fixes:

- field names and order,
- integer and decimal representation,
- Unicode normalization/validation,
- timestamps and time zones,
- absent versus explicit zero/null,
- ordering of lines, sources, evidence, and ledger entries, and
- digest and signature domains.

Invalid Unicode, duplicate map keys, unknown critical fields, out-of-range
integers, unsupported schema versions, and non-canonical encodings are refused.
The digest binds bytes, not a lossy parser's interpretation.

Customer exports may replace sensitive source fields with stable commitments,
but the owning customer can request the evidence needed to verify their own
charge. Redaction is explicit and digest-covered.

---

## 4. Offline verifier

The planned public command is:

```bash
billing-verify verify charge-bundle.json
```

It checks, without contacting MirrorStack or a payment provider:

1. canonical encoding and bundle digest;
2. build/source and policy references;
3. source-event uniqueness and subject binding;
4. module billing-manifest/version binding;
5. price selection and effective windows;
6. exact integer arithmetic, tiers, credits, tax, and rounding;
7. line/subtotal/tax/total equality;
8. authorization scope, currency, cadence, ceilings, and time window;
9. notice content equality and minimum wait;
10. one-settlement and correction-chain structure; and
11. per-currency ledger balance.

It reports `verified`, `invalid`, or `unsupported`; an unknown schema/policy is
never reported as verified.

Optional online mode may refresh provider evidence through customer-authorized
read-only APIs. Provider reachability cannot affect offline arithmetic verdicts.

---

## 5. Test layers

### Example and golden tests

Golden vectors cross package and repository boundaries. At minimum they pin:

- canonical intent and receipt bytes/digests;
- each charge kind and tax status;
- each currency settlement exponent and boundary rounding;
- standing and one-time authorization evaluation;
- exact notice bytes and wait calculation;
- ledger entries and correction chains; and
- normalized Stripe and NewebPay adapter fixtures.

Changing a golden digest requires a schema/policy version change and an explicit
migration decision. Regenerating constants to make tests green is not a fix.

### Property tests and fuzzing

Properties include:

- accepted usage requests contain no monetary authority;
- rating is deterministic and independent of input order;
- replaying an event never increases the result;
- partitioning/combining facts preserves documented aggregation semantics;
- no integer overflow, sign inversion, or second rounding point is accepted;
- missing price/tax/authorization/notice always yields no executable intent;
- a sealed intent cannot change without changing its digest;
- a provider callback cannot originate or enlarge an attempt;
- an intent cannot settle twice across providers;
- all ledger transactions balance in one currency; and
- read-only components cannot reach provider-write capabilities.

Fuzz targets must cover canonical parsing/digesting, rating, policy selection,
tax-result decoding, authorization evaluation, receipt verification, provider
event normalization, and ledger transitions.

### Crash and concurrency tests

Fault injection covers every boundary before and after:

- intent seal,
- notice provider acceptance and receipt commit,
- settlement claim,
- provider request,
- provider response and local attempt commit,
- callback arrival,
- ledger commit, and
- receipt publication.

Tests race duplicate schedulers, callbacks, provider switches, ceiling changes,
authorization revocation, tax-policy withdrawal, and account/payer transfer.
No run may rely only on a provider's short idempotency-retention window.

### Adapter conformance suite

Every payment adapter runs the same provider-neutral contract suite:

- capabilities are truthful and unsupported operations refuse locally;
- amount/currency/payer/merchant binding is exact;
- customer-action and callback states are normalized without guessing;
- signatures/authentication and duplicate events are enforced;
- ambiguous writes are read/reconciled before retry;
- refunds/voids cannot exceed the authorized source effect;
- trace nodes map back to exactly one attempt; and
- adapter reads cannot mutate provider state.

Provider-specific tests may add constraints but cannot weaken the shared suite.

---

## 6. Mutation testing

A passing test suite proves only that the implementation satisfied its tests.
Mutation testing breaks each public invariant deliberately and records which test
noticed.

Required mutations include:

| invariant deliberately broken | expected detector |
|---|---|
| admit `amount` or `price` on usage request | closed-vocabulary/source-shape test |
| allow mutable price fallback | price-policy property/golden test |
| treat `tax.status = unknown` as zero | tax fail-closed test |
| skip notice delivery or wait | executor eligibility test |
| widen authorization ceiling/currency | authorization binding test |
| execute from caller-supplied amount | package/API capability test |
| allow second provider settlement | DB uniqueness + concurrency test |
| let callback create an attempt | webhook-origin property test |
| let a read path import/invoke writer | architecture/source test |
| remove amount/currency provider reconciliation | adapter conformance test |
| unbalance one ledger entry | ledger property test |
| omit build/policy digest from receipt | canonical golden test |

The mutation report is created only after running the pass. Equivalent mutants
and survivors are recorded rather than hidden. CI verdicts are judged by process
exit status, not by grepping output for successful packages.

---

## 7. Static architecture checks

CI must mechanically enforce:

- provider SDK imports exist only in adapter packages;
- provider-write interfaces are injected only into executor/refund binaries;
- planner, read, usage-ingress, notifier, and reconciler binaries cannot compile
  against write ports;
- every provider mutation method is in a generated allow-list mapped to a
  documented effect in [`CHARGES.md`](CHARGES.md);
- public request structs contain no forbidden monetary/authority fields;
- the charge-kind enum and documentation catalog are exhaustive and equal;
- all shipped binaries stamp commit/artifact identity; and
- production readiness cannot pass while any legacy money path is reachable.

This specifically prevents a nominal status read, usage ingest, or
infrastructure synchronization job from triggering auto top-up or any other
payment effect.

---

## 8. Transparency and release gate

Before `executeNotBefore`, the engine publishes an append-only commitment to the
intent digest, notice evidence digest, build identity, and policy digests. A
later receipt links to that checkpoint. The log must not expose private line
details; it proves that the committed bytes existed before execution.

Release remains manual while this architecture is introduced. Production
promotion requires:

1. reviewed source and target-state document consistency;
2. signed build provenance;
3. all tests, fuzz smoke, adapter conformance, and recorded mutation pass;
4. shadow-intent reconciliation with no unexplained monetary differences;
5. public runtime identity and verifier availability;
6. customer authorization/notice/tax readiness; and
7. `legacyMoneyPaths: 0` with legacy provider credentials revoked.

No automatic merge or deployment is implied by a green unit-test run.

---

## 9. Known limits

Verification can prove which bytes were displayed/delivered and when a delivery
provider accepted them. It cannot prove a person read an email.

Public source plus build provenance makes tampering detectable; it cannot protect
against a fully compromised deployment, signing keys, database, notification
provider, and payment-provider account acting together. The threat model in
[`THREAT-MODEL.md`](THREAT-MODEL.md) states the remaining trust assumptions.

Provider payout/balance visibility differs by rail and merchant contract. A
trace marks unsupported evidence explicitly and never treats it as proof of
absence.
