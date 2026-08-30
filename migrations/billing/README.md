# `ms_billing` migrations

Authoritative source for the `ms_billing` schema. The canonical docs live in
[`mirrorstack-docs`](https://github.com/mirrorstack-ai/mirrorstack-docs) under
`db/ms_billing/`; if a doc disagrees with a migration here, **the migration
wins**.

## How migrations are applied

Every runner in this repo applies the `*.up.sql` files **in filename
(lexical) order** — there is no sequential gap-checking runner:

- **CI** (`.github/workflows/ci.yml`) globs `migrations/billing/*.up.sql` and
  `psql`-applies each in sorted order, then rolls back via `*.down.sql` in
  reverse and re-applies (down/up idempotency check).
- **Local dev** (`scripts/init-db.sql`) `\i`-includes each file explicitly,
  in order.

Because application is by filename sort (not "N must follow N-1"), a gap in the
numbering is **tolerated**: a file simply slots into its sorted position. New
migrations must still be born-clean (no create-then-rename churn) and land main
applying cleanly on a fresh DB.

### Migration 050 billing-cycle cutover

`050_combined_proration_attempts.up.sql` is expand-only for the old binary, but
its new exact Stripe-item ownership contract requires an atomic charge-worker
cutover:

1. Keep scheduled and manual `billing-cycle` invocations idle.
2. Apply migration 050; its first statement rejects every legacy app with
   `proration_attempted_at` set and no durable invoice guard, including rows
   later marked skipped.
3. Atomically move the non-canary `billing-cycle` Lambda alias to the matching
   binary.
4. After the first new worker invocation can create a combined-attempt header,
   do not invoke or roll back to an older charge worker; use forward recovery.

The canaried account API does not run creation charging and is outside this
constraint. Header-scoped DB triggers reject old terminal writes without
changing old no-header behavior before cutover.

### Migration 055 keyed-meter observation cutover

Migration 055 preserves existing v1 data and producers, but its ledger indexes
and aggregate-arbiter swap are intentionally applied in a short maintenance
window: this repository's psql runner does not provide a transaction-safe
`CREATE INDEX CONCURRENTLY` phase, and an old cycle binary cannot infer its old
`ON CONFLICT` target after the legacy constraint is removed. Old rows and old
requests keep receipt-time billing through `recorded_at`; their count, sum,
ordinary peak, and time-weighted rollups are unchanged. Deploy in this order:

1. On a production-sized clone, measure 055's constraint validation and index
   builds and reserve a maintenance window at least that long; do not guess the
   lock budget.
2. Quiesce custom/infra metering writes plus scheduled and manual billing-cycle
   invocations. Billing reads can remain online.
3. Apply 055, then atomically move `account-api` and `billing-cycle` to the
   matching binary before resuming v1 traffic. The replacement unique index is
   built before the legacy arbiter is dropped, so schema is never unguarded.
   The migration also repairs historical `billing_periods.status`: a period
   with an aggregate or nonterminal run becomes `closing`, and one with an
   invoiced run becomes `invoiced`. This is intentionally a forward data
   correction—pre-055 rollup/charge left those rows at the default `open`.
4. Verify both binaries share the account-period advisory lock, resume v1
   producers, and only then enable the api-platform/SDK v2 observation shape.

The v2 wire additions are exactly `v`, `subject`, `metadata`, and
`occurred_at`. `subject` is an authoritative opaque end-user identity. It is
required only when the catalog declares `aggregation_key = 'subject'`, must be
valid UTF-8 without control characters, and is limited to 256 bytes. Metadata
is diagnostic only—never an aggregation key—and must be a top-level JSON
object. Before storage it is limited to 4 KiB raw and canonical bytes, 32 total
object members, depth 4 (including the root), 64-byte ASCII identifier keys,
512-byte strings, 32 array items, and finite float64 JSON numbers of at most
128 source bytes.

New writes persist a SHA-256 fingerprint over the normalized observation
version, authoritative app/module/owner/metric, model/module version, value,
subject, canonical metadata, and UTC occurrence
time. Server receipt time, resolved account id, and mutable catalog-derived
kind/aggregation mode are deliberately excluded; a catalog change cannot turn
an otherwise identical delivery retry into an event-id conflict.
The same event id and fingerprint is an idempotent success; a different
fingerprint is `CONFLICT`, including when the first attempt was policy-rejected.

Occurrence admission is explicit:

- more than five minutes in the future is rejected;
- more than 35 × 24 hours in the past is rejected;
- an older observation inside that window is accepted only while its anchored
  billing period remains open (`late_open`);
- an in-window occurrence before an account's first funded anchor boundary is
  billed at that boundary (`first_funded`) because no earlier cycle can ever
  close it; the original `occurred_at` remains unchanged;
- `closing` or `invoiced` periods reject new observations.

Because the normal cycle targets only the current and immediately previous
anchored windows, an older window is treated as logically closed even if an
empty or missed sweep left no `billing_periods` row. This prevents a valid
35-day retry from being accepted into a window no future cycle will select.
`first_funded` is the explicit exception: direct funded-user admission and the
lazy-org repoint sweep clamp pre-funding usage forward into the first period
that can actually close, while retaining the original occurrence timestamp.
Card-less calendar rollup continues to freeze v1 events exactly as before, but
leaves every v2 observation pending. First activation holds the same account
row barrier as ingest/rollup and atomically moves those pending v2 rows into the
first funded anchored window, even when the legacy calendar period was already
rolled. An activation racing the unactivated work list therefore cannot orphan
v2 usage in an unchargeable calendar aggregate.

Thirty-five days covers the longest calendar-month billing period (31 days)
plus four days of delivery/retry margin without reopening arbitrary history.
Rejected decisions are recorded in `usage_observation_rejections` with a
bounded reason and fingerprint, but never the rejected diagnostic metadata.
New writers persist `billable_at` from `occurred_at` for v2 and `recorded_at`
for v1; reads use `recorded_at` when an old v1 binary left that additive column
NULL. A lazy-org sweep sets `billable_at = max(original billing time, first
funded window start)` for every swept row, so the first-funded-period clamp
remains effective without mutating the original v2 occurrence audit field—even
when receipt time was already inside that funded window.

`aggregation_key = 'subject'` is catalog-owned and valid only for `peak`. Its
period result is `SUM(MAX(value) per subject)` within the existing authoritative
bill-line dimensions (account, app, module, metric, model, module version, and
billing window). Model/version changes remain separate price definitions; no
arrival-order rule moves a subject between them. Keyed peak is cardinality-like
and receives no level-window proration. The aggregate uniqueness key includes
the aggregation mode so a mid-period catalog mode change retains both immutable
lines rather than overwriting one.

Rollback is forward-only after any v2/keyed catalog, event, aggregate, or
rejection row exists. The down migration fails before changing schema in that
state: dropping the fields would silently turn occurrence-windowed/keyed facts
into receipt-windowed ordinary peaks, while mode-coexisting aggregates cannot
fit the legacy unique key. An exceptional rollback must stop producers and the
cycle, reconcile or evacuate that state explicitly, and only then apply 055
down. As with every dependent migration, unwind 055 before manually
round-tripping an ancestor such as 023; historical migrations are never edited
to know about future indexes.

> If a future sequential-only runner (golang-migrate / goose) is adopted, the
> reserved slots below must be materialized first (see "Reserved slots").

## Numbering

| Slot    | File                              | Status                                   |
| ------- | --------------------------------- | ---------------------------------------- |
| 001–005 | init + payment-method mirror      | shipped                                  |
| 006–010 | metering core (metric_definitions, usage_events, billing_periods, usage_aggregates, module_visibility) | shipped |
| 011–013 | *(reserved)*                      | **RESERVED** for the meter charge-chain PRs — `invoices` / `billing_runs` / `developer_settlements`. Not in the tree yet. |
| 014     | `014_budgets.up.sql`              | budgets (per-app spending caps + thresholds) |
| 015     | `015_budget_alerts.up.sql`        | recorded threshold crossings             |
| 016–023 | collection + infra-metric catalog + display groups + usage `module_version` | shipped |
| 024     | `024_billing_svc_grants.up.sql`   | production `billing_svc` role grants (RDS-IAM via RDS Proxy); NOTICE-skips when the role is absent (dev/CI) |

### Reserved slots (011–013)

011–013 are intentionally **left empty** for the in-flight meter charge PRs.
Budgets (014–015) do **not** depend on them: they only *read*
`usage_events × metric_definitions` for the spend sum and *reference*
`accounts`. The gap is deliberate, not a missing file — do not "fill" it with
placeholder migrations (an empty placeholder that a later PR edits in place
would break anyone who already applied it). The charge PRs introduce real
011/012/013 files, which then slot in before 014/015 by filename sort.
