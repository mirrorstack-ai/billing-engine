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
