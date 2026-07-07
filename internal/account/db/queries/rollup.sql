-- Queries backing internal/account/cycle.pgxStore (the period-rollup +
-- developer-settlement Store interface). All operate on the ms_billing schema.
--
-- Milestone D, PR #5 (aggregation + settlement). This file ships the rollup
-- write path deferred from PR #3 (which would have shipped them as dead code,
-- having no caller):
--   OpenPeriodForAccount     idempotent billing_periods upsert (period anchor)
--   RollupSumKinds           count/sum → SUM(value) per (app, module, metric)
--   RollupPeakKind           peak      → MAX(value)
--   RollupTimeWeightedKind   time_weighted → ∫ v dt (step-function integral)
--   UpsertUsageAggregate     snapshotted billable record (idempotent upsert)
--   ModuleVisibility         the developer margin-share class for a module
--   ModuleIncome             Σ charged_micros per module for a period
--   UpsertDeveloperSettlement  the settlement ledger row (idempotent upsert)
--
-- Pricing plane (design §4 Axis 3): a custom metric is charged at the
-- developer's declared unit_price with NO markup (10/10); the flat 12/10
-- applies only to platform-infra / built-in metrics. The arithmetic is in
-- Go (cycle.Service); these queries only AGGREGATE quantity + snapshot price.

-- OpenPeriodForAccount upserts the billing_periods row keyed
-- (account_id, period_start). The rollup needs a stable period row to attach
-- usage_aggregates to; this returns the existing row's id+status on a re-run
-- (the DO UPDATE is a no-op SET so RETURNING fires on conflict too) and a
-- fresh 'open' row otherwise. period_end is the anchored-period window end (the
-- next card-binding-day boundary — ADR 0005), supplied by the caller.
-- name: OpenPeriodForAccount :one
INSERT INTO ms_billing.billing_periods (account_id, period_start, period_end, status)
VALUES ($1, $2, $3, 'open')
ON CONFLICT (account_id, period_start)
DO UPDATE SET status = ms_billing.billing_periods.status
RETURNING id, status;

-- RollupSumKinds aggregates the additive kinds (count, sum) by SUM(value)
-- over [period_start, period_end) per (app, module, metric, model,
-- module_version). count and sum both roll up by SUM; kind is carried
-- through so the aggregate row snapshots the right accumulation semantics.
-- model is grouped so AI events aggregate PER MODEL (the pricing dimension,
-- migration 018) rather than collapsing models that differ ~15× into one
-- row; module_version is grouped so events aggregate PER VERSION (the
-- attribution dimension, migration 023 — never affects price) rather than
-- blending versions into one row. COALESCE(…, '') keys an event that carries
-- neither dimension (NULL model / NULL module_version) under a stable empty
-- string.
-- name: RollupSumKinds :many
SELECT
    app_id                         AS app_id,
    module_id                      AS module_id,
    metric                         AS metric,
    kind                           AS kind,
    COALESCE(model, '')            AS model,
    COALESCE(module_version, '')   AS module_version,
    COALESCE(SUM(value), 0)::numeric AS billable_quantity
FROM ms_billing.usage_events
WHERE account_id = $1
  AND recorded_at >= $2
  AND recorded_at <  $3
  AND kind IN ('count', 'sum')
GROUP BY app_id, module_id, metric, kind, COALESCE(model, ''), COALESCE(module_version, '');

-- RollupPeakKind aggregates the peak kind PER VERSION (usage-time-pricing
-- Phase 1, docs-temp/usage-time-pricing/design.md — supersedes the
-- fix/peak-multiversion-overcharge (#58) exploration, which collapsed
-- module_version out of this query entirely). For each (app, module, metric,
-- model, module_version): billable_quantity is MAX(value) over THAT
-- version's OWN events only (never the period-wide max), and active_seconds
-- is the version's ACTIVE WINDOW (window_v) — the summed duration of the
-- time-ordered segments it opened.
--
-- window_v is derived by LEAD-ing across the FULL (app, module, metric,
-- model) stream WITH module_version deliberately OUT of the PARTITION BY —
-- this is #58's one surviving insight: a successor version's first sample
-- terminates its predecessor's segment at the TRUE handoff instant, so no
-- window tail-bleeds past a version boundary. module_version is then back in
-- the OUTER GROUP BY (the disposition that supersedes #58: peak no longer
-- collapses it) so each version's MAX + window are attributed and priced
-- independently.
--
-- UNIFIED LEVEL MODEL: every LEVEL metric (peak, time_weighted) bills
-- charge_v = representative_level_v × (window_v / P) × price_v, P being the
-- whole period length. For peak, representative_level_v is this query's
-- billable_quantity (the version's own MAX) and window_v is active_seconds;
-- cycle/money.go applies the window_v/P proration at pricing time — this
-- query only aggregates quantity + the window snapshot, it never prices.
-- Peak's OLD (pre-this-PR) price convention had ZERO time-weighting (a flat
-- MAX × price regardless of how long the level was held), so this factor is
-- a genuinely NEW proration, not a double-count: a single-version period has
-- window_v == P (Σ window_v == P by construction once ≥1 sample exists), so
-- the factor is 1 and the charge is byte-for-byte the pre-this-PR number —
-- the load-bearing no-regression invariant.
--
-- model stays in the GROUP BY (it prices infra.ai.* lines); module_version
-- is ALSO now a pricing key via metric_version_prices (migration 044),
-- version-first-resolved in Go (cycle.MetricPriceMicros).
-- name: RollupPeakKind :many
WITH raw_events AS (
    SELECT
        app_id, module_id, metric, kind,
        COALESCE(model, '')          AS model,
        COALESCE(module_version, '') AS module_version,
        value, recorded_at, event_id
    FROM ms_billing.usage_events
    WHERE account_id = $1
      AND recorded_at >= $2
      AND recorded_at <  $3
      AND kind = 'peak'
),
windowed AS (
    SELECT
        app_id, module_id, metric, kind, model, module_version, value, recorded_at,
        LEAD(recorded_at, 1, $3::timestamptz)
            OVER (PARTITION BY app_id, module_id, metric, model ORDER BY recorded_at, event_id) AS segment_end
    FROM raw_events
)
SELECT
    app_id,
    module_id,
    metric,
    kind,
    model,
    module_version,
    COALESCE(MAX(value), 0)::numeric AS billable_quantity,
    COALESCE(SUM(EXTRACT(EPOCH FROM (segment_end - recorded_at))), 0)::numeric AS active_seconds
FROM windowed
GROUP BY app_id, module_id, metric, kind, model, module_version;

-- RollupTimeWeightedKind integrates the step function under the ordered
-- samples PER VERSION (usage-time-pricing Phase 1 — supersedes #58's
-- collapse of this query): each sample's value is held until the NEXT
-- sample (or until period_end for the stream's last sample). The segment
-- duration is LEAD(recorded_at, 1, period_end) - recorded_at; the integral
-- is Σ value × duration. EXTRACT(EPOCH ...) yields seconds; /3600 converts
-- to hours, so segment_byte_hours is in byte-hours for a storage gauge (NOT
-- micro-dollars). A period with no samples produces no row (skipped) — its
-- integral is undefined / 0 (design §8). The window ORDER BY is
-- (recorded_at, event_id): event_id is the TEXT PK, so it breaks
-- recorded_at ties deterministically and the LEAD assigns the remaining
-- duration to a stable last row regardless of plan or vacuum.
--
-- module_version is OUT of the LEAD's PARTITION BY (#58's one surviving
-- insight): the window walks the FULL time-ordered (app, module, metric,
-- model) stream, so a successor version's first sample terminates its
-- predecessor's LAST segment at the TRUE handoff instant — no tail bleeds
-- past a version boundary (the double-charge #58 fixed). module_version is
-- back in the OUTER GROUP BY (the disposition that supersedes #58:
-- time_weighted no longer collapses it either), so billable_quantity here is
-- I_v = Σ(value × duration) over THAT version's own segments only, and
-- active_seconds is the same segments' summed duration (window_v) —
-- reproducibility snapshot only, see below.
--
-- UNIT CONVENTION (design doc resolved decision #3 — do not "fix" this by
-- copying peak's proration here): time_weighted's price is ALREADY
-- per-unit-HOUR (e.g. storage.gib_hours — $/GiB-hour), so I_v is already the
-- fully time-weighted billable quantity: charge_v = I_v × price_v, exactly
-- as before this PR. It must NOT ALSO be scaled by (window_v / P) — that
-- would double-normalize time for every per-hour-priced time_weighted
-- metric (storage included), since the integral already bakes in precisely
-- how long each version's level was held. (Contrast RollupPeakKind, whose
-- price is a flat period-wide rate with ZERO built-in time-weighting, so ITS
-- charge genuinely needs the explicit window_v/P factor.) active_seconds is
-- carried through purely for the reproducibility snapshot / a future
-- "used N of P days" display, never as a second charge multiplier. A
-- single-version period trivially reproduces the pre-this-PR number (there
-- is only one version's I_v to sum) — the load-bearing no-regression
-- invariant.
-- name: RollupTimeWeightedKind :many
WITH raw_events AS (
    SELECT
        app_id, module_id, metric, kind,
        COALESCE(model, '')          AS model,
        COALESCE(module_version, '') AS module_version,
        value, recorded_at, event_id
    FROM ms_billing.usage_events
    WHERE account_id = $1
      AND recorded_at >= $2
      AND recorded_at <  $3
      AND kind = 'time_weighted'
),
windowed AS (
    SELECT
        app_id, module_id, metric, kind, model, module_version, value, recorded_at,
        LEAD(recorded_at, 1, $3::timestamptz)
            OVER (PARTITION BY app_id, module_id, metric, model ORDER BY recorded_at, event_id) AS segment_end
    FROM raw_events
),
segments AS (
    SELECT
        app_id, module_id, metric, kind, model, module_version,
        EXTRACT(EPOCH FROM (segment_end - recorded_at)) AS duration_seconds,
        value * EXTRACT(EPOCH FROM (segment_end - recorded_at)) / 3600.0 AS segment_byte_hours
    FROM windowed
)
SELECT
    app_id,
    module_id,
    metric,
    kind,
    model,
    module_version,
    COALESCE(SUM(segment_byte_hours), 0)::numeric AS billable_quantity,
    COALESCE(SUM(duration_seconds), 0)::numeric   AS active_seconds
FROM segments
GROUP BY app_id, module_id, metric, kind, model, module_version;

-- LookupMetricPrice returns the per-unit customer price for a (module, metric)
-- at rollup time, to snapshot onto the aggregate. NULL price → unpriced
-- (decoded to 0 in Go). The rollup prices every aggregated metric through this
-- when the event carries NO model (the catalog row is the fallback price).
-- name: LookupMetricPrice :one
SELECT unit_price_micros
FROM ms_billing.metric_definitions
WHERE module_id = $1 AND metric = $2;

-- LookupMetricVersionPrice returns the per-unit customer price SNAPSHOTTED
-- for a (module, metric, module_version) at version-publish time (migration
-- 044) — the VERSION-FIRST price resolution the rollup tries before falling
-- back to LookupMetricPrice's version-blind catalog row. pgx.ErrNoRows means
-- no snapshot exists for this version (a module_version='' event — pre
-- version-stamping — or any version published with no SetMetricVersionPrices
-- sync for whatever legacy reason); the Go caller (cycle.MetricPriceMicros)
-- falls back to LookupMetricPrice on that error, exactly like a missing
-- per-model row falls back to the catalog for AI metrics. This table is
-- INSERT-ONLY (no UPDATE path — see migration 044), so a row returned here is
-- the price this version was ALWAYS published at: a LATER version's re-price
-- can never change what this query returns for an EARLIER version. This is
-- the fix for the mid-period-reprice bug (design doc "usage-time-pricing")
-- that catalog-only LookupMetricPrice cannot avoid — a catalog row is
-- mutated in place by every SetMetricDefinitions sync, so resolving through
-- it alone would retroactively re-bill already-accrued usage at whatever
-- price the CURRENT publish happens to carry.
-- name: LookupMetricVersionPrice :one
SELECT unit_price_micros
FROM ms_billing.metric_version_prices
WHERE module_id = $1 AND metric = $2 AND module_version = $3;

-- LookupModelPrice returns the RAW provider COGS for a (metric, model) pair from
-- the per-model side-table (migration 018) — the AUTHORITATIVE price when a
-- usage_event carries a model. unit_price_micros is NOT NULL here (a row exists
-- only to price), so it is a plain BIGINT.
--
-- It does NOT filter active in the WHERE: it returns the active flag so the Go
-- caller can DISTINGUISH "no row at all" (pgx.ErrNoRows → fall back to the
-- LookupMetricPrice catalog row, the legitimate unpriced-model path) from "a row
-- exists but was RETIRED to active = false". The latter must NOT silently fall
-- back to the catalog's conservative (Haiku-floor) fallback price — that would
-- under-bill a deliberately-retired model at a cheaper rate, defeating the loud
-- revenue-leak guard the rollup enforces for missing infra prices. The Go caller
-- fails the cycle loud on an inactive AI price instead.
-- name: LookupModelPrice :one
SELECT unit_price_micros, active
FROM ms_billing.metric_model_prices
WHERE metric = $1 AND model = $2;

-- UpsertUsageAggregate writes the snapshotted billable record idempotently:
-- a rollup re-run for the same (period, app, module, metric, model,
-- module_version) upserts the SAME row (identical values) rather than
-- duplicating it. model is '' for non-AI metrics and the roster model id for
-- infra.ai.* (migration 018); module_version is '' for a version-less event
-- and the emitting module's version otherwise (migration 023, attribution
-- only — never priced). Both are part of the idempotency key so two models
-- or two versions on one metric are distinct billable rows. Snapshots
-- billable_quantity + unit_price + the markup multiplier + raw/charged so a
-- closed invoice is reproducible. active_seconds/period_days (migration 044)
-- are the window-proration reproducibility snapshot: NULL for additive kinds
-- (count/sum — proration never applies), populated for peak/time_weighted so
-- a closed invoice can re-derive the exact per-version window fraction
-- without re-reading usage_events.
-- name: UpsertUsageAggregate :exec
INSERT INTO ms_billing.usage_aggregates (
    period_id, account_id, app_id, module_id, metric, model, module_version, kind,
    billable_quantity, unit_price_micros,
    customer_markup_num, customer_markup_den,
    raw_cost_micros, charged_micros, active_seconds, period_days, rolled_up_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, now()
)
ON CONFLICT (period_id, app_id, module_id, metric, model, module_version)
DO UPDATE SET
    billable_quantity   = EXCLUDED.billable_quantity,
    unit_price_micros   = EXCLUDED.unit_price_micros,
    customer_markup_num = EXCLUDED.customer_markup_num,
    customer_markup_den = EXCLUDED.customer_markup_den,
    raw_cost_micros     = EXCLUDED.raw_cost_micros,
    charged_micros      = EXCLUDED.charged_micros,
    active_seconds      = EXCLUDED.active_seconds,
    period_days         = EXCLUDED.period_days,
    rolled_up_at        = EXCLUDED.rolled_up_at;

-- ModuleIncomeForPeriod sums charged_micros per module across the period's
-- usage_aggregates — the settlement "income" input. Grouped by module so the
-- developer-settlement rollup gets one income figure per module.
-- name: ModuleIncomeForPeriod :many
SELECT
    module_id                              AS module_id,
    COALESCE(SUM(charged_micros), 0)::bigint AS income_micros
FROM ms_billing.usage_aggregates
WHERE period_id = $1
GROUP BY module_id;

-- ModuleVisibility returns a module's developer margin-share class. No row →
-- the caller defaults to private (30%) so the platform never under-collects
-- on a lagging publish (design §7-B).
-- name: ModuleVisibility :one
SELECT visibility
FROM ms_billing.module_visibility
WHERE module_id = $1;

-- UpsertDeveloperSettlement writes the settlement ledger row idempotently per
-- (period, module): a re-run upserts the same row. developer_id is NULL until
-- a module→developer sync exists; status defaults 'accrued' (payout deferred).
-- name: UpsertDeveloperSettlement :exec
INSERT INTO ms_billing.developer_settlements (
    period_id, account_id, module_id, developer_id,
    income_micros, infra_micros, margin_share_class,
    platform_take_micros, developer_owed_micros, status
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, 'accrued'
)
ON CONFLICT (period_id, module_id)
DO UPDATE SET
    account_id            = EXCLUDED.account_id,
    income_micros         = EXCLUDED.income_micros,
    infra_micros          = EXCLUDED.infra_micros,
    margin_share_class    = EXCLUDED.margin_share_class,
    platform_take_micros  = EXCLUDED.platform_take_micros,
    developer_owed_micros = EXCLUDED.developer_owed_micros;
