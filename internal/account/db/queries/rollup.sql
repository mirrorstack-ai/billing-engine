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

-- RollupPeakKind aggregates the peak kind by MAX(value) — the highest
-- absolute level observed in the window is the billable quantity. model and
-- module_version are grouped (COALESCE(…, '')) for parity with the other
-- rollups; peak AI / versioned metrics are not expected today but the
-- dimensions stay consistent.
-- name: RollupPeakKind :many
SELECT
    app_id                         AS app_id,
    module_id                      AS module_id,
    metric                         AS metric,
    kind                           AS kind,
    COALESCE(model, '')            AS model,
    COALESCE(module_version, '')   AS module_version,
    COALESCE(MAX(value), 0)::numeric AS billable_quantity
FROM ms_billing.usage_events
WHERE account_id = $1
  AND recorded_at >= $2
  AND recorded_at <  $3
  AND kind = 'peak'
GROUP BY app_id, module_id, metric, kind, COALESCE(model, ''), COALESCE(module_version, '');

-- RollupTimeWeightedKind integrates the step function under the ordered
-- samples: each sample's value is held until the NEXT sample (or until
-- period_end for the last sample). The segment duration is
-- LEAD(recorded_at, 1, period_end) - recorded_at; the integral is
-- Σ value × duration. EXTRACT(EPOCH ...) yields seconds; /3600 converts to
-- hours, so the inner expression is in byte-hours for a storage gauge (NOT
-- micro-dollars — the alias is segment_byte_hours to avoid confusion with the
-- micro-dollar money columns elsewhere in this schema). A period with no
-- samples produces no row (skipped) — its integral is undefined / 0 (design
-- §8). The window ORDER BY is (recorded_at, event_id): event_id is the TEXT PK,
-- so it breaks recorded_at ties deterministically and the LEAD assigns the
-- remaining duration to a stable last row regardless of plan or vacuum. The
-- PARTITION BY carries model + module_version so the step function is held
-- separately per (model, module_version), matching the other two rollups.
-- name: RollupTimeWeightedKind :many
SELECT
    app_id,
    module_id,
    metric,
    kind,
    model,
    module_version,
    COALESCE(SUM(segment_byte_hours), 0)::numeric AS billable_quantity
FROM (
    SELECT
        app_id,
        module_id,
        metric,
        kind,
        COALESCE(model, '') AS model,
        COALESCE(module_version, '') AS module_version,
        value * EXTRACT(EPOCH FROM (
            LEAD(recorded_at, 1, $3::timestamptz)
                OVER (PARTITION BY app_id, module_id, metric, COALESCE(model, ''), COALESCE(module_version, '') ORDER BY recorded_at, event_id)
            - recorded_at
        )) / 3600.0 AS segment_byte_hours
    FROM ms_billing.usage_events
    WHERE account_id = $1
      AND recorded_at >= $2
      AND recorded_at <  $3
      AND kind = 'time_weighted'
) segments
GROUP BY app_id, module_id, metric, kind, model, module_version;

-- LookupMetricPrice returns the per-unit customer price for a (module, metric)
-- at rollup time, to snapshot onto the aggregate. NULL price → unpriced
-- (decoded to 0 in Go). The rollup prices every aggregated metric through this
-- when the event carries NO model (the catalog row is the fallback price).
-- name: LookupMetricPrice :one
SELECT unit_price_micros
FROM ms_billing.metric_definitions
WHERE module_id = $1 AND metric = $2;

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
-- closed invoice is reproducible.
-- name: UpsertUsageAggregate :exec
INSERT INTO ms_billing.usage_aggregates (
    period_id, account_id, app_id, module_id, metric, model, module_version, kind,
    billable_quantity, unit_price_micros,
    customer_markup_num, customer_markup_den,
    raw_cost_micros, charged_micros, rolled_up_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, now()
)
ON CONFLICT (period_id, app_id, module_id, metric, model, module_version)
DO UPDATE SET
    billable_quantity   = EXCLUDED.billable_quantity,
    unit_price_micros   = EXCLUDED.unit_price_micros,
    customer_markup_num = EXCLUDED.customer_markup_num,
    customer_markup_den = EXCLUDED.customer_markup_den,
    raw_cost_micros     = EXCLUDED.raw_cost_micros,
    charged_micros      = EXCLUDED.charged_micros,
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
