-- Down 018 — reverse the per-model AI-token pricing foundation.
--
-- Drops only what 018 added: the per-model seed rows, the sentinel-keyed AI
-- metric_definitions rows, the usage_events.model column, and the
-- metric_model_prices table (its set_updated_at trigger drops with the table).
-- Leaves metric_definitions, usage_events, the metric_kind type, and every
-- pre-018 row intact.

-- Sentinel-keyed AI catalog rows seeded by 018.
DELETE FROM ms_billing.metric_definitions
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN (
      'infra.ai.input.tokens',
      'infra.ai.output.tokens',
      'infra.ai.cache_write.tokens',
      'infra.ai.cache_read.tokens',
      'infra.ai.requests'
  );

-- Restore the pre-018 usage_aggregates idempotency key + drop the model column.
--
-- ORDER + DEDUP MATTER. On a DB that ran real AI rollups there are MULTIPLE rows
-- sharing (period, app, module, metric) but differing by model (haiku + sonnet).
-- The pre-018 narrow UNIQUE (period, app, module, metric) cannot coexist with
-- them, and merely dropping the model COLUMN is NOT enough — the distinct ROWS
-- remain, now identical on the narrow key, so re-adding the UNIQUE still fails on
-- a duplicate. So this reversal must first COLLAPSE the model-split rows back
-- into one row per (period, app, module, metric).
--
-- The collapse SUMS the additive money/quantity columns (billable_quantity,
-- raw_cost_micros, charged_micros) so the reconstructed single row carries the
-- same TOTAL the pre-split aggregate would have — no money is lost on down. The
-- snapshot/scalar columns (unit_price_micros, the markup pair) are inherently
-- per-model and cannot be reconstructed for a merged row; the representative
-- MIN(id) row's values are kept (an accepted lossy reversal — down on a
-- model-split DB is by nature lossy, but the BILLED TOTALS are preserved).
--
-- Steps (order is load-bearing):
--   1. drop the model-keyed UNIQUE,
--   2. fold each model group's additive totals into its MIN(id) representative,
--   3. delete the now-redundant non-representative rows,
--   4. drop the model column,
--   5. re-add the pre-018 narrow UNIQUE (now collision-free).
ALTER TABLE ms_billing.usage_aggregates
    DROP CONSTRAINT IF EXISTS usage_aggregates_period_app_module_metric_model_key;

-- 2) Fold additive totals into the representative (MIN(id)) row per group.
WITH grp AS (
    SELECT
        period_id, app_id, module_id, metric,
        -- Postgres has no MIN(uuid); cast to text for a deterministic
        -- representative pick, then back to uuid.
        (MIN(id::text))::uuid     AS keep_id,
        SUM(billable_quantity)   AS sum_qty,
        SUM(raw_cost_micros)     AS sum_raw,
        SUM(charged_micros)      AS sum_charged
    FROM ms_billing.usage_aggregates
    GROUP BY period_id, app_id, module_id, metric
    HAVING COUNT(*) > 1
)
UPDATE ms_billing.usage_aggregates ua
SET billable_quantity = grp.sum_qty,
    raw_cost_micros   = grp.sum_raw,
    charged_micros    = grp.sum_charged
FROM grp
WHERE ua.id = grp.keep_id;

-- 3) Delete the non-representative duplicate rows (their totals were folded in).
DELETE FROM ms_billing.usage_aggregates ua
USING (
    SELECT
        period_id, app_id, module_id, metric,
        (MIN(id::text))::uuid AS keep_id
    FROM ms_billing.usage_aggregates
    GROUP BY period_id, app_id, module_id, metric
    HAVING COUNT(*) > 1
) grp
WHERE ua.period_id = grp.period_id
  AND ua.app_id    = grp.app_id
  AND ua.module_id = grp.module_id
  AND ua.metric    = grp.metric
  AND ua.id <> grp.keep_id;

-- 4) Now the model column carries no information that distinguishes rows.
ALTER TABLE ms_billing.usage_aggregates
    DROP COLUMN IF EXISTS model;

-- 5) Re-add the pre-018 narrow key — collision-free after the collapse.
ALTER TABLE ms_billing.usage_aggregates
    ADD CONSTRAINT usage_aggregates_period_app_module_metric_key
        UNIQUE (period_id, app_id, module_id, metric);

-- The per-event model dimension (its covering index drops with the column, but
-- drop it explicitly first for clarity / parity with the up migration).
DROP INDEX IF EXISTS ms_billing.usage_events_app_module_metric_model_time_idx;

ALTER TABLE ms_billing.usage_events
    DROP COLUMN IF EXISTS model;

-- The per-(metric, model) price side-table (drops its trigger with it).
DROP TABLE IF EXISTS ms_billing.metric_model_prices;
