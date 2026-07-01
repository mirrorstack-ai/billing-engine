-- Down 023 — reverse the module_version attribution dimension.
--
-- Drops only what 023 added: the usage_aggregates.module_version column (+
-- widened UNIQUE), its covering index, and the usage_events.module_version
-- column. Leaves usage_events, usage_aggregates' pre-023 shape (including the
-- migration-018 model column + UNIQUE), and every other row intact.

-- Restore the pre-023 usage_aggregates idempotency key + drop the
-- module_version column.
--
-- ORDER + DEDUP MATTER, identical to 018's down. On a DB that rolled up
-- version-split usage there are MULTIPLE rows sharing (period, app, module,
-- metric, model) but differing by module_version. The pre-023 UNIQUE (period,
-- app, module, metric, model) cannot coexist with them, and merely dropping
-- the module_version COLUMN is NOT enough — the distinct ROWS remain, now
-- identical on the narrow key, so re-adding the UNIQUE still fails on a
-- duplicate. So this reversal must first COLLAPSE the version-split rows back
-- into one row per (period, app, module, metric, model).
--
-- The collapse SUMS the additive money/quantity columns (billable_quantity,
-- raw_cost_micros, charged_micros) so the reconstructed single row carries the
-- same TOTAL the pre-split aggregate would have — no money is lost on down.
-- The snapshot/scalar columns (unit_price_micros, the markup pair) are
-- inherently per-(module, metric, model) and unaffected by version, so the
-- representative MIN(id) row's values are already correct for the merged row
-- (unlike 018's down, module_version never changes pricing).
--
-- Steps (order is load-bearing):
--   1. drop the version-keyed UNIQUE,
--   2. fold each version group's additive totals into its MIN(id)
--      representative,
--   3. delete the now-redundant non-representative rows,
--   4. drop the module_version column,
--   5. re-add the pre-023 (018-era) UNIQUE (now collision-free).
ALTER TABLE ms_billing.usage_aggregates
    DROP CONSTRAINT IF EXISTS usage_aggregates_period_app_module_metric_model_version_key;

-- 2) Fold additive totals into the representative (MIN(id)) row per group.
WITH grp AS (
    SELECT
        period_id, app_id, module_id, metric, model,
        -- Postgres has no MIN(uuid); cast to text for a deterministic
        -- representative pick, then back to uuid.
        (MIN(id::text))::uuid     AS keep_id,
        SUM(billable_quantity)   AS sum_qty,
        SUM(raw_cost_micros)     AS sum_raw,
        SUM(charged_micros)      AS sum_charged
    FROM ms_billing.usage_aggregates
    GROUP BY period_id, app_id, module_id, metric, model
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
        period_id, app_id, module_id, metric, model,
        (MIN(id::text))::uuid AS keep_id
    FROM ms_billing.usage_aggregates
    GROUP BY period_id, app_id, module_id, metric, model
    HAVING COUNT(*) > 1
) grp
WHERE ua.period_id = grp.period_id
  AND ua.app_id    = grp.app_id
  AND ua.module_id = grp.module_id
  AND ua.metric    = grp.metric
  AND ua.model     = grp.model
  AND ua.id <> grp.keep_id;

-- 4) Now the module_version column carries no information that distinguishes
--    rows.
ALTER TABLE ms_billing.usage_aggregates
    DROP COLUMN IF EXISTS module_version;

-- 5) Re-add the pre-023 (018-era) key — collision-free after the collapse.
ALTER TABLE ms_billing.usage_aggregates
    ADD CONSTRAINT usage_aggregates_period_app_module_metric_model_key
        UNIQUE (period_id, app_id, module_id, metric, model);

-- The per-event version dimension (its covering index drops with the column,
-- but drop it explicitly first for clarity / parity with the up migration).
DROP INDEX IF EXISTS ms_billing.usage_events_app_module_metric_model_version_time_idx;

ALTER TABLE ms_billing.usage_events
    DROP COLUMN IF EXISTS module_version;
