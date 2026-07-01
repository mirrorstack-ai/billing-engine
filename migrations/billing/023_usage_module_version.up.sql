-- Migration 023 — module_version ATTRIBUTION dimension on usage tracking.
--
-- Adds a `module_version` dimension so usage can be reported PER VERSION of
-- the module that emitted it (trend charts + a per-version cost/income
-- breakdown). This is materially SIMPLER than migration 018's per-model AI
-- pricing dimension: version does NOT affect price. There is no
-- metric_model_prices-style side-table and no seed data — module_version is
-- purely an attribution/reporting dimension that flows through the pipeline
-- unpriced, exactly like `model` does for a non-AI event.
--
-- STRUCTURAL PATTERN (cloned from 018, the per-model precedent):
--   1. add a nullable `module_version` column to usage_events (the per-event
--      dimension; NULL for every event that doesn't carry a version — the
--      historical default, no backfill needed),
--   2. cover the new GROUP BY dimension with an index (the rollup now groups
--      usage_events BY (app, module, metric, model, COALESCE(module_version,
--      '')) — see rollup.sql),
--   3. add a NOT NULL DEFAULT '' `module_version` column to usage_aggregates
--      (every pre-023 row keys under the empty string, matching the rollup's
--      COALESCE, so existing aggregates upsert idempotently with no behavior
--      change) and widen the UNIQUE idempotency key to include it (two
--      versions of the same metric under the same module are now DISTINCT
--      billable rows, exactly like two models are distinct under 018).
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md (usage_events.module_version,
--       usage_aggregates.module_version).

-- 1) The per-event version dimension. NULL for every event that carries no
--    version (the historical default — no backfill, no column DEFAULT
--    needed; a pure ADD COLUMN ... NULL is a safe online change on
--    Postgres 17).
ALTER TABLE ms_billing.usage_events
    ADD COLUMN module_version TEXT NULL;

-- 1a) Cover the new GROUP BY dimension. The rollup now groups usage_events BY
--     (app, module, metric, model, COALESCE(module_version, '')) (rollup.sql
--     RollupSumKinds et al.), so module_version joins model after it in the
--     per-(app, module) attribution index. Appending it after the existing
--     model+time columns keeps the pre-023 leading-column lookups unchanged
--     while letting the rollup's grouping be covered. IF NOT EXISTS for
--     re-run safety.
CREATE INDEX IF NOT EXISTS usage_events_app_module_metric_model_version_time_idx
    ON ms_billing.usage_events (app_id, module_id, metric, model, module_version, recorded_at);

-- 2) The per-aggregate version dimension on usage_aggregates. The rollup now
--    GROUPs usage_events BY (app, module, metric, model, module_version), so
--    two version-split aggregate rows for the same (module, metric, model)
--    differ ONLY by module_version. The pre-023 UNIQUE (period, app, module,
--    metric, model) would collide them — the second version's upsert would
--    clobber the first, under-counting usage. Add module_version to the row
--    + the idempotency key. DEFAULT '' (NOT NULL) keys every pre-023 /
--    version-less row under the empty string, matching the rollup's
--    COALESCE(module_version, '') so existing aggregates upsert idempotently
--    with no behavior change.
ALTER TABLE ms_billing.usage_aggregates
    ADD COLUMN module_version TEXT NOT NULL DEFAULT '';

-- IF EXISTS for defensiveness + symmetry with the down migration's own DROP:
-- in sequential migration order the named constraint always exists (018
-- created it), but a manual rename/drop upstream should not hard-fail this
-- migration.
ALTER TABLE ms_billing.usage_aggregates
    DROP CONSTRAINT IF EXISTS usage_aggregates_period_app_module_metric_model_key;

ALTER TABLE ms_billing.usage_aggregates
    ADD CONSTRAINT usage_aggregates_period_app_module_metric_model_version_key
        UNIQUE (period_id, app_id, module_id, metric, model, module_version);
