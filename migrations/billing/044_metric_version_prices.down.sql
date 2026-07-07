-- Down for 044: drop the reproducibility snapshot columns + the per-version
-- price-snapshot table. Money-safe: active_seconds/period_days are audit-only
-- (never read back into a charge computation), and metric_version_prices is
-- a pure ADDITIVE side-table (like metric_model_prices under 018) with no
-- other table referencing it — no collapse/backfill step is needed, unlike
-- 023's module_version down (which had to fold split usage_aggregates rows
-- back together before dropping the widened UNIQUE key).

ALTER TABLE ms_billing.usage_aggregates
    DROP COLUMN IF EXISTS period_days,
    DROP COLUMN IF EXISTS active_seconds;

DROP TABLE IF EXISTS ms_billing.metric_version_prices;
