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
ALTER TABLE ms_billing.usage_aggregates
    DROP CONSTRAINT IF EXISTS usage_aggregates_period_app_module_metric_model_key;

ALTER TABLE ms_billing.usage_aggregates
    ADD CONSTRAINT usage_aggregates_period_app_module_metric_key
        UNIQUE (period_id, app_id, module_id, metric);

ALTER TABLE ms_billing.usage_aggregates
    DROP COLUMN IF EXISTS model;

-- The per-event model dimension.
ALTER TABLE ms_billing.usage_events
    DROP COLUMN IF EXISTS model;

-- The per-(metric, model) price side-table (drops its trigger with it).
DROP TABLE IF EXISTS ms_billing.metric_model_prices;
