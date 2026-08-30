-- Forward-only once the v2 contract has been used. Silently dropping these
-- fields would turn occurrence-windowed/keyed facts into receipt-windowed
-- ordinary peaks, and mode-coexisting aggregate rows cannot be represented by
-- the legacy uniqueness key. Operators must evacuate/reconcile the v2 state
-- explicitly before a schema rollback.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM ms_billing.metric_definitions
        WHERE aggregation_key IS NOT NULL
    ) OR EXISTS (
        SELECT 1 FROM ms_billing.usage_events
        WHERE observation_version = 2 OR aggregation_key IS NOT NULL
    ) OR EXISTS (
        SELECT 1 FROM ms_billing.usage_aggregates
        WHERE aggregation_key IS NOT NULL
    ) OR EXISTS (
        SELECT 1 FROM ms_billing.usage_observation_rejections
    ) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'migration 055 is forward-only after v2/keyed-meter use',
            HINT = 'stop producers and cycle, reconcile or evacuate all v2/keyed rows and rejection evidence, then retry the down migration';
    END IF;
END
$$;

DROP TABLE IF EXISTS ms_billing.usage_observation_rejections;

DROP INDEX IF EXISTS ms_billing.usage_events_app_module_metric_occurrence_idx;
DROP INDEX IF EXISTS ms_billing.usage_events_account_metric_occurrence_idx;
DROP INDEX IF EXISTS ms_billing.usage_events_keyed_subject_peak_idx;

ALTER TABLE ms_billing.usage_events
    DROP COLUMN IF EXISTS occurrence_policy,
    DROP COLUMN IF EXISTS payload_fingerprint,
    DROP COLUMN IF EXISTS aggregation_key,
    DROP COLUMN IF EXISTS billable_at,
    DROP COLUMN IF EXISTS occurred_at,
    DROP COLUMN IF EXISTS metadata,
    DROP COLUMN IF EXISTS subject,
    DROP COLUMN IF EXISTS observation_version;

DROP INDEX IF EXISTS ms_billing.usage_aggregates_period_line_aggregation_key;

ALTER TABLE ms_billing.usage_aggregates
    DROP COLUMN IF EXISTS aggregation_key;

ALTER TABLE ms_billing.usage_aggregates
    ADD CONSTRAINT usage_aggregates_period_app_module_metric_model_version_key
    UNIQUE (period_id, app_id, module_id, metric, model, module_version);

ALTER TABLE ms_billing.metric_definitions
    DROP COLUMN IF EXISTS aggregation_key;
