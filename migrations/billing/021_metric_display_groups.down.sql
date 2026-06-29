-- Down 021 — drop the metric display-group taxonomy.
--
-- Drops the display_group column (which reverts every backfilled infra.* row's
-- group in one shot — the values live only in this column) and then the enum
-- type. Order matters: the column depends on the type, so the column drops
-- first. No data outside this column is touched; metric_definitions and every
-- other row return to their pre-021 shape.
ALTER TABLE ms_billing.metric_definitions
    DROP COLUMN display_group;

DROP TYPE ms_billing.metric_group;
