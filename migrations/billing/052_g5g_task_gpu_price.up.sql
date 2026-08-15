-- Migration 052 — exact managed-task g5g.xlarge price and safe catalog default.
--
-- Migration 051 predates the managed task plane's ARM64 capacity decision and
-- seeded the broader historical g4dn/g5 SDK roster. The managed plane admits
-- and provisions only g5g.xlarge. Its authoritative raw Tokyo Linux on-demand
-- price, verified 2026-08-14, is $0.5669 per instance-hour.
--
-- Money is stored as whole micro-dollars (migration 018 BIGINT):
--     $0.5669/hour * 1,000,000 microdollars/dollar
--       = 566,900 microdollars/hour exactly.
-- This is raw COGS. The billing rollup applies the reserved-infra 12/10 markup
-- exactly once, so one metered hour charges 680,280 microdollars.
--
-- The per-model row is authoritative. The catalog row is also corrected to the
-- only admitted model so existing live-estimate queries display the same raw
-- price; cycle.MetricPriceMicros still requires the exact active model row and
-- never falls back to this catalog value for infra.task.gpu.hours.
INSERT INTO ms_billing.metric_model_prices (
    metric, model, unit_price_micros, active
) VALUES (
    'infra.task.gpu.hours', 'g5g.xlarge', 566900, true
)
ON CONFLICT (metric, model) DO UPDATE
SET unit_price_micros = EXCLUDED.unit_price_micros,
    active = EXCLUDED.active;

UPDATE ms_billing.metric_definitions
SET unit_price_micros = 566900
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.task.gpu.hours';
