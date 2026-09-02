-- Migration 070 (down) — restore migration 051's task GPU catalog state.
DELETE FROM ms_billing.metric_model_prices
WHERE metric = 'infra.task.gpu.hours'
  AND model = 'g5g.xlarge';

UPDATE ms_billing.metric_definitions
SET unit_price_micros = 710000
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.task.gpu.hours';
