-- Migration 051 (down) — restore the exact pre-051 catalog state.
--
-- Down migrations restore history; they do not improve it. Consequently the
-- two UPDATEs below restore migration 020's known-wrong region prices. The
-- DELETEs are key-based and price-independent, as in 020.down,
-- so a finance edit does not block removal of rows introduced by 051.

-- Remove the dimension rows first, then their catalog fallback/registry peers.
DELETE FROM ms_billing.metric_model_prices
WHERE metric = 'infra.task.gpu.hours';

DELETE FROM ms_billing.metric_definitions
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN (
      'infra.task.vcpu.hours',
      'infra.task.memory.gib_hours',
      'infra.task.gpu.hours'
  );

-- Restore migration 020's seeded values.
UPDATE ms_billing.metric_definitions
SET    unit_price_micros = 90000
WHERE  module_id = '00000000-0000-0000-0000-000000000000'
  AND  metric    = 'infra.egress.api.bytes';

UPDATE ms_billing.metric_definitions
SET    unit_price_micros = 32
WHERE  module_id = '00000000-0000-0000-0000-000000000000'
  AND  metric    = 'infra.storage.gib_hours';
