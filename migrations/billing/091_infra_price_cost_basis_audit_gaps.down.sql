-- Restore the audited values, each only where the row is still at what 091 set.
UPDATE ms_billing.metric_definitions
SET unit_price_micros = 137
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.db.gib_hours'
  AND unit_price_micros = 164;

UPDATE ms_billing.metric_definitions
SET unit = 'request', unit_price_micros = 1
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.request.count'
  AND unit = '1k requests'
  AND unit_price_micros = 1490;

UPDATE ms_billing.metric_definitions
SET unit = 'fire', unit_price_micros = 1
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.cron.count'
  AND unit = '1k fires'
  AND unit_price_micros = 1250;

UPDATE ms_billing.metric_definitions
SET unit_price_micros = 5
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN ('infra.storage.put.count', 'infra.storage.list.count')
  AND unit_price_micros = 4700;

UPDATE ms_billing.metric_definitions
SET unit_price_micros = 1
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.compute.walltime.ms'
  AND unit_price_micros = 0;

DELETE FROM ms_billing.metric_definitions
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.task.ephemeral.gib_hours';
