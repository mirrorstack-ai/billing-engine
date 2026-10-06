UPDATE ms_billing.metric_definitions
SET unit_price_micros = 114
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.db.gib_hours'
  AND unit_price_micros = 137;
