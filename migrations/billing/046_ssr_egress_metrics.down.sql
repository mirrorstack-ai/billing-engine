-- Migration 046 (down) — remove the SSR-origin egress metering catalog seed.
DELETE FROM ms_billing.metric_definitions
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.compute.ssr.egress.bytes';
