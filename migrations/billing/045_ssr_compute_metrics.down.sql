-- Migration 045 (down) — remove the SSR compute metering catalog seed.
DELETE FROM ms_billing.metric_definitions
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN ('infra.compute.ssr.gb_seconds', 'infra.compute.ssr.request.count');
