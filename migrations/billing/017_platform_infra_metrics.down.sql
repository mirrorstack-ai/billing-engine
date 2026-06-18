-- Down 017 — remove the platform-infra metric catalog seed only.
--
-- Deletes the sentinel-keyed infra.* rows this migration seeded; leaves the
-- metric_definitions table + the metric_kind type (owned by migration 006)
-- and every module-declared row intact.
DELETE FROM ms_billing.metric_definitions
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN ('infra.compute.ms', 'infra.egress.bytes');
