-- 078 (down) — remove the static CDN egress catalog seed. infra.egress.bytes
-- was never touched by the up, so nothing to restore there.
DELETE FROM ms_billing.metric_definitions
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.egress.cdn.bytes';
