-- 080 (down): remove the two request-count seeds and move the regrouped rows
-- back to the groups they held before (078 seeded infra.egress.cdn.bytes in
-- 'network'; 045/046 set none, so the SSR rows were 'other').
DELETE FROM ms_billing.metric_definitions
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN ('infra.cdn.request.count', 'infra.cdn.r2.read.count');

UPDATE ms_billing.metric_definitions
SET    display_group = 'network'
WHERE  module_id = '00000000-0000-0000-0000-000000000000'
  AND  metric = 'infra.egress.cdn.bytes';

UPDATE ms_billing.metric_definitions
SET    display_group = 'other'
WHERE  module_id = '00000000-0000-0000-0000-000000000000'
  AND  metric IN ('infra.compute.ssr.gb_seconds', 'infra.compute.ssr.request.count', 'infra.compute.ssr.egress.bytes');
