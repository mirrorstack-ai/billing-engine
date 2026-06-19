-- Down 018 — reverse the infra catalog hygiene, restoring 017's seeded state.
--
-- Order matters: delete the deprecated alias FIRST, then rename the row back.
-- The up migration created TWO sentinel rows ('infra.compute.walltime.ms' +
-- the 'infra.compute.ms' alias). UNIQUE(module_id, metric) means renaming
-- walltime.ms back to 'infra.compute.ms' while the alias still exists would
-- collide. Dropping the alias first vacates the slot.

-- (a) Remove the deprecated alias row (it exists only in 018, never in 017).
DELETE FROM ms_billing.metric_definitions
WHERE  module_id = '00000000-0000-0000-0000-000000000000'
  AND  metric    = 'infra.compute.ms';

-- (b) Rename infra.compute.walltime.ms back to infra.compute.ms (017's name).
-- The unit = 'millisecond' write is a defensive no-op (017 and 018 both use
-- 'millisecond'); only `metric` actually changes. Kept symmetric with 018.up
-- step (1) so the pair reads as an exact rename/un-rename.
UPDATE ms_billing.metric_definitions
SET    metric = 'infra.compute.ms',
       unit   = 'millisecond'
WHERE  module_id = '00000000-0000-0000-0000-000000000000'
  AND  metric    = 'infra.compute.walltime.ms';

-- (c) Restore infra.egress.bytes to 017's seeded per-byte placeholder price.
UPDATE ms_billing.metric_definitions
SET    unit_price_micros = 1
WHERE  module_id = '00000000-0000-0000-0000-000000000000'
  AND  metric    = 'infra.egress.bytes';
