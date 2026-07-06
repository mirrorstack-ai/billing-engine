-- 036 down: drop the charge-attempt recovery markers.

ALTER TABLE ms_billing.app_module_overage_timers
    DROP COLUMN IF EXISTS charge_attempted_at;

ALTER TABLE ms_billing.apps
    DROP COLUMN IF EXISTS proration_attempted_at;
