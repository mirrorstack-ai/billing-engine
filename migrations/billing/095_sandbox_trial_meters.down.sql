-- 095 down: drop the sandbox budget rows the narrower CHECK cannot hold, the
-- CHECKs, the index, the columns and the two meters. The usage events stay in
-- the ledger (still dev_served, so still never charged) and lose only their tags.
DELETE FROM ms_billing.budgets WHERE category = 'sandbox';

ALTER TABLE ms_billing.budgets
    DROP CONSTRAINT IF EXISTS budgets_sandbox_org_or_account_only,
    DROP CONSTRAINT IF EXISTS budgets_category_known;
ALTER TABLE ms_billing.budgets
    ADD CONSTRAINT budgets_category_known CHECK (category IN ('all', 'ai', 'exposure'));

DROP INDEX IF EXISTS ms_billing.usage_events_sandbox_trial_idx;

ALTER TABLE ms_billing.usage_events
    DROP CONSTRAINT IF EXISTS usage_events_run_id_needs_source,
    DROP CONSTRAINT IF EXISTS usage_events_sandbox_trial_shape,
    DROP CONSTRAINT IF EXISTS usage_events_source_known;
ALTER TABLE ms_billing.usage_events
    DROP COLUMN IF EXISTS stamped_price_micros,
    DROP COLUMN IF EXISTS run_id,
    DROP COLUMN IF EXISTS source;

DELETE FROM ms_billing.metric_definitions
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN ('infra.sandbox.vcpu.hours', 'infra.sandbox.memory.gib_hours');
