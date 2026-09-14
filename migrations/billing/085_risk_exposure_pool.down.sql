-- 085 down: drop the system exposure rows the narrower CHECK cannot hold, the
-- CHECK, and the curve table.
DELETE FROM ms_billing.budgets WHERE category = 'exposure';
ALTER TABLE ms_billing.budgets
    DROP CONSTRAINT IF EXISTS budgets_category_known;
ALTER TABLE ms_billing.budgets
    ADD CONSTRAINT budgets_category_known CHECK (category IN ('all', 'ai'));
DROP INDEX IF EXISTS ms_billing.usage_events_app_billable_metric_idx;
DROP INDEX IF EXISTS ms_billing.usage_events_account_billable_metric_idx;
DROP TABLE IF EXISTS ms_billing.risk_ramp_config;
