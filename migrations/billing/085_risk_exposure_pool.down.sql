-- 085 down: drop the system exposure rows the narrower CHECK cannot hold, the
-- CHECK, and the curve table.
DELETE FROM ms_billing.budgets WHERE category = 'exposure';
ALTER TABLE ms_billing.budgets
    DROP CONSTRAINT IF EXISTS budgets_category_known;
ALTER TABLE ms_billing.budgets
    ADD CONSTRAINT budgets_category_known CHECK (category IN ('all', 'ai'));
DROP TABLE IF EXISTS ms_billing.risk_ramp_config;
