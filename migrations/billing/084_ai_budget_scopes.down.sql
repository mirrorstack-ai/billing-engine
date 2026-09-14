-- 084 down: drop the category/template rows the old unique key cannot hold,
-- then the columns, then restore the (scope, scope_id) key.
ALTER TABLE ms_billing.usage_events
    DROP COLUMN IF EXISTS template_key;
DELETE FROM ms_billing.budgets WHERE category <> 'all' OR template_key <> '';
ALTER TABLE ms_billing.budgets
    DROP CONSTRAINT IF EXISTS budgets_scope_scope_id_category_template_key,
    DROP CONSTRAINT IF EXISTS budgets_template_key_ai_app_only;
ALTER TABLE ms_billing.budgets
    DROP COLUMN IF EXISTS category,
    DROP COLUMN IF EXISTS template_key,
    DROP COLUMN IF EXISTS hard_cap,
    DROP COLUMN IF EXISTS allow_overage;
ALTER TABLE ms_billing.budgets
    ADD CONSTRAINT budgets_scope_scope_id_key UNIQUE (scope, scope_id);
