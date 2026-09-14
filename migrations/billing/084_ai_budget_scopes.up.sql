-- 084: AI budgets that ENFORCE (T101 follow-up; owner decisions 2026-09-14).
--
-- ms_billing.budgets (014) held ONE alert-only cap per app over all of its
-- spend. Two AI scenarios need caps that a consumer REFUSES work over:
--   1. the ai-assistant module in a custom app — a cap PER TEMPLATE (and an
--      app-wide AI row beneath it), written by the app owner from the console;
--   2. the console operator agent — a cap per ORG (an org-context conversation)
--      or per PERSONAL ACCOUNT, hard stop by default with an opt-in overage.
-- Four additive columns express both without a second table or unit:
--
--   category      which spend the cap measures — 'all' (every usage event,
--                 the existing row) or 'ai' (infra.ai.* events only, priced
--                 per model like the bill).
--   template_key  the ai-assistant template the cap is for ('' = the scope's
--                 AI-wide row). TEXT: templates are keyed ^[a-z][a-z0-9_-]*$
--                 in the module, not uuids. One row per
--                 (scope, scope_id, category, template_key).
--   hard_cap      false = alert-only (today's behaviour); true = the status
--                 read reports `exhausted` at spend >= limit and the consumer
--                 refuses further work (the agent gate). billing-engine never
--                 stops metered work itself — usage already incurred is
--                 always recorded.
--   allow_overage the customer's own opt-out of a hard cap (scenario 2):
--                 true = alerts still fire, `exhausted` stays false. It never
--                 lifts the account's risk-graded exposure limit (a later
--                 migration), which is finance-owned.
--
-- usage_events.template_key carries the template an infra.ai.* event was
-- produced under, stamped by api-platform from the conversation (never from a
-- per-turn client value), so a per-template cap can sum its own spend.
--
-- Expand-only: existing budget rows become category='all', template_key='',
-- hard_cap=false, allow_overage=false — exactly what they were.
ALTER TABLE ms_billing.budgets
    ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'all'
        CONSTRAINT budgets_category_known CHECK (category IN ('all', 'ai')),
    ADD COLUMN IF NOT EXISTS template_key TEXT NOT NULL DEFAULT ''
        CONSTRAINT budgets_template_key_shape CHECK (template_key = '' OR template_key ~ '^[a-z][a-z0-9_-]*$'),
    ADD COLUMN IF NOT EXISTS hard_cap BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS allow_overage BOOLEAN NOT NULL DEFAULT false;

-- A template cap is an AI cap on an app: no template rows under 'all' or on
-- org/account scopes.
ALTER TABLE ms_billing.budgets
    ADD CONSTRAINT budgets_template_key_ai_app_only
        CHECK (template_key = '' OR (category = 'ai' AND scope = 'app'));

ALTER TABLE ms_billing.budgets
    DROP CONSTRAINT IF EXISTS budgets_scope_scope_id_key;
ALTER TABLE ms_billing.budgets
    ADD CONSTRAINT budgets_scope_scope_id_category_template_key UNIQUE (scope, scope_id, category, template_key);

ALTER TABLE ms_billing.usage_events
    ADD COLUMN IF NOT EXISTS template_key TEXT NULL;

COMMENT ON COLUMN ms_billing.budgets.category IS
    'Which spend the cap measures: all (every usage event) or ai (infra.ai.* only, priced per model). One row per (scope, scope_id, category, template_key).';
COMMENT ON COLUMN ms_billing.budgets.template_key IS
    'ai-assistant template this AI cap is for; '''' = the scope''s AI-wide row. Text key as the module stores it.';
COMMENT ON COLUMN ms_billing.budgets.hard_cap IS
    'false = alert-only; true = GetBudgetStatus reports exhausted at spend >= limit and the consumer refuses further work (the agent gate).';
COMMENT ON COLUMN ms_billing.budgets.allow_overage IS
    'Customer opt-out of a hard cap: alerts still fire, exhausted stays false. Never lifts the risk-graded exposure limit.';
COMMENT ON COLUMN ms_billing.usage_events.template_key IS
    'ai-assistant template an infra.ai.* event was produced under (stamped by api-platform from the conversation); NULL for every other event.';
