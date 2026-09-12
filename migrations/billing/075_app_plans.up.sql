-- 075: the per-app billing plan (core-v2#1412, billing-engine#202 PR-1).
--
-- Every app is priced from ITS OWN plan — free | pro | business — instead of one
-- account-level default. This column records only WHICH plan an app is on. The
-- plan TERMS (base fee, included modules and domains, the 部署費用 allowance) are
-- reviewed Go constants in internal/account/usage/plans.go, deliberately NOT rows
-- here: a price is a reviewed code change like every other billing tunable, and a
-- second copy in a table would be a second source of truth.
--
-- Expand-only. Every existing row becomes 'pro', whose base fee is the flat $20
-- every app paid before plans, so no bill changes at cut-over. Until the
-- charge legs are plan-aware (billing-engine#202 PR-2),
-- SetAppPlan accepts only 'pro' — see cycle.SetAppPlan for why.
ALTER TABLE ms_billing.apps
    ADD COLUMN IF NOT EXISTS plan TEXT NOT NULL DEFAULT 'pro'
        CONSTRAINT apps_plan_known CHECK (plan IN ('free', 'pro', 'business'));
