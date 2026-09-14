-- Queries backing internal/account/budget.pgxStore (the per-app budget
-- engine). All operate on the ms_billing schema (migrations 014 + 015).
--
-- billing-engine is CANONICAL for budget config: api-platform writes a
-- budget over SetBudget and reads status/alerts back; it never touches
-- ms_billing SQL directly (trust boundary). v1 wires scope='app' only; the
-- enum carries 'org'/'account' for forward-compat (rejected in Go for now).

-- UpsertBudget writes one budget, keyed (scope, scope_id). SetBudget calls
-- it; a re-set updates limit/alert_percents/active in place (account_id is
-- updated too so a lazy budget backfills its account on conversion). Returns
-- the row so the caller can echo the persisted (deduped+sorted) percents.
-- name: UpsertBudget :one
INSERT INTO ms_billing.budgets (
    scope, scope_id, category, template_key, account_id, limit_micros, alert_percents, active, hard_cap, allow_overage
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
)
ON CONFLICT (scope, scope_id, category, template_key)
DO UPDATE SET
    account_id     = EXCLUDED.account_id,
    limit_micros   = EXCLUDED.limit_micros,
    alert_percents = EXCLUDED.alert_percents,
    active         = EXCLUDED.active,
    hard_cap       = EXCLUDED.hard_cap,
    allow_overage  = EXCLUDED.allow_overage
RETURNING *;

-- GetBudget resolves the budget for a (scope, scope_id), or no row when none
-- exists. The ingest-path hook uses it to skip evaluation when an app has no
-- budget; GetBudgetStatus uses it to read the cap + thresholds.
-- name: GetBudget :one
SELECT *
FROM ms_billing.budgets
WHERE scope = $1 AND scope_id = $2 AND category = $3 AND template_key = $4;

-- AppPeriodSpendMicros sums the app's current-period spend in micro-dollars:
-- Σ usage_events.value × metric_definitions.unit_price_micros for the app in
-- [period_start, period_end). The LEFT JOIN + COALESCE(...,0) treats an
-- undeclared/deleted metric's NULL price as zero contribution (cost-safe).
-- Returns a NUMERIC the caller decodes with microsFromNumeric (the same
-- single-rounding-point helper CurrentPeriodUsage uses). This is the SAME
-- spend window GetUsageSummary shows the user (current calendar month).
--
-- 🔴 dev_served EVENTS ARE EXCLUDED (migration 073). A budget is a statement
-- about MONEY THE APP WILL BE ASKED FOR, and tunnel-served usage is never
-- charged, so counting it would burn a production budget — and fire the
-- threshold alerts an owner reads as "we are overspending" — because a
-- developer spent an afternoon exercising a paid meter on a laptop. The
-- exclusion is here rather than in the ingest hook so it also holds for the
-- periodic re-evaluation, which never sees an individual event.
-- name: AppPeriodSpendMicros :one
WITH base_events AS (
    SELECT
        module_id, metric, aggregation_key, subject,
        COALESCE(model, '') AS model,
        COALESCE(module_version, '') AS module_version,
        value
    FROM ms_billing.usage_events
    WHERE app_id = $1
      AND COALESCE(billable_at, recorded_at) >= $2
      AND COALESCE(billable_at, recorded_at) <  $3
      AND dev_served = false
),
billable_events AS (
    SELECT module_id, metric, model, module_version, value AS billable_value
    FROM base_events
    WHERE aggregation_key IS DISTINCT FROM 'subject'
    UNION ALL
    SELECT
        module_id, metric, model, module_version,
        MAX(value)::numeric AS billable_value
    FROM base_events
    WHERE aggregation_key = 'subject'
    GROUP BY module_id, metric, model, module_version, subject
)
SELECT COALESCE(SUM(e.billable_value * COALESCE(md.unit_price_micros, 0)), 0)::numeric AS total_raw_cost_micros
FROM billable_events e
LEFT JOIN ms_billing.metric_definitions md
    ON md.module_id = e.module_id AND md.metric = e.metric;

-- AppAccountActivatedAt resolves the billing-period ANCHOR (migration 025) for an
-- APP-scoped budget: the payer account's activated_at, found via the app's own
-- usage. A budget is keyed by app_id only, but the anchor lives on the paying
-- account, so we resolve app → account through the app's most-recent attributed
-- usage_event (account_id IS NOT NULL) and read that account's activated_at. This
-- agrees with the ingest-path budget window, which anchors on the SAME payer
-- account. No attributed usage yet (or a NULL anchor) → the Go layer falls back to
-- AppPeriodAISpendMicros is AppPeriodSpendMicros restricted to the app's AI
-- events (infra.ai.*) and priced the way the bill prices them (migration 018):
-- the per-(metric, model) row of metric_model_prices is authoritative when the
-- event carries a model, else the catalog row keyed by the event's module_id
-- (the platform-infra sentinel for RecordInfraUsage events), else 0. A RETIRED
-- model row (active = false) still prices at its own rate here: this feeds a
-- CAP check, where pricing a retired model at a cheaper fallback would let
-- spend slip under the ceiling — the rollup separately fails loud on it.
-- Same window, same dev_served exclusion, same subject-aggregation shape as
-- AppPeriodSpendMicros so category='ai' and category='all' agree on the events.
-- @template_key '' = the app's whole AI spend; a key = that template's events
-- only (usage_events.template_key, stamped by api-platform from the
-- conversation — migration 084).
-- name: AppPeriodAISpendMicros :one
WITH base_events AS (
    SELECT
        module_id, metric, aggregation_key, subject,
        COALESCE(model, '') AS model,
        value
    FROM ms_billing.usage_events
    WHERE app_id = $1
      AND COALESCE(billable_at, recorded_at) >= $2
      AND COALESCE(billable_at, recorded_at) <  $3
      AND dev_served = false
      AND metric LIKE 'infra.ai.%'
      AND (@template_key::text = '' OR template_key = @template_key::text)
),
billable_events AS (
    SELECT module_id, metric, model, value AS billable_value
    FROM base_events
    WHERE aggregation_key IS DISTINCT FROM 'subject'
    UNION ALL
    SELECT
        module_id, metric, model,
        MAX(value)::numeric AS billable_value
    FROM base_events
    WHERE aggregation_key = 'subject'
    GROUP BY module_id, metric, model, subject
)
SELECT COALESCE(SUM(e.billable_value * COALESCE(mp.unit_price_micros, md.unit_price_micros, 0)), 0)::numeric AS total_raw_cost_micros
FROM billable_events e
LEFT JOIN ms_billing.metric_model_prices mp
    ON mp.metric = e.metric AND mp.model = e.model AND e.model <> ''
LEFT JOIN ms_billing.metric_definitions md
    ON md.module_id = e.module_id AND md.metric = e.metric;

-- AccountPeriodAISpendMicros is the account-level twin of AppPeriodAISpendMicros
-- (scenario 2: the console operator agent budgeted per personal account or per
-- org, whose events carry account_id). Same window, exclusions and per-model
-- pricing.
-- name: AccountPeriodAISpendMicros :one
WITH base_events AS (
    SELECT
        module_id, metric, aggregation_key, subject,
        COALESCE(model, '') AS model,
        value
    FROM ms_billing.usage_events
    WHERE account_id = $1
      AND COALESCE(billable_at, recorded_at) >= $2
      AND COALESCE(billable_at, recorded_at) <  $3
      AND dev_served = false
      AND metric LIKE 'infra.ai.%'
),
billable_events AS (
    SELECT module_id, metric, model, value AS billable_value
    FROM base_events
    WHERE aggregation_key IS DISTINCT FROM 'subject'
    UNION ALL
    SELECT
        module_id, metric, model,
        MAX(value)::numeric AS billable_value
    FROM base_events
    WHERE aggregation_key = 'subject'
    GROUP BY module_id, metric, model, subject
)
SELECT COALESCE(SUM(e.billable_value * COALESCE(mp.unit_price_micros, md.unit_price_micros, 0)), 0)::numeric AS total_raw_cost_micros
FROM billable_events e
LEFT JOIN ms_billing.metric_model_prices mp
    ON mp.metric = e.metric AND mp.model = e.model AND e.model <> ''
LEFT JOIN ms_billing.metric_definitions md
    ON md.module_id = e.module_id AND md.metric = e.metric;

-- BudgetOrgAccountID resolves an org-scoped budget to the org's own billing
-- account (the account whose events the console agent bills in an org-context
-- conversation). No row = the org has no account yet (a lazy org): its AI
-- spend is 0 and its anchor is the calendar month.
-- name: BudgetOrgAccountID :one
SELECT id
FROM ms_billing.accounts
WHERE owner_kind = 'org' AND owner_org_id = $1
LIMIT 1;

-- anchor day 1 (UTC calendar month). Returns at most one row.
-- name: AppAccountActivatedAt :one
SELECT a.activated_at
FROM ms_billing.accounts a
WHERE a.id = (
    SELECT e.account_id
    FROM ms_billing.usage_events e
    WHERE e.app_id = $1 AND e.account_id IS NOT NULL
    ORDER BY COALESCE(e.billable_at, e.recorded_at) DESC
    LIMIT 1
);

-- InsertBudgetAlert records one threshold crossing, idempotent on
-- (budget_id, period_start, percent). ON CONFLICT DO NOTHING makes a
-- re-evaluation of the same crossing a no-op. :execrows so the caller can
-- tell a fresh crossing (1) from an already-recorded one (0).
-- name: InsertBudgetAlert :execrows
INSERT INTO ms_billing.budget_alerts (
    budget_id, period_start, percent, spend_micros, limit_micros
) VALUES (
    $1, $2, $3, $4, $5
)
ON CONFLICT (budget_id, period_start, percent) DO NOTHING;

-- ListBudgetAlerts returns a budget's recorded crossings for a period,
-- ordered by threshold. Backs GetBudgetAlerts (api-platform in-app display).
-- name: ListBudgetAlerts :many
SELECT id, budget_id, period_start, percent, spend_micros, limit_micros, fired_at
FROM ms_billing.budget_alerts
WHERE budget_id = $1 AND period_start = $2
ORDER BY percent;
