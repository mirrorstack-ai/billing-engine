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
    scope, scope_id, account_id, limit_micros, alert_percents, active
) VALUES (
    $1, $2, $3, $4, $5, $6
)
ON CONFLICT (scope, scope_id)
DO UPDATE SET
    account_id     = EXCLUDED.account_id,
    limit_micros   = EXCLUDED.limit_micros,
    alert_percents = EXCLUDED.alert_percents,
    active         = EXCLUDED.active
RETURNING id, scope, scope_id, account_id, limit_micros, alert_percents, active, created_at, updated_at;

-- GetBudget resolves the budget for a (scope, scope_id), or no row when none
-- exists. The ingest-path hook uses it to skip evaluation when an app has no
-- budget; GetBudgetStatus uses it to read the cap + thresholds.
-- name: GetBudget :one
SELECT id, scope, scope_id, account_id, limit_micros, alert_percents, active, created_at, updated_at
FROM ms_billing.budgets
WHERE scope = $1 AND scope_id = $2;

-- AppPeriodSpendMicros sums the app's current-period spend in micro-dollars:
-- Σ usage_events.value × metric_definitions.unit_price_micros for the app in
-- [period_start, period_end). The LEFT JOIN + COALESCE(...,0) treats an
-- undeclared/deleted metric's NULL price as zero contribution (cost-safe).
-- Returns a NUMERIC the caller decodes with microsFromNumeric (the same
-- single-rounding-point helper CurrentPeriodUsage uses). This is the SAME
-- spend window GetUsageSummary shows the user (current calendar month).
-- name: AppPeriodSpendMicros :one
SELECT COALESCE(SUM(e.value * COALESCE(md.unit_price_micros, 0)), 0)::numeric AS total_raw_cost_micros
FROM ms_billing.usage_events e
LEFT JOIN ms_billing.metric_definitions md
    ON md.module_id = e.module_id AND md.metric = e.metric
WHERE e.app_id = $1
  AND e.recorded_at >= $2
  AND e.recorded_at <  $3;

-- AppAccountActivatedAt resolves the billing-period ANCHOR (migration 025) for an
-- APP-scoped budget: the payer account's activated_at, found via the app's own
-- usage. A budget is keyed by app_id only, but the anchor lives on the paying
-- account, so we resolve app → account through the app's most-recent attributed
-- usage_event (account_id IS NOT NULL) and read that account's activated_at. This
-- agrees with the ingest-path budget window, which anchors on the SAME payer
-- account. No attributed usage yet (or a NULL anchor) → the Go layer falls back to
-- anchor day 1 (UTC calendar month). Returns at most one row.
-- name: AppAccountActivatedAt :one
SELECT a.activated_at
FROM ms_billing.accounts a
WHERE a.id = (
    SELECT e.account_id
    FROM ms_billing.usage_events e
    WHERE e.app_id = $1 AND e.account_id IS NOT NULL
    ORDER BY e.recorded_at DESC
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
