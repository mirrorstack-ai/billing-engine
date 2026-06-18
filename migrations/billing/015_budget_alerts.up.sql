-- Migration 015 — recorded budget threshold crossings (alert state).
--
-- One row per (budget, period, percent) threshold that has FIRED. Inserted
-- by the best-effort budget evaluation on the RecordUsage ingest path when
-- the app's current-period spend first crosses a threshold (design
-- docs-temp/budget-alerts/design.md §5 / §10).
--
-- IDEMPOTENCY: the UNIQUE(budget_id, period_start, percent) key is the
-- idempotency anchor — a threshold fires AT MOST ONCE per period. Evaluation
-- inserts ON CONFLICT DO NOTHING, so re-evaluating the same spend (every
-- subsequent usage event in the period) records nothing new. This is how the
-- ingest-path hook stays cheap and exactly-once per crossing without a
-- running counter.
--
-- spend_micros / limit_micros snapshot the spend and cap AT crossing time, so
-- a later limit edit can't rewrite the historical alert. Money is BIGINT
-- micro-dollars, never float.
--
-- billing-engine RECORDS crossings here and exposes them via GetBudgetAlerts
-- for api-platform to read (in-app display). billing NEVER sends mail — email
-- delivery is an api-platform follow-up (design §10).
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#budget_alerts

CREATE TABLE IF NOT EXISTS ms_billing.budget_alerts (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- The budget whose threshold fired. ON DELETE CASCADE drops a deleted
    -- budget's alert history.
    budget_id    UUID NOT NULL REFERENCES ms_billing.budgets(id) ON DELETE CASCADE,

    -- Start of the period the crossing occurred in (first-of-month 00:00 UTC,
    -- the same window GetUsageSummary / evaluation use). Part of the
    -- idempotency key so the same threshold can fire again in the NEXT period.
    period_start TIMESTAMPTZ NOT NULL,

    -- The threshold percentage that was crossed (one of the budget's
    -- alert_percents).
    percent      INT NOT NULL,

    -- Snapshot of spend + cap at crossing time, micro-dollars.
    spend_micros BIGINT NOT NULL,
    limit_micros BIGINT NOT NULL,

    fired_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- A threshold fires at most once per period — the ingest-path idempotency
    -- key. Evaluation inserts ON CONFLICT DO NOTHING against this.
    CONSTRAINT budget_alerts_budget_period_percent_key UNIQUE (budget_id, period_start, percent)
);

-- Per-budget period scan: GetBudgetAlerts lists a budget's crossings for a
-- period window.
CREATE INDEX IF NOT EXISTS budget_alerts_budget_period_idx
    ON ms_billing.budget_alerts (budget_id, period_start);
