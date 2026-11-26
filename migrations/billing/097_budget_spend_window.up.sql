-- 097: a budget's spend WINDOW (billing-engine#378).
--
-- Every budget so far measured its spend over the scope's anchored billing
-- period (billingperiod.AnchoredPeriodWindow). A per-day cap ("at most US$3 of
-- AI a day") has nowhere to live in that shape, so a row now names its window:
--
--   period  the anchored billing period — what every row meant until now.
--   day     the UTC calendar day: [00:00 UTC, next 00:00 UTC). The spend, the
--           exhausted verdict and the alert marks restart at 00:00 UTC, and
--           budget_alerts.period_start holds the day's start, so each day's
--           crossings are recorded once.
--
-- Expand-only: one defaulted column. Existing rows read 'period', exactly what
-- they were, and an INSERT that omits the column (a binary built before this
-- migration; sqlc spells the column list out, so its reads are unaffected)
-- still writes 'period'. Adding a NOT NULL column with a constant default is
-- metadata-only on Postgres 11+; the CHECK validates the existing rows once.
ALTER TABLE ms_billing.budgets
    ADD COLUMN IF NOT EXISTS spend_window TEXT NOT NULL DEFAULT 'period'
        CONSTRAINT budgets_spend_window_known CHECK (spend_window IN ('period', 'day'));

COMMENT ON COLUMN ms_billing.budgets.spend_window IS
    'The span the cap measures spend over: period (the scope''s anchored billing period, the default) or day (the UTC calendar day, resetting at 00:00 UTC).';
