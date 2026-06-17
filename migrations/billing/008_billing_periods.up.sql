-- Migration 008 — first-class billing period rows.
--
-- Milestone D, Axis 3. A billing period is the window a rollup
-- aggregates and an invoice covers. Making it a row (vs deriving the
-- window on the fly) gives the rollup a stable target to attach
-- usage_aggregates to and a status lifecycle (design §4 Axis 3:
-- "billing_periods is a first-class row, open→closing→invoiced").
--
--   open      currently accumulating usage_events
--   closing   window ended; rollup in progress (PR #5)
--   invoiced  rolled up + charged (PR #6)
--
-- This PR ships the table only — the schema lands clean ahead of its
-- writer. No PR #3 query touches it: GetUsageSummary's live estimate uses
-- a calendar-month-to-date window, and the OpenPeriodForAccount upsert +
-- the closing/invoiced transitions are driven by cmd/billing-cycle in
-- later PRs (#5/#6), shipping with their first caller.
--
-- UNIQUE(account_id, period_start) makes "the period that starts at T
-- for this account" idempotent — a re-run of the period-open path
-- returns the same row instead of duplicating it.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#billing_periods

CREATE TYPE ms_billing.billing_period_status AS ENUM (
    'open',
    'closing',
    'invoiced'
);

CREATE TABLE IF NOT EXISTS ms_billing.billing_periods (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id   UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,
    period_start TIMESTAMPTZ NOT NULL,
    period_end   TIMESTAMPTZ NOT NULL,
    status       ms_billing.billing_period_status NOT NULL DEFAULT 'open',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT billing_periods_account_start_key UNIQUE (account_id, period_start)
);

-- The ON DELETE CASCADE FK benefits from an index on the referencing
-- column to avoid a seq scan on parent-row deletes; it also serves the
-- "current period for account" lookup.
CREATE INDEX IF NOT EXISTS billing_periods_account_idx
    ON ms_billing.billing_periods (account_id);
