-- Migration 014 — customer spending budgets (per-scope caps + alert thresholds).
--
-- A customer-set spending cap with threshold ALERTS, so there are no
-- surprise bills (design docs-temp/budget-alerts/design.md §3 / §4 / §10).
-- billing-engine is CANONICAL for budget config — it owns the only complete
-- spend picture (metered usage today; the app fee + infra fold in once the
-- meter charge PRs #5/#6 and infra PR #10 land). api-platform writes a
-- budget over the internal-secret billing client (SetBudget) and reads
-- status back (GetBudgetStatus / GetBudgetAlerts) for in-app display; it
-- never reads ms_billing SQL directly (trust boundary).
--
-- SCOPE: the enum carries 'org' and 'account' for forward-compat, but v1
-- WIRES ONLY scope='app' — the service rejects org/account budgets with
-- INVALID_INPUT until those scopes are implemented (design §10). Generalizing
-- to org/account is additive: store their limits the same way and sync through
-- the same SetBudget RPC.
--
-- EVALUATION: runs best-effort on the RecordUsage ingest path. After a usage
-- event is inserted, the app's current-period spend is recomputed (Σ
-- usage_events.value × metric_definitions.unit_price_micros for that app over
-- the current calendar-month window — the SAME window GetUsageSummary shows)
-- and any newly-crossed threshold is recorded in budget_alerts (migration
-- 015), idempotent per period+percent. A budget-eval error never fails the
-- usage ingest.
--
-- v1 is ALERT-ONLY (soft): crossing a threshold records an alert; it never
-- stops metered work mid-cycle. The hard-stop (action='cap') is a later phase
-- (design §6), so this table carries no action column yet.
--
-- DELIBERATE DEFERRALS vs design §4: the spec lists a budget_action
-- ENUM('alert','cap') column and a period_anchor column. Both are omitted in
-- v1. action is alert-only (above). period_anchor is unused because evaluation
-- keys on the app payer's current ANCHORED period (the card-binding-day window —
-- ADR 0005 — the SAME window GetUsageSummary shows); the per-scope period_anchor
-- column folds in with the authoritative billing_periods rollup (meter charge PRs
-- #5/#6). Both are additive when those phases land.
--
-- Money is BIGINT micro-dollars (1e-6 USD), never float (the ms_billing
-- money convention). limit_micros mirrors the platform-side budget value
-- (cents ×10⁴).
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#budgets

CREATE TYPE ms_billing.budget_scope AS ENUM (
    'app',
    'org',
    'account'
);

CREATE TABLE IF NOT EXISTS ms_billing.budgets (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- The dimension the cap applies to. v1 wires 'app' only; 'org'/'account'
    -- exist for forward-compat and are rejected by the service for now.
    scope          ms_billing.budget_scope NOT NULL,

    -- The id of the scoped entity (app_id for scope='app'). Soft FK — the
    -- target lives in another schema (ms_applications) and is platform-owned.
    scope_id       UUID NOT NULL,

    -- Owner billing account, for cascade cleanup + future per-account rollup.
    -- NULLABLE: an app's owner may not have a billing account yet (lazy —
    -- same rationale as usage_events.account_id). ON DELETE CASCADE drops a
    -- deleted account's budgets.
    account_id     UUID NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,

    -- The spending cap in micro-dollars. The 100% threshold is measured
    -- against this. Non-negative, and capped at 1e15 micros (= $1B) so the
    -- threshold math (the per-threshold target limit×percent) can never
    -- overflow int64: 1e15 × 100 = 1e17 is two orders of magnitude under
    -- math.MaxInt64 (~9.2e18). The crossing comparison never multiplies the
    -- (unbounded) spend, so only the limit needs bounding. No real per-app
    -- budget approaches $1B; the ceiling exists purely to keep the integer
    -- crossing comparison total.
    limit_micros   BIGINT NOT NULL CHECK (limit_micros >= 0 AND limit_micros <= 1000000000000000),

    -- Threshold percentages that fire an alert as spend approaches the cap.
    -- Default {80,100}: warn at 80% of the cap and again at the cap. The
    -- service validates each percent is 1..100 and stores them deduped+sorted.
    alert_percents INT[] NOT NULL DEFAULT '{80,100}',

    -- Soft on/off — an inactive budget is not evaluated.
    active         BOOLEAN NOT NULL DEFAULT true,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One budget per scoped entity. SetBudget upserts on this key.
    CONSTRAINT budgets_scope_scope_id_key UNIQUE (scope, scope_id)
);

-- Auto-maintained updated_at, matching the ms_billing convention
-- (ms_billing.set_updated_at ships in migration 001).
CREATE TRIGGER budgets_set_updated_at
BEFORE UPDATE ON ms_billing.budgets
FOR EACH ROW
EXECUTE FUNCTION ms_billing.set_updated_at();
