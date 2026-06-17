-- Migration 009 — period-rolled usage aggregates (the billable record).
--
-- Milestone D, Axis 3. One row per (period, app, module, metric): the
-- snapshotted, reproducible billable unit a closed invoice is built
-- from. The raw usage_events stay pure; THIS is where pricing is
-- frozen so a closed invoice never changes (design §4 Axis 3).
--
-- ============================================================
-- PRICING MODEL — READ BEFORE TOUCHING THIS TABLE
-- ============================================================
-- charge = billable_quantity × unit_price_micros × num / den.
--
-- For a third-party CUSTOM metric the price is the developer's DECLARED
-- per-unit customer price applied DIRECTLY: the multiplier is 10/10 (= 1×,
-- NO markup). This is the default this migration ships. The platform's cut
-- on third-party usage is the dev-side margin-share at SETTLEMENT
-- (margin_share_class, below), NOT a customer markup.
--
-- The flat 12/10 (= 1.2×) markup applies ONLY to platform-infra / built-in
-- metrics, whose prices the platform sets (= cost × 1.2). Those rows (and
-- the 12/10 multiplier they carry) arrive in PR #5/#10; this PR ships only
-- the table shape + custom-metric default (10/10). There is NO blanket
-- customer markup and NO private/published CUSTOMER markup.
--
-- customer_markup_num/den default to 10/10 and are snapshotted per row so a
-- future rate change for a different metric class never rewrites a closed
-- invoice:
--   charged_micros = round_half_up( raw_cost_micros * num / den )
-- The arithmetic + the rollup writes (billable_quantity, raw_cost_micros,
-- charged_micros) land in PR #5 (cmd/billing-cycle); this PR ships the
-- table shape + the snapshot columns only.
--
-- margin_share_class is the DEVELOPER-SETTLEMENT dimension (private vs
-- published), NOT a customer markup. It governs only the developer's
-- margin-share rate at settlement (published 15% / private 30% of
-- income - infra); it never multiplies the customer charge. It is
-- carried on module_visibility (migration 010); it is named here to kill
-- any customer-markup framing. Nothing in this table applies
-- margin_share_class to charged_micros.
-- ============================================================
--
-- Money is micro-dollar BIGINT; quantity is NUMERIC (design §8).
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#usage_aggregates

-- Developer-settlement dimension (NOT a customer markup). Defined here
-- because usage_aggregates references it; module_visibility (010) is the
-- per-module carrier.
CREATE TYPE ms_billing.margin_share_class AS ENUM (
    'private',
    'published'
);

CREATE TABLE IF NOT EXISTS ms_billing.usage_aggregates (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    period_id          UUID NOT NULL REFERENCES ms_billing.billing_periods(id) ON DELETE CASCADE,
    account_id         UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,

    app_id             UUID NOT NULL,
    module_id          UUID NOT NULL,
    metric             TEXT NOT NULL,

    -- Snapshotted accumulation semantics for this aggregate.
    kind               ms_billing.metric_kind NOT NULL,

    -- Rolled-up quantity per kind (count/sum→SUM, peak→MAX,
    -- time_weighted→integral). Written by the PR #5 rollup; defaults 0.
    billable_quantity  NUMERIC NOT NULL DEFAULT 0 CHECK (billable_quantity >= 0),

    -- Per-unit CUSTOMER price snapshotted from metric_definitions at
    -- rollup. For custom metrics this is the developer's declared price.
    unit_price_micros  BIGINT NOT NULL DEFAULT 0 CHECK (unit_price_micros >= 0),

    -- Customer markup multiplier, snapshotted per row. Defaults 10/10 (= 1×,
    -- NO markup) — the custom-metric case. The 12/10 platform-infra case
    -- arrives in PR #5/#10. NOT a per-visibility multiplier — see the header.
    customer_markup_num INT NOT NULL DEFAULT 10 CHECK (customer_markup_num > 0),
    customer_markup_den INT NOT NULL DEFAULT 10 CHECK (customer_markup_den > 0),

    -- raw_cost = billable_quantity * unit_price (micro-dollars), pre-markup.
    raw_cost_micros    BIGINT NOT NULL DEFAULT 0 CHECK (raw_cost_micros >= 0),

    -- charged = round_half_up(raw_cost * num / den). The customer-billed
    -- amount. Written by the PR #5 rollup.
    charged_micros     BIGINT NOT NULL DEFAULT 0 CHECK (charged_micros >= 0),

    rolled_up_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One aggregate per (period, app, module, metric); a rollup re-run
    -- upserts the same row (ON CONFLICT) rather than duplicating.
    CONSTRAINT usage_aggregates_period_app_module_metric_key
        UNIQUE (period_id, app_id, module_id, metric)
);

-- FK index + per-period scan when assembling an invoice.
CREATE INDEX IF NOT EXISTS usage_aggregates_period_idx
    ON ms_billing.usage_aggregates (period_id);

CREATE INDEX IF NOT EXISTS usage_aggregates_account_idx
    ON ms_billing.usage_aggregates (account_id);
