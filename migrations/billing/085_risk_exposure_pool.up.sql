-- 085: the PaaS risk-exposure pool the AI gate refuses over (T101 PR-B;
-- owner 2026-09-14).
--
-- PaaS ("standard" billing_mode) is pay-AFTER-use: nothing is pre-funded, so
-- the platform's exposure is bounded by a risk-graded limit. The owner ruled
-- that this limit REFUSES AI turns live (v1: AI only; other paid work keeps
-- settling at cycle close) with a curve of their own: strict at first, then
-- flattening —
--
--     no verified card            → no_card_micros           ($5)
--     verified card, k paid inv.  → card_base_micros × √(1+k) ($10, $14.1, $17.3, $20 …)
--     never above                 → ceiling_micros            (~$200)
--
-- The constants are FINANCE-OWNED and live here, in one row, so they are
-- tuned by an UPDATE and never by a code literal (collection.go's trust ramp
-- keeps its placeholders for the cycle-close judge; a later change unifies
-- them). accounts.credit_limit_micros is deliberately NOT reused: credits mode
-- writes its $5 wallet default into that column (SetCreditAccountBillingMode).
--
-- Alerts before the cliff: the pool is exposed through a per-account SYSTEM
-- budget row of category 'exposure' (scope 'account', template_key '',
-- hard_cap true), upserted by billing-engine whenever the pool is evaluated,
-- so budget_alerts records the 80% / 100% crossings exactly like a customer
-- budget and the console can warn BEFORE the assistant stops for every member
-- at once (94's review note: with a $5 floor the 80% notice is the only
-- warning anyone gets).
CREATE TABLE IF NOT EXISTS ms_billing.risk_ramp_config (
    id                 SMALLINT PRIMARY KEY DEFAULT 1 CONSTRAINT risk_ramp_config_singleton CHECK (id = 1),
    no_card_micros     BIGINT NOT NULL CONSTRAINT risk_ramp_no_card_nonneg CHECK (no_card_micros >= 0),
    card_base_micros   BIGINT NOT NULL CONSTRAINT risk_ramp_card_base_nonneg CHECK (card_base_micros >= 0),
    ceiling_micros     BIGINT NOT NULL CONSTRAINT risk_ramp_ceiling_nonneg CHECK (ceiling_micros >= 0),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO ms_billing.risk_ramp_config (id, no_card_micros, card_base_micros, ceiling_micros)
VALUES (1, 5000000, 10000000, 200000000)
ON CONFLICT (id) DO NOTHING;

COMMENT ON TABLE ms_billing.risk_ramp_config IS
    'The PaaS exposure curve (owner 2026-09-14): no card → no_card_micros; verified card with k paid invoices → card_base_micros × sqrt(1+k); capped at ceiling_micros. Finance-owned; tune by UPDATE.';

ALTER TABLE ms_billing.budgets
    DROP CONSTRAINT IF EXISTS budgets_category_known;
ALTER TABLE ms_billing.budgets
    ADD CONSTRAINT budgets_category_known CHECK (category IN ('all', 'ai', 'exposure'));

COMMENT ON COLUMN ms_billing.budgets.category IS
    'Which spend the cap measures: all (every usage event), ai (infra.ai.* only, priced per model), or exposure (the SYSTEM row billing-engine maintains per PaaS account so budget_alerts can record crossings of the risk-exposure pool; never written by a customer).';
