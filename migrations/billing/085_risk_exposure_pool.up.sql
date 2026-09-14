-- 085: the PaaS risk-exposure pool the AI gate refuses over (T101 PR-B;
-- owner 2026-09-14).
--
-- PaaS ("standard" billing_mode) is pay-AFTER-use: nothing is pre-funded, so
-- the platform's exposure is bounded by a risk-graded limit. The owner ruled
-- that this limit REFUSES AI turns live (v1: AI only; other paid work keeps
-- settling at cycle close) with a curve of their own: strict at first, then
-- flattening —
--
--     no verified card            → no_card_micros              ($5)
--     verified card, k paid inv.  → card_base_micros + range_micros × (1 − e^(−k/tau))
--                                   (owner's FINAL shape 2026-09-14: $10 + $190 ×
--                                    (1 − e^(−k/5)) — $10 at k=0, $44 at k=1,
--                                    $130 at k=5, saturating toward $200)
--     never above                 → ceiling_micros              ($200, the hard clamp)
--
-- The SHAPE is config too (coordinator, PR-B review): base, range and tau
-- are columns, so the next tuning is an UPDATE.
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
-- ai_enforcement_paused is the INCIDENT KILL-SWITCH (coordinator, PR-B review):
-- read on every AI verdict, so flipping it through the SetAIEnforcementPaused
-- admin RPC pauses refusals platform-wide without a wave — the agent's own
-- enforcement flag is a provision-time env snapshot and cannot do that. While
-- paused every verdict is allowed and decided_by = 'paused'; alerts keep
-- recording so the pause leaves an audit trail of what WOULD have refused.
CREATE TABLE IF NOT EXISTS ms_billing.risk_ramp_config (
    id                    SMALLINT PRIMARY KEY DEFAULT 1 CONSTRAINT risk_ramp_config_singleton CHECK (id = 1),
    no_card_micros        BIGINT NOT NULL CONSTRAINT risk_ramp_no_card_nonneg CHECK (no_card_micros >= 0),
    card_base_micros      BIGINT NOT NULL CONSTRAINT risk_ramp_card_base_nonneg CHECK (card_base_micros >= 0),
    ceiling_micros        BIGINT NOT NULL CONSTRAINT risk_ramp_ceiling_nonneg CHECK (ceiling_micros >= 0),
    -- Saturating growth: range_micros is how far above the base the curve
    -- can climb, tau (in paid invoices) how fast it gets there — 63% of the
    -- range at k = tau, 95% at 3·tau.
    range_micros          BIGINT NOT NULL DEFAULT 190000000 CONSTRAINT risk_ramp_range_nonneg CHECK (range_micros >= 0),
    tau                   NUMERIC(6,3) NOT NULL DEFAULT 5.000 CONSTRAINT risk_ramp_tau_positive CHECK (tau > 0),
    -- Delinquency (owner's pick 2026-09-14): while an invoice is open /
    -- uncollectible with a balance the limit is the curve DIVIDED by
    -- delinquent_divisor, never below no_card_micros (the owner found a hard
    -- $5 cliff too harsh; a huge divisor reproduces it, 1 disables the
    -- reduction). Once settled each late invoice costs late_penalty_k paid
    -- invoices of trust: k_eff = max(0, paid − late_penalty_k × late)
    -- (late_penalty_k large = any late payment resets k).
    delinquent_divisor    INT NOT NULL DEFAULT 3 CONSTRAINT risk_ramp_divisor_min CHECK (delinquent_divisor >= 1),
    late_penalty_k        INT NOT NULL DEFAULT 2 CONSTRAINT risk_ramp_late_penalty_nonneg CHECK (late_penalty_k >= 0),
    ai_enforcement_paused BOOLEAN NOT NULL DEFAULT false,
    -- Who paused it, why, and since when — so "why was enforcement off for
    -- six hours" is answered from the row, not from archaeology. Cleared on
    -- resume.
    paused_reason         TEXT NULL,
    paused_by             TEXT NULL,
    paused_at             TIMESTAMPTZ NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO ms_billing.risk_ramp_config (id, no_card_micros, card_base_micros, ceiling_micros)
VALUES (1, 5000000, 10000000, 200000000)
ON CONFLICT (id) DO NOTHING;

COMMENT ON TABLE ms_billing.risk_ramp_config IS
    'The PaaS exposure curve (owner 2026-09-14): no card → no_card_micros; verified card with k paid invoices → card_base_micros + range_micros × (1 − exp(−k/tau)); capped at ceiling_micros. Delinquency: curve / delinquent_divisor (never below no_card_micros) while delinquent; k reduced by late_penalty_k per late invoice once settled. Finance-owned; tune by UPDATE.';

ALTER TABLE ms_billing.budgets
    DROP CONSTRAINT IF EXISTS budgets_category_known;
ALTER TABLE ms_billing.budgets
    ADD CONSTRAINT budgets_category_known CHECK (category IN ('all', 'ai', 'exposure'));

COMMENT ON COLUMN ms_billing.risk_ramp_config.ai_enforcement_paused IS
    'Incident kill-switch: true = every AI budget verdict is allowed (decided_by paused) while alerts keep recording. Flip via the SetAIEnforcementPaused admin RPC; no deploy needed.';

-- Covering indexes for the per-event spend sums the AI caps and the exposure
-- pool run on every ai status read and every infra.ai.* ingest (084/085):
-- the predicates are app_id / account_id + COALESCE(billable_at, recorded_at)
-- range + metric, so the expression is indexed as the queries spell it —
-- the existing (…, metric, recorded_at) / occurrence indexes do not match
-- the COALESCE and put module_id in the middle of the app key.
CREATE INDEX IF NOT EXISTS usage_events_app_billable_metric_idx
    ON ms_billing.usage_events (app_id, (COALESCE(billable_at, recorded_at)), metric);
CREATE INDEX IF NOT EXISTS usage_events_account_billable_metric_idx
    ON ms_billing.usage_events (account_id, (COALESCE(billable_at, recorded_at)), metric);

COMMENT ON COLUMN ms_billing.budgets.category IS
    'Which spend the cap measures: all (every usage event), ai (infra.ai.* only, priced per model), or exposure (the SYSTEM row billing-engine maintains per PaaS account so budget_alerts can record crossings of the risk-exposure pool; never written by a customer).';
