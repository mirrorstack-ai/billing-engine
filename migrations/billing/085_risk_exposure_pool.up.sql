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
--     verified card, k paid inv.  → card_base_micros + range_micros × growth(k)
--                                   growth(k) is the configured SHAPE:
--                                     'sigmoid' (owner 2026-09-14, FINAL):
--                                        [g(k) − g(0)] / [g(k_max) − g(0)],
--                                        g(x) = 1 / (1 + e^(−p_a·(x − p_k0))),
--                                        = 1 for k ≥ k_max  → exactly base + range
--                                     'exp': 1 − e^(−k/tau)
--     never above                 → ceiling_micros              (the hard clamp)
--
-- The SHAPE is config (coordinator, PR-B review): shape, base, range, the
-- shape's parameters and the cap are columns, so the next change is an
-- UPDATE and not a PR. 🔴 CAP MOVED $200 → $1,000 (a 5× risk-appetite
-- change, owner's decision 2026-09-14 via f5). Seed = S2 (k0 10, a 0.6):
-- $10 (k=0), $22 (k=3), $90 (k=6), $504 (k=10), $918 (k=14), $1,000 (k=24);
-- S1 (k0 6, a 0.8) and S3 (k0 14, a 0.5) are the alternatives on the PR.
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
    -- range_micros is how far above the base the curve can climb; the shape
    -- and its parameters say how fast: 'sigmoid' uses p_a (steepness), p_k0
    -- (the midpoint in paid invoices) and k_max (where it reaches base+range
    -- exactly); 'exp' uses tau (63% of the range at k = tau).
    range_micros          BIGINT NOT NULL DEFAULT 990000000 CONSTRAINT risk_ramp_range_nonneg CHECK (range_micros >= 0),
    shape                 TEXT NOT NULL DEFAULT 'sigmoid' CONSTRAINT risk_ramp_shape_known CHECK (shape IN ('exp', 'sigmoid')),
    p_a                   NUMERIC(6,3) NOT NULL DEFAULT 0.600 CONSTRAINT risk_ramp_p_a_positive CHECK (p_a > 0),
    p_k0                  NUMERIC(6,3) NOT NULL DEFAULT 10.000,
    k_max                 INT NOT NULL DEFAULT 24 CONSTRAINT risk_ramp_k_max_positive CHECK (k_max > 0),
    tau                   NUMERIC(6,3) NOT NULL DEFAULT 5.000 CONSTRAINT risk_ramp_tau_positive CHECK (tau > 0),
    -- Delinquency = ONE demerit score S per account (owner FINAL 2026-09-14,
    -- via f5): limit = curve(k) ÷ (1 + S), never below no_card_micros.
    --   +demerit_per_unpaid_cycle  the first time an invoice fails (or is
    --                              marked uncollectible), and again at every
    --                              cycle close it has then stayed unpaid;
    --   −demerit_recovery          the day a late invoice is settled, and at
    --                              every cycle close with nothing late;
    --   never above demerit_max, never below 0 — so the worst history is
    --   back to Normal after demerit_max ÷ demerit_recovery clean cycles.
    -- Unpaid DOLLARS are not scored: the open balance counts against the
    -- pool as money (arrears), exactly. A VOID is our cancellation: never late.
    demerit_per_unpaid_cycle NUMERIC(5,2) NOT NULL DEFAULT 2.00 CONSTRAINT risk_ramp_demerit_p_nonneg CHECK (demerit_per_unpaid_cycle >= 0),
    demerit_recovery         NUMERIC(5,2) NOT NULL DEFAULT 1.00 CONSTRAINT risk_ramp_demerit_r_nonneg CHECK (demerit_recovery >= 0),
    demerit_max              NUMERIC(5,2) NOT NULL DEFAULT 12.00 CONSTRAINT risk_ramp_demerit_max_nonneg CHECK (demerit_max >= 0),
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
VALUES (1, 5000000, 10000000, 1000000000)
ON CONFLICT (id) DO NOTHING;

COMMENT ON TABLE ms_billing.risk_ramp_config IS
    'The PaaS exposure curve (owner 2026-09-14): no card → no_card_micros; verified card with k paid invoices → card_base_micros + range_micros × growth(k) where growth is the configured shape (sigmoid: normalised logistic reaching 1 at k_max; exp: 1 − exp(−k/tau)); capped at ceiling_micros. Delinquency: curve ÷ (1 + accounts.demerit_score), never below no_card_micros; S moves by demerit_per_unpaid_cycle / demerit_recovery, capped at demerit_max (see the column comments). Finance-owned; tune by UPDATE.';

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

-- The demerit score lives on the ACCOUNT (one row, updated by three
-- transitions, each idempotent):
--   accounts.demerit_score      S, 0 = Normal;
--   accounts.demerit_closed_at  the last cycle close the score accounted for,
--                               so a re-run of the same close changes nothing;
--   invoices.demerit_failed_at  when the invoice's first failure was charged
--                               (+p), so a repeated payment_failed is a no-op
--                               and a close knows whether the invoice has
--                               stayed unpaid a FULL cycle since;
--   invoices.demerit_settled_at when the settle credit (−r) was given, once.
ALTER TABLE ms_billing.accounts
    ADD COLUMN IF NOT EXISTS demerit_score     NUMERIC(5,2) NOT NULL DEFAULT 0 CONSTRAINT accounts_demerit_nonneg CHECK (demerit_score >= 0),
    ADD COLUMN IF NOT EXISTS demerit_closed_at TIMESTAMPTZ NULL;
ALTER TABLE ms_billing.invoices
    ADD COLUMN IF NOT EXISTS demerit_failed_at  TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS demerit_settled_at TIMESTAMPTZ NULL;
COMMENT ON COLUMN ms_billing.accounts.demerit_score IS
    'Delinquency demerit S (owner 2026-09-14): the exposure limit is curve(k) ÷ (1 + S). +demerit_per_unpaid_cycle on an invoice''s first failure and at each cycle close it stays unpaid; −demerit_recovery on settling a late invoice and at each clean cycle close; 0 ≤ S ≤ demerit_max. Written only by the budget store''s three demerit transitions.';
