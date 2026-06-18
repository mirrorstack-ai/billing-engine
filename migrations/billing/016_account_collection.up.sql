-- Migration 016 — risk-graded collection fields on the billing account.
--
-- Milestone D, PR #9 (the GA gate for the off-session arrears leg). Makes the
-- accrued-arrears charge SAFE before it can ship, per billing-tiers §3
-- ("collection = risk-graded, prepaid is the fallback not the default") and
-- design §7-A.
--
-- Adds three per-account collection controls + extends the billing_runs status
-- vocabulary with a 'skipped_prepaid' terminal state.
--
-- ============================================================
-- THE COLLECTION CONTROLS (design §7-A / billing-tiers §3, §6)
-- ============================================================
--   usage_billing_mode ∈ {arrears, prepaid}
--     - arrears (DEFAULT): accrued usage above allowance is charged off-session
--       at cycle close (the existing RunBillingCycle leg), GATED on credit_limit
--       + spend_ceiling + a usable PM.
--     - prepaid: the off-session arrears leg is NOT permitted. RunBillingCycle
--       marks the run 'skipped_prepaid' and RETAINS the usage (never lost). The
--       prepaid-credit WALLET (balance / top-ups / deduct-on-usage) is a
--       DEFERRED follow-up; in v1 'prepaid' means "arrears not collectible
--       off-session" — the safe first cut (billing-tiers §6 GA gate).
--     The risk-judge auto-flips this toward prepaid on a delinquency signal /
--     over-limit accrual / usage spike (design §7-A); it relaxes back toward
--     arrears only on sustained clean standing (conservative).
--
--   credit_limit_micros (BIGINT, NOT NULL, DEFAULT conservative)
--     Max uncharged-arrears exposure the account may accrue before the
--     risk-judge tightens it to prepaid. Trust-ramped per account
--     (collection.TrustRampedCreditLimit): new / no-verified-PM / short-tenure
--     accounts get a low limit; it grows with payment history + tenure + a
--     verified PM. The DEFAULT here is the conservative new-account floor —
--     exact ramp numbers are FINANCE-OWNED (billing-tiers §4 "Finance-owned
--     later: credit-limit ramp + anomaly thresholds").
--
--   spend_ceiling_micros (BIGINT, NULL = no ceiling)
--     A hard per-cycle bill-shock cap the CUSTOMER sets: the off-session leg
--     never auto-charges accrued arrears above this in a single cycle
--     (billing-tiers §3 "stop at a per-account spend ceiling"). NULL = unset =
--     no ceiling.
-- ============================================================
--
-- Money is micro-dollar BIGINT end-to-end (no float), matching the rest of
-- ms_billing. updated_at is auto-maintained by the existing
-- accounts_set_updated_at trigger (migration 001) — no new trigger needed.
--
-- Born clean at slot 016 (next free after 015). sqlc picks up the new columns +
-- the enum type from migrations/billing/ automatically.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#accounts

-- usage_billing_mode enum. Matches the CREATE TYPE ... AS ENUM convention used
-- by the rest of ms_billing (metric_kind, margin_share_class, budget_scope, …).
CREATE TYPE ms_billing.usage_billing_mode AS ENUM (
    'arrears',
    'prepaid'
);

-- The conservative new-account credit-limit floor, in micro-dollars. $25.00 =
-- 25_000_000 micros. Deliberately low: a brand-new account with no payment
-- history accrues at most this much uncharged usage before the risk-judge
-- tightens it to prepaid. FINANCE-OWNED — revise alongside collection.go's
-- TrustRampedCreditLimit consts; kept in sync as the documented default.
ALTER TABLE ms_billing.accounts
    ADD COLUMN usage_billing_mode   ms_billing.usage_billing_mode NOT NULL DEFAULT 'arrears',
    ADD COLUMN credit_limit_micros  BIGINT NOT NULL DEFAULT 25000000
                                    CHECK (credit_limit_micros >= 0),
    ADD COLUMN spend_ceiling_micros BIGINT NULL
                                    CHECK (spend_ceiling_micros IS NULL OR spend_ceiling_micros >= 0);

-- Extend the billing_runs status vocabulary with the two risk-graded skip
-- reasons, kept SEMANTICALLY DISTINCT so the audit trail is unambiguous:
--   'skipped_prepaid'  the account is in (or was just tightened to) prepaid
--                      usage_billing_mode — a MODE-driven skip.
--   'skipped_ceiling'  the netted arrears would breach the customer-set
--                      spend_ceiling (hard per-cycle bill-shock cap) — a
--                      per-cycle CAP skip that does NOT change the mode.
-- An operator querying billing_runs can tell "account is in prepaid mode" apart
-- from "arrears exceeded the spend ceiling this cycle". Both RETAIN the usage.
-- Drop + re-add the inline CHECK (status is plain TEXT+CHECK, not a CREATE TYPE
-- enum, per migration 012).
ALTER TABLE ms_billing.billing_runs
    DROP CONSTRAINT billing_runs_status_check;
ALTER TABLE ms_billing.billing_runs
    ADD CONSTRAINT billing_runs_status_check
        CHECK (status IN ('pending', 'invoiced', 'skipped_no_pm', 'failed', 'skipped_prepaid', 'skipped_ceiling'));
