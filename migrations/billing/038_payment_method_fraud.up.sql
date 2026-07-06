-- Migration 038 — fraud flag on the payment-method mirror (service-block gate).
--
-- The service-block eligibility gate (internal/account/eligibility) requires
-- "at least one NON-FRAUD card on file". payment_methods_mirror (002/005) held
-- no risk state at all — only brand/last4/exp + is_default + deleted_at — so a
-- card that Stripe later flags (an early-fraud warning or a dispute) could not
-- be excluded from the usable-card count. These columns add that risk state.
--
-- fraud_blocked is the authoritative "exclude this card" flag the gate reads;
-- fraud_reason / fraud_flagged_at are the audit trail (which signal, when).
-- All default to the non-fraud state, so EVERY existing row is treated as a
-- good card the moment this lands — the gate is fully functional on the card
-- count immediately. The webhook that SETS fraud_blocked=true
-- (radar.early_fraud_warning.created + charge.dispute.created) is a follow-up
-- PR: shipping the column now with no writer just means every card stays
-- non-fraud until that path lands, which is the safe interim (never a false
-- block).
--
-- fraud_blocked is a ONE-WAY latch in practice (a flagged card is not
-- auto-cleared) — a disputed/fraudulent card is detached + re-added, not
-- un-flagged in place. No index: cards-per-account is tiny and the count query
-- already rides the pmm_account_active_idx (002) partial index on account_id.
--
-- Born clean at slot 038 (next free after 037). sqlc picks up the columns from
-- migrations/billing/ automatically.

ALTER TABLE ms_billing.payment_methods_mirror
    ADD COLUMN fraud_blocked    BOOLEAN     NOT NULL DEFAULT false,
    ADD COLUMN fraud_reason     TEXT        NULL,
    ADD COLUMN fraud_flagged_at TIMESTAMPTZ NULL;

COMMENT ON COLUMN ms_billing.payment_methods_mirror.fraud_blocked IS
    'True once Stripe flags this card as fraud/dispute risk (set by the '
    'radar.early_fraud_warning / charge.dispute webhook — follow-up PR). The '
    'service-block gate EXCLUDES fraud_blocked cards from the usable-card count. '
    'Defaults false so every existing card is treated non-fraud.';

COMMENT ON COLUMN ms_billing.payment_methods_mirror.fraud_reason IS
    'Audit: the signal that set fraud_blocked (e.g. early_fraud_warning, dispute). NULL until flagged.';

COMMENT ON COLUMN ms_billing.payment_methods_mirror.fraud_flagged_at IS
    'Audit: when fraud_blocked was set. NULL until flagged.';
