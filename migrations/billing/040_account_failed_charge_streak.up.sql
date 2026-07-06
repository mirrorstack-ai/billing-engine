-- Migration 040 — consecutive failed-charge streak on the billing account
-- (service-block gate).
--
-- The service-block gate (internal/account/eligibility) blocks an account whose
-- failed-charge count reaches 2 ("< 2 failed" — 2 excluded). The streak is
-- RECOVERABLE, not a lifetime tally: it counts CONSECUTIVE distinct failed
-- invoices and RESETS to 0 on the next successful charge, so an account self-
-- heals the moment it pays (product decision: auto-cure on next success).
--
-- Maintained entirely by the invoice.* webhook:
--   invoice.payment_failed  -> +1, but only on the first failure of a distinct
--                              invoice (gated by invoices.ever_failed, migration
--                              039, so Stripe's per-retry events don't over-count).
--   invoice.paid            -> reset to 0.
-- The gate READS this column directly (no aggregation) alongside the usable-card
-- count and the first-charge outcome.
--
-- Defaults 0 for every existing account (a clean streak). Born clean at slot 040
-- (next free after 039). updated_at is trigger-maintained (001); sqlc picks up
-- the column automatically.

ALTER TABLE ms_billing.accounts
    ADD COLUMN failed_charge_streak INT NOT NULL DEFAULT 0
        CHECK (failed_charge_streak >= 0);

COMMENT ON COLUMN ms_billing.accounts.failed_charge_streak IS
    'Consecutive distinct failed invoices, maintained by the invoice webhook '
    '(+1 on the first payment_failed of a row via invoices.ever_failed, reset to '
    '0 on invoice.paid). The service-block gate blocks at >= 2. Recoverable — '
    'self-heals to 0 on the next successful charge.';
