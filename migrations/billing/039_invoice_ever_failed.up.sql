-- Migration 039 — sticky per-invoice "ever failed" marker (service-block gate).
--
-- The service-block gate needs to count an account's failed-charge STREAK. That
-- streak is DERIVED at read time (ServiceBlockSignals) as the number of distinct
-- FAILED invoices created after the account's most-recent PAID invoice — a
-- delivery-order-immune reading of the invoice mirror, not a mutable counter.
--
-- ever_failed is the fact that read needs: an invoice can sit at status 'open'
-- AFTER a failed payment (Stripe keeps retrying), so status alone can't tell an
-- open-because-just-finalized invoice from an open-because-it-failed one. The
-- webhook latches ever_failed=true on a failure event (payment_failed /
-- marked_uncollectible); the derivation then counts (ever_failed OR currently
-- 'uncollectible') invoices. It is set-only and never cleared — a historical
-- "this invoice failed at least once" fact that survives a later flip to paid
-- (that invoice is excluded from the streak by the created_at cutoff, not by
-- clearing the flag). Distinct from the delinquency signal (AccountHasUnpaidInvoice,
-- CURRENT status only).
--
-- Set-only + invoice-keyed makes the webhook write idempotent under Stripe's
-- at-least-once + out-of-order delivery — there is no counter to double-count.
-- Every historic row defaults false; only a NEW failure event sets it. Born
-- clean at slot 039 (next free after 038). sqlc picks up the column
-- automatically. updated_at is trigger-maintained (001).

ALTER TABLE ms_billing.invoices
    ADD COLUMN ever_failed BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN ms_billing.invoices.ever_failed IS
    'Sticky, set-only: true once this invoice failed a payment '
    '(payment_failed / marked_uncollectible), never cleared. Lets the '
    'service-block gate DERIVE the failed-charge streak at read time — counting '
    '(ever_failed OR uncollectible) invoices created after the last paid one — '
    'so it survives a later flip to paid and is immune to webhook delivery order.';
