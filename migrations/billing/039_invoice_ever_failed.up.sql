-- Migration 039 — sticky per-invoice "ever failed" marker (service-block gate).
--
-- The service-block gate counts an account's CONSECUTIVE failed-charge streak
-- (accounts.failed_charge_streak, migration 040): +1 the first time an invoice
-- fails, reset to 0 on the next successful charge, block at >= 2. Stripe smart-
-- retries a failed invoice and fires invoice.payment_failed on EACH attempt
-- (distinct event ids, so the webhook's event-level dedup does NOT collapse
-- them). Counting every payment_failed event would over-count a single
-- invoice's retries.
--
-- ever_failed is the per-invoice guard that makes the streak count DISTINCT
-- invoices: the webhook flips it false->true on the first payment_failed for a
-- row (an UPDATE ... WHERE NOT ever_failed, so RowsAffected=1 only once) and
-- increments the account streak ONLY on that flip. It is never cleared — it is
-- a historical "this invoice failed at least once" fact, independent of the
-- invoice's later status (a failed-then-paid invoice keeps ever_failed=true
-- while the account streak resets). Distinct from the derived delinquency
-- signal (AccountHasUnpaidInvoice, which reflects CURRENT status only).
--
-- Every historic row defaults false; only a NEW payment_failed sets it. Born
-- clean at slot 039 (next free after 038). sqlc picks up the column
-- automatically. updated_at is trigger-maintained (001).

ALTER TABLE ms_billing.invoices
    ADD COLUMN ever_failed BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN ms_billing.invoices.ever_failed IS
    'Sticky: set true on the FIRST invoice.payment_failed for this row and never '
    'cleared. The service-block gate uses it to count DISTINCT failed invoices '
    '(not per-retry events) into accounts.failed_charge_streak. Independent of '
    'the current status — survives a later flip to paid.';
