-- 093: unpaid episodes — the clock behind the 14-day notice before an
-- unpaid-bill service block (billing-engine#329, core-v2#1881, owner order D592).
--
-- The Terms promise 14 days' notice by email before paid features are limited or
-- suspended for an unpaid invoice. The invoice mirror has no due date, so the
-- engine records its own "unpaid since" episode: one row per stretch during
-- which an account's unpaid-bill gates (first charge failed, failed streak,
-- unpaid invoices) were failing.
--
--   started_at      when the engine first saw the unpaid condition
--   notice_sent_at  when the 14-day email left (NULL = no notice yet, so no block)
--   closed_at       when the account healed (NULL = the episode is open)
--
-- This migration only creates the table. billing-engine#329 READS it behind the
-- UNPAID_GRACE flag (default off); billing-engine#330 writes it. At most one
-- open episode per account, enforced by the partial unique index.
CREATE TABLE IF NOT EXISTS ms_billing.unpaid_episodes (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id     uuid        NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,
    started_at     timestamptz NOT NULL,
    notice_sent_at timestamptz NULL,
    closed_at      timestamptz NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS unpaid_episodes_one_open_per_account
    ON ms_billing.unpaid_episodes (account_id)
    WHERE closed_at IS NULL;
