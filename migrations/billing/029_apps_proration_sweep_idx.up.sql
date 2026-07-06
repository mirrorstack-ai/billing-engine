-- Migration 029 — index for the creation-proration grace sweep.
--
-- Creation grace (owner spec 2026-07-05, D1e follow-up): a newly created app is
-- no longer charged its creation-period base synchronously in RegisterApp. A
-- periodic sweep (cmd/billing-cycle) instead charges apps that have SURVIVED the
-- grace window, so an app soft-deleted within grace is never billed. The sweep's
-- work list is:
--
--   SELECT app_id FROM ms_billing.apps
--   WHERE created_at <= now() - '3 days'
--     AND proration_invoice_id IS NULL   -- one-shot guard: not yet charged
--     AND deleted_at IS NULL             -- excludes apps deleted within grace
--
-- A PARTIAL index on created_at, restricted to the exact NULL predicates the
-- sweep filters on, keeps the index tiny (it holds ONLY the still-pending apps —
-- every charged or deleted app drops out) and lets the sweep range-scan by
-- created_at without touching the full roster. It is preferred over a plain
-- composite (deleted_at, proration_invoice_id, created_at) because almost every
-- row is eventually charged (proration_invoice_id set) and thus excluded from
-- the partial index, so it stays a fraction of the table's size.

CREATE INDEX IF NOT EXISTS apps_pending_proration_idx
    ON ms_billing.apps (created_at)
    WHERE deleted_at IS NULL AND proration_invoice_id IS NULL;
