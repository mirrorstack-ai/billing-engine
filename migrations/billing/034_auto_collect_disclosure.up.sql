-- Migration 034 — large auto-collected charge DISCLOSURE (transparency flag).
--
-- A post-hoc TRANSPARENCY surface for off-session charges that ALREADY
-- SUCCEEDED, once they cross a per-account size threshold — so a customer is
-- never surprised by a large automatic debit. This is COMPLEMENTARY to (and
-- deliberately DISTINCT from) the spend-ceiling gate (migration 016): the
-- ceiling SKIPS a charge that would breach a bill-shock cap BEFORE it fires;
-- this flag changes NO charging behaviour at all — it only records, after the
-- fact, that a successful charge was "large" so the web billing page can
-- disclose it. Money is micro-dollar BIGINT end-to-end (no float).
--
-- Two additive columns, one migration:
--
--   accounts.auto_collect_threshold_micros (BIGINT, NULL)
--     The per-account size threshold above which a successful off-session
--     charge is disclosed as "large". NULL = use the platform default
--     ($100.00 = 100_000_000 micros, collection.DefaultAutoCollectThresholdMicros).
--     Resolved AT CHARGE TIME (never later), so the flag reflects the threshold
--     that applied when the charge actually fired, even if it is changed after.
--
--   invoices.is_large_auto_collect (BOOLEAN, NOT NULL, DEFAULT false)
--     The server-computed verdict, frozen on the invoice mirror row at
--     invoice-create time: true iff the charged amount (netted arrears + advance
--     base, in micros, pre-cents-conversion) exceeded the account's resolved
--     threshold. Every historic row defaults false (no disclosure) — the flag is
--     only ever set true by a NEW charge going forward.
--
-- Born clean at slot 034 (renumbered from 031 when merged into
-- feat/unified-base-fee-overage, whose creation-grace stack already owns
-- 031/032). sqlc picks up both columns from migrations/billing/ automatically.
-- updated_at on both tables is trigger-maintained (001).
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#accounts + #invoices.

ALTER TABLE ms_billing.accounts
    ADD COLUMN auto_collect_threshold_micros BIGINT NULL
        CHECK (auto_collect_threshold_micros IS NULL OR auto_collect_threshold_micros >= 0);

COMMENT ON COLUMN ms_billing.accounts.auto_collect_threshold_micros IS
    'Per-account size threshold (micro-USD) above which a SUCCESSFUL off-session '
    'charge is disclosed as "large" on the billing page. NULL = platform default '
    '($100 = 100000000 micros). Resolved at charge time; pure disclosure, changes no charging behaviour.';

ALTER TABLE ms_billing.invoices
    ADD COLUMN is_large_auto_collect BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN ms_billing.invoices.is_large_auto_collect IS
    'Server-computed at invoice-create time: true iff the charged amount (netted '
    'arrears + advance base, micros) exceeded the account auto_collect_threshold_micros '
    '(or the default when NULL) that applied WHEN THE CHARGE FIRED. Post-hoc disclosure only.';
