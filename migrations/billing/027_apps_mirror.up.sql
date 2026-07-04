-- Migration 027 — app existence mirror (the base-fee charging roster).
--
-- Base-fee v1 (docs-temp/account-billing-read/DESIGN.md, owner spec
-- 2026-07-05, D1c). billing-engine cannot see api-platform's ms_apps (and MUST
-- NOT join it across the trust boundary), so it keeps its OWN mirror of app
-- existence: one row per platform app, written ONLY via the RegisterApp /
-- SyncAppModules RPCs that api-platform's applications-service fires
-- (fire-and-forget with retry) on app create / module install / uninstall /
-- app delete. This roster is what the charge spine bills the per-app base fee
-- from:
--
--   app base for a period = BaseFee + Overage × max(0, module_count − included)
--
--   * boundary (advance) leg: at period close the cycle sums the base over
--     every LIVE (deleted_at IS NULL) row and adds it to the usage arrears —
--     one invoice.
--   * creation (proration) leg: RegisterApp charges the creation period's
--     remaining whole UTC days immediately (base × remain_days / period_days,
--     round-half-up), gated on account activation + a usable PM (D1d).
--
-- module_count is the SNAPSHOT count of installed modules, upserted by
-- SyncAppModules; mid-period installs/uninstalls take effect at the NEXT
-- boundary (D1b — no mid-period micro-invoices or refunds).
--
-- proration_invoice_id is the ONE-SHOT guard on the creation-proration charge:
-- the Stripe invoice id of that charge, set AT MOST ONCE (UPDATE … WHERE
-- proration_invoice_id IS NULL). A RegisterApp retry that finds it set skips
-- charging — the proration can never double-bill.
--
-- deleted_at soft-deletes the row out of FUTURE base fees (D1e — no refunds;
-- the current period's base is spent, and the deleted app's usage arrears
-- still bill at the boundary). created_at is the platform's app-creation
-- instant (supplied by RegisterApp, NOT defaulted here) — it drives the
-- proration window and the per-period display math, so it must survive
-- retries unchanged.
--
-- Accounts with NO rows here (pre-backfill) bill exactly as before this
-- migration (usage arrears only); the api-platform backfill PR populates the
-- roster.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#apps (follow-up docs PR).

CREATE TABLE IF NOT EXISTS ms_billing.apps (
    -- The platform app id (ms_apps.id), mirrored verbatim. PRIMARY KEY so
    -- RegisterApp's upsert is idempotent on the app identity.
    app_id                UUID PRIMARY KEY,

    account_id            UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,

    -- Snapshot of the app's installed-module count (SyncAppModules). Drives
    -- the overage tier above IncludedModules; never negative.
    module_count          INT NOT NULL DEFAULT 0 CHECK (module_count >= 0),

    -- Platform app-creation instant (RegisterApp input; immutable across
    -- retries). Anchors the creation-proration window and the display math.
    created_at            TIMESTAMPTZ NOT NULL,

    -- One-shot proration guard: the Stripe invoice id of the creation-
    -- proration charge, set at most once. NULL = not charged (unactivated
    -- account, zero remaining days, or rounded to 0 cents — all legitimate).
    proration_invoice_id  TEXT NULL,

    -- Soft delete (app deleted on the platform). Excludes the row from future
    -- advance base fees; historical display + usage arrears are unaffected.
    deleted_at            TIMESTAMPTZ NULL,

    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- FK index + the boundary charge's per-account live-roster scan.
CREATE INDEX IF NOT EXISTS apps_account_idx
    ON ms_billing.apps (account_id);

-- Auto-maintained updated_at (SyncAppModules updates count / deleted_at in
-- place), matching the 001 accounts convention.
CREATE TRIGGER apps_set_updated_at
BEFORE UPDATE ON ms_billing.apps
FOR EACH ROW
EXECUTE FUNCTION ms_billing.set_updated_at();
