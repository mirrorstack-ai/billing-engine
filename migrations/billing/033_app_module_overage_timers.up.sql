-- Migration 033 — per-module-instance overage timers (owner session amendment
-- 2026-07-05, DESIGN.md "Base fee — v2: creation grace + per-module overage
-- timers"). SUPERSEDES the account-wide single-timer pooled overage model
-- (migration 032): overage is no longer ONE grace timer per account tiering on
-- SUM(module_count); it is now ONE independently-anchored grace timer per module
-- INSTALL EVENT, each priced from its OWN install date.
--
-- Migration 032's account-wide schema never shipped to main, so this fully
-- REPLACES it (not deprecate-in-place): the up drops accounts.overage_since and
-- ms_billing.account_overage_snapshots outright, and the down recreates them so
-- up/down/up round-trips cleanly against 032.
--
-- The model (DESIGN.md v2):
--   * RegisterApp(count=K) inserts K rows anchored installed_at = created_at;
--     SyncAppModules N→M inserts M−N new rows (installed_at = now) when growing,
--     LIFO soft-removes N−M rows (newest installs first) when shrinking; app
--     deletion soft-removes all the app's still-live rows.
--   * "included vs over" is derived LIVE at every grace-check, never cached: across
--     the account's currently-live rows ordered (installed_at ASC, id ASC), the
--     first IncludedModules (5) are "included", the rest are "over". Monotonicity:
--     a new install always gets the latest installed_at, so an existing row's rank
--     can only improve (over → included, never included → over) — so once a
--     grace-check finds a row "included" that verdict is PERMANENT (grace_resolved).
--   * Leg 1 (the per-module grace charge): once removed_at IS NULL AND
--     grace_expires_at <= now AND grace_resolved = false, the sweep charges the
--     "over" rows $3 prorated from installed_at's UTC day to the current period end
--     (install-anchored), via a per-timer Stripe invoice, then stamps
--     grace_charged_at / grace_resolved and the REAL Stripe ids.
--
-- Money never lives on this table — the overage rate is a code constant
-- (usage.ModuleOverageFeeMicros); the table carries only the per-install timer
-- lifecycle. Companion docs update pending in mirrorstack-docs/db/ms_billing/.

-- Retire the superseded account-wide pooled overage schema (migration 032).
-- Drop the snapshot ledger first, then the grace-anchor column.
DROP TABLE IF EXISTS ms_billing.account_overage_snapshots;

ALTER TABLE ms_billing.accounts
    DROP COLUMN IF EXISTS overage_since;

CREATE TABLE IF NOT EXISTS ms_billing.app_module_overage_timers (
    -- Surrogate PK: one row per module INSTALL EVENT (not per module identity —
    -- the RPC layer carries only an integer module_count, so instances are
    -- synthesized). The id is the stable charge identity: the per-timer Stripe
    -- Idempotency-Keys (mod-overage-ii-<id> / mod-overage-inv-<id>) derive from it.
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- The billing account the install belongs to. Cascade: dropping the account
    -- drops its timers. Denormalized from the app (rather than joined every
    -- FIFO-check) so the live-FIFO ordering query is a single-table scan.
    account_id            UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,

    -- The app the module was installed into. Cascade off the apps mirror row.
    app_id                UUID NOT NULL REFERENCES ms_billing.apps(app_id) ON DELETE CASCADE,

    -- The install instant — BOTH the FIFO ordering key (earliest installs are the
    -- "included" 5) AND the proration anchor (Leg 1 prorates $3 from this UTC day
    -- to the period end). For RegisterApp's co-created modules this is created_at;
    -- for a later SyncAppModules install it is that sync's now().
    installed_at          TIMESTAMPTZ NOT NULL,

    -- installed_at + the 3-day grace window. Stored (not derived on read) so the
    -- sweep's work-list is a plain indexed range scan on this column.
    grace_expires_at      TIMESTAMPTZ NOT NULL,

    -- NULL while the install is LIVE; set to the removal instant on a
    -- SyncAppModules uninstall (LIFO) or an app deletion. A removed row leaves the
    -- live-FIFO ordering and the sweep work-list. No refund (D1e): removing a
    -- module already charged this period never claws back money.
    removed_at            TIMESTAMPTZ NULL,

    -- The instant Leg 1 actually charged this install's overage. NULL until a
    -- grace-check decides "over" and the Stripe charge succeeds (an "included"
    -- resolution leaves it NULL — nothing was charged).
    grace_charged_at      TIMESTAMPTZ NULL,

    -- TRUE once a grace-check has run and reached a TERMINAL verdict for this
    -- install: either "included" (permanent, monotonicity — never charge, never
    -- re-check) or "over and charged". Drops the row from the sweep work-list.
    grace_resolved        BOOLEAN NOT NULL DEFAULT false,

    -- The genuine Stripe invoice / invoice-item ids of the Leg 1 overage charge
    -- (NULL until charged, or for an "included" resolution). The item id is the
    -- REAL Stripe object id, never the idempotency-key string.
    grace_invoice_id      TEXT NULL,
    grace_invoice_item_id TEXT NULL,

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Live-FIFO ordering: the "which installs are included vs over" query scans an
-- account's live rows ordered (installed_at, id). Partial on removed_at IS NULL so
-- the index holds only live installs.
CREATE INDEX IF NOT EXISTS app_module_overage_timers_live_fifo_idx
    ON ms_billing.app_module_overage_timers (account_id, installed_at, id)
    WHERE removed_at IS NULL;

-- Sweep work-list: rows whose grace has elapsed and are not yet resolved. Partial
-- on the exact predicates the sweep filters (removed_at IS NULL AND grace_resolved
-- = false) so it stays tiny — every resolved or removed row drops out.
CREATE INDEX IF NOT EXISTS app_module_overage_timers_sweep_idx
    ON ms_billing.app_module_overage_timers (grace_expires_at)
    WHERE removed_at IS NULL AND grace_resolved = false;

-- LIFO removal: SyncAppModules shrink removes the app's NEWEST live installs
-- first, ordered (installed_at DESC, id DESC). Partial on the app's live rows.
CREATE INDEX IF NOT EXISTS app_module_overage_timers_app_live_idx
    ON ms_billing.app_module_overage_timers (app_id, installed_at DESC, id DESC)
    WHERE removed_at IS NULL;
