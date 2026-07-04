-- Migration 028 — per-app-period base-fee snapshots (display == invoice).
--
-- Base-fee v1 review follow-up (docs-temp/account-billing-read/DESIGN.md
-- "Base fee — v1 spec": display and invoices must never disagree). The apps
-- mirror (027) carries only the CURRENT module_count, so after a
-- SyncAppModules the display math recomputed an already-charged period's base
-- from the NEW count and drifted away from what the invoice billed. This
-- table freezes what each charge leg actually billed: one row per
-- (app, period), written AT THE MOMENT the base is charged, and GetAppBill
-- prefers it over the live-count estimate for that period.
--
-- Writers (both charge legs, exactly the two places an app-period base can
-- be billed):
--   * RegisterApp's creation-proration leg → source = 'proration'.
--     base_micros is the PRORATED amount actually invoiced for the partial
--     [creation-day, period_end) window; the row is keyed by the FULL
--     anchored period_start (the period identity the display looks up), and
--     a retry upserts identical values (idempotent).
--   * The boundary advance leg → source = 'advance', one row per live
--     pre-existing app for the NEW window, ON CONFLICT (app_id, period_start)
--     DO NOTHING — an existing proration row WINS if both somehow exist (the
--     proration is the more specific charge for a creation period).
--
-- module_count is the count snapshotted at charge time (D1b); base_micros is
-- integer micro-dollars (NEVER float), the exact amount the invoice's base
-- component billed. Periods with NO row here (pre-feature history,
-- unactivated accounts) fall back to the live-count DISPLAY ESTIMATE in
-- GetAppBill — display-only, since nothing was invoiced for them.
--
-- Born clean at slot 028 (the runner applies *.up.sql in filename order, so
-- this lands after 027's ms_billing.apps, which the FK requires).
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#app_base_snapshots
-- (follow-up docs PR).

CREATE TABLE IF NOT EXISTS ms_billing.app_base_snapshots (
    -- The mirrored app (027). Cascade: dropping an app's mirror row drops its
    -- charge history snapshots with it.
    app_id        UUID NOT NULL REFERENCES ms_billing.apps(app_id) ON DELETE CASCADE,

    -- The FULL anchored billing-period window this base charge covers.
    -- period_start is the display lookup key (exact match from GetAppBill);
    -- a proration row still keys on the full window's start even though its
    -- base_micros covers only [creation-day, period_end).
    period_start  TIMESTAMPTZ NOT NULL,
    period_end    TIMESTAMPTZ NOT NULL,

    -- The module_count the charge tiered on, snapshotted at charge time (D1b).
    module_count  INT NOT NULL,

    -- The base amount actually billed for this app-period, integer micros.
    base_micros   BIGINT NOT NULL CHECK (base_micros >= 0),

    -- Which charge leg billed it: RegisterApp's creation proration, or the
    -- boundary advance leg.
    source        TEXT NOT NULL CHECK (source IN ('proration', 'advance')),

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One base charge per app-period — the "charged exactly once" invariant,
    -- enforced at the ledger.
    PRIMARY KEY (app_id, period_start)
);
