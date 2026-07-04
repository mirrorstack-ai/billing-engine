-- Migration 031 — permanent-skip marker for the creation-proration charge
-- (review finding, creation-grace PR #46, D1d).
--
-- D1d: v1 never retroactively catches up an app's creation-period base once
-- the window it covers has already closed with the account never having been
-- chargeable during it (never activated). Pre-grace, RegisterApp charged
-- synchronously at creation, so this could only happen if activation itself
-- landed after the creation period ended — and the code skipped the charge
-- (0 cents, guard left unarmed) in exactly that case.
--
-- Creation grace (migration 029) removed that gate outright when it moved the
-- charge into the async sweep, reasoning (wrongly, in general) that the
-- creation period is billed by no other leg so charging it whenever the guard
-- is unarmed "can never double-bill". That reasoning misses D1d: an app whose
-- account sat unactivated across the app's ENTIRE creation period and only
-- activates months later would, on the very next sweep, be retroactively
-- billed for a period it was never eligible to be charged for.
--
-- proration_skipped_at records that this decision was made and is PERMANENT:
-- once an app's creation-proration charge is determined to be a would-be
-- retroactive catch-up (the account only activated at/after the app's
-- anchored creation period had already closed), the app is marked here and
-- dropped from the sweep's pending work list for good — never charged, and
-- never re-evaluated on every future sweep (which would otherwise happen
-- forever, since proration_invoice_id would stay NULL).
--
-- The apps_pending_proration_idx partial index (migration 029) is redefined to
-- add the same predicate so the sweep's work-list query keeps hitting the
-- index.

ALTER TABLE ms_billing.apps
    ADD COLUMN proration_skipped_at TIMESTAMPTZ NULL;

COMMENT ON COLUMN ms_billing.apps.proration_skipped_at IS
    'Set once (never unset) when ChargeCreationProration determines the '
    'account only activated at/after this app''s anchored creation period had '
    'already closed — a would-be retroactive catch-up charge (D1d). The app '
    'is permanently excluded from the proration sweep from then on.';

DROP INDEX IF EXISTS ms_billing.apps_pending_proration_idx;

CREATE INDEX IF NOT EXISTS apps_pending_proration_idx
    ON ms_billing.apps (created_at)
    WHERE deleted_at IS NULL
      AND proration_invoice_id IS NULL
      AND proration_skipped_at IS NULL;
