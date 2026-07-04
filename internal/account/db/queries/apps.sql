-- Queries backing the ms_billing.apps mirror (migration 027) — the base-fee
-- roster the RegisterApp / SyncAppModules RPCs write and the charge spine +
-- GetAppBill read. Money never lives here; the table carries only existence,
-- the installed-module-count snapshot, and the one-shot proration guard.

-- InsertAppMirror registers an app row idempotently: ON CONFLICT (app_id) DO
-- NOTHING so a RegisterApp retry (or a concurrent double-fire) never rewrites
-- created_at / module_count / the proration guard of the original insert —
-- the FIRST registration's values are the stable proration anchor. :execrows
-- so the caller can tell a fresh insert (1) from a retry no-op (0), though
-- both are success.
-- name: InsertAppMirror :execrows
INSERT INTO ms_billing.apps (app_id, account_id, module_count, created_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (app_id) DO NOTHING;

-- SelectAppMirror reads one roster row (deleted or not — the caller decides
-- what deletion means for its path: SyncAppModules no-ops a count update,
-- GetAppBill still displays the spent creation-period base).
-- name: SelectAppMirror :one
SELECT app_id, account_id, module_count, created_at, proration_invoice_id, deleted_at
FROM ms_billing.apps
WHERE app_id = $1;

-- SelectAppMirrorForUpdate reads one roster row under a ROW LOCK (FOR UPDATE) —
-- the creation-proration charge's race-safety primitive. In ONE transaction the
-- charge locks the row here, re-verifies deleted_at IS NULL and
-- proration_invoice_id IS NULL under the lock, performs the Stripe charge, and
-- arms the guard. A concurrent SyncAppModules soft-delete (MarkAppDeleted) is
-- thereby mutually exclusive: whichever of {delete, charge} commits first wins
-- and the loser blocks on the lock, then sees the winner's write and no-ops — so
-- a within-grace delete can never coincide with a charge (no refund path, D1e).
-- name: SelectAppMirrorForUpdate :one
SELECT app_id, account_id, module_count, created_at, proration_invoice_id, deleted_at
FROM ms_billing.apps
WHERE app_id = $1
FOR UPDATE;

-- AppsPendingProration is the creation-proration sweep's work list: apps that
-- have survived the grace window ($1 = now() − GraceDays) and were never charged
-- their creation-period base. proration_invoice_id IS NULL is the one-shot guard
-- (an already-charged app drops out); deleted_at IS NULL excludes apps
-- soft-deleted within grace (they are NEVER charged). Ordered by created_at so
-- the oldest pending app charges first. Backed by the partial index
-- apps_pending_proration_idx (migration 029).
-- name: AppsPendingProration :many
SELECT app_id
FROM ms_billing.apps
WHERE created_at <= $1
  AND proration_invoice_id IS NULL
  AND deleted_at IS NULL
ORDER BY created_at;

-- SetAppProrationInvoice arms the ONE-SHOT proration guard: it records the
-- Stripe invoice id of the creation-proration charge, and the WHERE
-- proration_invoice_id IS NULL makes the write first-charge-wins — a retry or
-- concurrent double-fire affects 0 rows and the original invoice id survives.
-- :execrows so the caller can observe (and tolerate) the already-set case.
-- name: SetAppProrationInvoice :execrows
UPDATE ms_billing.apps
SET proration_invoice_id = $2
WHERE app_id = $1
  AND proration_invoice_id IS NULL;

-- SetAppModuleCount snapshots a new installed-module count (SyncAppModules).
-- WHERE deleted_at IS NULL makes a count update on a deleted app a no-op
-- (D1e — a deleted app accrues no future base, so its count is frozen).
-- :execrows; the service resolves 0 rows via the SelectAppMirror existence
-- check it already performed (unknown app → NOT_FOUND, deleted app → no-op).
-- name: SetAppModuleCount :execrows
UPDATE ms_billing.apps
SET module_count = $2
WHERE app_id = $1
  AND deleted_at IS NULL;

-- MarkAppDeleted soft-deletes the roster row out of future advance base fees.
-- WHERE deleted_at IS NULL keeps the FIRST deletion instant (idempotent — a
-- re-fire affects 0 rows and never moves the timestamp).
-- name: MarkAppDeleted :execrows
UPDATE ms_billing.apps
SET deleted_at = now()
WHERE app_id = $1
  AND deleted_at IS NULL;

-- LiveAppModuleCountsCreatedBefore returns (app_id, module_count) for every
-- LIVE (deleted_at IS NULL) app on the account created STRICTLY BEFORE the
-- cutoff — the boundary charge's advance-base input:
-- advance base = Σ (BaseFee + Overage × max(0, module_count − included)).
-- The cutoff is the NEW period's start (the closed window's period_end): an
-- app created INSIDE the new period is excluded, because RegisterApp's
-- creation-proration leg already charged that app's new-period base (full or
-- prorated) — summing it here would double-bill the same period (same-day
-- cron race, and deterministically on reclaimed skipped_no_pm/failed runs).
-- Such an app joins the advance leg at the NEXT boundary. Deleted apps are
-- excluded (D1e); an account with no rows (pre-backfill) sums to base 0 and
-- keeps the pre-027 arrears-only invoice. app_id is returned so the charge
-- leg can write the per-app-period base snapshot (migration 028) it bills.
-- name: LiveAppModuleCountsCreatedBefore :many
SELECT app_id, module_count
FROM ms_billing.apps
WHERE account_id = $1
  AND deleted_at IS NULL
  AND created_at < $2;

-- UpsertProrationBaseSnapshot records what RegisterApp's creation-proration
-- leg billed one app for its creation period (migration 028). Keyed by the
-- FULL anchored period_start (the display identity); base_micros is the
-- PRORATED amount actually invoiced for the partial [creation-day,
-- period_end) window. ON CONFLICT DO UPDATE so a retry is idempotent
-- (identical values) and the proration row WINS over an 'advance' row if
-- both somehow exist — the proration is the more specific charge for a
-- creation period.
-- name: UpsertProrationBaseSnapshot :exec
INSERT INTO ms_billing.app_base_snapshots
    (app_id, period_start, period_end, module_count, base_micros, source)
VALUES ($1, $2, $3, $4, $5, 'proration')
ON CONFLICT (app_id, period_start) DO UPDATE
SET period_end   = EXCLUDED.period_end,
    module_count = EXCLUDED.module_count,
    base_micros  = EXCLUDED.base_micros,
    source       = 'proration';

-- InsertAdvanceBaseSnapshot records what the boundary advance leg billed one
-- app for the NEW period (migration 028). ON CONFLICT (app_id, period_start)
-- DO NOTHING: an existing row — a proration snapshot, or a prior reclaimed
-- attempt's own row — wins, so a re-run never rewrites what was already
-- recorded as billed. :execrows so the caller can observe the no-op, though
-- both outcomes are success.
-- name: InsertAdvanceBaseSnapshot :execrows
INSERT INTO ms_billing.app_base_snapshots
    (app_id, period_start, period_end, module_count, base_micros, source)
VALUES ($1, $2, $3, $4, $5, 'advance')
ON CONFLICT (app_id, period_start) DO NOTHING;

-- SelectAppBaseSnapshot reads the frozen base charge for ONE (app, period) —
-- the display read behind GetAppBill's 基本費用 for a charged period. Exact
-- period_start match (both writers key on the anchored window start); no row
-- means the period was never base-charged and the caller falls back to the
-- live-count display estimate.
-- name: SelectAppBaseSnapshot :one
SELECT module_count, base_micros, source
FROM ms_billing.app_base_snapshots
WHERE app_id = $1
  AND period_start = $2;

-- MirroredAppIDsOverlappingWindow enumerates the account's ms_billing.apps
-- roster rows whose existence interval [created_at, deleted_at) overlaps ONE
-- period window [@period_start, @period_end) — the mirror half of
-- GetAccountBill's app roster (the usage half is AppIDsWithUsage). The overlap
-- test is the standard half-open one:
--
--   created_at < period_end AND (deleted_at IS NULL OR deleted_at > period_start)
--
-- so a just-created zero-usage app still surfaces its (prorated) base on the
-- account bill, an app deleted DURING the period keeps its spent base visible
-- (D1e: no refunds), and an app deleted BEFORE the period opened drops out
-- (its base for the window is 0 and it can have no NEW usage — any residual
-- ledger rows still enumerate through the usage half). Pre-backfill apps have
-- no row here at all and are covered by the usage half alone. ORDER BY app_id
-- (bytewise) for a deterministic scan; the service re-sorts after the merge.
-- name: MirroredAppIDsOverlappingWindow :many
SELECT app_id
FROM ms_billing.apps
WHERE account_id = @account_id::uuid
  AND created_at < @period_end::timestamptz
  AND (deleted_at IS NULL OR deleted_at > @period_start::timestamptz)
ORDER BY app_id;
