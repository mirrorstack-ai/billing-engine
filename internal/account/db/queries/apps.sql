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

-- LiveAppModuleCounts returns the module_count of every LIVE (deleted_at IS
-- NULL) app on the account — the boundary charge's advance-base input:
-- advance base = Σ (BaseFee + Overage × max(0, module_count − included)).
-- Deleted apps are excluded (D1e); an account with no rows (pre-backfill)
-- sums to base 0 and keeps the pre-027 arrears-only invoice.
-- name: LiveAppModuleCounts :many
SELECT module_count
FROM ms_billing.apps
WHERE account_id = $1
  AND deleted_at IS NULL;
