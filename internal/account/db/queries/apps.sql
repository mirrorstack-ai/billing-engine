-- Queries backing the ms_billing.apps mirror (migration 027) — the base-fee
-- roster the RegisterApp / SyncAppModules RPCs write and the charge spine +
-- GetAppBill read. Money never lives here; the table carries only existence,
-- the installed-module-count snapshot, and the one-shot proration guard.

-- InsertAppMirror registers an app row idempotently: ON CONFLICT (app_id) DO
-- NOTHING so a RegisterApp retry (or a concurrent double-fire) never rewrites
-- created_at / module_count / created_module_count / the proration guard of
-- the original insert — the FIRST registration's values are the stable
-- proration anchor. created_module_count is stamped from the SAME $3 value as
-- module_count and NEVER written again by any other query (migration 030) —
-- it is the frozen count ChargeCreationProration prices its historical window
-- from, immune to a later SyncAppModules install/uninstall during grace.
-- :execrows so the caller can tell a fresh insert (1) from a retry no-op (0),
-- though both are success.
-- name: InsertAppMirror :execrows
-- name ($5) is frozen from the FIRST registration like created_at /
-- module_count (ON CONFLICT DO NOTHING keeps the first value across retries);
-- SyncAppModules updates it while the app is live (SetAppName). account_id
-- ($2) is NULL for an org-owned app whose org has not designated funding yet
-- (an UNBILLED roster row, migration 041); owner_org_id ($6) is stamped on
-- every org-owned registration — funded or not — so the RepointOrgUsage sweep
-- can scope the org's NULL-account events through the roster.
INSERT INTO ms_billing.apps (app_id, account_id, module_count, created_module_count, created_at, name, owner_org_id)
VALUES ($1, $2, $3, $3, $4, $5, $6)
ON CONFLICT (app_id) DO NOTHING;

-- SelectAppMirror reads one roster row (deleted or not — the caller decides
-- what deletion means for its path: SyncAppModules no-ops a count update,
-- GetAppBill still displays the spent creation-period base).
-- name: SelectAppMirror :one
SELECT app_id, account_id, module_count, created_module_count, created_at, name,
       proration_invoice_id, proration_skipped_at, proration_attempted_at, deleted_at
FROM ms_billing.apps
WHERE app_id = $1;

-- SelectAppMirrorForUpdate reads one roster row under a ROW LOCK (FOR UPDATE) —
-- the creation-proration charge's race-safety primitive. The charge locks the
-- row here just long enough to re-verify deleted_at IS NULL and
-- proration_invoice_id IS NULL and read the frozen created_module_count,
-- releasing the lock immediately after (ChargeProrationLocked runs the actual
-- Stripe network call OUTSIDE this lock — see store.go); a concurrent
-- SyncAppModules soft-delete (MarkAppDeleted) only ever contends for the brief
-- read, never for the duration of a Stripe HTTP call.
-- name: SelectAppMirrorForUpdate :one
SELECT app_id, account_id, module_count, created_module_count, created_at, name,
       proration_invoice_id, proration_skipped_at, proration_attempted_at, deleted_at
FROM ms_billing.apps
WHERE app_id = $1
FOR UPDATE;

-- AppsPendingProration is the creation-proration sweep's work list: apps that
-- have survived the grace window (@created_before = now() − GraceDays) and were
-- never charged their creation-period base. proration_invoice_id IS NULL is the
-- one-shot guard (an already-charged app drops out); the deleted_at predicate
-- excludes ONLY apps soft-deleted WITHIN their grace (never charged, scenario
-- 1) — an app deleted AFTER its grace elapsed SURVIVED it and still owes the
-- creation charge (wave 2, D11: grace only delays WHEN the charge fires, and
-- the H2 boundary exclusion leaves no other leg as a backstop; pre-fix this
-- was a user-timable ~$22 dodge in the grace-elapse→sweep window). Grace in
-- HOURS (D5 — session-TZ/DST safety). proration_skipped_at IS NULL excludes
-- apps permanently skipped as a would-be retroactive catch-up (migration 031,
-- D1d). Ordered by created_at so the oldest pending app charges first.
-- account_id IS NOT NULL excludes UNBILLED org roster rows (migration 041) —
-- an org app enters this sweep only once RepointOrgUsage attaches it.
-- name: AppsPendingProration :many
SELECT app_id
FROM ms_billing.apps
WHERE created_at <= @created_before::timestamptz
  AND account_id IS NOT NULL
  AND proration_invoice_id IS NULL
  AND (deleted_at IS NULL
       OR deleted_at >= created_at + make_interval(hours => @grace_hours::int))
  AND proration_skipped_at IS NULL
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

-- SetAppProrationSkipped arms the PERMANENT skip marker (migration 031, D1d):
-- the account only activated at/after this app's anchored creation period had
-- already closed, so charging it now would be a retroactive catch-up. The
-- WHERE clause makes it idempotent (a re-evaluation, or a concurrent one,
-- affects 0 rows once set) and defensively refuses to mark an app that was
-- somehow already charged in the meantime. :execrows so the caller can
-- observe (and tolerate) the already-set / already-charged cases.
-- name: SetAppProrationSkipped :execrows
UPDATE ms_billing.apps
SET proration_skipped_at = now()
WHERE app_id = $1
  AND proration_skipped_at IS NULL
  AND proration_invoice_id IS NULL;

-- MarkAppProrationAttempted stamps the recovery marker (036) BEFORE a
-- creation-proration charge attempt's first Stripe call. First-write-wins
-- (the FIRST attempt instant is the durable one); never cleared.
-- name: MarkAppProrationAttempted :exec
UPDATE ms_billing.apps
SET proration_attempted_at = $2
WHERE app_id = $1
  AND proration_attempted_at IS NULL;

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

-- SetAppName updates the frozen display name (SyncAppModules rename path).
-- WHERE deleted_at IS NULL freezes the name once deleted — the same
-- freeze-on-delete posture as SetAppModuleCount, so a rename after deletion is
-- a documented no-op (0 rows), keeping the last-known name for the bill.
-- name: SetAppName :execrows
UPDATE ms_billing.apps
SET name = $2
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
-- LIVE (deleted_at IS NULL) app on the account that has JOINED the advance
-- base mechanism by the cutoff — the boundary charge's advance-base input:
-- advance base = Σ (BaseFee + Overage × max(0, module_count − included)).
-- The cutoff is the NEW period's start (the closed window's period_end). Two
-- conditions, mirroring the module-timer coverage contract (review 2026-07-06):
--   * created_at < @created_before — an app created INSIDE the new period is
--     excluded, because RegisterApp's creation-proration leg already charged
--     that app's new-period base (full or prorated) — summing it here would
--     double-bill the same period (same-day cron race, and deterministically
--     on reclaimed skipped_no_pm/failed runs).
--   * created_at + grace < @created_before — an app whose CREATION GRACE had
--     not yet elapsed when the new period opened is excluded: it has not
--     survived grace yet (an app deleted in grace is NEVER charged, scenario
--     1 — precharging its next-period base here would bill a full month for
--     an app that can still be deleted for free), and when it does survive,
--     its creation-proration charge covers through the END of the period its
--     grace elapses into (the straddled period), so this boundary's new
--     period is already that leg's coverage. Spec: apps "join this boundary
--     mechanism starting at the NEXT boundary after their own creation charge
--     fires".
-- Deleted apps are excluded (D1e); an account with no rows (pre-backfill)
-- sums to base 0 and keeps the pre-027 arrears-only invoice. app_id is
-- returned so the charge leg can write the per-app-period base snapshot
-- (migration 028) it bills. The grace interval is expressed in HOURS, not
-- days (wave 2, D5): timestamptz + a day-interval is evaluated in the SESSION
-- timezone (DST-shifting), while the Go legs' grace is a fixed GraceDays*24h
-- UTC window (moduleGraceExpiry) — a non-UTC session would disagree with them
-- by an hour around DST and double-bill or gap a whole period.
-- name: LiveAppModuleCountsCreatedBefore :many
SELECT app_id, module_count
FROM ms_billing.apps
WHERE account_id = @account_id::uuid
  AND deleted_at IS NULL
  AND created_at < @created_before::timestamptz
  AND created_at + make_interval(hours => @grace_hours::int) < @created_before::timestamptz;

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

-- SettledNewCreationCharges is the SETTLED half of the ListNewCreationCharges read (the
-- web-account bill's 本期新建立 / "new this period" section): every app CREATED
-- in the resolved window whose creation-proration leg has already minted its
-- one invoice (proration_invoice_id armed, migration 027), joined to that
-- invoice in the ms_billing.invoices mirror so the row carries the ACTUAL
-- settled total — which may include co-created over-module line items billed on
-- the SAME combined invoice (proration.go scenario 3), NOT just a base
-- snapshot. Membership is the app's created_at ∈ [@period_start, @period_end);
-- the join is 1:1 (stripe_invoice_id is unique in the mirror). The
-- amount_due > 0 AND status <> 'void' filters drop a $0 / voided invoice —
-- a skipped_period_closed / no_charge proration never arms proration_invoice_id
-- at all (the guard stays NULL), so it is excluded by the join gate here rather
-- than needing a status predicate. amount_due is NUMERIC whole cents (Stripe
-- minor units); the store converts it to int64 micros (×10_000). Ordered by the
-- invoice created_at DESC (the display's "recorded at", newest-first), app_id
-- breaking ties for a deterministic scan.
--
-- The per-component BREAKDOWN columns let the UI split the row into
-- "基礎費用" + "N 加購模組": a.name is the frozen display name (037);
-- a.created_module_count is the count frozen at registration (030) the add-on
-- tier is derived from; s.base_micros is the SETTLED creation base from the
-- app's 'proration' base snapshot (028), LEFT-joined so a settled app missing
-- its snapshot still returns (base NULL → the service treats it as 0 and the
-- whole amount folds into add-ons). The snapshot join is 1:1: there is exactly
-- one source='proration' row per app (its creation period).
-- name: SettledNewCreationCharges :many
SELECT a.app_id,
       a.name,
       a.created_module_count,
       s.base_micros,
       i.id AS invoice_id,
       i.number,
       i.amount_due,
       i.created_at AS recorded_at
FROM ms_billing.apps a
JOIN ms_billing.invoices i ON i.stripe_invoice_id = a.proration_invoice_id
LEFT JOIN ms_billing.app_base_snapshots s
       ON s.app_id = a.app_id AND s.source = 'proration'
WHERE a.account_id = @account_id::uuid
  AND a.created_at >= @period_start::timestamptz
  AND a.created_at < @period_end::timestamptz
  AND a.proration_invoice_id IS NOT NULL
  AND i.status <> 'void'
  AND i.amount_due > 0
ORDER BY i.created_at DESC, a.app_id;

-- PendingNewCreationCharges is the PENDING half of the ListNewCreationCharges read: apps
-- CREATED in the resolved window that are STILL IN GRACE — not yet charged
-- (proration_invoice_id IS NULL), still live (deleted_at IS NULL), not
-- permanently skipped (proration_skipped_at IS NULL, migration 031), and whose
-- creation grace has NOT yet elapsed (created_at > @grace_cutoff, the service's
-- now − GraceDays, matching AppsPendingProration's cutoff from the other side).
-- Only the CURRENT live window can hold in-grace apps (a past period's apps have
-- all elapsed grace), so the service issues this query only for the current
-- window. This query carries no amount columns because there is no invoice or
-- base snapshot yet — the service COMPUTES the preview from these rows
-- (created_at + created_module_count) through the shared creation-charge math
-- (usage.CreationChargeBaseMicros + usage.CreationChargeAddonMicros), the same
-- functions the sweep charges through, so preview and charge agree. Ordered by
-- created_at (equivalently by the ETA, created_at + GraceDays) for a stable,
-- soonest-first scan.
--
-- name feeds the breakdown label; created_at anchors both the ETA and the
-- prorated preview amount; created_module_count (frozen at registration) sets
-- the add-on-module count AND the co-created overage projection.
-- name: PendingNewCreationCharges :many
SELECT app_id, name, created_module_count, created_at
FROM ms_billing.apps
WHERE account_id = @account_id::uuid
  AND created_at >= @period_start::timestamptz
  AND created_at < @period_end::timestamptz
  AND proration_invoice_id IS NULL
  AND proration_skipped_at IS NULL
  AND deleted_at IS NULL
  AND created_at > @grace_cutoff::timestamptz
ORDER BY created_at;
