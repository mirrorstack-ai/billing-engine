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
-- member_count ($7, migration 077) is the live app-member count at creation
-- and plan ($8, migration 075) the plan the app is created ON — a creation
-- that chooses Free lands on it directly instead of registering on the default
-- and changing plan inside its grace. Both are validated by RegisterApp.
-- created_plan is stamped from the SAME $8 value as plan and never written
-- again (migration 077): the creation charge prices from it.
INSERT INTO ms_billing.apps (app_id, account_id, module_count, created_module_count, created_at, name, owner_org_id, member_count, plan, created_plan)
VALUES ($1, $2, $3, $3, $4, $5, $6, $7, $8, $8)
ON CONFLICT (app_id) DO NOTHING;

-- SelectAppMirror reads one roster row (deleted or not — the caller decides
-- what deletion means for its path: SyncAppModules no-ops a count update,
-- GetAppBill still displays the spent creation-period base).
-- name: SelectAppMirror :one
SELECT app_id, account_id, module_count, created_module_count, created_at, name,
       proration_invoice_id, proration_skipped_at, proration_attempted_at, deleted_at, plan,
       owner_org_id, member_count, created_plan
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
       proration_invoice_id, proration_skipped_at, proration_attempted_at, deleted_at, plan,
       owner_org_id, member_count, created_plan
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
-- settlement reference (a Stripe invoice id or synthetic wallet ref), and the WHERE
-- proration_invoice_id IS NULL makes the write first-charge-wins — a retry or
-- concurrent double-fire affects 0 rows and the original reference survives.
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

-- SetAppProrationSkippedOnPlan arms the permanent skip marker for a window
-- priced at $0 — ONLY while the row still carries the plan the derivation
-- priced and none of the three creation markers (billing-engine#208 round 4).
-- The derivation runs on an unlocked read; a plan-change fold committing
-- between that read and this write (under the app lock, flipping apps.plan)
-- would otherwise be made terminal at $0 and its days priced by nobody. The
-- row-level UPDATE re-evaluates the predicate under the row lock, so 0 rows
-- means "the row moved: re-derive next sweep". :execrows for that reason.
-- name: SetAppProrationSkippedOnPlan :execrows
UPDATE ms_billing.apps
SET proration_skipped_at = now()
WHERE app_id = $1
  AND plan = $2
  AND proration_skipped_at IS NULL
  AND proration_invoice_id IS NULL
  AND proration_attempted_at IS NULL;

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

-- LiveAppModuleCountsCreatedBefore returns (app_id, module_count, plan) for every
-- LIVE (deleted_at IS NULL) app on the account that has JOINED the advance
-- base mechanism by the cutoff — the boundary charge's advance-base input:
-- advance base = Σ each app's plan base (module overage rides per-install
-- timers, migration 033).
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
-- member_count is deliberately NOT returned: the members fee (migration 077)
-- is the CLOSED period's, priced in arrears from MemberHighWaterForAccount —
-- nothing on this roster is an advance-members component.
-- The plan column returned is the plan whose base the NEW period [boundary,
-- next boundary) is owed at, not the row's live plan. From the ledger
-- (migration 076 — every change flips apps.plan in the transaction that
-- writes its row, so the chain is exact): the earliest effective change that
-- does NOT price the new period carries, as its from_plan, the plan this
-- roster bills. That is every change effective strictly after the boundary
-- — and an UPGRADE effective exactly AT it, because upgradeNow's delta for
-- an instant on the boundary covers the whole new period (its anchored
-- period is half-open at the start). A downgrade applied at the boundary is
-- the opposite case: it has taken effect for the new period, so it is not
-- excluded and a.plan (the plan it moved to) is billed. An upgrade requested
-- in the gap between the boundary and the cron that closes it has already
-- charged (new − old) for the new period on its own row; pricing this base
-- at the live plan would bill the new base a second time — and a reclaim of
-- a failed run days later must derive the SAME figure the first attempt did,
-- whatever the plan has become since.
-- name: LiveAppModuleCountsCreatedBefore :many
SELECT a.app_id, a.module_count,
       COALESCE((SELECT c.from_plan FROM ms_billing.app_plan_changes c
                 WHERE c.app_id = a.app_id
                   AND c.status IN ('pending', 'settled', 'applied')
                   AND (c.effective_at > @created_before::timestamptz
                        OR (c.effective_at = @created_before::timestamptz AND c.kind = 'upgrade'))
                 ORDER BY c.effective_at, c.id LIMIT 1), a.plan)::text AS plan
FROM ms_billing.apps a
WHERE a.account_id = @account_id::uuid
  AND a.deleted_at IS NULL
  AND a.created_at < @created_before::timestamptz
  AND a.created_at + make_interval(hours => @grace_hours::int) < @created_before::timestamptz;

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
-- in the resolved window whose creation-proration guard is armed. Stripe-settled
-- rows join their invoice mirror; credit-wallet rows deliberately have no
-- invoice mirror and instead recover the settled base draw from credit_ledger.
-- Membership is the SETTLEMENT instant ∈ [@period_start, @period_end), not the
-- app's created_at. Creation grace can cross an anchored period boundary (for
-- example, create Aug 30, settle Sep 2); the charge belongs in the period in
-- which it actually settled. Stripe's
-- amount_due > 0 AND status <> 'void' filters apply only to its branch. The
-- wallet branch exposes its synthetic proration ref as the stable display
-- identity and converts the ledger's micro-dollar sum to NUMERIC cents so the
-- store's established cents-to-micros boundary remains unchanged. The wallet
-- recorded_at comes from the wallet ledger itself, so later app metadata syncs
-- cannot move a settled charge into a different display period. Ordered
-- newest-first with app_id breaking ties for a deterministic scan.
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
       COALESCE(i.id, a.app_id) AS invoice_id,
       COALESCE(i.number, NULLIF(a.proration_invoice_id, i.stripe_invoice_id)) AS number,
       (CASE
           WHEN a.proration_invoice_id LIKE 'wallet:%' THEN wallet_draw.amount_due
           ELSE i.amount_due
       END)::numeric AS amount_due,
       COALESCE(i.created_at, wallet_draw.recorded_at) AS recorded_at
FROM ms_billing.apps a
LEFT JOIN ms_billing.invoices i ON i.stripe_invoice_id = a.proration_invoice_id
LEFT JOIN ms_billing.app_base_snapshots s
       ON s.app_id = a.app_id AND s.source = 'proration'
LEFT JOIN LATERAL (
    SELECT COALESCE(-SUM(cl.amount_micros), 0)::numeric / 10000 AS amount_due,
           MIN(cl.created_at) AS recorded_at
    FROM ms_billing.credit_ledger cl
    WHERE cl.account_id = a.account_id
      AND cl.status = 'settled'
      AND cl.type = 'usage_draw'
      AND cl.idempotency_key LIKE 'wallet-draw:app-creation:' || a.app_id::text || ':%'
) wallet_draw ON a.proration_invoice_id LIKE 'wallet:%'
WHERE a.account_id = @account_id::uuid
  AND a.proration_invoice_id IS NOT NULL
  AND (
      a.proration_invoice_id LIKE 'wallet:%'
      OR (
          a.proration_invoice_id NOT LIKE 'wallet:%'
          AND i.status <> 'void'
          AND i.amount_due > 0
      )
  )
  AND COALESCE(i.created_at, wallet_draw.recorded_at) >= @period_start::timestamptz
  AND COALESCE(i.created_at, wallet_draw.recorded_at) < @period_end::timestamptz
ORDER BY recorded_at DESC, a.app_id;

-- SettledNewCreationChargesLegacy is the migration-048-independent OFF path.
-- It intentionally preserves the pre-wallet Stripe-only query byte-for-byte so
-- CREDIT_WALLET_ENABLED=false prepares and executes no SQL naming credit_ledger
-- or accounts.billing_mode. The usage store selects the wallet-aware query
-- above only after the startup capability probe succeeds.
-- name: SettledNewCreationChargesLegacy :many
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
  AND a.proration_invoice_id IS NOT NULL
  AND i.status <> 'void'
  AND i.amount_due > 0
  AND i.created_at >= @period_start::timestamptz
  AND i.created_at < @period_end::timestamptz
ORDER BY i.created_at DESC, a.app_id;

-- PendingNewCreationCharges is the PENDING half of the ListNewCreationCharges read: apps
-- CREATED in the resolved window that are STILL IN GRACE — not yet charged
-- (proration_invoice_id IS NULL), still live (deleted_at IS NULL), not
-- permanently skipped (proration_skipped_at IS NULL, migration 031), and whose
-- creation grace has NOT yet elapsed (created_at > @grace_cutoff, the service's
-- now − GraceDays, matching AppsPendingProration's cutoff from the other side).
-- Only the CURRENT live window can hold in-grace apps (a past period's apps have
-- all elapsed grace), so the service issues this query only for the current
-- window. This query carries no amount columns (there is no invoice or base
-- snapshot for an in-grace app yet); the service DERIVES the base preview from
-- created_at through usage.CreationChargeBaseMicros — the creation-period
-- proration plus the straddle top-up, the EXACT base the sweep will charge,
-- deterministic because it is anchored to created_at. Ordered by created_at
-- (equivalently by the ETA, created_at + GraceDays) for a stable, soonest-first
-- scan.
--
-- name feeds the breakdown label; created_module_count sets the add-on-module
-- COUNT (max(0, count − IncludedModules), known at creation). Only the base
-- MONEY is previewed — the co-created add-on overage is NOT projected (its count
-- surfaces, AddonMicros stays 0): the sweep counts over-modules by an
-- account-level FIFO rank that can shift before it fires, so unlike the
-- created_at-anchored base that dollar amount is not deterministic here.
-- name: PendingNewCreationCharges :many
SELECT app_id, name, created_module_count, created_at, plan
FROM ms_billing.apps
WHERE account_id = @account_id::uuid
  AND created_at >= @period_start::timestamptz
  AND created_at < @period_end::timestamptz
  AND proration_invoice_id IS NULL
  AND proration_skipped_at IS NULL
  AND deleted_at IS NULL
  AND created_at > @grace_cutoff::timestamptz
ORDER BY created_at;

-- SetAppPlan moves a LIVE app onto a billing plan (core-v2#1412, migration 075).
-- A deleted row is frozen like SetAppModuleCount's (no future base, so no plan to
-- move), which is why the caller reads rows affected: 0 means deleted or never
-- registered, never a silent success.
-- name: SetAppPlan :execrows
UPDATE ms_billing.apps
SET plan = $2
WHERE app_id = $1
  AND deleted_at IS NULL;

-- SetAppMemberCount snapshots a new app-member count (SyncAppModules,
-- migration 077). WHERE deleted_at IS NULL freezes the count once deleted —
-- the same posture as SetAppModuleCount: a deleted app accrues no future
-- member fee, so there is no future count to move. :execrows; the service
-- resolves 0 rows through the SelectAppMirror existence check it already made.
-- name: SetAppMemberCount :execrows
UPDATE ms_billing.apps
SET member_count = $2
WHERE app_id = $1
  AND deleted_at IS NULL;

-- CountUserPlanCommitments counts one PERSONAL account's live apps that are
-- ON a plan or have a SCHEDULED downgrade TO it (owner_org_id IS NULL — a
-- user-owned roster row), excluding @except_app_id so a change of the app
-- being asked about never counts itself. The Free cap (usage.PlanTerms.MaxApps:
-- 3 per personal account) is enforced against it, under the owner's advisory
-- lock (LockPlanCapOwner) so two concurrent commitments cannot both pass.
-- name: CountUserPlanCommitments :one
SELECT count(*)::bigint AS live_count
FROM ms_billing.apps a
WHERE a.account_id = @account_id::uuid
  AND a.owner_org_id IS NULL
  AND a.deleted_at IS NULL
  AND a.app_id <> @except_app_id::uuid
  AND (a.plan = @plan::text
       OR EXISTS (SELECT 1 FROM ms_billing.app_plan_changes c
                  WHERE c.app_id = a.app_id AND c.status = 'scheduled' AND c.to_plan = @plan::text));

-- CountOrgPlanCommitments is the org twin: one ORGANIZATION's, keyed by
-- owner_org_id rather than the funding account (a sponsored org's apps sit on
-- the sponsor's account, and the cap is per org, not per payer —
-- usage.PlanTerms.MaxAppsPerOrg: 1 Free app per org).
-- name: CountOrgPlanCommitments :one
SELECT count(*)::bigint AS live_count
FROM ms_billing.apps a
WHERE a.owner_org_id = @owner_org_id::uuid
  AND a.deleted_at IS NULL
  AND a.app_id <> @except_app_id::uuid
  AND (a.plan = @plan::text
       OR EXISTS (SELECT 1 FROM ms_billing.app_plan_changes c
                  WHERE c.app_id = a.app_id AND c.status = 'scheduled' AND c.to_plan = @plan::text));

-- LockPlanCapOwner serializes every Free-cap decision for one owner (a
-- personal account or an org) for the rest of the transaction: the count
-- above and the write that commits to the plan happen under it, so two
-- concurrent RegisterApp / SetAppPlan / TransferApp calls cannot both read
-- "2 of 3" and both commit. A transaction-scoped advisory lock keyed on the
-- owner id, released at commit/rollback.
-- name: LockPlanCapOwner :exec
SELECT pg_advisory_xact_lock(hashtext('plan-cap:' || @owner_id::text));

-- InsertAppMemberCount appends one member-count history row (migration 077).
-- name: InsertAppMemberCount :exec
INSERT INTO ms_billing.app_member_counts (app_id, count, recorded_at)
VALUES (@app_id::uuid, @count::int, @recorded_at::timestamptz);

-- MemberHighWaterForAccount is the boundary's members input (migration 077,
-- owner 2026-09-13): per app that HELD members during the closed period
-- [period_start, period_end) — live, or deleted inside it, created before it
-- ended, in or out of its creation grace — the period's high-water mark:
-- max(count in force when the period opened, max count recorded inside it).
-- The plan returned is the plan IN FORCE WHEN THE PERIOD OPENED: the earliest
-- ledger change effective strictly after period_start carries it as its
-- from_plan (a change effective exactly at period_start — a downgrade the
-- previous boundary applied, or an upgrade whose delta covered this whole
-- period — is in force); otherwise the row's plan. Within a period plans only
-- rise (a downgrade lands at a boundary), so pricing the included count at
-- the opening plan means a last-day upgrade cannot buy the period's member
-- allowance for one day's delta — and a reclaim after the boundary apply
-- flipped the row still prices the closed period the same.
-- name: MemberHighWaterForAccount :many
SELECT a.app_id,
       COALESCE((SELECT c.from_plan FROM ms_billing.app_plan_changes c
                 WHERE c.app_id = a.app_id
                   AND c.status IN ('pending', 'settled', 'applied')
                   AND c.effective_at > @period_start::timestamptz
                 ORDER BY c.effective_at, c.id LIMIT 1), a.plan)::text AS plan,
       GREATEST(
           COALESCE((SELECT c.count FROM ms_billing.app_member_counts c
                     WHERE c.app_id = a.app_id AND c.recorded_at < @period_start::timestamptz
                     ORDER BY c.recorded_at DESC, c.id DESC LIMIT 1), 0),
           COALESCE((SELECT MAX(c.count) FROM ms_billing.app_member_counts c
                     WHERE c.app_id = a.app_id
                       AND c.recorded_at >= @period_start::timestamptz
                       AND c.recorded_at < @period_end::timestamptz), 0)
       )::int AS member_hwm
FROM ms_billing.apps a
WHERE a.account_id = @account_id::uuid
  AND a.created_at < @period_end::timestamptz
  AND (a.deleted_at IS NULL OR a.deleted_at >= @period_start::timestamptz);
