-- Queries backing the per-module-instance overage timers (migration 033,
-- DESIGN.md "Base fee — v2"). One row per module INSTALL EVENT; the charge
-- layer (cycle/overage.go) synthesizes instances (the RPC carries only an
-- integer module_count), determines included-vs-over LIVE by FIFO, and charges
-- the over rows once their own grace elapses.

-- LiveModuleTimerCountForApp counts an app's currently-live (removed_at IS NULL)
-- install timers — the reconciliation input RegisterApp / SyncAppModules use to
-- decide how many rows to insert or LIFO-remove so the live-timer set matches the
-- app's module_count idempotently across fire-and-forget retries. ::bigint keeps
-- it a non-nullable scalar.
-- name: LiveModuleTimerCountForApp :one
SELECT COALESCE(count(*), 0)::bigint AS live_count
FROM ms_billing.app_module_overage_timers
WHERE app_id = $1
  AND removed_at IS NULL;

-- InsertModuleOverageTimers inserts N identical install timers for one app, all
-- anchored at the SAME installed_at / grace_expires_at (RegisterApp's K
-- co-created modules share created_at; a SyncAppModules grow shares now()).
-- generate_series(1, @count) with @count <= 0 yields no rows — a safe no-op.
-- name: InsertModuleOverageTimers :exec
INSERT INTO ms_billing.app_module_overage_timers
    (account_id, app_id, installed_at, grace_expires_at)
SELECT @account_id::uuid, @app_id::uuid, @installed_at::timestamptz, @grace_expires_at::timestamptz
FROM generate_series(1, @count::int);

-- SoftRemoveNewestModuleTimers LIFO-soft-removes the N NEWEST currently-live
-- install timers for one app (a SyncAppModules shrink removes what was added most
-- recently). Ordered (installed_at DESC, id DESC) — the reverse of the FIFO
-- ordering. Sets removed_at = @removed_at on exactly those rows.
-- name: SoftRemoveNewestModuleTimers :exec
UPDATE ms_billing.app_module_overage_timers
SET removed_at = @removed_at::timestamptz
WHERE id IN (
    SELECT id
    FROM ms_billing.app_module_overage_timers
    WHERE app_id = @app_id::uuid
      AND removed_at IS NULL
    ORDER BY installed_at DESC, id DESC
    LIMIT @limit_count::int
);

-- SoftRemoveAllModuleTimersForApp soft-removes EVERY still-live install timer for
-- an app — the app-deletion path. Idempotent: a re-fire affects the rows already
-- removed 0 times (WHERE removed_at IS NULL).
-- name: SoftRemoveAllModuleTimersForApp :exec
UPDATE ms_billing.app_module_overage_timers
SET removed_at = $2
WHERE app_id = $1
  AND removed_at IS NULL;

-- ModuleOverageTimersPastGrace is Leg 1's work list: live, unresolved install
-- timers whose grace window has elapsed as of $1, on accounts that are chargeable
-- (activated_at IS NOT NULL — the same activation gate as the spine + proration
-- leg). Each row carries the account's activation anchor so the sweep can resolve
-- the install's period window without a second read. Ordered (installed_at, id)
-- so the oldest install charges first (matches the FIFO ordering). Backed by
-- app_module_overage_timers_sweep_idx.
-- name: ModuleOverageTimersPastGrace :many
SELECT t.id, t.account_id, t.app_id, t.installed_at, t.grace_expires_at,
       a.activated_at
FROM ms_billing.app_module_overage_timers t
JOIN ms_billing.accounts a ON a.id = t.account_id
WHERE t.removed_at IS NULL
  AND t.grace_resolved = false
  AND t.grace_expires_at <= $1
  AND a.activated_at IS NOT NULL
ORDER BY t.installed_at, t.id;

-- LiveModuleTimerRankBefore returns how many of the account's currently-live
-- install timers order STRICTLY BEFORE a given (installed_at, id) under the FIFO
-- ordering (installed_at ASC, id ASC) — i.e. the target's 0-based FIFO rank.
-- rank < IncludedModules ⇒ "included"; rank >= IncludedModules ⇒ "over".
-- Computed fresh at every grace-check (never cached). Backed by
-- app_module_overage_timers_live_fifo_idx.
-- name: LiveModuleTimerRankBefore :one
SELECT COALESCE(count(*), 0)::bigint AS rank
FROM ms_billing.app_module_overage_timers
WHERE account_id = @account_id::uuid
  AND removed_at IS NULL
  AND (installed_at < @installed_at::timestamptz
       OR (installed_at = @installed_at::timestamptz AND id < @timer_id::uuid));

-- MarkModuleTimerIncluded stamps a TERMINAL "included" verdict (grace_resolved =
-- true, no charge) on a timer the grace-check found within the included 5.
-- WHERE grace_resolved = false is first-write-wins (a concurrent sweep that
-- already resolved it affects 0 rows). Monotonicity makes this verdict permanent
-- — the row is never re-checked.
-- name: MarkModuleTimerIncluded :exec
UPDATE ms_billing.app_module_overage_timers
SET grace_resolved = true
WHERE id = $1
  AND grace_resolved = false;

-- MarkModuleTimerCharged stamps the TERMINAL "over and charged" verdict once
-- Leg 1's Stripe charge succeeded: grace_charged_at + grace_resolved = true and
-- the GENUINE Stripe invoice / invoice-item ids (never idempotency-key strings).
-- WHERE grace_resolved = false keeps a crash-retry idempotent (the deterministic
-- per-timer Stripe keys already dedupe the charge itself).
-- name: MarkModuleTimerCharged :exec
UPDATE ms_billing.app_module_overage_timers
SET grace_resolved        = true,
    grace_charged_at      = @grace_charged_at::timestamptz,
    grace_invoice_id      = @grace_invoice_id,
    grace_invoice_item_id = @grace_invoice_item_id
WHERE id = @timer_id::uuid
  AND grace_resolved = false;

-- CountOngoingOverModuleTimers is Leg 2's boundary-precharge input (scenario 6):
-- the count of the account's currently-live install timers that are BOTH "over"
-- (live-FIFO rank >= included) AND already charged at least once (grace_charged_at
-- IS NOT NULL) — i.e. ongoing over-modules continuing into the new period, each
-- owed a FULL $3 precharge on the boundary invoice. row_number() over the whole
-- live set gives every live timer its 1-based FIFO rank; rn > @included_modules is
-- exactly the 0-based rank >= included ("over") predicate. A timer whose grace has
-- not elapsed yet (grace_charged_at IS NULL) is EXCLUDED — it stays on Leg 1's own
-- timer and is never double-counted here. "over" is re-derived LIVE, so a charged
-- timer that has since flipped to "included" (an earlier install removed) is not
-- counted.
-- name: CountOngoingOverModuleTimers :one
SELECT COALESCE(count(*), 0)::bigint AS over_count
FROM (
    SELECT grace_charged_at,
           row_number() OVER (ORDER BY installed_at, id) AS rn
    FROM ms_billing.app_module_overage_timers
    WHERE account_id = @account_id::uuid
      AND removed_at IS NULL
) ranked
WHERE rn > @included_modules::int
  AND grace_charged_at IS NOT NULL;

-- CoCreatedOverModuleTimers backs the scenario-3 combined creation invoice: the
-- ids of the app's live, unresolved install timers whose install instant IS the
-- app's created_at (co-created at app creation) AND that are "over" (live-FIFO
-- rank >= included). Their grace elapses at the SAME instant as the app's own
-- creation grace, so the creation-proration charge folds them onto ONE invoice.
-- The rank window spans ALL the account's live timers (an included module still
-- occupies a FIFO slot), so rn > @included_modules is the 0-based rank >= included
-- "over" predicate; the outer filter keeps only this app's co-created, still-
-- unresolved rows. Ordered (installed_at, id) for a deterministic charge order.
-- name: CoCreatedOverModuleTimers :many
SELECT id
FROM (
    SELECT id, app_id, installed_at, grace_resolved,
           row_number() OVER (ORDER BY installed_at, id) AS rn
    FROM ms_billing.app_module_overage_timers
    WHERE account_id = @account_id::uuid
      AND removed_at IS NULL
) ranked
WHERE app_id = @app_id::uuid
  AND installed_at = @created_at::timestamptz
  AND grace_resolved = false
  AND rn > @included_modules::int
ORDER BY installed_at, id;

-- CountLiveModuleTimersForAccount returns the account's currently-live
-- (removed_at IS NULL) install-timer count — the DISPLAY read behind
-- GetAccountBill's account-overage line under the per-module-instance model
-- (migration 033). The steady-state estimate $3 × max(0, live − included) counts
-- the live "over" rows (the FIFO tail past the included 5); reading the timer
-- table (the overage model's source of truth) rather than SUM(apps.module_count)
-- keeps the shown overage tied to the rows the charge legs actually tier on.
-- ::bigint keeps the aggregate a non-nullable scalar.
-- name: CountLiveModuleTimersForAccount :one
SELECT COALESCE(count(*), 0)::bigint AS live_count
FROM ms_billing.app_module_overage_timers
WHERE account_id = $1
  AND removed_at IS NULL;
