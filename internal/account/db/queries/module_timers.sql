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
-- the install's period window without a second read, and the charge_attempted_at
-- recovery marker (036) so a retried candidate reconciles against Stripe first.
-- Ordered (installed_at, id) so the oldest install charges first (matches the
-- FIFO ordering). Backed by app_module_overage_timers_sweep_idx.
-- name: ModuleOverageTimersPastGrace :many
SELECT t.id, t.account_id, t.app_id, t.installed_at, t.grace_expires_at,
       t.charge_attempted_at, a.activated_at
FROM ms_billing.app_module_overage_timers t
JOIN ms_billing.accounts a ON a.id = t.account_id
WHERE t.removed_at IS NULL
  AND t.grace_resolved = false
  AND t.grace_expires_at <= $1
  AND a.activated_at IS NOT NULL
ORDER BY t.installed_at, t.id;

-- MarkModuleTimerChargeAttempted stamps the recovery marker (036) BEFORE a
-- charge attempt's first Stripe call. First-write-wins (the FIRST attempt
-- instant is the durable one); never cleared.
-- name: MarkModuleTimerChargeAttempted :exec
UPDATE ms_billing.app_module_overage_timers
SET charge_attempted_at = $2
WHERE id = $1
  AND charge_attempted_at IS NULL;

-- ModuleTimerStillPending is the charge-time re-verification read (review
-- 2026-07-06, M2): the sweep's work list is read ONCE and can be minutes stale
-- by the time a late candidate is processed — re-check live + unresolved
-- immediately before acting so a module removed (or resolved by a concurrent
-- sweep) mid-batch is not charged.
-- name: ModuleTimerStillPending :one
SELECT (removed_at IS NULL AND grace_resolved = false)::bool AS pending
FROM ms_billing.app_module_overage_timers
WHERE id = $1;

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
-- the count of the account's currently-live install timers that are "over"
-- (live-FIFO rank >= included) AND owed a FULL $3 precharge for the NEW period
-- [@period_end, next boundary) — ongoing over-modules continuing into it.
-- row_number() over the whole live set gives every live timer its 1-based FIFO
-- rank; rn > @included_modules is exactly the 0-based rank >= included ("over")
-- predicate. "over" is re-derived LIVE, so a charged timer that has since flipped
-- to "included" (an earlier install removed) is not counted.
--
-- The coverage contract with the grace legs (review 2026-07-06, tightened in
-- wave 2 D1) — a timer is "ongoing" for the new period iff BOTH of:
--   * installed_at < @period_end — it existed before the new period opened. A
--     module installed INSIDE the new period had that period covered by its OWN
--     grace charge (Leg 1 / scenario 3), exactly the same cutoff the advance-base
--     leg applies via LiveAppsCreatedBefore; without it a reclaimed
--     skipped_no_pm/failed boundary run double-bills the period.
--   * grace_expires_at < @period_end — its grace elapsed BEFORE the new period
--     opened. Every grace charge covers install → the END of the period its grace
--     elapses into, so a boundary-straddling timer's new period belongs to Leg 1,
--     not this precharge (counting it would double-bill; skipping the NEXT
--     boundary would leave a gap — this predicate does neither).
--
-- DELIBERATELY NOT a condition: grace_resolved (wave 2, D1). Resolution state
-- is MUTABLE and set only by the sweeps, so keying on it made the precharge
-- depend on cron ordering: a timer whose grace expired in the ~24h before the
-- boundary was still unresolved when the boundary run executed, got excluded,
-- and its post-boundary period was then billed by NO leg (Leg 1's coverage is
-- derived from immutable timestamps and stops at the boundary). Both cutoffs
-- above are immutable, so the precharge decision is identical whenever the run
-- (or its reclaim) executes. An expired-unresolved timer counted here is
-- charged its own install-period coverage by Leg 1 later — disjoint windows,
-- never a double-bill. D1d resolved-uncharged rows count too (only the
-- pre-activation install period is forgiven). Residual edge (accepted,
-- verdict-at-boundary-time semantics): a timer whose live rank improves
-- over→included between this run and its own sweep keeps the one precharge —
-- no refund (D1e); the next boundary excludes it by rank.
-- name: CountOngoingOverModuleTimers :one
SELECT COALESCE(count(*), 0)::bigint AS over_count
FROM (
    SELECT installed_at, grace_expires_at,
           row_number() OVER (ORDER BY installed_at, id) AS rn
    FROM ms_billing.app_module_overage_timers
    WHERE account_id = @account_id::uuid
      AND removed_at IS NULL
) ranked
WHERE rn > @included_modules::int
  AND installed_at < @period_end::timestamptz
  AND grace_expires_at < @period_end::timestamptz;

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
