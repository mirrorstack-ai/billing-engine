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

-- ModuleOverageTimersPastGrace is Leg 1's work list: unresolved install timers
-- whose grace window has elapsed as of $1, on accounts that are chargeable
-- (activated_at IS NOT NULL — the same activation gate as the spine + proration
-- leg). Like AppsPendingProration, an attempted row survives later removal so a
-- possibly in-flight Stripe attempt can converge to its terminal guard.
-- charge_attempted_at is stamped only after grace has already elapsed, so this
-- carve-out never admits a timer removed inside its grace window. Each row carries
-- the account's activation anchor so the sweep can resolve the install's period
-- window without a second read, and the charge_attempted_at recovery marker (036)
-- so a retried candidate reconciles against Stripe first.
-- Ordered (installed_at, id) so the oldest install charges first (matches the
-- FIFO ordering). Backed by app_module_overage_timers_sweep_idx.
-- name: ModuleOverageTimersPastGrace :many
SELECT t.id, t.account_id, t.app_id, t.installed_at, t.grace_expires_at,
       t.charge_attempted_at, a.activated_at
FROM ms_billing.app_module_overage_timers t
JOIN ms_billing.accounts a ON a.id = t.account_id
WHERE (t.removed_at IS NULL OR t.charge_attempted_at IS NOT NULL)
  AND t.grace_resolved = false
  AND t.grace_expires_at <= $1
  AND a.activated_at IS NOT NULL
ORDER BY t.installed_at, t.id;

-- MarkModuleTimerChargeAttempted stamps the recovery marker (036) BEFORE a
-- charge attempt's first Stripe call. First-write-wins (the FIRST attempt instant
-- is the durable one, preserved by COALESCE); never cleared. The grace_resolved =
-- false guard (billing-engine Job 3 hardening) makes this stamp the serialization
-- point against a concurrent credit-wallet settlement: DrawModuleOverageFromWallet
-- arms grace_resolved (+ grace_charged_at) inside its own row-locked transaction,
-- so a Stripe worker that lost the race matches 0 rows here and MUST abort as stale
-- rather than draft/finalize a second charge on an already-settled timer. Returns
-- rows-affected so the caller can detect the lost race: 0 rows ⟺ grace_resolved is
-- already true. It deliberately does NOT gate on charge_attempted_at IS NULL — a
-- crash-recovery retry (marker set by a prior attempt that died before creating its
-- invoice, with nothing found on Stripe) must still re-charge, so it matches the
-- row (1 affected) while COALESCE keeps the original attempt instant.
-- name: MarkModuleTimerChargeAttempted :execrows
UPDATE ms_billing.app_module_overage_timers
SET charge_attempted_at = COALESCE(charge_attempted_at, $2)
WHERE id = $1
  AND grace_resolved = false
  AND NOT EXISTS (
      SELECT 1
      FROM ms_billing.app_combined_proration_attempt_timers owned
      JOIN ms_billing.app_combined_proration_attempts attempt
        ON attempt.app_id = owned.app_id
      WHERE owned.timer_id = ms_billing.app_module_overage_timers.id
        AND attempt.resolved_at IS NULL
  );

-- ModuleTimerStillPending is the charge-time re-verification read (review
-- 2026-07-06, M2): the sweep's work list is read ONCE and can be minutes stale
-- by the time a late candidate is processed. Mid-batch removal of an UNATTEMPTED
-- timer still skips the charge; only a timer that already reached Stripe survives
-- removal so its crashed attempt can be reconciled. A timer resolved by a
-- concurrent sweep is always skipped.
-- name: ModuleTimerStillPending :one
SELECT (
    (timer.removed_at IS NULL OR timer.charge_attempted_at IS NOT NULL)
    AND timer.grace_resolved = false
    AND NOT EXISTS (
        SELECT 1
        FROM ms_billing.app_combined_proration_attempt_timers owned
        JOIN ms_billing.app_combined_proration_attempts attempt
          ON attempt.app_id = owned.app_id
        WHERE owned.timer_id = timer.id
          AND attempt.resolved_at IS NULL
    )
)::bool AS pending
FROM ms_billing.app_module_overage_timers timer
WHERE timer.id = $1;

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
  AND grace_resolved = false
  AND NOT EXISTS (
      SELECT 1
      FROM ms_billing.app_combined_proration_attempt_timers owned
      JOIN ms_billing.app_combined_proration_attempts attempt
        ON attempt.app_id = owned.app_id
      WHERE owned.timer_id = ms_billing.app_module_overage_timers.id
        AND attempt.resolved_at IS NULL
  );

-- SelectModuleTimerForUpdate reads one install timer under a ROW LOCK
-- (FOR UPDATE) — the credit-wallet module-overage draw's race-safety primitive
-- (Job 3, mirrors apps.sql SelectAppMirrorForUpdate for the creation leg).
-- DrawModuleOverageFromWallet locks the timer row to re-verify, UNDER the lock,
-- that it is still live (removed_at IS NULL) and unresolved (grace_resolved =
-- false) and that no concurrent Stripe attempt is in flight (charge_attempted_at
-- IS NULL) before drawing the wallet and arming the guard atomically. account_id
-- feeds the wallet allocation. Unlike the creation leg there is no Stripe network
-- call to keep outside the lock, so the whole draw + guard-arm runs in one tx.
-- name: SelectModuleTimerForUpdate :one
SELECT account_id, removed_at, grace_resolved, charge_attempted_at
FROM ms_billing.app_module_overage_timers
WHERE id = $1
FOR UPDATE;

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

-- PendingAddonModuleCharges is the pending ADD-ON half of the ListNewCreationCharges
-- read (本期新建立): the account's live, unresolved install timers still INSIDE
-- their own grace window as of @now — add-on charges that WILL fire (Leg 1 /
-- cycle/overage.go) but haven't yet — grouped per app. Only timers "over" per
-- the live FIFO rank (rn > @included_modules) are add-on charges at all.
-- Co-created timers (installed_at = the app's created_at) are EXCLUDED: their
-- pending charge is already represented by the app's own pending creation row
-- (the scenario-3 combined invoice bills them together), and a co-created timer
-- is in grace iff its app is — listing both would double-show one upcoming
-- charge. Deleted apps cannot appear (deletion soft-removes every timer).
-- One row per app: the frozen name, the count of pending add-on timers, and the
-- EARLIEST grace expiry as the charge ETA. Ordered soonest-first, app_id
-- breaking ties for a deterministic scan.
-- name: PendingAddonModuleCharges :many
SELECT ranked.app_id,
       a.name,
       count(*)::bigint AS addon_count,
       min(ranked.grace_expires_at)::timestamptz AS charge_eta
FROM (
    SELECT id, app_id, installed_at, grace_expires_at, grace_resolved,
           row_number() OVER (ORDER BY installed_at, id) AS rn
    FROM ms_billing.app_module_overage_timers
    WHERE account_id = @account_id::uuid
      AND removed_at IS NULL
) ranked
JOIN ms_billing.apps a ON a.app_id = ranked.app_id
WHERE ranked.rn > @included_modules::int
  AND ranked.grace_resolved = false
  AND ranked.grace_expires_at > @now::timestamptz
  AND ranked.installed_at <> a.created_at
GROUP BY ranked.app_id, a.name
ORDER BY min(ranked.grace_expires_at), ranked.app_id;

-- UnresolvedOneTimeCharges is GetAccountBill's authoritative one-time
-- projection input. One SQL statement returns both mutable fresh candidates
-- and immutable migration-050 combined-attempt components from one MVCC
-- snapshot. A frozen app/timer identity is excluded from the dynamic branches
-- and emitted exactly once from its header/child rows; removal, FIFO drift, or
-- a delayed retry can never rewrite money already claimed by Stripe recovery.
--
-- Dynamic recovery eligibility mirrors the charge legs:
--   * an attempted app survives a later within-grace deletion;
--   * an attempted timer survives removal or an over→included rank change;
--   * unattempted candidates still use D11 + live account-FIFO rank.
-- A legacy attempted app without a migration-050 header is explicitly flagged
-- so the service fails closed instead of reconstructing unknown ownership.
--
-- Frozen rows carry raw micros and both exact snapshot windows. The service
-- subtracts a recurring unit only when the frozen full-period snapshot exactly
-- equals the projected next period AND that exact app/child is still represented
-- by the live recurring forecast. Declared/actual child counts are repeated on
-- every frozen row (including the base), making incomplete ownership loud.
-- name: UnresolvedOneTimeCharges :many
WITH live_timer_ranks AS (
    SELECT id,
           row_number() OVER (ORDER BY installed_at, id) AS rn
    FROM ms_billing.app_module_overage_timers
    WHERE account_id = @account_id::uuid
      AND removed_at IS NULL
),
frozen_headers AS (
    SELECT attempt.*,
           (
               SELECT count(*)::bigint
               FROM ms_billing.app_combined_proration_attempt_timers child
               WHERE child.app_id = attempt.app_id
           ) AS actual_timer_count
    FROM ms_billing.app_combined_proration_attempts attempt
    WHERE attempt.account_id = @account_id::uuid
      AND attempt.resolved_at IS NULL
),
dynamic_creations AS (
    SELECT 'creation_base'::text AS charge_kind,
           app.app_id AS charge_id,
           app.app_id,
           app.created_at AS charge_at,
           app.created_at + make_interval(hours => @grace_hours::int) AS grace_expires_at,
           account.activated_at,
           (app.deleted_at IS NULL) AS counts_toward_recurring,
           false AS frozen,
           0::bigint AS frozen_amount_micros,
           NULL::timestamptz AS frozen_snapshot_period_start,
           NULL::timestamptz AS frozen_snapshot_period_end,
           NULL::bigint AS frozen_snapshot_base_micros,
           NULL::timestamptz AS frozen_straddle_period_start,
           NULL::timestamptz AS frozen_straddle_period_end,
           NULL::bigint AS frozen_straddle_base_micros,
           false AS frozen_has_straddle,
           0::int AS frozen_declared_timer_count,
           0::bigint AS frozen_actual_timer_count,
           -- Any attempted row that reaches the dynamic branch is inconsistent:
           -- a healthy migration-050 attempt has an unresolved header and was
           -- excluded above. This also rejects resolved-header/no-app-terminal
           -- split state instead of reconstructing mutable money.
           (
               app.proration_attempted_at IS NOT NULL
               OR EXISTS (
                   SELECT 1
                   FROM ms_billing.app_combined_proration_attempts any_attempt
                   WHERE any_attempt.app_id = app.app_id
               )
           ) AS ownership_unknown
    FROM ms_billing.apps app
    JOIN ms_billing.accounts account ON account.id = app.account_id
    WHERE app.account_id = @account_id::uuid
      AND account.activated_at IS NOT NULL
      AND app.proration_invoice_id IS NULL
      AND NOT EXISTS (
          SELECT 1
          FROM frozen_headers frozen
          WHERE frozen.app_id = app.app_id
      )
      AND (
          app.proration_attempted_at IS NOT NULL
          OR (
              app.proration_skipped_at IS NULL
              AND (
                  app.deleted_at IS NULL
                  OR app.deleted_at >= app.created_at + make_interval(hours => @grace_hours::int)
              )
          )
      )
),
dynamic_timers AS (
    SELECT 'module_timer'::text AS charge_kind,
           timer.id AS charge_id,
           timer.app_id,
           timer.installed_at AS charge_at,
           timer.grace_expires_at,
           account.activated_at,
           (
               timer.removed_at IS NULL
               AND rank.rn > @included_modules::int
           ) AS counts_toward_recurring,
           false AS frozen,
           0::bigint AS frozen_amount_micros,
           NULL::timestamptz AS frozen_snapshot_period_start,
           NULL::timestamptz AS frozen_snapshot_period_end,
           NULL::bigint AS frozen_snapshot_base_micros,
           NULL::timestamptz AS frozen_straddle_period_start,
           NULL::timestamptz AS frozen_straddle_period_end,
           NULL::bigint AS frozen_straddle_base_micros,
           false AS frozen_has_straddle,
           0::int AS frozen_declared_timer_count,
           0::bigint AS frozen_actual_timer_count,
           EXISTS (
               SELECT 1
               FROM ms_billing.app_combined_proration_attempt_timers any_owned
               WHERE any_owned.timer_id = timer.id
           ) AS ownership_unknown
    FROM ms_billing.app_module_overage_timers timer
    JOIN ms_billing.accounts account ON account.id = timer.account_id
    LEFT JOIN live_timer_ranks rank ON rank.id = timer.id
    WHERE account.activated_at IS NOT NULL
      AND timer.account_id = @account_id::uuid
      AND timer.grace_resolved = false
      AND NOT EXISTS (
          SELECT 1
          FROM ms_billing.app_combined_proration_attempt_timers child
          JOIN frozen_headers frozen ON frozen.app_id = child.app_id
          WHERE child.timer_id = timer.id
      )
      AND (
          timer.charge_attempted_at IS NOT NULL
          OR EXISTS (
              SELECT 1
              FROM ms_billing.app_combined_proration_attempt_timers any_owned
              WHERE any_owned.timer_id = timer.id
          )
          OR (
              timer.removed_at IS NULL
              AND rank.rn > @included_modules::int
          )
      )
),
frozen_bases AS (
    SELECT 'creation_base'::text AS charge_kind,
           frozen.app_id AS charge_id,
           frozen.app_id,
           app.created_at AS charge_at,
           app.created_at + make_interval(hours => @grace_hours::int) AS grace_expires_at,
           account.activated_at,
           (app.deleted_at IS NULL) AS counts_toward_recurring,
           true AS frozen,
           frozen.base_charge_micros AS frozen_amount_micros,
           frozen.snapshot_period_start AS frozen_snapshot_period_start,
           frozen.snapshot_period_end AS frozen_snapshot_period_end,
           frozen.snapshot_base_micros AS frozen_snapshot_base_micros,
           frozen.straddle_period_start AS frozen_straddle_period_start,
           frozen.straddle_period_end AS frozen_straddle_period_end,
           frozen.straddle_base_micros AS frozen_straddle_base_micros,
           (frozen.straddle_period_start IS NOT NULL) AS frozen_has_straddle,
           frozen.timer_count AS frozen_declared_timer_count,
           frozen.actual_timer_count AS frozen_actual_timer_count,
           false AS ownership_unknown
    FROM frozen_headers frozen
    JOIN ms_billing.apps app ON app.app_id = frozen.app_id
    JOIN ms_billing.accounts account ON account.id = frozen.account_id
),
frozen_timers AS (
    SELECT 'module_timer'::text AS charge_kind,
           child.timer_id AS charge_id,
           frozen.app_id,
           timer.installed_at AS charge_at,
           timer.grace_expires_at,
           account.activated_at,
           (
               timer.removed_at IS NULL
               AND rank.rn > @included_modules::int
           ) AS counts_toward_recurring,
           true AS frozen,
           frozen.module_charge_micros AS frozen_amount_micros,
           frozen.snapshot_period_start AS frozen_snapshot_period_start,
           frozen.snapshot_period_end AS frozen_snapshot_period_end,
           frozen.snapshot_base_micros AS frozen_snapshot_base_micros,
           frozen.straddle_period_start AS frozen_straddle_period_start,
           frozen.straddle_period_end AS frozen_straddle_period_end,
           frozen.straddle_base_micros AS frozen_straddle_base_micros,
           (frozen.straddle_period_start IS NOT NULL) AS frozen_has_straddle,
           frozen.timer_count AS frozen_declared_timer_count,
           frozen.actual_timer_count AS frozen_actual_timer_count,
           false AS ownership_unknown
    FROM frozen_headers frozen
    JOIN ms_billing.app_combined_proration_attempt_timers child
      ON child.app_id = frozen.app_id
    JOIN ms_billing.app_module_overage_timers timer
      ON timer.id = child.timer_id
    JOIN ms_billing.accounts account ON account.id = frozen.account_id
    LEFT JOIN live_timer_ranks rank ON rank.id = timer.id
),
all_charges AS (
    SELECT charge_kind, charge_id, app_id, charge_at, grace_expires_at,
           activated_at, counts_toward_recurring, frozen,
           frozen_amount_micros,
           frozen_snapshot_period_start, frozen_snapshot_period_end,
           frozen_snapshot_base_micros,
           frozen_straddle_period_start, frozen_straddle_period_end,
           frozen_straddle_base_micros,
           frozen_has_straddle,
           frozen_declared_timer_count, frozen_actual_timer_count,
           ownership_unknown
    FROM dynamic_creations
    UNION ALL
    SELECT charge_kind, charge_id, app_id, charge_at, grace_expires_at,
           activated_at, counts_toward_recurring, frozen,
           frozen_amount_micros,
           frozen_snapshot_period_start, frozen_snapshot_period_end,
           frozen_snapshot_base_micros,
           frozen_straddle_period_start, frozen_straddle_period_end,
           frozen_straddle_base_micros,
           frozen_has_straddle,
           frozen_declared_timer_count, frozen_actual_timer_count,
           ownership_unknown
    FROM dynamic_timers
    UNION ALL
    SELECT charge_kind, charge_id, app_id, charge_at, grace_expires_at,
           activated_at, counts_toward_recurring, frozen,
           frozen_amount_micros,
           frozen_snapshot_period_start, frozen_snapshot_period_end,
           frozen_snapshot_base_micros,
           frozen_straddle_period_start, frozen_straddle_period_end,
           frozen_straddle_base_micros,
           frozen_has_straddle,
           frozen_declared_timer_count, frozen_actual_timer_count,
           ownership_unknown
    FROM frozen_bases
    UNION ALL
    SELECT charge_kind, charge_id, app_id, charge_at, grace_expires_at,
           activated_at, counts_toward_recurring, frozen,
           frozen_amount_micros,
           frozen_snapshot_period_start, frozen_snapshot_period_end,
           frozen_snapshot_base_micros,
           frozen_straddle_period_start, frozen_straddle_period_end,
           frozen_straddle_base_micros,
           frozen_has_straddle,
           frozen_declared_timer_count, frozen_actual_timer_count,
           ownership_unknown
    FROM frozen_timers
)
SELECT charge_kind::text,
       charge_id::uuid,
       app_id::uuid,
       charge_at::timestamptz,
       grace_expires_at::timestamptz,
       activated_at::timestamptz,
       counts_toward_recurring::boolean,
       frozen::boolean,
       frozen_amount_micros::bigint,
       COALESCE(frozen_snapshot_period_start, 'epoch'::timestamptz)::timestamptz
           AS frozen_snapshot_period_start,
       COALESCE(frozen_snapshot_period_end, 'epoch'::timestamptz)::timestamptz
           AS frozen_snapshot_period_end,
       COALESCE(frozen_snapshot_base_micros, 0)::bigint
           AS frozen_snapshot_base_micros,
       COALESCE(frozen_straddle_period_start, 'epoch'::timestamptz)::timestamptz
           AS frozen_straddle_period_start,
       COALESCE(frozen_straddle_period_end, 'epoch'::timestamptz)::timestamptz
           AS frozen_straddle_period_end,
       COALESCE(frozen_straddle_base_micros, 0)::bigint
           AS frozen_straddle_base_micros,
       frozen_has_straddle::boolean,
       frozen_declared_timer_count::int,
       frozen_actual_timer_count::bigint,
       ownership_unknown::boolean
FROM all_charges
ORDER BY charge_at, charge_kind, charge_id;
