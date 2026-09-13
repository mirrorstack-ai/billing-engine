-- Queries backing ms_billing.app_plan_changes (migration 076): the plan-change
-- ledger cycle.SetAppPlan writes and the reconciler / boundary apply read. The
-- row is the idempotency key of every money step of a change — see the
-- migration header.

-- InsertPlanChange opens one change. The service inserts it inside the same
-- transaction that flips apps.plan for an upgrade (OpenPlanChange in
-- store.go), under the app row lock, so the plan in force and the row that
-- says what it cost can never disagree. The partial unique index refuses a
-- second open row for the app; the store turns that into "resume the open
-- one" rather than an error.
-- name: InsertPlanChange :one
INSERT INTO ms_billing.app_plan_changes (
    app_id, account_id, from_plan, to_plan, kind,
    requested_at, effective_at, period_start, period_end,
    folded_into_creation, amount_micros, wallet_micros, card_micros,
    wallet_decided_at, status, settled_at
) VALUES (
    @app_id::uuid, @account_id::uuid, @from_plan::text, @to_plan::text, @kind::text,
    @requested_at::timestamptz, @effective_at::timestamptz, @period_start::timestamptz, @period_end::timestamptz,
    @folded_into_creation::boolean, @amount_micros::bigint, 0, 0,
    sqlc.narg(wallet_decided_at)::timestamptz, @status::text, sqlc.narg(settled_at)::timestamptz
)
RETURNING *;

-- OpenPlanChangeForApp reads the app's one open change (a pending upgrade or
-- a scheduled downgrade), if any.
-- name: OpenPlanChangeForApp :one
SELECT *
FROM ms_billing.app_plan_changes
WHERE app_id = $1
  AND status IN ('pending', 'scheduled');

-- PlanChangeByID reads one row by id.
-- name: PlanChangeByID :one
SELECT *
FROM ms_billing.app_plan_changes
WHERE id = $1;

-- LockPlanChange reads one row under a ROW LOCK so a wallet decision and a
-- card settlement serialize against a concurrent retry of the same change.
-- name: LockPlanChange :one
SELECT *
FROM ms_billing.app_plan_changes
WHERE id = $1
FOR UPDATE;

-- PendingPlanChanges is the reconciler's work list (SweepPendingPlanChanges):
-- upgrades whose money steps did not all commit, oldest first.
-- name: PendingPlanChanges :many
SELECT *
FROM ms_billing.app_plan_changes
WHERE status = 'pending'
  AND requested_at <= @requested_before::timestamptz
ORDER BY requested_at, id;

-- EffectivePlanChangesForApp lists every change that has TAKEN EFFECT on the
-- app — settled upgrades (folded or charged) and applied downgrades — in
-- effect order: the segments the creation charge prices by day
-- (usage.SegmentedProratedBaseMicros), chained from apps.created_plan.
-- Pending upgrades are in force too (the plan flipped with the row) and are
-- included; scheduled and cancelled downgrades never moved the plan and are
-- not.
-- name: EffectivePlanChangesForApp :many
SELECT *
FROM ms_billing.app_plan_changes
WHERE app_id = $1
  AND status IN ('pending', 'settled', 'applied')
ORDER BY effective_at, id;

-- DuePlanChangesAll is the driver's global apply list: every scheduled
-- downgrade whose boundary has arrived, on any account, under a row lock.
-- name: DuePlanChangesAll :many
SELECT *
FROM ms_billing.app_plan_changes
WHERE status = 'scheduled'
  AND effective_at <= @due_at::timestamptz
ORDER BY effective_at, id
FOR UPDATE;

-- CancelPlanChangeByID withdraws one scheduled downgrade by id — the apply's
-- refusal when the destination plan's cap is full at the boundary.
-- name: CancelPlanChangeByID :execrows
UPDATE ms_billing.app_plan_changes
SET status     = 'cancelled',
    settled_at = @cancelled_at::timestamptz
WHERE id = @id::uuid
  AND status = 'scheduled';

-- SetPlanChangeCardWindow stores the card intent's window anchor and returns
-- the surviving value, so every retry seals the same digest — unless the
-- stored anchor's window has already CLOSED (stored < @reanchor_before, the
-- seal instant minus the execution window): an anchor a failed seal left
-- behind, or one whose sealed document is dead anyway, is replaced rather
-- than reused to seal a document that could never be collected.
-- name: SetPlanChangeCardWindow :one
UPDATE ms_billing.app_plan_changes
SET card_window_start = CASE
    WHEN card_window_start IS NULL OR card_window_start < @reanchor_before::timestamptz
    THEN @window_start::timestamptz
    ELSE card_window_start
END
WHERE id = @id::uuid
RETURNING card_window_start;

-- DecidePlanChangeWallet records the wallet decision ONCE: what the wallet
-- drew (0 when it held nothing, or the account is not in credits mode) and
-- the instant it was decided. When the draw covers the whole amount the row
-- settles here — there is no card leg to wait for. First-write-wins on
-- wallet_decided_at IS NULL: a retry that lost the race affects 0 rows and
-- the caller re-reads the surviving decision.
-- name: DecidePlanChangeWallet :execrows
UPDATE ms_billing.app_plan_changes
SET wallet_micros     = @wallet_micros::bigint,
    wallet_decided_at = @decided_at::timestamptz,
    status            = CASE WHEN @wallet_micros::bigint >= amount_micros THEN 'settled' ELSE status END,
    settled_at        = CASE WHEN @wallet_micros::bigint >= amount_micros THEN @decided_at::timestamptz ELSE settled_at END
WHERE id = @id::uuid
  AND status = 'pending'
  AND wallet_decided_at IS NULL;

-- SettlePlanChangeCard records the card remainder's sealed intent and settles
-- the row. WHERE status = 'pending' makes it idempotent: a retry after the
-- settle affects 0 rows.
-- name: SettlePlanChangeCard :execrows
UPDATE ms_billing.app_plan_changes
SET card_micros = @card_micros::bigint,
    card_ref    = @card_ref::text,
    status      = 'settled',
    settled_at  = @settled_at::timestamptz
WHERE id = @id::uuid
  AND status = 'pending';

-- CancelScheduledPlanChange withdraws the app's scheduled downgrade (the owner
-- upgraded back before the boundary, or asked for the current plan again).
-- No money moves: a downgrade never charged anything. 0 rows = nothing was
-- scheduled, which the service reports as a plain no-op.
-- name: CancelScheduledPlanChange :execrows
UPDATE ms_billing.app_plan_changes
SET status     = 'cancelled',
    settled_at = @cancelled_at::timestamptz
WHERE app_id = @app_id::uuid
  AND status = 'scheduled';

-- DuePlanChangesForAccount lists the account's scheduled downgrades whose
-- boundary has arrived, under a row lock, for ApplyDuePlanChanges.
-- name: DuePlanChangesForAccount :many
SELECT *
FROM ms_billing.app_plan_changes
WHERE account_id = @account_id::uuid
  AND status = 'scheduled'
  AND effective_at <= @due_at::timestamptz
ORDER BY effective_at, id
FOR UPDATE;

-- MarkPlanChangeApplied closes a due downgrade after apps.plan moved.
-- name: MarkPlanChangeApplied :execrows
UPDATE ms_billing.app_plan_changes
SET status     = 'applied',
    settled_at = @applied_at::timestamptz
WHERE id = @id::uuid
  AND status = 'scheduled';
