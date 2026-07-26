-- Durable ownership for the combined app-creation Stripe charge (migration
-- 050). The header is the "known set" bit: zero child rows is an intentionally
-- empty timer set, while an app attempt marker without a header is legacy /
-- incomplete state and must be rejected by the service.

-- SelectCombinedProrationAttempt reads the immutable request/snapshot shape
-- plus terminal state. Child IDs are read separately so an empty set remains
-- distinguishable from no header.
-- name: SelectCombinedProrationAttempt :one
SELECT app_id,
       account_id,
       attempted_at,
       currency,
       base_charge_micros,
       base_charge_cents,
       module_charge_micros,
       module_charge_cents,
       timer_count,
       coverage_start,
       coverage_end,
       base_description,
       module_description,
       snapshot_period_start,
       snapshot_period_end,
       snapshot_base_micros,
       snapshot_module_count,
       straddle_period_start,
       straddle_period_end,
       straddle_base_micros,
       resolved_at,
       resolved_invoice_id
FROM ms_billing.app_combined_proration_attempts
WHERE app_id = $1
FOR UPDATE;

-- name: CombinedProrationAttemptTimerIDs :many
SELECT timer_id
FROM ms_billing.app_combined_proration_attempt_timers
WHERE app_id = $1
ORDER BY timer_id;

-- InsertCombinedProrationAttempt is called only while the owning app row is
-- locked. No ON CONFLICT path is needed: a concurrent freezer must first take
-- that same lock, then reads and returns the winning header.
-- name: InsertCombinedProrationAttempt :exec
INSERT INTO ms_billing.app_combined_proration_attempts (
    app_id,
    account_id,
    attempted_at,
    currency,
    base_charge_micros,
    base_charge_cents,
    module_charge_micros,
    module_charge_cents,
    timer_count,
    coverage_start,
    coverage_end,
    base_description,
    module_description,
    snapshot_period_start,
    snapshot_period_end,
    snapshot_base_micros,
    snapshot_module_count,
    straddle_period_start,
    straddle_period_end,
    straddle_base_micros
) VALUES (
    @app_id::uuid,
    @account_id::uuid,
    @attempted_at::timestamptz,
    @currency,
    @base_charge_micros,
    @base_charge_cents,
    @module_charge_micros,
    @module_charge_cents,
    @timer_count,
    @coverage_start::timestamptz,
    @coverage_end::timestamptz,
    @base_description,
    @module_description,
    @snapshot_period_start::timestamptz,
    @snapshot_period_end::timestamptz,
    @snapshot_base_micros,
    @snapshot_module_count,
    @straddle_period_start,
    @straddle_period_end,
    @straddle_base_micros
);

-- InsertCombinedProrationAttemptTimers freezes all selected timer IDs in one
-- statement. An empty array inserts zero rows and is the intentional-empty
-- representation paired with the already-inserted header.
-- name: InsertCombinedProrationAttemptTimers :exec
INSERT INTO ms_billing.app_combined_proration_attempt_timers (app_id, timer_id)
SELECT @app_id::uuid, selected.timer_id
FROM unnest(@timer_ids::uuid[]) AS selected(timer_id);

-- MarkAppProrationAttemptedWithFreeze stamps the legacy app recovery marker in
-- the same transaction as the header + children. The locked row was already
-- checked, but the guards make the invariant explicit and rows-affected lets
-- the store reject an impossible split state.
-- name: MarkAppProrationAttemptedWithFreeze :execrows
UPDATE ms_billing.apps
SET proration_attempted_at = @attempted_at::timestamptz
WHERE app_id = @app_id::uuid
  AND proration_attempted_at IS NULL
  AND proration_invoice_id IS NULL
  AND proration_skipped_at IS NULL;

-- CoCreatedOverModuleTimersForAttempt selects the fresh ownership set at one
-- MVCC statement boundary. It deliberately excludes a timer whose standalone
-- Leg-1 marker already won; the subsequent row-lock/recheck query closes the
-- uncommitted-marker/removal race before the header is inserted.
-- name: CoCreatedOverModuleTimersForAttempt :many
SELECT id
FROM (
    SELECT id, app_id, installed_at, grace_resolved, charge_attempted_at,
           row_number() OVER (ORDER BY installed_at, id) AS rn
    FROM ms_billing.app_module_overage_timers
    WHERE account_id = @account_id::uuid
      AND removed_at IS NULL
) ranked
WHERE app_id = @app_id::uuid
  AND installed_at = @created_at::timestamptz
  AND grace_resolved = false
  AND charge_attempted_at IS NULL
  AND rn > @included_modules::int
ORDER BY installed_at, id;

-- LockCombinedProrationCandidateTimers locks every selected row in stable UUID
-- order and returns the chargeability facts for a post-lock recheck. A
-- standalone marker/removal/resolution that committed after selection is
-- therefore observed before the freeze can commit.
-- name: LockCombinedProrationCandidateTimers :many
SELECT id, account_id, app_id, installed_at, removed_at, grace_resolved,
       charge_attempted_at
FROM ms_billing.app_module_overage_timers
WHERE id = ANY(@timer_ids::uuid[])
ORDER BY id
FOR UPDATE;

-- ResolveCombinedProrationAttempt is part of the same transaction that mirrors
-- the invoice, arms apps.proration_invoice_id, and marks every frozen timer.
-- A retry may reapply the exact same invoice id; a different id is rejected.
-- name: ResolveCombinedProrationAttempt :execrows
UPDATE ms_billing.app_combined_proration_attempts
SET resolved_at = COALESCE(resolved_at, @resolved_at::timestamptz),
    resolved_invoice_id = COALESCE(resolved_invoice_id, @resolved_invoice_id)
WHERE app_id = @app_id::uuid
  AND (
      resolved_invoice_id IS NULL
      OR resolved_invoice_id = @resolved_invoice_id
  );

-- MarkCombinedProrationTimerCharged is the only terminal child writer for a
-- frozen combined attempt. The header is resolved first in the SAME
-- transaction, then this statement proves exact child membership and updates
-- one still-unresolved timer. Resolving first is safe because no other
-- transaction observes it until commit; it also lets the mixed-version DB
-- guard reject legacy timer writers while allowing this owner transaction.
-- rows-affected MUST be one for every frozen child or the caller rolls the
-- whole header/app/timer terminal transaction back.
-- name: MarkCombinedProrationTimerCharged :execrows
UPDATE ms_billing.app_module_overage_timers timer
SET grace_resolved        = true,
    grace_charged_at      = @grace_charged_at::timestamptz,
    grace_invoice_id      = @grace_invoice_id,
    grace_invoice_item_id = @grace_invoice_item_id
WHERE timer.id = @timer_id::uuid
  AND timer.grace_resolved = false
  AND EXISTS (
      SELECT 1
      FROM ms_billing.app_combined_proration_attempt_timers owned
      JOIN ms_billing.app_combined_proration_attempts attempt
        ON attempt.app_id = owned.app_id
      WHERE owned.app_id = @app_id::uuid
        AND owned.timer_id = timer.id
        AND attempt.resolved_invoice_id = @resolved_invoice_id
        AND attempt.resolved_at IS NOT NULL
  );

-- TimerHasUnresolvedCombinedProrationOwner is the durable ownership guard used
-- by standalone timer paths before any live-rank or wallet/Stripe claim.
-- name: TimerHasUnresolvedCombinedProrationOwner :one
SELECT EXISTS (
    SELECT 1
    FROM ms_billing.app_combined_proration_attempt_timers t
    JOIN ms_billing.app_combined_proration_attempts a
      ON a.app_id = t.app_id
    WHERE t.timer_id = $1
      AND a.resolved_at IS NULL
)::bool;

-- UnresolvedCombinedProrationAttempts exposes exact raw money for the strict
-- bill/runtime projection. The service verifies frozen_timer_count equals
-- timer_count; a mismatch is corruption and must fail closed, never understate.
-- name: UnresolvedCombinedProrationAttempts :many
SELECT a.app_id,
       a.account_id,
       a.base_charge_micros,
       a.module_charge_micros,
       a.timer_count,
       count(t.timer_id)::bigint AS frozen_timer_count
FROM ms_billing.app_combined_proration_attempts a
LEFT JOIN ms_billing.app_combined_proration_attempt_timers t
  ON t.app_id = a.app_id
WHERE a.account_id = $1
  AND a.resolved_at IS NULL
GROUP BY a.app_id,
         a.account_id,
         a.base_charge_micros,
         a.module_charge_micros,
         a.timer_count
ORDER BY a.app_id;
