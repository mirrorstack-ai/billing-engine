-- AppTransferEventByRequest reads the idempotency record for a request_id.
-- A hit means this transfer already happened: the caller gets the STORED
-- result — every field of it, the window and recurring_from included, so a
-- retry that lands after a boundary answers with the same dates the first
-- call did — and a different target for the same key is a conflict rather
-- than a second transfer.
-- name: AppTransferEventByRequest :one
SELECT request_id, app_id, from_account, to_account, mode, moved_event_count, at,
       open_period_start, open_period_end, recurring_from
FROM ms_billing.app_transfer_events
WHERE request_id = $1;

-- InsertAppTransferEvent records what the transfer did AND what it answered,
-- including what it forfeited (the forfeit_* columns; 071 says when a
-- transfer forfeits and when it refuses instead). request_id is UNIQUE, so a
-- concurrent duplicate loses on the index rather than transferring twice.
-- name: InsertAppTransferEvent :exec
INSERT INTO ms_billing.app_transfer_events (
    request_id, app_id, from_account, to_account, mode, moved_event_count, at,
    open_period_start, open_period_end, recurring_from,
    forfeited_proration, forfeited_domain_count, forfeited_timer_count, forfeit_reason
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);

-- LockAppForTransfer takes the app's roster row FOR UPDATE and returns its
-- current attribution. Everything the transfer decides is read here, under the
-- lock, so a concurrent RegisterApp/SyncAppModules or a second TransferApp
-- cannot interleave between the read and the writes.
--
-- deleted_at IS NULL, as every LIVE-roster read in apps.sql spells it: a
-- soft-deleted app has nothing left to transfer — it is already out of every
-- future base fee (D1e) — and re-keying its row would hand the NEW account
-- whatever the deletion left behind on it. No row ⇒ NOT_FOUND, the same answer
-- as an app this service never mirrored, because to the caller both are "no
-- billing here to move".
-- name: LockAppForTransfer :one
SELECT app_id, account_id, owner_org_id
FROM ms_billing.apps
WHERE app_id = $1
  AND deleted_at IS NULL
FOR UPDATE;

-- AppUnresolvedOneTimeCharges classifies every MID-PERIOD ONE-TIME charge
-- still owed for this app by whoever owns it now, so the transfer can decide
-- between refusing and forfeiting (see 071 and transfer_store.go).
--
-- 🔴 THIS IS A MONEY GUARD, NOT A TIDINESS CHECK. All three legs read the
-- CURRENT owner off the row at charge time and bill whoever it points at:
-- creation proration (apps.sql AppsPendingProration), custom-domain activation
-- (domains.sql DomainsPendingCharge) and per-module grace overage
-- (module_timers.sql). Re-key an app while one of them is unresolved and the
-- NEW account pays for a window it did not own — the exact inverse of the rule
-- that keeps prepaid recurring with the old account.
--
-- Two readings per leg:
--   *_pending    — unresolved, by the leg's own terminal predicate. A creation
--                  proration whose combined attempt is RESOLVED counts as
--                  settled even while apps.proration_invoice_id is NULL: the
--                  attempt was sealed as an intent (MarkCombinedProrationProposed
--                  resolves the header, and only the header), so the intent
--                  rail owns it and nothing here may forfeit or re-bill it. A
--                  timer that a combined attempt owns is that attempt's line
--                  (resolved or not), never a standalone charge of its own.
--   *_in_flight  — a SUBSET of *_pending: unresolved AND already armed at the
--                  provider, or frozen into an unresolved combined attempt.
--                  Money may have moved.
--                  The transfer REFUSES these regardless of whether the old
--                  account could settle: forfeiting a row whose invoice may
--                  already be finalized would leave a collected charge with no
--                  mirror, and the 050 terminal guard raises on the app row
--                  anyway. A refusal is retryable; a forfeit over moved money
--                  is not.
--
-- No account_id predicate on the proration leg — an UNBILLED org roster row
-- (account_id NULL) owes its creation window just as much, and RekeyAppRoster
-- would hand exactly that window to the new owner: the row acquires an
-- account, AppsPendingProration selects it, and the D1d check compares only
-- activation against the creation period, which a long-activated target
-- passes. The NULL-source case is forfeited, never carried.
--
-- Evaluated INSIDE the transfer transaction, under the app row lock, so the
-- proration sweep (which locks the same row) cannot slip between this read
-- and the re-key; the domain and timer sweeps lock only their own rows, and
-- the forfeit writers below re-check the arm marker in their WHERE so a
-- concurrent arm is observed as a row-count shortfall, never silently
-- forfeited.
-- name: AppUnresolvedOneTimeCharges :one
SELECT
    EXISTS (
        SELECT 1 FROM ms_billing.apps a
        WHERE a.app_id = @app_id::uuid
          AND a.proration_invoice_id IS NULL
          AND a.proration_skipped_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM ms_billing.app_combined_proration_attempts att
              WHERE att.app_id = a.app_id
                AND att.resolved_at IS NOT NULL
          )
    )::bool AS proration_pending,
    EXISTS (
        SELECT 1 FROM ms_billing.apps a
        WHERE a.app_id = @app_id::uuid
          AND a.proration_invoice_id IS NULL
          AND a.proration_skipped_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM ms_billing.app_combined_proration_attempts att
              WHERE att.app_id = a.app_id
                AND att.resolved_at IS NOT NULL
          )
          AND (a.proration_attempted_at IS NOT NULL
               OR EXISTS (
                   SELECT 1 FROM ms_billing.app_combined_proration_attempts att
                   WHERE att.app_id = a.app_id
                     AND att.resolved_at IS NULL
               ))
    )::bool AS proration_in_flight,
    (
        SELECT count(*) FROM ms_billing.app_custom_domains d
        WHERE d.app_id = @app_id::uuid
          AND d.removed_at IS NULL
          AND d.charge_resolved = false
    )::bigint AS domain_pending,
    (
        SELECT count(*) FROM ms_billing.app_custom_domains d
        WHERE d.app_id = @app_id::uuid
          AND d.removed_at IS NULL
          AND d.charge_resolved = false
          AND d.charge_attempted_at IS NOT NULL
    )::bigint AS domain_in_flight,
    (
        SELECT count(*) FROM ms_billing.app_module_overage_timers m
        WHERE m.app_id = @app_id::uuid
          AND m.removed_at IS NULL
          AND m.grace_resolved = false
          AND NOT EXISTS (
              SELECT 1 FROM ms_billing.app_combined_proration_attempt_timers owned
              WHERE owned.timer_id = m.id
          )
    )::bigint AS timer_pending,
    (
        SELECT count(*) FROM ms_billing.app_module_overage_timers m
        WHERE m.app_id = @app_id::uuid
          AND m.removed_at IS NULL
          AND m.grace_resolved = false
          AND m.charge_attempted_at IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM ms_billing.app_combined_proration_attempt_timers owned
              WHERE owned.timer_id = m.id
          )
    )::bigint AS timer_in_flight;

-- TransferSourceSettlement reads whether the OLD account could settle its
-- unresolved one-time charges SOON, which decides refuse-versus-forfeit.
--
-- The three facts are exactly the gates the charge legs apply before they
-- collect, read from the same rows: activation (the D1d gate,
-- ChargeCreationProration / DomainsPendingCharge / ModuleOverageTimersPastGrace
-- all skip an unactivated account), collection mode (offSessionChargePermitted:
-- a prepaid account is never auto-charged off-session, H10) and a usable
-- payment method ON THE FUNDER — the account_funding_authorizations row, which
-- is what ArmDomainStripeCharge / ArmModuleTimerStripeCharge /
-- StripeFundingAuthorization arm against, with the same not-deleted,
-- not-expired card predicate as HasUsableDefaultPM. All three true ⇒ the next
-- sweep collects and the transfer refuses; any false ⇒ the sweep would skip
-- transiently, on every run, for as long as the account stays so — the
-- forever-blocked transfer the bounded rule exists to prevent.
-- The LEFT JOIN keeps the row for an account with no authorization row (the
-- 052 trigger creates one on every account insert, so this is belt and
-- braces): no funder ⇒ no usable card.
-- name: TransferSourceSettlement :one
SELECT (a.activated_at IS NOT NULL)::bool AS activated,
       (a.usage_billing_mode = 'arrears')::bool AS arrears,
       EXISTS (
           SELECT 1
           FROM ms_billing.payment_methods_mirror payment_method
           WHERE payment_method.account_id = funding_auth.funding_account_id
             AND payment_method.deleted_at IS NULL
             AND (payment_method.exp_year, payment_method.exp_month) >= (
                 EXTRACT(YEAR FROM current_date)::INT,
                 EXTRACT(MONTH FROM current_date)::INT
             )
       )::bool AS has_usable_payment_method
FROM ms_billing.accounts a
LEFT JOIN ms_billing.account_funding_authorizations funding_auth
  ON funding_auth.account_id = a.id
WHERE a.id = @account_id::uuid;

-- ForfeitAppProrationOnTransfer arms the PERMANENT skip marker (031) at the
-- transfer instant: the creation window is never charged, to anyone. The
-- predicates are SetAppProrationSkipped's plus proration_attempted_at IS NULL
-- — an attempted proration is in flight and the store refuses before reaching
-- here; the predicate makes the writer refuse it too. :execrows so the store
-- can assert the marker landed on the one row it read as pending.
-- name: ForfeitAppProrationOnTransfer :execrows
UPDATE ms_billing.apps
SET proration_skipped_at = @at::timestamptz
WHERE app_id = @app_id::uuid
  AND proration_skipped_at IS NULL
  AND proration_invoice_id IS NULL
  AND proration_attempted_at IS NULL;

-- ForfeitAppDomainChargesOnTransfer resolves every live, unresolved,
-- never-armed activation charge for the app WITHOUT charging it and stamps
-- the transfer that did so (071). charge_attempted_at IS NULL is the
-- linearization point against a concurrently arming domain sweep: the arm
-- statement row-locks and re-checks charge_resolved = false, this one
-- row-locks and re-checks charge_attempted_at IS NULL, so whichever commits
-- second sees the other's write — an arm that won makes this UPDATE skip the
-- row, which the store reads as a count shortfall and aborts on. :execrows.
-- name: ForfeitAppDomainChargesOnTransfer :execrows
UPDATE ms_billing.app_custom_domains
SET charge_resolved     = true,
    charge_forfeited_by = @request_id::uuid
WHERE app_id = @app_id::uuid
  AND removed_at IS NULL
  AND charge_resolved = false
  AND charge_attempted_at IS NULL;

-- ForfeitAppModuleTimersOnTransfer is the timer twin. A timer owned by a
-- combined creation attempt is excluded (it is that attempt's line, and the
-- 050 terminal guard would raise on an unresolved owner anyway); the arm
-- marker predicate is the same linearization point as on the domain writer.
-- :execrows.
-- name: ForfeitAppModuleTimersOnTransfer :execrows
UPDATE ms_billing.app_module_overage_timers timer
SET grace_resolved     = true,
    grace_forfeited_by = @request_id::uuid
WHERE timer.app_id = @app_id::uuid
  AND timer.removed_at IS NULL
  AND timer.grace_resolved = false
  AND timer.charge_attempted_at IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM ms_billing.app_combined_proration_attempt_timers owned
      WHERE owned.timer_id = timer.id
  );

-- RekeyAppRoster moves the app's billing attribution.
--
-- 🔴 THERE IS NO owner_user_id ON THIS TABLE, and the contract's wording
-- ("owner_user_id/owner_org_id") does not match the schema. ms_billing.apps
-- carries account_id plus owner_org_id (041) only; a user owner is identified
-- by the ACCOUNT (accounts.owner_kind / owner_user_id), not by the app row. So
-- a transfer to a user sets owner_org_id NULL and lets the account carry the
-- identity, and a transfer to an org sets it — which is also what the repoint
-- sweep needs, since it scopes NULL-account usage_events to an org through
-- this column.
--
-- Runs FIRST so the deferred attribution triggers see a roster to agree with.
-- name: RekeyAppRoster :execrows
UPDATE ms_billing.apps
SET account_id = $2,
    owner_org_id = $3
WHERE app_id = $1;

-- RekeyAppTimers follows the roster. Only LIVE timers move: a removed timer's
-- charge, if any, already resolved against the account that owned it.
-- name: RekeyAppTimers :execrows
UPDATE ms_billing.app_module_overage_timers
SET account_id = $2
WHERE app_id = $1 AND removed_at IS NULL;

-- RekeyAppDomains follows the roster, same live-only rule.
-- name: RekeyAppDomains :execrows
UPDATE ms_billing.app_custom_domains
SET account_id = $2
WHERE app_id = $1 AND removed_at IS NULL;

-- MoveAppOpenUsage re-attributes this app's usage events for mode="move".
--
-- 🔴 EVERY TERM IN THE WINDOW IS LOAD-BEARING.
--   app_id          — this app only; an account's other apps keep their usage.
--   account_id      — only rows still attributed to the OLD account.
--   the WINDOW EXPRESSION is COALESCE(billable_at, recorded_at), which is what
--                     the ROLLUP itself buckets on (rollup.sql:66-69) and what
--                     every bill read and the 055 index use. It is NOT
--                     occurred_at: occurred_at is NULL for every infra.* and
--                     platform.* event and for every legacy/v1 observation, and
--                     `NULL >= $1` is NULL, so an occurred_at filter silently
--                     moves NO infra or platform usage and leaves the OLD
--                     account invoiced for the app's egress, AI and GPU. Every
--                     writer keeps billable_at >= occurred_at, so filtering on
--                     the rollup's own expression does not loosen the INV-011
--                     bound — it applies it to the rows the bill actually reads.
--   >= window_start — max(old.openStart, new.openStart): an event older than
--                     the TARGET's open period would be backdated into a period
--                     it has already closed and billed, which INV-011 forbids.
--   <  window_end   — the transfer instant. Usage after it belongs to the new
--                     account by ordinary attribution, not by re-attribution.
-- The rollup reads usage_events.account_id and never joins the app roster, so
-- moving these rows is exactly and only what changes which account bills them.
-- name: MoveAppOpenUsage :execrows
UPDATE ms_billing.usage_events
SET account_id = $3
WHERE app_id = $1
  AND account_id = $2
  AND COALESCE(billable_at, recorded_at) >= @window_start::timestamptz
  AND COALESCE(billable_at, recorded_at) <  @window_end::timestamptz;
