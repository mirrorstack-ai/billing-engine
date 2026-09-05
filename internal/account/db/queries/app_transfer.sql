-- AppTransferEventByRequest reads the idempotency record for a request_id.
-- A hit means this transfer already happened: the caller gets the STORED
-- result — every field of it, the window and recurring_from included, so a
-- retry that lands after a boundary answers with the same dates the first
-- call did — and a different app, target or mode for the same key is a
-- conflict rather than a second transfer.
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

-- AppHasUnbilledUsageBacklog reports whether this app still has usage
-- recorded with NO account — the lazy, never-billed backlog an org app
-- accrues before its org designates funding (migration 041).
--
-- 🔴 THE TRANSFER REFUSES WHILE ONE EXISTS. The org repoint sweep
-- (org.sql RepointOrgNullAccountEvents) finds that backlog through
-- apps.owner_org_id, which the transfer REWRITES: re-key to org B and the
-- next sweep for B bills B for everything org A's members did before A ever
-- paid — unbounded, and for usage B never saw; re-key to a user and no sweep
-- can ever reach it, so it is stranded unbilled forever. Neither is a money
-- outcome this RPC may choose. The backlog is A's to fund (or to leave), and
-- the transfer waits for that: CONFLICT app_transfer_unbilled_backlog.
-- Keyed on app_id (the app's own events), not owner_org_id — an org's OTHER
-- apps' backlog is not this transfer's concern.
-- name: AppHasUnbilledUsageBacklog :one
SELECT EXISTS (
    SELECT 1 FROM ms_billing.usage_events e
    WHERE e.app_id = @app_id::uuid
      AND e.account_id IS NULL
)::bool AS has_backlog;

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
--
-- Read UNDER the account's activation row lock (LockUsageAccountActivation,
-- FOR SHARE, taken by barrierBothAccounts before this runs). Activation is
-- the one fact here with a FOR UPDATE writer (ActivateAccountIfUnset), and a
-- plain read that preceded the lock could classify an account as
-- "unactivated, forfeit" one statement before its card-bind committed.
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

-- TerminateAppLevelStreamsOnTransfer closes every LEVEL stream the OLD
-- account is still holding for this app, by writing one zero-level sample
-- per stream at the instant its attribution ends.
--
-- 🔴 WHY A SAMPLE, AND WHY ZERO. A time_weighted gauge is not a fact per
-- event: the rollup integrates a STEP FUNCTION under the samples, holding
-- each level until the next sample — or, for the stream's LAST sample in a
-- period, until PERIOD END (rollup.sql RollupTimeWeightedKind, the LEAD
-- default). After a transfer the old account's rollup still sees its last
-- sample, still extends it to period end, and bills the level for a stretch
-- the app spent on the NEW account — which bills the same stretch from its
-- own samples. Both modes: keep leaves every sample where it was; move
-- re-attributes the open window and the old account's last sample BEFORE that
-- window still extends across it. A zero-level sample at the hand-off is the
-- one write that makes the rollup's own arithmetic stop the old integral
-- there, without teaching the rollup about transfers.
--
-- WHICH INSTANT. @at is where the old account's attribution of the stream
-- ENDS, and the store passes it: the transfer instant in keep mode, the move
-- window's start in move mode (every sample from there was just moved).
-- Writing it at the transfer instant in move mode would leave the old
-- account holding its last level across the very window the new account now
-- bills from moved samples. The old account's rollup reads it because it
-- lies inside [@period_start, its period end), where @period_start is the
-- OLD account's open-period start.
--
-- WHICH STREAMS. Exactly the rollup's partition — (app, module, metric,
-- COALESCE(model, '')) — restricted to kind time_weighted, and only where the
-- old account still holds a sample in [@period_start, @at): a stream with
-- nothing left there contributes nothing to the old integral, so there is
-- nothing to stop. The sample carries the LAST held sample's module_version so
-- its zero-level tail groups under the version that was running (the
-- rollup groups the integral per version); it adds 0 × duration to that
-- version's billable_quantity and only extends its active_seconds, which for
-- time_weighted is a reproducibility snapshot and never a multiplier.
--
-- WHAT THIS IS NOT. Peak is left alone: MAX ignores a zero, and the old
-- period keeps its peak because a peak is a peak — the level WAS reached in
-- that period. The new account's first sample being back-filled to ITS period
-- start (RollupPeakKind row_num = 1) is the rollup's pre-existing semantics
-- for any stream that begins mid-period, not a transfer artefact.
--
-- Shape: observation_version 1 (always in the rollup, whatever the account's
-- activation state), billable_at explicit so the rollup's window expression
-- reads the instant directly, value 0, kind time_weighted, a deterministic
-- event_id naming the transfer and the stream, and the request_id in
-- metadata so an auditor reading a zero can see who wrote it. :execrows.
-- name: TerminateAppLevelStreamsOnTransfer :execrows
INSERT INTO ms_billing.usage_events (
    event_id, account_id, app_id, module_id, metric, kind, value,
    recorded_at, billable_at, model, module_version,
    observation_version, occurrence_policy, metadata
)
SELECT
    'app_transfer:' || @request_id::text || ':' || stream.module_id::text
        || ':' || stream.metric || ':' || COALESCE(stream.model, ''),
    @account_id::uuid,
    @app_id::uuid,
    stream.module_id,
    stream.metric,
    'time_weighted'::ms_billing.metric_kind,
    0::numeric,
    @at::timestamptz,
    @at::timestamptz,
    stream.model,
    stream.module_version,
    1::smallint,
    'v1_ingest_time',
    json_build_object('synthesized_by', 'app_transfer', 'request_id', @request_id::text)
FROM (
    SELECT DISTINCT ON (module_id, metric, COALESCE(model, ''))
        module_id, metric, model, module_version
    FROM ms_billing.usage_events
    WHERE app_id = @app_id::uuid
      AND account_id = @account_id::uuid
      AND kind = 'time_weighted'
      AND COALESCE(billable_at, recorded_at) >= @period_start::timestamptz
      AND COALESCE(billable_at, recorded_at) <  @at::timestamptz
    ORDER BY module_id, metric, COALESCE(model, ''),
             COALESCE(billable_at, recorded_at) DESC, event_id DESC
) stream;
