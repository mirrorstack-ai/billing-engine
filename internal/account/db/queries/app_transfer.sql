-- AppTransferEventByRequest reads the idempotency record for a request_id.
-- A hit means this transfer already happened: the caller gets the STORED
-- result, and a different target for the same key is a conflict rather than a
-- second transfer.
-- name: AppTransferEventByRequest :one
SELECT request_id, app_id, from_account, to_account, mode, moved_event_count, at
FROM ms_billing.app_transfer_events
WHERE request_id = $1;

-- InsertAppTransferEvent records what the transfer did. request_id is UNIQUE,
-- so a concurrent duplicate loses on the index rather than transferring twice.
-- name: InsertAppTransferEvent :exec
INSERT INTO ms_billing.app_transfer_events (
    request_id, app_id, from_account, to_account, mode, moved_event_count, at
) VALUES ($1, $2, $3, $4, $5, $6, $7);

-- LockAppForTransfer takes the app's roster row FOR UPDATE and returns its
-- current attribution. Everything the transfer decides is read here, under the
-- lock, so a concurrent RegisterApp/SyncAppModules or a second TransferApp
-- cannot interleave between the read and the writes.
-- name: LockAppForTransfer :one
SELECT app_id, account_id, owner_org_id
FROM ms_billing.apps
WHERE app_id = $1
FOR UPDATE;

-- AppHasUnresolvedOneTimeCharge reports whether any MID-PERIOD ONE-TIME charge
-- is still owed for this app by whoever owns it now.
--
-- 🔴 THIS IS A MONEY GUARD, NOT A TIDINESS CHECK. All three legs read the
-- CURRENT owner off the row at charge time and bill whoever it points at:
-- creation proration (apps.sql AppsPendingProration), custom-domain activation
-- (domains.sql DomainsPendingCharge) and per-module grace overage
-- (module_timers.sql). Re-key an app while one of them is unresolved and the
-- NEW account pays for a window it did not own — the exact inverse of the rule
-- that keeps prepaid recurring with the old account. The transfer refuses
-- instead, and the caller retries once the sweeps have settled.
--
-- Evaluated INSIDE the transfer transaction, under the same row lock, so a
-- sweep cannot slip between this check and the re-key.
-- name: AppHasUnresolvedOneTimeCharge :one
SELECT EXISTS (
    SELECT 1 FROM ms_billing.apps a
    WHERE a.app_id = $1
      AND a.account_id IS NOT NULL
      AND a.proration_invoice_id IS NULL
      AND a.proration_skipped_at IS NULL
) OR EXISTS (
    SELECT 1 FROM ms_billing.app_custom_domains d
    WHERE d.app_id = $1
      AND d.removed_at IS NULL
      AND d.charge_resolved = false
) OR EXISTS (
    SELECT 1 FROM ms_billing.app_module_overage_timers m
    WHERE m.app_id = $1
      AND m.removed_at IS NULL
      AND m.grace_resolved = false
) AS unresolved;

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
--   occurred_at >=  — the caller passes max(old.openStart, new.openStart): an
--                     event older than the TARGET's open period would be
--                     backdated into a period the target has already closed and
--                     billed, which INV-011 forbids. Taking the later of the
--                     two starts is what keeps a transfer from rewriting a
--                     billed fact.
--   occurred_at <   — the transfer instant. Usage after it belongs to the new
--                     account by ordinary attribution, not by re-attribution.
-- The rollup reads usage_events.account_id and never joins the app roster, so
-- moving these rows is exactly and only what changes which account bills them.
-- name: MoveAppOpenUsage :execrows
UPDATE ms_billing.usage_events
SET account_id = $3
WHERE app_id = $1
  AND account_id = $2
  AND occurred_at >= @window_start::timestamptz
  AND occurred_at < @window_end::timestamptz;
