-- Queries backing the lazy USER usage sweep (migration 088, core-v2#340): the
-- work list, the repoint and the disclosure figure for usage a user's app
-- recorded while that user had no ms_billing.accounts row. All operate on
-- ms_billing.
--
-- 🔴 EVERY QUERY HERE KEYS ON usage_events.owner_user_id — THE INGEST STAMP —
-- AND NEVER ON THE APPS ROSTER. ms_billing.apps.account_id names an app's
-- PAYER, and during api-platform's payer re-seat (before TransferApp lands, or
-- while it is refused — TransferTargetUnfunded) the roster still names the OLD
-- payer while ingest already stamps the NEW one on every event. A sweep that
-- found lazy rows through the roster would hand the new payer's usage to the
-- old payer's account. The stamp is what the request said at the instant the
-- usage was recorded; the transfer's own NULL-row rules
-- (app_transfer.sql AppHasUnbilledUsageBacklog,
-- RepointAppNullAccountEventsOnTransfer) rest on the same fact.

-- UserAccountsWithUnsweptUsage is the self-healing work list for the user
-- sweep: every ACTIVATED user account with at least one lazy row stamped for
-- its owner that the sweep could still reach.
--
-- The time bound is a coarse PREFILTER, not the rule. The exact open window is
-- derived in Go (billingperiod.AnchoredPeriodWindow from the activation
-- anchor) and applied by RepointUserNullAccountEvents; this predicate only
-- drops an account whose every lazy row is older than ANY window the account
-- could have open, so it is not re-listed and re-swept for nothing every day.
-- It is a safe lower bound on the window start twice over: an anchored window
-- never starts before the UTC day the account activated (the first window
-- opens on that day), and no window is longer than 31 days, so an open window
-- never starts before @as_of − 31 days.
-- name: UserAccountsWithUnsweptUsage :many
SELECT a.id, a.owner_user_id::uuid AS owner_user_id
FROM ms_billing.accounts a
WHERE a.owner_kind = 'user'
  AND a.owner_user_id IS NOT NULL
  AND a.activated_at IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM ms_billing.usage_events e
      WHERE e.account_id IS NULL
        AND e.owner_user_id = a.owner_user_id
        AND COALESCE(e.billable_at, e.recorded_at) >= GREATEST(
            date_trunc('day', a.activated_at, 'UTC'),
            (@as_of::timestamptz) - interval '31 days'
        )
  )
ORDER BY a.id;

-- RepointUserNullAccountEvents hands a user's lazy usage inside the account's
-- open window to the user's account — the rows ingest stamped with this owner
-- (migration 088) because the user had no accounts row when they happened.
--
-- The SET is RepointOrgNullAccountEvents' (org.sql) and
-- RepointAppNullAccountEventsOnTransfer's (app_transfer.sql), verbatim: the
-- same clamp of billable_at to the window start, the same first_funded policy
-- for a v2 observation that occurred before it, the same repointed_from /
-- recorded_at treatment — a repointed row is shaped exactly as every other
-- repointed row, so the rollup and every audit read see one kind of repointed
-- row. Under THIS query's window filter the clamp can bind only on a row whose
-- billable_at was NULL (a pre-055 row: ingest has written billable_at on every
-- row since, and it is occurred_at or recorded_at, which the filter already
-- bounds), so on an ingest-written row it sets what is already there; it is
-- not a second rule.
--
-- 🔴 The WHERE differs from the org sweep in ONE term, and that term is the
-- decision: >= @window_start (D1d, host decision 2026-09-05 — the rule
-- RepointAppNullAccountEventsOnTransfer applies). A lazy row older than the
-- account's open window was recorded for a payer that had no account when the
-- usage happened, and it is never caught up: it stays NULL and unbilled. The
-- org sweep clamps its WHOLE backlog forward because an org DESIGNATED funding
-- after the backlog was disclosed to it (decision 1, migration 041); a user
-- who binds a card made no such choice about usage it was never shown.
-- Reversing to the org rule is dropping that one term, and it is the owner's
-- call (D125), not this query's.
--
-- @window_start is the open window the caller's barrier settled on
-- (cycle pgxStore.RepointUserNullAccountEvents): the account's anchored window
-- at the sweep instant, advanced past any period rollup has already closed.
-- No org_deletion_finalizations guard: a stamped row names a user, never an
-- org. Idempotent: account_id IS NULL never matches a swept row again.
-- name: RepointUserNullAccountEvents :execrows
UPDATE ms_billing.usage_events
SET account_id              = @account_id::uuid,
    billable_at             = GREATEST(
        COALESCE(occurred_at, recorded_at),
        @window_start::timestamptz
    ),
    occurrence_policy       = CASE
        WHEN observation_version = 2
         AND occurred_at < @window_start::timestamptz
        THEN 'first_funded'
        ELSE occurrence_policy
    END,
    repointed_from          = CASE WHEN recorded_at < @window_start::timestamptz
                                   THEN recorded_at ELSE repointed_from END,
    recorded_at             = GREATEST(recorded_at, @window_start::timestamptz)
WHERE account_id IS NULL
  AND owner_user_id = @owner_user_id::uuid
  AND COALESCE(billable_at, recorded_at) >= @window_start::timestamptz;

-- UserUnbilledBacklogMicros is the user twin of OrgUnbilledBacklogMicros
-- (org.sql): the user's lazy rows at or after @window_start, priced exactly
-- like the live bill display — the pricing CTE is the org query's, verbatim
-- (declared price ×1 for custom metrics, ×12/10 for reserved
-- infra.*/platform.*, a subject-keyed peak counted once per subject scope).
-- The caller picks the window: the zero instant for everything stamped for
-- the user, the sweep's open window for the part the sweep will bill.
--
-- 🔴 dev_served EVENTS ARE EXCLUDED (migration 073), for the org query's
-- reason: this is a disclosure figure, and tunnel-served usage is never
-- charged to anyone.
-- name: UserUnbilledBacklogMicros :one
WITH base_events AS (
    SELECT
        e.app_id, e.module_id, e.metric, e.aggregation_key, e.subject,
        COALESCE(e.model, '') AS model,
        COALESCE(e.module_version, '') AS module_version,
        e.value
    FROM ms_billing.usage_events e
    WHERE e.account_id IS NULL
      AND e.dev_served = false
      AND e.owner_user_id = @owner_user_id::uuid
      AND COALESCE(e.billable_at, e.recorded_at) >= @window_start::timestamptz
),
billable_events AS (
    SELECT app_id, module_id, metric, model, module_version, value AS billable_value
    FROM base_events
    WHERE aggregation_key IS DISTINCT FROM 'subject'
    UNION ALL
    -- The same opaque subject may legitimately exist in multiple apps. Take
    -- its peak inside each authoritative app/meter/price scope, then let the
    -- outer disclosure sum those independently billable app contributions.
    SELECT
        app_id, module_id, metric, model, module_version,
        MAX(value)::numeric AS billable_value
    FROM base_events
    WHERE aggregation_key = 'subject'
    GROUP BY app_id, module_id, metric, model, module_version, subject
)
SELECT COALESCE(SUM(
    CASE
        WHEN e.metric LIKE 'infra.%' OR e.metric LIKE 'platform.%'
            THEN e.billable_value * COALESCE(md.unit_price_micros, 0) * 12 / 10
        ELSE e.billable_value * COALESCE(md.unit_price_micros, 0)
    END), 0)::numeric AS backlog_micros
FROM billable_events e
LEFT JOIN ms_billing.metric_definitions md
    ON md.module_id = e.module_id AND md.metric = e.metric;
