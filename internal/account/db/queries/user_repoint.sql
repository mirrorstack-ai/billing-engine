-- The USER twin of the org attach sweep's events half (core-v2#340): usage a
-- USER-rostered app recorded with NO account is reachable by no sweep, because
-- the org sweep is scoped through apps.owner_org_id. Ingest stamps the app's
-- payer; a payer with no ms_billing.accounts row lands the event NULL
-- (usage/service.go, infra.go), and every bill read filters account_id.

-- RepointUserNullAccountEvents hands a user account the NULL-account usage of
-- the apps rostered to it. The roster is the key, exactly as it is for the
-- ownerless infra events (RecordInfraUsage, migration 086): a user-rostered
-- row always carries its account (041's CHECK — only an org row may be NULL),
-- so apps.account_id IS the account that pays for the app.
--
-- The SET is RepointOrgNullAccountEvents' (org.sql), verbatim, so the rollup
-- and every audit read see one kind of repointed row.
--
-- The WHERE's window term is RepointAppNullAccountEventsOnTransfer's
-- (app_transfer.sql), and it is the decision: a user did not designate funding
-- for a backlog, so D1d applies — rows inside the account's open window are
-- the account's and bill with it; rows older than that were recorded while no
-- account could take them, were never on any bill the user saw, and stay NULL
-- and unbilled (UserUnbilledBacklogMicros prices them). Widening to the org's
-- clamp-everything rule is one term, and waits for a disclosure the user sees
-- before they are charged for it.
--
-- @window_start is the account's open-window start, resolved under its
-- activation lock and period barrier (pgxStore.RepointUserNullAccountEvents).
-- Idempotent: account_id IS NULL never matches a swept row again.
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
  AND app_id IN (
      SELECT app_id FROM ms_billing.apps
      WHERE account_id = @account_id::uuid AND owner_org_id IS NULL
  )
  AND COALESCE(billable_at, recorded_at) >= @window_start::timestamptz;

-- UsersWithUnsweptUsage is the self-healing work list: activated USER accounts
-- with a NULL-account event on one of their rostered apps that the repoint can
-- still take. "Can still take" is bounded by the newest closed period's end
-- (or the first anchored window, activation day 00:00 UTC, before any closes),
-- a superset of the open window only between a boundary and its rollup, so the
-- list converges within a day of the rollup rather than re-listing D1d rows
-- forever.
-- name: UsersWithUnsweptUsage :many
SELECT a.id
FROM ms_billing.accounts a
WHERE a.owner_kind = 'user'
  AND a.activated_at IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM ms_billing.usage_events e
      JOIN ms_billing.apps ap ON ap.app_id = e.app_id
      WHERE ap.account_id = a.id
        AND ap.owner_org_id IS NULL
        AND e.account_id IS NULL
        AND COALESCE(e.billable_at, e.recorded_at) >= GREATEST(
            date_trunc('day', a.activated_at, 'UTC'),
            COALESCE((
                SELECT MAX(p.period_end)
                FROM ms_billing.billing_periods p
                WHERE p.account_id = a.id
                  AND p.status IN ('closing', 'invoiced')
            ), '-infinity'::timestamptz)
        )
  )
ORDER BY a.id;

-- UserUnbilledBacklogMicros prices the NULL-account usage of the apps rostered
-- to a user account that the repoint does NOT take — rows older than its open
-- window. OrgUnbilledBacklogMicros' pricing, verbatim (dev_served excluded,
-- ×12/10 for reserved infra.*/platform.*, keyed subjects peak per app scope):
-- the figure a user must be shown before any rule bills it.
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
      AND e.app_id IN (
          SELECT app_id FROM ms_billing.apps
          WHERE account_id = @account_id::uuid AND owner_org_id IS NULL
      )
      AND COALESCE(e.billable_at, e.recorded_at) < @window_start::timestamptz
),
billable_events AS (
    SELECT app_id, module_id, metric, model, module_version, value AS billable_value
    FROM base_events
    WHERE aggregation_key IS DISTINCT FROM 'subject'
    UNION ALL
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
