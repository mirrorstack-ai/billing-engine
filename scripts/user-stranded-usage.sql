-- How much usage is stranded at account_id NULL, and how much of it can the
-- user attach sweep still reach? (core-v2#340, migration 088)
--
-- A user-owned app's usage recorded while its owner had no billing account
-- lands with account_id NULL, and every bill read filters on account_id. From
-- migration 088 on, ingest stamps such a row with its owner
-- (usage_events.owner_user_id), and cmd/billing-cycle's user attach sweep
-- (cycle.SweepUnattachedUserUsage) hands the stamped rows inside the account's
-- OPEN window to the user's account once it activates. Rows older than that
-- window stay NULL and unbilled (D1d), and rows written before 088 carry no
-- stamp at all — nothing attributes them. This script sizes each of those
-- populations so the next decision about them is made from a measurement.
--
-- 🔴 EVERY ROW IS AN AGGREGATE. No account id, no user id, no email, no money
-- figure that belongs to anybody — the same rule as billing-census.sql, for
-- the same reason: it runs against production and its output gets pasted
-- where the database's access controls do not reach. Counts only; the money
-- a user would be billed is GetUserUnbilledBacklog's, per user, behind the
-- internal RPC.
--
-- Each statement returns exactly one row of (subject, total, detail), the
-- census's shape. SELECT-only; run it by hand in a read-only session, so the
-- database itself refuses a write even if one were ever added here:
--
--   PGOPTIONS='-c default_transaction_read_only=on' \
--     psql "$BILLING_DB_URL" -X -v ON_ERROR_STOP=1 -f scripts/user-stranded-usage.sql
--
-- Needs migration 088 (it reads usage_events.owner_user_id). dev_served rows
-- are tunnel-served and never charged to anyone (migration 073): they are
-- counted once, in (1), and excluded from every subject after it.

-- 1. The whole lazy population, and the part no one will ever be charged for.
SELECT
    'lazy_events_all'                                          AS subject,
    count(*)                                                   AS total,
    'usage_events with NULL account_id, all time'              AS detail
FROM ms_billing.usage_events
WHERE account_id IS NULL;

SELECT
    'lazy_events_dev_served'                                   AS subject,
    count(*)                                                   AS total,
    'of those, dev_served — never charged; excluded below'     AS detail
FROM ms_billing.usage_events
WHERE account_id IS NULL
  AND dev_served;

-- 2. What kind of usage it is: the platform's own infra.* / platform.*
-- samplers, or a module's declared metrics.
SELECT
    'lazy_events_infra'                                        AS subject,
    count(*)                                                   AS total,
    'billable lazy rows on infra.% / platform.% metrics'       AS detail
FROM ms_billing.usage_events
WHERE account_id IS NULL
  AND NOT dev_served
  AND (metric LIKE 'infra.%' OR metric LIKE 'platform.%');

SELECT
    'lazy_events_module'                                       AS subject,
    count(*)                                                   AS total,
    'billable lazy rows on module-declared metrics'            AS detail
FROM ms_billing.usage_events
WHERE account_id IS NULL
  AND NOT dev_served
  AND NOT (metric LIKE 'infra.%' OR metric LIKE 'platform.%');

-- 3. Stamped vs unstamped. Only a stamped row can ever be attributed to a
-- user; an unstamped one is either the org sweep's (§5) or stranded.
SELECT
    'lazy_user_stamped'                                        AS subject,
    count(*)                                                   AS total,
    'billable lazy rows carrying owner_user_id (post-088)'     AS detail
FROM ms_billing.usage_events
WHERE account_id IS NULL
  AND NOT dev_served
  AND owner_user_id IS NOT NULL;

SELECT
    'lazy_user_stamped_users'                                  AS subject,
    count(DISTINCT owner_user_id)                              AS total,
    'distinct owner users those stamped rows name'             AS detail
FROM ms_billing.usage_events
WHERE account_id IS NULL
  AND NOT dev_served
  AND owner_user_id IS NOT NULL;

SELECT
    'lazy_unstamped'                                           AS subject,
    count(*)                                                   AS total,
    'billable lazy rows with no owner_user_id stamp'           AS detail
FROM ms_billing.usage_events
WHERE account_id IS NULL
  AND NOT dev_served
  AND owner_user_id IS NULL;

-- 4. The stamped rows, by what the sweep will do with them.
--
-- "In reach" is the sweep work list's own coarse prefilter
-- (user_usage.sql UserAccountsWithUnsweptUsage): the owner has an ACTIVATED
-- user account and the row is no older than GREATEST(the activation's UTC
-- day, now − 31 days). It is an UPPER bound on the next pass — the sweep then
-- applies the exact anchored window in Go, so a row from late in the previous
-- window counts here and is still left NULL. "Out of reach" rows are older
-- than any window the account could have open: D1d leaves them unbilled for
-- good. "No activated account" rows wait for a card; the window that card
-- opens decides which of them bill.
SELECT
    'lazy_user_stamped_in_sweep_reach'                         AS subject,
    count(*)                                                   AS total,
    'activated owner, inside the prefilter — upper bound'      AS detail
FROM ms_billing.usage_events e
JOIN ms_billing.accounts a
  ON a.owner_kind = 'user'
 AND a.owner_user_id = e.owner_user_id
 AND a.activated_at IS NOT NULL
WHERE e.account_id IS NULL
  AND NOT e.dev_served
  AND COALESCE(e.billable_at, e.recorded_at) >= GREATEST(
      date_trunc('day', a.activated_at, 'UTC'),
      now() - interval '31 days'
  );

SELECT
    'lazy_user_stamped_out_of_reach'                           AS subject,
    count(*)                                                   AS total,
    'activated owner, older than any open window (D1d)'        AS detail
FROM ms_billing.usage_events e
JOIN ms_billing.accounts a
  ON a.owner_kind = 'user'
 AND a.owner_user_id = e.owner_user_id
 AND a.activated_at IS NOT NULL
WHERE e.account_id IS NULL
  AND NOT e.dev_served
  AND COALESCE(e.billable_at, e.recorded_at) < GREATEST(
      date_trunc('day', a.activated_at, 'UTC'),
      now() - interval '31 days'
  );

SELECT
    'lazy_user_stamped_no_activated_account'                   AS subject,
    count(*)                                                   AS total,
    'owner has no account, or one with no card bound yet'      AS detail
FROM ms_billing.usage_events e
WHERE e.account_id IS NULL
  AND NOT e.dev_served
  AND e.owner_user_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM ms_billing.accounts a
      WHERE a.owner_kind = 'user'
        AND a.owner_user_id = e.owner_user_id
        AND a.activated_at IS NOT NULL
  );

-- 5. The unstamped rows, by the app's roster row. This is a CENSUS, not an
-- attribution: the roster names an app's payer today, which during a payer
-- re-seat is not who the usage was recorded for (that is why the sweep keys on
-- the stamp and never on this).
--
--   org-rostered   the org attach sweep's (RepointOrgNullAccountEvents), which
--                  bills them once the org designates funding.
--   user-rostered  the stranded population #340 is about: lazy user rows
--                  written before 088, which name no user and which nothing
--                  will ever bill. Split by metric family below.
--   no roster row  an app billing never mirrored (or an app-less sample).
SELECT
    'lazy_unstamped_org_rostered'                              AS subject,
    count(*)                                                   AS total,
    'app roster owner_org_id set — the org sweep''s backlog'   AS detail
FROM ms_billing.usage_events e
JOIN ms_billing.apps ap ON ap.app_id = e.app_id
WHERE e.account_id IS NULL
  AND NOT e.dev_served
  AND e.owner_user_id IS NULL
  AND ap.owner_org_id IS NOT NULL;

SELECT
    'lazy_unstamped_user_rostered'                             AS subject,
    count(*)                                                   AS total,
    'app roster owner_org_id NULL — stranded, pre-088'         AS detail
FROM ms_billing.usage_events e
JOIN ms_billing.apps ap ON ap.app_id = e.app_id
WHERE e.account_id IS NULL
  AND NOT e.dev_served
  AND e.owner_user_id IS NULL
  AND ap.owner_org_id IS NULL;

SELECT
    'lazy_unstamped_user_rostered_infra'                       AS subject,
    count(*)                                                   AS total,
    'of those, infra.% / platform.% metrics'                   AS detail
FROM ms_billing.usage_events e
JOIN ms_billing.apps ap ON ap.app_id = e.app_id
WHERE e.account_id IS NULL
  AND NOT e.dev_served
  AND e.owner_user_id IS NULL
  AND ap.owner_org_id IS NULL
  AND (e.metric LIKE 'infra.%' OR e.metric LIKE 'platform.%');

SELECT
    'lazy_unstamped_user_rostered_apps'                        AS subject,
    count(DISTINCT e.app_id)                                   AS total,
    'distinct user-rostered apps holding such rows'            AS detail
FROM ms_billing.usage_events e
JOIN ms_billing.apps ap ON ap.app_id = e.app_id
WHERE e.account_id IS NULL
  AND NOT e.dev_served
  AND e.owner_user_id IS NULL
  AND ap.owner_org_id IS NULL;

SELECT
    'lazy_unstamped_no_roster'                                 AS subject,
    count(*)                                                   AS total,
    'no apps roster row for the event''s app'                  AS detail
FROM ms_billing.usage_events e
WHERE e.account_id IS NULL
  AND NOT e.dev_served
  AND e.owner_user_id IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM ms_billing.apps ap WHERE ap.app_id = e.app_id
  );
