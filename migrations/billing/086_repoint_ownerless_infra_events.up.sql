-- 086: attach the platform samplers' ownerless infra events to their app's
-- account (T183; owner 2026-09-15, "missing infra storage and infra cache (R2)").
--
-- infra-storage-sync, infra-egress-sync and infra-ssr-compute-sync record
-- usage_events with NO principal — only the app the bytes belong to — and the
-- ingest path resolved the account solely from the request's owner, so every
-- one of their rows landed with account_id NULL "to be backfilled on
-- conversion". The only backfill that reaches an ownerless row is the org
-- attach sweep (RepointOrgNullAccountEvents), which runs for FUNDED orgs and
-- swept 0 on 09-15. Every app-bill query filters `account_id = @account_id`,
-- so twkpa-edu's 儲存空間 and CDN lines existed in the ledger and were
-- invisible on the bill — absent, not zero — while dispatch's compute events,
-- which stamp the app's owner, rendered beside them.
--
-- The code fix (RecordInfraUsage) resolves the account from the apps roster at
-- record time from this migration on. This statement is the ONE-SHOT repoint of
-- the rows already written NULL, keyed exactly the way the fixed ingest keys
-- them: app_id → ms_billing.apps.account_id. Bounded on purpose:
--
--   * metric LIKE 'infra.%'     — the samplers' rows only; a module's own
--                                  metered usage is never touched here.
--   * account_id IS NULL        — idempotent: a swept row never matches again.
--   * a.account_id IS NOT NULL  — an unfunded org app (migration 041 NULL)
--                                  stays lazy for the org sweep, as designed.
--   * recorded_at >= 2026-09-01 — the open cycle the owner is looking at. An
--                                  older period has been rolled already and its
--                                  bill reads usage_aggregates, so repointing an
--                                  older row would change nothing on any bill
--                                  and is left for a deliberate backfill.
--
-- billable_at is left as written (the samplers stamp the sample instant), and
-- nothing else on the row moves. The count is SELECTed at the end so the
-- migrate log prints it.
UPDATE ms_billing.usage_events e
SET    account_id = a.account_id
FROM   ms_billing.apps a
WHERE  e.account_id IS NULL
  AND  e.app_id      = a.app_id
  AND  a.account_id IS NOT NULL
  AND  e.metric LIKE 'infra.%'
  AND  e.recorded_at >= '2026-09-01T00:00:00Z';

SELECT count(*) AS still_ownerless_infra_rows_since_2026_09_01
FROM   ms_billing.usage_events
WHERE  account_id IS NULL
  AND  metric LIKE 'infra.%'
  AND  recorded_at >= '2026-09-01T00:00:00Z';
