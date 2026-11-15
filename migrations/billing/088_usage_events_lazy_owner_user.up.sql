-- 088: stamp a lazy USER usage row with the user it was recorded for, so the
-- row can ever be attributed (core-v2#340).
--
-- Ingest resolves the payer from the request's owner (usage/service.go
-- RecordUsage, usage/infra.go RecordInfraUsage). An org owner with no funded
-- account records the event with account_id NULL, and the org attach sweep
-- (org.sql RepointOrgNullAccountEvents) finds it later through the app's
-- roster. A USER owner with no ms_billing.accounts row records it NULL too —
-- and nothing ever found it again: every bill read filters on account_id, the
-- org sweep is scoped by owner_org_id, and the row carried nothing that named
-- the user. The usage was retained and silently never billed.
--
-- owner_user_id is that name. It is written ONLY on a lazy USER row: the
-- stamped owner was a user (no org), and that user had no accounts row at
-- ingest, so account_id is NULL. It is NULL on every other row — an
-- account-attributed row already names its payer, and an org or ownerless
-- lazy row has no user to name. The user sweep
-- (user_usage.sql RepointUserNullAccountEvents) and the disclosure read
-- (UserUnbilledBacklogMicros) key on it.
--
-- 🔴 IT IS THE INGEST STAMP, NEVER A ROSTER LOOKUP. ms_billing.apps.account_id
-- names the app's PAYER, and during api-platform's payer re-seat — before
-- TransferApp lands, or while it is refused — the roster still names the OLD
-- payer while ingest already stamps the NEW one. A sweep keyed on the roster
-- would bill the old payer for the new payer's usage; the stamp is what the
-- request said, at the instant it was recorded.
--
-- A soft FK, like accounts.owner_user_id (001): nothing in this repository
-- reads ms_account. Pre-088 lazy rows carry no stamp and stay unattributed —
-- nothing here guesses at them; scripts/user-stranded-usage.sql counts them.
--
-- Expand-only: the column is nullable with no default, so every existing row
-- reads NULL, which is exactly what it would have been written as.
ALTER TABLE ms_billing.usage_events
    ADD COLUMN IF NOT EXISTS owner_user_id UUID NULL;

COMMENT ON COLUMN ms_billing.usage_events.owner_user_id IS
    'Lazy USER rows only: the owner user a usage event was recorded for when that user had no billing account (account_id NULL). NULL on every other row. Soft FK. The user attach sweep repoints the rows inside the account''s open window once it activates.';

-- The sweep's work list, its repoint and the backlog disclosure all read
-- "this user's lazy rows at or after an instant", so the expression is indexed
-- as those queries spell it (085's style). Partial: a stamped row leaves the
-- index the moment the sweep gives it an account, so it stays the size of the
-- unswept backlog rather than of the ledger.
CREATE INDEX IF NOT EXISTS usage_events_lazy_owner_user_idx
    ON ms_billing.usage_events (owner_user_id, (COALESCE(billable_at, recorded_at)))
    WHERE account_id IS NULL AND owner_user_id IS NOT NULL;
