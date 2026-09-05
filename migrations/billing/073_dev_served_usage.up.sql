-- Migration 073 — dev_served: usage a DEVELOPER TUNNEL produced, metered and
-- priced but NEVER charged.
--
-- A module served from a developer's local dev tunnel still emits usage
-- through the SDK meter: the meter cannot tell a laptop from a deployed
-- Lambda, and it must not — the developer is exercising the real metering
-- path on purpose. api-platform CAN tell them apart, because the tunnel
-- request is authenticated by the module's LIVE TUNNEL SESSION secret rather
-- than by its deployed credential, and it stamps that distinction onto the
-- RecordUsage call as `dev_served`.
--
-- The product rule this column exists to hold:
--
--     dev_served usage is RECORDED, PRICED and DISPLAYED, and CHARGED NEVER.
--
-- Priced, because a developer testing a paid meter needs to see what it would
-- have cost — charged_micros is computed and snapshotted on the aggregate
-- exactly as for any other row, and that is the figure the console shows.
-- Never charged, because nobody bought anything: every sum that takes money
-- or reconciles an invoice filters these rows out (arrears, settlement
-- income, budgets, the org backlog disclosure, the shadow rater).
--
-- 🔴 IT IS A PROPERTY OF THE FACT, NOT OF THE MODULE'S CURRENT STATE, so it
-- lives on the EVENT and survives to the AGGREGATE. A module deployed for
-- three weeks and tunnelled for the last one has BOTH kinds of usage inside
-- ONE period and owes the deployed part. Deriving "is this module tunnelled
-- right now?" at rollup time would answer that question wrong for every event
-- that is not the most recent one.
--
-- NO BACKFILL. Nothing can be dev_served today — api-platform has never sent
-- the field — which is exactly why the DEFAULT is false: every historical row
-- is, correctly and by construction, charged usage.
--
-- 🔴 THE AGGREGATE'S UNIQUENESS KEY MUST GAIN dev_served, OR THE TWO KINDS
-- SILENTLY OVERWRITE EACH OTHER. The rollup now groups usage_events by
-- dev_served alongside (app, module, metric, model, module_version,
-- aggregation_key), so a module with both tunnelled and deployed usage of the
-- SAME metric in one period produces TWO aggregate rows that differ ONLY by
-- this flag. Under the pre-073 key they are the same row: the second upsert's
-- ON CONFLICT DO UPDATE clobbers the first, and whichever kind the rollup
-- happened to emit last becomes the whole period — under-billing the deployed
-- usage, or charging the tunnelled usage. Widening the key makes them two
-- immutable bill lines, which is what they are.
--
-- 🔴 THE OLD KEY IS DROPPED BY CATALOG LOOKUP, NOT BY NAME. Migration 055
-- replaced 023's named UNIQUE CONSTRAINT with a bare `CREATE UNIQUE INDEX
-- usage_aggregates_period_line_aggregation_key` — an INDEX, which has no
-- pg_constraint row at all, so `ALTER TABLE … DROP CONSTRAINT IF EXISTS
-- <name>` cannot drop it however correctly the name is guessed, and fails
-- SILENTLY when it misses: the narrow arbiter survives, the wide one is
-- created beside it, `migrate` records 073 applied, and the collision this
-- migration exists to prevent still happens on every mixed module. Selecting
-- the unique indexes on the table from pg_index — everything that is not the
-- primary key and not the replacement built above — cannot miss that way, and
-- picks up a legacy UNIQUE CONSTRAINT too if some environment still carries
-- 018's or 023's.
--
-- Ordering follows 055: BUILD the replacement arbiter first, then drop the
-- legacy one, so no committed interval is left without a uniqueness guard.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md (usage_events.dev_served,
--       usage_aggregates.dev_served) — owed an update in this same cycle.

-- 1) The per-event flag. NOT NULL DEFAULT false: an absent `dev_served` on the
--    RecordUsage wire means "charged", i.e. today's behaviour, so a sender
--    that has not been updated yet keeps working unchanged. A plain ADD COLUMN
--    with a non-volatile DEFAULT is metadata-only on Postgres 17 — it does not
--    rewrite the live usage ledger.
ALTER TABLE ms_billing.usage_events
    ADD COLUMN dev_served BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN ms_billing.usage_events.dev_served IS
    'Migration 073: the event was authenticated by a module live-tunnel session secret, not its deployed credential. Recorded and priced, never charged. A property of the fact, immutable like the rest of the row.';

-- 1a) Dev usage is RARE (a developer testing locally), so cover it with a
--     PARTIAL index rather than widening the three full occurrence indexes 055
--     already carries. This serves the console's "what did my tunnel cost?"
--     read and any operator sweep for tunnelled facts; the rollup itself still
--     scans by (account, window) exactly as before and simply carries one more
--     GROUP BY column, which needs no index of its own on a boolean.
CREATE INDEX IF NOT EXISTS usage_events_dev_served_idx
    ON ms_billing.usage_events (account_id, COALESCE(billable_at, recorded_at))
    WHERE dev_served;

-- 2) The per-aggregate flag. NOT NULL DEFAULT false for the same reason: every
--    pre-073 aggregate is charged usage.
ALTER TABLE ms_billing.usage_aggregates
    ADD COLUMN dev_served BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN ms_billing.usage_aggregates.dev_served IS
    'Migration 073: this billable line is tunnel-served developer usage. charged_micros IS computed and snapshotted (the console shows it), but the row is excluded from every money sum: arrears, settlement income, budgets, backlog disclosure, invoice reconciliation.';

-- 3) The replacement arbiter, built BEFORE the legacy one is dropped. It is
--    055's index plus dev_served. COALESCE(aggregation_key, '') is carried
--    over verbatim: an ordinary UNIQUE treats NULLs as distinct, and the
--    legacy NULL key must stay one deterministic identity.
CREATE UNIQUE INDEX usage_aggregates_period_line_dev_served_key
    ON ms_billing.usage_aggregates (
        period_id, app_id, module_id, metric, model, module_version,
        COALESCE(aggregation_key, ''), dev_served
    );

-- 4) Drop every OTHER uniqueness arbiter on usage_aggregates by lookup. The
--    set is deliberately defined by exclusion — "not the primary key, not the
--    index created above" — so it catches 055's index whatever it is called,
--    plus 018's / 023's UNIQUE CONSTRAINT if any environment still carries one
--    because an earlier drop missed. A constraint-backed index must be dropped
--    through ALTER TABLE (DROP INDEX refuses it), so the two cases are split
--    on conindid rather than assumed.
DO $$
DECLARE
    victim record;
BEGIN
    FOR victim IN
        SELECT idx.relname AS index_name,
               con.conname  AS constraint_name
        FROM pg_index i
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        LEFT JOIN pg_constraint con
               ON con.conindid = i.indexrelid AND con.contype IN ('u', 'p')
        WHERE ns.nspname = 'ms_billing'
          AND tbl.relname = 'usage_aggregates'
          AND i.indisunique
          AND NOT i.indisprimary
          AND idx.relname <> 'usage_aggregates_period_line_dev_served_key'
    LOOP
        IF victim.constraint_name IS NOT NULL THEN
            EXECUTE format('ALTER TABLE ms_billing.usage_aggregates DROP CONSTRAINT %I',
                           victim.constraint_name);
        ELSE
            EXECUTE format('DROP INDEX ms_billing.%I', victim.index_name);
        END IF;
    END LOOP;
END
$$;

COMMENT ON INDEX ms_billing.usage_aggregates_period_line_dev_served_key IS
    'Migration 073: the usage_aggregates idempotency key. Migration 055 key + dev_served, so tunnel-served and deployed usage of the same metric in one period are TWO immutable bill lines instead of one overwriting the other.';
