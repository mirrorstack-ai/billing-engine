-- Migration 068: re-issue the read-only grants, now that the role exists —
-- and REFUSE rather than skip if it does not.
--
-- 🔴 WHY THIS MIGRATION HAS TO EXIST AT ALL.
--
-- Migration 058 already contains exactly the right grant set for a read-only
-- ops identity: USAGE on the schema, SELECT on all tables, and the
-- ALTER DEFAULT PRIVILEGES half that is easy to omit and expensive to notice.
-- It deliberately does not grant sequence USAGE, because a read-only role that
-- can advance a sequence is not read-only.
--
-- None of it ran in production. 058's body is wrapped in
--
--     IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
--         ...grants...
--     ELSE
--         RAISE NOTICE '... skipping GRANT to billing_ro (role does not exist)';
--
-- and nothing created billing_ro: there was no CREATE ROLE in any migration in
-- this repository, and roles are minted by db-bootstrap from config.DbServices,
-- which lives in mirrorstack-infra. So in production 058 took the ELSE branch,
-- raised a NOTICE, exited 0, and was recorded as APPLIED.
--
-- A migration is applied once. 058 will never run again, so CREATING THE ROLE
-- IS NOT SUFFICIENT — the grants have to be re-issued by a new migration, which
-- is this one.
--
-- 🔴 AND THE SAME THING HAPPENED TO 064'S REVOKE, WHICH IS WORSE.
--
-- Migration 064 revokes ms_billing.evidence_records from billing_ro, because
-- 058's ALTER DEFAULT PRIVILEGES would otherwise make the INV-014 evidence
-- outbox readable by the ops role the moment it is created. That REVOKE is
-- gated identically, so it also did nothing and is also recorded as applied.
--
-- The two skipped statements do not cancel out. Re-issuing 058's grants alone
-- would GRANT SELECT ON ALL TABLES — which includes evidence_records, an
-- existing table — and there would be no surviving REVOKE to take it back. The
-- fix for the grants would silently undo the protection. So this migration
-- re-issues BOTH, in that order, and the order is load-bearing.
--
-- 🔴 IT RAISES INSTEAD OF SKIPPING.
--
-- Repeating 058's gate here would repeat 058's failure: applied, recorded,
-- granted nothing, and the next reader would have to discover it the same way.
-- A migration that cannot do its job must fail so the deploy stops, which makes
-- the ordering against role creation explicit instead of accidental:
--
--     1. mirrorstack-infra adds billing_ro to config.DbServices; db-bootstrap
--        mints it.
--     2. THEN this migration runs.
--
-- Run out of order it fails loudly and is retried after step 1. That is the
-- intended behaviour, not an inconvenience to be gated away.
DO $$
BEGIN
    -- 🔴 CREATE IT IF ABSENT, rather than raising or skipping.
    --
    -- Both alternatives were wrong. SKIPPING is what 058 did: gated on the
    -- role existing, it took the ELSE branch, raised a NOTICE, exited 0, and
    -- was recorded as APPLIED having granted nothing — and a migration is
    -- applied once, so it can never repair itself. RAISING fixes that but
    -- moves the cost: this file runs inside a multi-repo deploy, and a
    -- migration that halts it strands every other repo's rollout on an
    -- ordering nothing enforces. db-bootstrap is invoked by a Makefile target,
    -- not by any workflow, so "infra runs first" is a convention rather than a
    -- guarantee — which is precisely how 058 came to skip.
    --
    -- So 068 does not depend on that ordering at all. The role is a privilege
    -- bundle; creating it is idempotent, and infra's ensureReadOnlyRole is
    -- equally idempotent when it runs afterwards. LOGIN with no password
    -- matches what db-bootstrap creates and cannot authenticate on its own:
    -- connecting requires the rds_iam grant, which stays infra's to give.
    --
    -- The division "infra owns identities, migrations own privileges" is still
    -- right about ownership. It is not a reason to leave a deploy hostage to an
    -- unordered step.
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
        RAISE NOTICE 'migration 068: creating role billing_ro (db-bootstrap has not run here yet); it grants rds_iam separately and idempotently';
        CREATE ROLE billing_ro LOGIN;
    END IF;

    -- 058's body, verbatim. Idempotent, so re-issuing is safe where 058 did
    -- happen to run (a dev database whose role predates it).
    EXECUTE 'GRANT USAGE ON SCHEMA ms_billing TO billing_ro';
    EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA ms_billing TO billing_ro';
    EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing GRANT SELECT ON TABLES TO billing_ro';

    -- 064's body, verbatim, AFTER the grants above. The GRANT ON ALL TABLES
    -- covers evidence_records because it already exists; this is what takes it
    -- back. Reversing these two statements would leave the outbox readable.
    EXECUTE 'REVOKE ALL ON ms_billing.evidence_records FROM billing_ro';
END
$$;

-- The assertion, in the same transaction as the work.
--
-- A grant that did not take is the failure this whole file is about, so it is
-- checked rather than assumed. has_table_privilege answers for the role as
-- Postgres actually resolved it, including through any role membership, which
-- is the thing a reading of the GRANT statements cannot tell you.
DO $$
BEGIN
    IF NOT has_table_privilege('billing_ro', 'ms_billing.invoices', 'SELECT') THEN
        RAISE EXCEPTION 'migration 068: billing_ro cannot SELECT ms_billing.invoices after the grants ran';
    END IF;
    IF has_table_privilege('billing_ro', 'ms_billing.evidence_records', 'SELECT') THEN
        RAISE EXCEPTION 'migration 068: billing_ro can still SELECT ms_billing.evidence_records; the REVOKE did not take, and the INV-014 outbox is exposed to the ops role';
    END IF;
    IF has_table_privilege('billing_ro', 'ms_billing.invoices', 'INSERT')
        OR has_table_privilege('billing_ro', 'ms_billing.invoices', 'UPDATE')
        OR has_table_privilege('billing_ro', 'ms_billing.invoices', 'DELETE') THEN
        RAISE EXCEPTION 'migration 068: billing_ro holds a write privilege; it is not a read-only role';
    END IF;
END
$$;
