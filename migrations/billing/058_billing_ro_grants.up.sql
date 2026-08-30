-- Migration 058: production grants for the billing_ro read-only role.
--
-- The ops function (cmd/intent-shadow, invoked as a Lambda) answers two
-- questions against production: does the intent rater agree with billing
-- history, and are the seven legacy-drop preconditions clear. Both are reads.
--
-- Until now the only identity available was billing_svc, which holds
-- SELECT/INSERT/UPDATE/DELETE on every table in ms_billing plus ALTER DEFAULT
-- PRIVILEGES (migration 024). Running a diagnostic under it means read-only is
-- enforced by the CODE — a BeginTx{AccessMode: pgx.ReadOnly} the caller must
-- remember — rather than by the credential. That guard is real and tested, but
-- it is one forgotten transaction away from a write, and the thing it guards
-- is the production billing ledger.
--
-- billing_ro cannot write because it was never granted the privilege. That is
-- a property of the credential, not of a code path someone has to keep getting
-- right.
--
-- Follows migration 024's shape exactly, including the role-existence gate so
-- dev and CI (single owner, no service roles) apply it cleanly. Production
-- creates the LOGIN role with the rds_iam attachment beforehand; re-running is
-- safe because GRANT and ALTER DEFAULT PRIVILEGES are idempotent.
--
-- 🔴 ALTER DEFAULT PRIVILEGES is the half that is easy to omit and expensive
-- to miss: without it billing_ro can read today's tables and is silently blind
-- to every table a future migration creates. A diagnostic that cannot see new
-- data reports "no discrepancies" for the rows it cannot read.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
        EXECUTE 'GRANT USAGE ON SCHEMA ms_billing TO billing_ro';
        EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA ms_billing TO billing_ro';
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing GRANT SELECT ON TABLES TO billing_ro';

        -- No sequence grant. Reading a sequence's current value is not needed
        -- by any diagnostic, and USAGE on a sequence permits nextval(), which
        -- mutates it. A read-only role that can advance a sequence is not
        -- read-only.
    ELSE
        RAISE NOTICE 'migration 058: skipping GRANT to billing_ro (role does not exist; dev or pre-prod-bootstrap)';
    END IF;
END $$;
