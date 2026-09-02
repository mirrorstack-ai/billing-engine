-- Migration 059: apply 058's grants, because 058 ran and did nothing.
--
-- 🔴 A GATED MIGRATION THAT SKIPS IS STILL RECORDED AS APPLIED.
--
-- 058 wraps its GRANTs in `IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname =
-- 'billing_ro')`, so that dev and CI — which have a single owner and no
-- service roles — apply it cleanly. That gate is correct. What it does not
-- survive is the production ORDERING:
--
--   1. the release deploy ran `migrate`, which applied 058;
--   2. billing_ro did not exist yet, so 058 took the ELSE branch and raised a
--      NOTICE;
--   3. 058 is now recorded in the migration tracker as applied.
--
-- The role is created by mirrorstack-infra's db-bootstrap Lambda
-- (ensureReadOnlyRole), which is a MANUAL one-shot — it is not a CloudFormation
-- custom resource and no deploy invokes it. Its last production run was
-- 2026-07-12, months before ensureReadOnlyRole existed. So the create step
-- cannot have happened before the grant step, and re-running `migrate` will
-- never revisit 058.
--
-- The deploy reported success at every layer. `migrate` exited 0 because
-- skipping is not failing.
--
-- This migration is the same grant block, unchanged, so that it runs once the
-- role exists. It keeps the role-existence gate for dev and CI, where the
-- ELSE branch remains the correct outcome. GRANT and ALTER DEFAULT PRIVILEGES
-- are idempotent, so applying both 058 and 059 against a bootstrapped
-- production is a no-op the second time.
--
-- ⚠️ ORDER OF OPERATIONS for production, and it is not optional:
--     invoke db-bootstrap FIRST (creates billing_ro + GRANT rds_iam),
--     THEN deploy so migrate applies this file.
-- Reversing them burns 059 exactly as it burned 058, and the next fix would
-- have to be migration 060.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
        EXECUTE 'GRANT USAGE ON SCHEMA ms_billing TO billing_ro';
        EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA ms_billing TO billing_ro';
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing GRANT SELECT ON TABLES TO billing_ro';

        -- Still no sequence grant, for 058's reason: USAGE on a sequence
        -- permits nextval(), which mutates it. A read-only role that can
        -- advance a sequence is not read-only.
    ELSE
        RAISE NOTICE 'migration 059: skipping GRANT to billing_ro (role does not exist; dev, CI, or db-bootstrap not yet run)';
    END IF;
END $$;
