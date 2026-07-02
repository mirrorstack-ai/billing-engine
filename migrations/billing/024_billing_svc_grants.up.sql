-- Migration 024: production grants for the billing_svc service role.
--
-- Production Lambdas connect through RDS Proxy as billing_svc,
-- authenticating with a per-connection RDS-IAM token (DB_AUTH=rds-iam).
-- billing_svc owns no objects — migrations run as the admin user — so it
-- needs explicit grants on everything in ms_billing, plus default
-- privileges so tables/sequences created by FUTURE migrations are covered
-- without a per-migration GRANT.
--
-- Gated on role existence so this migration applies cleanly in dev and CI
-- (single mirrorstack user owns everything). Production deploys must
-- create billing_svc beforehand (LOGIN role with the rds_iam proxy auth
-- attachment); re-running this migration after the role appears is safe —
-- GRANT and ALTER DEFAULT PRIVILEGES are idempotent.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_svc') THEN
        EXECUTE 'GRANT USAGE ON SCHEMA ms_billing TO billing_svc';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ms_billing TO billing_svc';
        EXECUTE 'GRANT USAGE ON ALL SEQUENCES IN SCHEMA ms_billing TO billing_svc';
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO billing_svc';
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing GRANT USAGE ON SEQUENCES TO billing_svc';
    ELSE
        RAISE NOTICE 'migration 024: skipping GRANT to billing_svc (role does not exist; dev or pre-prod-bootstrap)';
    END IF;
END $$;
