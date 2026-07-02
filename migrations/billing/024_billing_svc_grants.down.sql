-- Reverses migration 024: revoke the billing_svc grants and the default
-- privileges. Gated on role existence, mirroring the up — in dev/CI the
-- role never existed and the up granted nothing, so there is nothing to
-- revoke. The role itself is NOT dropped: it is provisioned by production
-- infrastructure, not by this migration chain.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_svc') THEN
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing REVOKE USAGE ON SEQUENCES FROM billing_svc';
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM billing_svc';
        EXECUTE 'REVOKE USAGE ON ALL SEQUENCES IN SCHEMA ms_billing FROM billing_svc';
        EXECUTE 'REVOKE SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ms_billing FROM billing_svc';
        EXECUTE 'REVOKE USAGE ON SCHEMA ms_billing FROM billing_svc';
    ELSE
        RAISE NOTICE 'migration 024 down: skipping REVOKE from billing_svc (role does not exist)';
    END IF;
END $$;
