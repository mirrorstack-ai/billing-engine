-- Revokes what 058 granted. The role itself is created by infrastructure, not
-- by a migration, so this does not drop it — dropping a role a Lambda still
-- authenticates as would break the function rather than restrict it.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing REVOKE SELECT ON TABLES FROM billing_ro';
        EXECUTE 'REVOKE SELECT ON ALL TABLES IN SCHEMA ms_billing FROM billing_ro';
        EXECUTE 'REVOKE USAGE ON SCHEMA ms_billing FROM billing_ro';
    END IF;
END $$;
