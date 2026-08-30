-- Reverses 059's grants. Mirrors 058's down, including the role-existence
-- gate: REVOKE against a non-existent role is an error, not a no-op.
--
-- Note this also undoes 058 in practice — both granted the same privileges,
-- and a privilege revoked once is gone. That is correct: the pair exists
-- because 058 could not run, not because they grant different things.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing REVOKE SELECT ON TABLES FROM billing_ro';
        EXECUTE 'REVOKE SELECT ON ALL TABLES IN SCHEMA ms_billing FROM billing_ro';
        EXECUTE 'REVOKE USAGE ON SCHEMA ms_billing FROM billing_ro';
    END IF;
END $$;
