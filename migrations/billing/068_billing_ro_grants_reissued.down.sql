-- Reverse 068 by revoking what it granted.
--
-- Gated on the role existing, and here that is correct rather than the trap
-- 068's header describes: a REVOKE from a role that does not exist has nothing
-- to undo, whereas a GRANT that silently did not happen leaves a capability
-- nobody knows is missing. The asymmetry is the point — skipping is safe in the
-- direction that removes access and unsafe in the direction that adds it.
--
-- evidence_records is deliberately not mentioned. 068 revoked it; leaving it
-- revoked is the safe direction, and re-granting it here would use a down
-- migration to hand out access.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
        EXECUTE 'ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing REVOKE SELECT ON TABLES FROM billing_ro';
        EXECUTE 'REVOKE SELECT ON ALL TABLES IN SCHEMA ms_billing FROM billing_ro';
        EXECUTE 'REVOKE USAGE ON SCHEMA ms_billing FROM billing_ro';
    ELSE
        RAISE NOTICE 'migration 068 down: billing_ro does not exist; nothing to revoke';
    END IF;
END
$$;
