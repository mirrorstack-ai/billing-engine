-- 098: a durable watermark per scheduled infra puller (billing-engine#225).
--
-- infra-egress-sync and infra-ssr-compute-sync used to sweep a fixed 3 closed
-- hours and remember nothing, so an outage longer than 3 h lost every window
-- inside it but the last three (2026-09-13: egress 04:00-05:00Z, after five
-- Cloudflare 401s). Each now stores how far it has confirmed recording here and
-- sweeps from max(confirmed_through, now - 7 days); see internal/account/infrasync.
--
-- One row per source. confirmed_through is the exclusive end of the longest run
-- of hour windows confirmed recorded, oldest first — always a top of hour. The
-- writer upserts with GREATEST, so it never moves backwards.

CREATE TABLE IF NOT EXISTS ms_billing.infra_sync_watermarks (
    source            TEXT        PRIMARY KEY
                                  CHECK (source ~ '^[a-z][a-z0-9-]{0,62}$'),
    confirmed_through TIMESTAMPTZ NOT NULL
                                  CHECK (extract(epoch FROM confirmed_through)::bigint % 3600 = 0),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE ms_billing.infra_sync_watermarks IS
    'Per-source confirmed-through watermark of the scheduled infra pullers (billing-engine#225). Written by cmd/infra-egress-sync and cmd/infra-ssr-compute-sync.';

-- Grants, as 087 issues them: billing_svc reads and upserts (SELECT, INSERT,
-- UPDATE) and loses the DELETE 024's default privileges hand it — deleting a
-- row would silently reset a puller to the 3-hour first-run sweep. billing_ro
-- reads it explicitly rather than through 068's default.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_svc') THEN
        RAISE NOTICE 'migration 098: creating role billing_svc (db-bootstrap has not run here yet); it grants rds_iam separately and idempotently';
        CREATE ROLE billing_svc LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
        RAISE NOTICE 'migration 098: creating role billing_ro (db-bootstrap has not run here yet)';
        CREATE ROLE billing_ro LOGIN;
    END IF;
    EXECUTE 'GRANT SELECT, INSERT, UPDATE ON ms_billing.infra_sync_watermarks TO billing_svc';
    EXECUTE 'REVOKE DELETE ON ms_billing.infra_sync_watermarks FROM billing_svc';
    EXECUTE 'GRANT SELECT ON ms_billing.infra_sync_watermarks TO billing_ro';
END
$$;

-- Prove the grants landed (058 was recorded applied having granted nothing).
DO $$
BEGIN
    IF NOT has_table_privilege('billing_svc', 'ms_billing.infra_sync_watermarks', 'SELECT')
       OR NOT has_table_privilege('billing_svc', 'ms_billing.infra_sync_watermarks', 'INSERT')
       OR NOT has_table_privilege('billing_svc', 'ms_billing.infra_sync_watermarks', 'UPDATE') THEN
        RAISE EXCEPTION 'migration 098: billing_svc cannot SELECT/INSERT/UPDATE ms_billing.infra_sync_watermarks after the grant ran';
    END IF;
    IF has_table_privilege('billing_svc', 'ms_billing.infra_sync_watermarks', 'DELETE') THEN
        RAISE EXCEPTION 'migration 098: billing_svc still holds DELETE on ms_billing.infra_sync_watermarks after the revoke ran';
    END IF;
    IF NOT has_table_privilege('billing_ro', 'ms_billing.infra_sync_watermarks', 'SELECT') THEN
        RAISE EXCEPTION 'migration 098: billing_ro cannot SELECT ms_billing.infra_sync_watermarks after the grant ran';
    END IF;
END
$$;
