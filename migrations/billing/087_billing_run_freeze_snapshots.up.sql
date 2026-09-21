-- 087: the derivation a boundary freeze was taken from, kept per run and sealed
-- (billing-engine#218, core-v2#1485).
--
-- FreezeBillingRunCharge (cycle.sql) records a run's whole-cent figure
-- first-write-wins. What it does NOT keep is the set of lines that figure was
-- derived from: arrears = PeriodChargedTotal = Σ usage_aggregates.charged_micros,
-- the rollup re-runs on every cycle attempt, and UpsertUsageAggregate's
-- DO UPDATE overwrites each line in place. On 2026-09-14 a reclaim of account
-- 2ccc7c7b… derived 11202¢ against a 2026-09-11 freeze of 11390¢, and the
-- 09-11 lines behind the 11390 existed nowhere — only the total survived, so
-- the drift could not be attributed to a line.
--
-- These two tables are that first derivation. The cycle writes them once,
-- right after a fresh freeze wins, and only while the run still holds the
-- figure being described (the header INSERT … SELECT is bound to
-- billing_runs.frozen_charge_cents); a reclaim that finds a frozen figure
-- differing from its live derivation diffs these lines against the live
-- aggregates and logs every line that moved (BillingRunFreezeLineDrift).
--
-- Nothing reads them to move money. They are written best-effort — a failed
-- snapshot is logged, never fails the run — and are never consulted by the
-- charge, the split or the proposal. usage_aggregates stays the live table
-- every money reader uses; this migration changes none of them.
--
--   billing_run_freeze_snapshots       one row per frozen run: the frozen
--                                       cents and every component the
--                                       freezing attempt derived them from.
--   billing_run_freeze_snapshot_lines  the collectable (dev_served = false)
--                                       usage_aggregates rows — the exact set
--                                       PeriodChargedTotal sums — as they
--                                       stood when the snapshot was taken.
--
-- SEALED: an UPDATE or DELETE on either table raises. A snapshot that could be
-- edited would be one more overwrite of the first derivation, which is the
-- defect this migration exists to end. A re-freeze (billing-engine#217) does
-- not rewrite it either: the snapshot is the FIRST freeze's derivation, and the
-- re-freeze's own audit line logs the new figure.

CREATE TABLE IF NOT EXISTS ms_billing.billing_run_freeze_snapshots (
    run_id                 UUID        PRIMARY KEY
                                       REFERENCES ms_billing.billing_runs(id),
    frozen_cents           BIGINT      NOT NULL,
    -- PeriodChargedTotal as the freezing attempt read it; Σ of this run's
    -- snapshot lines equals it, or the snapshot is not written at all.
    usage_charged_micros   BIGINT      NOT NULL,
    allowance_micros       BIGINT      NOT NULL,
    arrears_micros         BIGINT      NOT NULL,
    advance_base_micros    BIGINT      NOT NULL,
    advance_overage_micros BIGINT      NOT NULL,
    advance_domains_micros BIGINT      NOT NULL,
    members_micros         BIGINT      NOT NULL,
    wallet_drawn_micros    BIGINT      NOT NULL,
    snapshotted_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ms_billing.billing_run_freeze_snapshot_lines (
    run_id              UUID                   NOT NULL
                        REFERENCES ms_billing.billing_run_freeze_snapshots(run_id),
    app_id              UUID                   NOT NULL,
    module_id           UUID                   NOT NULL,
    metric              TEXT                   NOT NULL,
    model               TEXT                   NOT NULL,
    module_version      TEXT                   NOT NULL,
    kind                ms_billing.metric_kind NOT NULL,
    aggregation_key     TEXT                   NULL,
    billable_quantity   NUMERIC                NOT NULL,
    unit_price_micros   BIGINT                 NOT NULL,
    customer_markup_num INTEGER                NOT NULL,
    customer_markup_den INTEGER                NOT NULL,
    raw_cost_micros     BIGINT                 NOT NULL,
    charged_micros      BIGINT                 NOT NULL,
    active_seconds      NUMERIC                NULL,
    period_days         NUMERIC                NULL,
    rolled_up_at        TIMESTAMPTZ            NOT NULL
);

-- One line per aggregate key within a run — the usage_aggregates arbiter
-- (073) minus period_id (one run is one period) and dev_served (only
-- collectable rows are snapshotted).
CREATE UNIQUE INDEX IF NOT EXISTS billing_run_freeze_snapshot_lines_key
    ON ms_billing.billing_run_freeze_snapshot_lines (
        run_id, app_id, module_id, metric, model, module_version,
        COALESCE(aggregation_key, '')
    );

CREATE OR REPLACE FUNCTION ms_billing.billing_run_freeze_snapshots_reject_edit()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION
        '% is sealed: a freeze snapshot is the first derivation and is never edited or deleted', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS billing_run_freeze_snapshots_sealed ON ms_billing.billing_run_freeze_snapshots;
CREATE TRIGGER billing_run_freeze_snapshots_sealed
    BEFORE UPDATE OR DELETE ON ms_billing.billing_run_freeze_snapshots
    FOR EACH ROW EXECUTE FUNCTION ms_billing.billing_run_freeze_snapshots_reject_edit();

DROP TRIGGER IF EXISTS billing_run_freeze_snapshot_lines_sealed ON ms_billing.billing_run_freeze_snapshot_lines;
CREATE TRIGGER billing_run_freeze_snapshot_lines_sealed
    BEFORE UPDATE OR DELETE ON ms_billing.billing_run_freeze_snapshot_lines
    FOR EACH ROW EXECUTE FUNCTION ms_billing.billing_run_freeze_snapshots_reject_edit();

-- Grants, as 071 issues them: the roles are created if absent (068 records
-- why a gate that skips is worse), billing_svc gets SELECT + INSERT and loses
-- the UPDATE/DELETE 024's default privileges handed it — the trigger refuses an
-- edit, this makes the service unable to attempt one — and billing_ro is
-- granted SELECT explicitly rather than through 068's default.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_svc') THEN
        RAISE NOTICE 'migration 087: creating role billing_svc (db-bootstrap has not run here yet); it grants rds_iam separately and idempotently';
        CREATE ROLE billing_svc LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
        RAISE NOTICE 'migration 087: creating role billing_ro (db-bootstrap has not run here yet)';
        CREATE ROLE billing_ro LOGIN;
    END IF;
    EXECUTE 'GRANT SELECT, INSERT ON ms_billing.billing_run_freeze_snapshots, ms_billing.billing_run_freeze_snapshot_lines TO billing_svc';
    EXECUTE 'REVOKE UPDATE, DELETE ON ms_billing.billing_run_freeze_snapshots, ms_billing.billing_run_freeze_snapshot_lines FROM billing_svc';
    EXECUTE 'GRANT SELECT ON ms_billing.billing_run_freeze_snapshots, ms_billing.billing_run_freeze_snapshot_lines TO billing_ro';
END
$$;

-- Prove the grants landed (058 was recorded applied having granted nothing).
DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['ms_billing.billing_run_freeze_snapshots', 'ms_billing.billing_run_freeze_snapshot_lines'] LOOP
        IF NOT has_table_privilege('billing_svc', t, 'INSERT') OR NOT has_table_privilege('billing_svc', t, 'SELECT') THEN
            RAISE EXCEPTION 'migration 087: billing_svc cannot SELECT/INSERT % after the grant ran', t;
        END IF;
        IF has_table_privilege('billing_svc', t, 'UPDATE') OR has_table_privilege('billing_svc', t, 'DELETE') THEN
            RAISE EXCEPTION 'migration 087: billing_svc still holds UPDATE or DELETE on % after the revoke ran; a freeze snapshot is sealed', t;
        END IF;
        IF NOT has_table_privilege('billing_ro', t, 'SELECT') THEN
            RAISE EXCEPTION 'migration 087: billing_ro cannot SELECT % after the grant ran', t;
        END IF;
    END LOOP;
END
$$;
