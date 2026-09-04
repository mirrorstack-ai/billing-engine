-- Migration 071 — the app billing-account transfer ledger, and the guard that
-- makes a split account attribution unrepresentable.
--
-- api-platform moves an app's billing account to another owner. TransferApp
-- re-keys the app's billing attribution and records WHAT IT DID here, exactly
-- once per request_id, so a fire-and-forget retry returns the first result
-- instead of transferring a second time.
--
-- 🔴 WHAT THIS DOES NOT CLAIM. docs/DESIGN.md §12 item 15 specifies
-- BillingResponsibilityTransfer as a typed payer cutoff, never a field update.
-- This is a field update. The owner settled the transfer half of decision 15 on
-- 2026-09-04 (see docs/DESIGN.md, decision 15) and this ships that decision, not
-- the cutoff. Every original instant is preserved and every transfer is recorded
-- here, so a typed cutoff stays reconstructible — but it has not been built, and
-- the pricing-migration half of decision 15 remains open.
--
-- 🔴 RECURRING FEES ARE PREPAID AND DO NOT MOVE. The boundary that CLOSES a
-- period bills that period's usage arrears plus the NEXT period's recurring
-- (internal/account/cycle/charge.go:322, and the advance snapshot's
-- `PeriodStart: periodEnd` at :855). So the recurring covering a transfer
-- instant was already collected from the OLD account when that period opened.
-- A transfer cannot move it and does not try: no refund, no proration, matching
-- this schema's prospective-removal posture (047:22-24, "an already-charged
-- period is never credited"). The NEW account's first recurring charge for the
-- app falls at ITS OWN next boundary, and because periods are anchored per
-- account those two boundaries differ — leaving at most one whole unit of gap
-- or overlap. Accepted for v1 and reported to the caller as recurring_from.

CREATE TABLE IF NOT EXISTS ms_billing.app_transfer_events (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- The caller's idempotency key (api-platform's transfer id). UNIQUE
    -- GLOBALLY, not per app: the same request_id replayed against a DIFFERENT
    -- app is a caller bug, and it has to surface as a conflict rather than as a
    -- second successful transfer.
    request_id        UUID NOT NULL,

    -- 🔴 Deliberately NOT a foreign key, against this schema's ordinary
    -- in-schema hard-FK rule. Both candidate parents delete out from under it:
    -- accounts(id) cascades to apps, and apps(app_id) cascades to every
    -- app-scoped child. The record an auditor most needs is precisely the one
    -- describing a transfer away from an account that was later deleted.
    app_id            UUID NOT NULL,

    -- NULL when the app was an UNBILLED org roster row (account_id NULL).
    -- "Transferred from no payer" is a real state and is not the same as
    -- "transferred from an unknown payer".
    from_account      UUID NULL,
    to_account        UUID NOT NULL,

    -- What the caller asked for. `mode` decides USAGE ONLY: keep moves no
    -- event, move re-attributes this app's not-yet-invoiced usage in the
    -- overlapping open window. The account re-key itself is unconditional.
    mode              TEXT NOT NULL CHECK (mode IN ('keep', 'move')),

    -- Rows the move actually re-attributed; always 0 for mode='keep'. This is
    -- the number a replay returns verbatim rather than recounting.
    moved_event_count BIGINT NOT NULL DEFAULT 0 CHECK (moved_event_count >= 0),

    -- The transfer instant (service clock, UTC) — the upper bound of the
    -- re-attribution window.
    at                TIMESTAMPTZ NOT NULL,

    -- 🔴 THE REST OF THE RESPONSE, STORED, so a replay is verbatim. The target
    -- account's open window at the transfer instant and the boundary at which
    -- its recurring fees for this app begin (see the header). These are
    -- functions of `at` and the target's anchor, and both move: a retry that
    -- arrives after the boundary would recompute a LATER period and a later
    -- recurring_from, and api-platform — which fires this post-commit with
    -- retry — would show the customer a date the first call never promised.
    -- Storing them costs three columns; recomputing them makes "replay returns
    -- the stored result" true of one field and false of three.
    open_period_start TIMESTAMPTZ NOT NULL,
    open_period_end   TIMESTAMPTZ NOT NULL,
    recurring_from    TIMESTAMPTZ NOT NULL,

    -- Wall-clock arrival, matching the append-only convention (064, 065).
    recorded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT app_transfer_events_keep_moves_nothing
        CHECK (mode <> 'keep' OR moved_event_count = 0),

    -- The open period is the one CONTAINING the transfer instant — that is
    -- how the writer derives it — and the first recurring boundary cannot
    -- precede that period's end, because the period in progress was prepaid
    -- by the old account and never moves. A row that says otherwise was not
    -- written by TransferApp.
    CONSTRAINT app_transfer_events_at_inside_open_period
        CHECK (open_period_start <= at AND at < open_period_end),
    CONSTRAINT app_transfer_events_recurring_after_open_period
        CHECK (recurring_from >= open_period_end)
);

CREATE UNIQUE INDEX IF NOT EXISTS app_transfer_events_request_id_uidx
    ON ms_billing.app_transfer_events (request_id);

CREATE INDEX IF NOT EXISTS app_transfer_events_app_at_idx
    ON ms_billing.app_transfer_events (app_id, at DESC);

COMMENT ON TABLE ms_billing.app_transfer_events IS
    'One row per accepted app billing-account transfer. request_id is the caller''s idempotency key; a replay returns the stored result and a different target for the same key is a conflict. Append-only.';

-- Append-only, for UPDATE and DELETE alike — the 064/065/066 convention, and
-- for the same reason. A row here is the answer a replay returns and the only
-- record of where a transfer moved usage FROM; edit it and the replay lies,
-- delete it and the next retry of that request_id transfers again. There is
-- no column anyone may change after the fact, so it is a blanket refusal
-- rather than a column comparison. A transfer that should not have happened
-- is undone by another transfer, with its own request_id and its own row.
CREATE OR REPLACE FUNCTION ms_billing.app_transfer_events_reject_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION
        'app_transfer_events is append-only: undo a transfer with another transfer, not by editing its record';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS app_transfer_events_append_only ON ms_billing.app_transfer_events;
CREATE TRIGGER app_transfer_events_append_only
    BEFORE UPDATE OR DELETE ON ms_billing.app_transfer_events
    FOR EACH ROW EXECUTE FUNCTION ms_billing.app_transfer_events_reject_mutation();

-- ---------------------------------------------------------------------------
-- 🔴 THE SPLIT GUARD. This is the half that is not a ledger.
--
-- ms_billing.apps.account_id is duplicated onto app_module_overage_timers
-- .account_id and app_custom_domains.account_id, and NOTHING tied the three
-- together: no foreign key between them (each is its own independent FK to
-- accounts(id) — 033:51, 047:15), no CHECK, no trigger, and no query that
-- asserts they agree. A split was already REPRESENTABLE; it was merely
-- unreachable, because no query in this repository wrote either denormalised
-- column after insert.
--
-- TransferApp is the first writer of those columns. The day it ships, a split
-- becomes reachable — and it would be SILENT: PendingAddonModuleCharges
-- (module_timers.sql) joins timers to apps with no account predicate on the app
-- side, so a bill rendered from a split renders without complaint. So the first
-- writer ships the first guard, in the same migration.
--
-- CONSTRAINT TRIGGER ... INITIALLY DEFERRED, not a row trigger: the transfer
-- necessarily violates the invariant BETWEEN statements (the roster moves, then
-- the children follow). Checking at COMMIT makes the guard independent of
-- statement order inside the transaction while still refusing to let a split
-- reach disk.
CREATE OR REPLACE FUNCTION ms_billing.app_account_attribution_agrees()
RETURNS TRIGGER AS $$
DECLARE
    target_app UUID;
    roster     UUID;
    offender   UUID;
    kind       TEXT;
BEGIN
    target_app := COALESCE(NEW.app_id, OLD.app_id);

    SELECT a.account_id INTO roster
    FROM ms_billing.apps a
    WHERE a.app_id = target_app;

    -- The app row is gone (cascade) or is an unbilled org roster row. Neither
    -- is a split: there is no roster account for a child to disagree with.
    IF NOT FOUND OR roster IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT t.account_id INTO offender
    FROM ms_billing.app_module_overage_timers t
    WHERE t.app_id = target_app
      AND t.removed_at IS NULL
      AND t.account_id IS DISTINCT FROM roster
    LIMIT 1;
    IF FOUND THEN
        kind := 'app_module_overage_timers';
    ELSE
        SELECT d.account_id INTO offender
        FROM ms_billing.app_custom_domains d
        WHERE d.app_id = target_app
          AND d.removed_at IS NULL
          AND d.account_id IS DISTINCT FROM roster
        LIMIT 1;
        IF FOUND THEN
            kind := 'app_custom_domains';
        END IF;
    END IF;

    IF offender IS NOT NULL THEN
        RAISE EXCEPTION
            'split app billing attribution: app % has roster account % but a live % row on account % — the base fee and the per-unit fees would bill different payers',
            target_app, roster, kind, offender;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS apps_attribution_agrees ON ms_billing.apps;
CREATE CONSTRAINT TRIGGER apps_attribution_agrees
    AFTER UPDATE OF account_id ON ms_billing.apps
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ms_billing.app_account_attribution_agrees();

DROP TRIGGER IF EXISTS app_module_overage_timers_attribution_agrees ON ms_billing.app_module_overage_timers;
CREATE CONSTRAINT TRIGGER app_module_overage_timers_attribution_agrees
    AFTER INSERT OR UPDATE OF account_id ON ms_billing.app_module_overage_timers
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ms_billing.app_account_attribution_agrees();

DROP TRIGGER IF EXISTS app_custom_domains_attribution_agrees ON ms_billing.app_custom_domains;
CREATE CONSTRAINT TRIGGER app_custom_domains_attribution_agrees
    AFTER INSERT OR UPDATE OF account_id ON ms_billing.app_custom_domains
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ms_billing.app_account_attribution_agrees();

-- ---------------------------------------------------------------------------
-- Grants. The role is CREATED IF ABSENT rather than gated on — migration 068
-- records why at length: a gate that skips exits 0, is recorded APPLIED, and
-- can never repair itself, and a gate that RAISES strands a multi-repo deploy
-- on an ordering nothing enforces.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_svc') THEN
        RAISE NOTICE 'migration 071: creating role billing_svc (db-bootstrap has not run here yet); it grants rds_iam separately and idempotently';
        CREATE ROLE billing_svc LOGIN;
    END IF;
    EXECUTE 'GRANT SELECT, INSERT ON ms_billing.app_transfer_events TO billing_svc';

    -- 🔴 AND TAKE BACK WHAT 024 HANDED OUT. The explicit GRANT above reads as
    -- the whole privilege set, and it is not: 024's ALTER DEFAULT PRIVILEGES
    -- gives billing_svc SELECT, INSERT, UPDATE, DELETE on every table the
    -- admin user creates in this schema, this one included, the moment
    -- CREATE TABLE ran. The trigger above refuses an edit; this makes the
    -- service unable to attempt one, so a bug that tries reads as 42501 at
    -- the connection rather than as an exception from inside a transaction.
    -- Written as a REVOKE, as 064 did for billing_ro, because the default is
    -- right for every other table and one exception should read as one.
    EXECUTE 'REVOKE UPDATE, DELETE ON ms_billing.app_transfer_events FROM billing_svc';

    -- 🔴 GRANT billing_ro EXPLICITLY rather than leaning on 068's
    -- ALTER DEFAULT PRIVILEGES. 068 is one of four migrations missing from
    -- scripts/init-db.sql, so on a database built by `make db-init` that
    -- default privilege was never established and an inherited-grant assumption
    -- would fail here — turning a read grant this table does not strictly need
    -- into a hard init failure. Issuing it is idempotent where 068 did run, and
    -- correct where it did not. Depending on another migration's side effect is
    -- the same class of mistake 058 made in the other direction.
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
        RAISE NOTICE 'migration 071: creating role billing_ro (db-bootstrap has not run here yet)';
        CREATE ROLE billing_ro LOGIN;
    END IF;
    EXECUTE 'GRANT SELECT ON ms_billing.app_transfer_events TO billing_ro';
END
$$;

-- 🔴 PROVE THE GRANT LANDED. A GRANT that runs is not a privilege that exists,
-- and this repository has already shipped one migration whose grants were
-- recorded applied having granted nothing (058, repaired by 068). Read the
-- privilege back and refuse if it is absent.
DO $$
BEGIN
    IF NOT has_table_privilege('billing_svc', 'ms_billing.app_transfer_events', 'INSERT') THEN
        RAISE EXCEPTION 'migration 071: billing_svc cannot INSERT ms_billing.app_transfer_events after the grant ran';
    END IF;
    IF NOT has_table_privilege('billing_svc', 'ms_billing.app_transfer_events', 'SELECT') THEN
        RAISE EXCEPTION 'migration 071: billing_svc cannot SELECT ms_billing.app_transfer_events after the grant ran';
    END IF;
    -- The REVOKE has to have landed too. A default privilege the REVOKE
    -- missed would leave the service able to rewrite its own idempotency
    -- record, and nothing downstream would notice until a replay answered
    -- differently from the first call.
    IF has_table_privilege('billing_svc', 'ms_billing.app_transfer_events', 'UPDATE')
        OR has_table_privilege('billing_svc', 'ms_billing.app_transfer_events', 'DELETE') THEN
        RAISE EXCEPTION 'migration 071: billing_svc still holds UPDATE or DELETE on ms_billing.app_transfer_events after the revoke ran; the ledger is append-only';
    END IF;
    IF NOT has_table_privilege('billing_ro', 'ms_billing.app_transfer_events', 'SELECT') THEN
        RAISE EXCEPTION 'migration 071: billing_ro cannot SELECT ms_billing.app_transfer_events after the explicit grant ran';
    END IF;
    IF has_table_privilege('billing_ro', 'ms_billing.app_transfer_events', 'INSERT')
        OR has_table_privilege('billing_ro', 'ms_billing.app_transfer_events', 'UPDATE')
        OR has_table_privilege('billing_ro', 'ms_billing.app_transfer_events', 'DELETE') THEN
        RAISE EXCEPTION 'migration 071: billing_ro holds a write privilege on ms_billing.app_transfer_events; it is not a read-only role';
    END IF;
END
$$;
