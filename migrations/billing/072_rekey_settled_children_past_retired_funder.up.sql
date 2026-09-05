-- Migration 072 — a SETTLED timer or domain may follow its app to a live
-- account even when the funder that settled it has since been retired.
--
-- 🔴 WHY. 052's guard_org_timer_write / guard_org_domain_write fire on EVERY
-- update of a live child row (NEW.removed_at IS NULL) and assert, through
-- assert_account_and_funding_billing_active, that the row's HISTORICAL
-- charge_funding_account_id still names an active organization. That funder
-- is frozen on the row at arm time and never changes afterwards — it is the
-- audit record of who paid. TransferApp (071) is the first writer that
-- updates a live child row for a reason other than charging it: RekeyAppTimers
-- / RekeyAppDomains move account_id so the row follows the roster (the split
-- guard, 071, requires that every live child agree with apps.account_id).
--
-- Put together: an org O's self-funded app has a module timer charged against
-- O's account (charge_funding_account_id = O, grace_resolved = true, still
-- installed so removed_at IS NULL). The app is transferred to a user; O is
-- later deleted and finalized (RetireOrgModuleTimers keys on apps.owner_org_id,
-- which is NULL by then, so the timer stays live; OrgBillingInFlightCount
-- counts nothing, the row is resolved). Every LATER transfer of that app runs
-- RekeyAppTimers over the row, the guard asserts O is active, and the
-- transaction aborts with 55000 — permanently. The app's billing account is
-- bound to whoever holds it until the module is uninstalled. No sweep or
-- reconcile ever trips this: they update pending rows (funder NULL until
-- armed) or set removed_at alone (which the guard already exempts).
--
-- 🔴 WHAT THIS PERMITS, EXACTLY. One shape of UPDATE: a row that is ALREADY
-- resolved (grace_resolved / charge_resolved true before and after), in which
-- account_id is the ONLY column that changes. Such an UPDATE moves nothing
-- billable — the charge is history — and asserts the lifecycle of the account
-- the row now bills to (assert_account_org_billing_active: its owner org and
-- its CURRENT funder) plus the app's owner org, so a retired principal can
-- still never be written INTO. What it stops asserting is the retired
-- HISTORICAL funder, which paid once and is not being asked to pay again.
-- Every other UPDATE — a resolution, an arm, a charge column, a removal, a
-- re-key of an UNRESOLVED row — takes exactly the 052 path, unchanged.
--
-- "Only account_id changed" is decided by comparing the whole row minus that
-- column (to_jsonb(NEW) - 'account_id'), not by enumerating the other
-- columns: an enumerated list decays the first time a column is added to the
-- table (054's sealed tuple did exactly that across 060/061/062), and a decayed
-- list here would silently widen what a re-key may carry.

CREATE OR REPLACE FUNCTION ms_billing.guard_org_timer_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_org_id UUID;
BEGIN
    -- 072: a settled row following its app to another account. The account it
    -- now bills to and the app's owner org must be live; the funder that
    -- already settled it is history and is not asked again.
    IF TG_OP = 'UPDATE'
        AND OLD.grace_resolved
        AND NEW.account_id IS DISTINCT FROM OLD.account_id
        AND (to_jsonb(NEW) - 'account_id') = (to_jsonb(OLD) - 'account_id') THEN
        PERFORM ms_billing.assert_account_org_billing_active(NEW.account_id);
        SELECT owner_org_id INTO v_org_id
          FROM ms_billing.apps
         WHERE app_id = NEW.app_id;
        IF v_org_id IS NOT NULL THEN
            PERFORM ms_billing.assert_org_billing_active(v_org_id);
        END IF;
        RETURN NEW;
    END IF;

    -- 052, verbatim.
    IF TG_OP = 'INSERT'
        OR NEW.removed_at IS NULL
        OR NEW.charge_attempted_at IS DISTINCT FROM OLD.charge_attempted_at
        OR NEW.grace_resolved IS DISTINCT FROM OLD.grace_resolved
        OR NEW.grace_charged_at IS DISTINCT FROM OLD.grace_charged_at
        OR NEW.grace_invoice_id IS DISTINCT FROM OLD.grace_invoice_id
        OR NEW.grace_invoice_item_id IS DISTINCT FROM OLD.grace_invoice_item_id THEN
        PERFORM ms_billing.assert_account_and_funding_billing_active(
            NEW.account_id,
            NEW.charge_funding_account_id
        );
        SELECT owner_org_id INTO v_org_id
          FROM ms_billing.apps
         WHERE app_id = NEW.app_id;
        IF v_org_id IS NOT NULL THEN
            PERFORM ms_billing.assert_org_billing_active(v_org_id);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION ms_billing.guard_org_domain_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_org_id UUID;
BEGIN
    -- 072: the domain twin of the settled re-key above.
    IF TG_OP = 'UPDATE'
        AND OLD.charge_resolved
        AND NEW.account_id IS DISTINCT FROM OLD.account_id
        AND (to_jsonb(NEW) - 'account_id') = (to_jsonb(OLD) - 'account_id') THEN
        PERFORM ms_billing.assert_account_org_billing_active(NEW.account_id);
        SELECT owner_org_id INTO v_org_id
          FROM ms_billing.apps
         WHERE app_id = NEW.app_id;
        IF v_org_id IS NOT NULL THEN
            PERFORM ms_billing.assert_org_billing_active(v_org_id);
        END IF;
        RETURN NEW;
    END IF;

    -- 052, verbatim.
    IF TG_OP = 'INSERT'
        OR NEW.removed_at IS NULL
        OR NEW.charge_attempted_at IS DISTINCT FROM OLD.charge_attempted_at
        OR NEW.charge_resolved IS DISTINCT FROM OLD.charge_resolved
        OR NEW.charged_at IS DISTINCT FROM OLD.charged_at
        OR NEW.charge_invoice_id IS DISTINCT FROM OLD.charge_invoice_id
        OR NEW.charge_invoice_item_id IS DISTINCT FROM OLD.charge_invoice_item_id THEN
        PERFORM ms_billing.assert_account_and_funding_billing_active(
            NEW.account_id,
            NEW.charge_funding_account_id
        );
        SELECT owner_org_id INTO v_org_id
          FROM ms_billing.apps
         WHERE app_id = NEW.app_id;
        IF v_org_id IS NOT NULL THEN
            PERFORM ms_billing.assert_org_billing_active(v_org_id);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

-- The triggers from 052 keep their names and stay bound to these functions;
-- CREATE OR REPLACE swaps the body under them. Prove the bindings survived
-- rather than assume it (058's lesson: a migration that ran is not a state
-- that exists).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger t
        JOIN pg_proc p ON p.oid = t.tgfoid
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE t.tgname = 'module_timers_reject_retired_org_write'
          AND n.nspname = 'ms_billing'
          AND p.proname = 'guard_org_timer_write'
    ) THEN
        RAISE EXCEPTION 'migration 072: module_timers_reject_retired_org_write is not bound to ms_billing.guard_org_timer_write';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger t
        JOIN pg_proc p ON p.oid = t.tgfoid
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE t.tgname = 'custom_domains_reject_retired_org_write'
          AND n.nspname = 'ms_billing'
          AND p.proname = 'guard_org_domain_write'
    ) THEN
        RAISE EXCEPTION 'migration 072: custom_domains_reject_retired_org_write is not bound to ms_billing.guard_org_domain_write';
    END IF;
END
$$;
