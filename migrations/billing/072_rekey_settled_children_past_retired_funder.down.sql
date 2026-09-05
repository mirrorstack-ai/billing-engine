-- Reverses 072: the two guard functions go back to their 052 bodies, verbatim.
-- The triggers were never dropped or recreated, so nothing else changes. After
-- this, a transfer of an app carrying a settled child whose historical funder
-- has been retired aborts again (55000) — the state 072 exists to end — which
-- is the intended meaning of reversing it.

CREATE OR REPLACE FUNCTION ms_billing.guard_org_timer_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_org_id UUID;
BEGIN
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
