-- Reverses 063.
--
-- 🔴 Restoring 054's function restores the hole: five sealed columns become
-- editable again, and any column added after this point is editable too.
-- Down is offered because every migration here has one, not because running
-- it is safe on a database holding intents.
--
-- Dropping collects_digest is safe only while charge_intents is empty (it was
-- on 2026-08-31): a receivable whose link is dropped can never reproduce its
-- digest, and Rehydrate refuses it forever.

ALTER TABLE ms_billing.charge_intents
    DROP CONSTRAINT IF EXISTS charge_intents_collects_digest_fkey;

ALTER TABLE ms_billing.charge_intents
    DROP COLUMN IF EXISTS collects_digest;

-- 054's trigger was BEFORE UPDATE only, so this restores the DELETE hole too.
DROP TRIGGER IF EXISTS charge_intents_sealed ON ms_billing.charge_intents;
CREATE TRIGGER charge_intents_sealed
    BEFORE UPDATE ON ms_billing.charge_intents
    FOR EACH ROW EXECUTE FUNCTION ms_billing.charge_intents_reject_sealed_update();

CREATE OR REPLACE FUNCTION ms_billing.charge_intents_reject_sealed_update()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.digest, NEW.payer_kind, NEW.payer_id, NEW.currency,
        NEW.kind, NEW.price_book_revision, NEW.terms_revision, NEW.notice_policy,
        NEW.tax_jurisdiction, NEW.tax_rule_revision, NEW.tax_amount_micros,
        NEW.subtotal_micros, NEW.total_micros, NEW.authorization_id,
        NEW.execute_not_before, NEW.execute_not_after, NEW.supersedes_digest)
       IS DISTINCT FROM
       (OLD.digest, OLD.payer_kind, OLD.payer_id, OLD.currency,
        OLD.kind, OLD.price_book_revision, OLD.terms_revision, OLD.notice_policy,
        OLD.tax_jurisdiction, OLD.tax_rule_revision, OLD.tax_amount_micros,
        OLD.subtotal_micros, OLD.total_micros, OLD.authorization_id,
        OLD.execute_not_before, OLD.execute_not_after, OLD.supersedes_digest)
    THEN
        RAISE EXCEPTION
            'charge_intents is sealed: supersede it instead of editing (INV-003)';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
