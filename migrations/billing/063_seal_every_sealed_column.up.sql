-- Migration 063: freeze every sealed column, by default, and store the one
-- sealed link that was digested but never written down.
--
-- 🔴 TWO DEFECTS, both introduced by this repository's own supersession work.
--
-- DEFECT 1 — the seal covered 17 of 26 columns, and only UPDATE.
--
-- 054:97-121 froze ms_billing.charge_intents by comparing a HARDCODED
-- 17-column tuple, which was every column it needed to freeze that day: 054
-- created 20 columns, three of which (state, state_changed_at, created_at)
-- it left out. Migrations
-- 060 (tax_verification), 061 (wallet_allocation_micros,
-- provider_remainder_micros) and 062 (selected_rail, routing_policy_revision)
-- each added a SEALED column — each one inside ChargeIntent.computeDigest —
-- and none of them extended the tuple. So until this migration:
--
--   UPDATE ms_billing.charge_intents SET selected_rail = 'other';
--
-- succeeded. The row then no longer reproduces its own digest, which is
-- exactly the state INV-003 exists to make impossible ("superseding is cheap;
-- editing is unanswerable"). internal/intent/store/store.go's comment — "the
-- trigger on the table permits these columns to change; everything sealed
-- stays frozen" — was false for five columns.
--
-- By 062 the table had 26 columns and the tuple still named 17, so NINE were
-- unchecked: the three 054 deliberately left out, plus 056's reserved_micros
-- (legitimately mutable, see below), plus the five sealed ones above. Of the
-- nine, six should never have moved — the five sealed columns and created_at.
--
-- DEFECT 1b — the seal was BEFORE UPDATE only, so DELETE was never covered.
--
-- `DELETE FROM ms_billing.charge_intents WHERE digest = '...'` succeeded, and
-- charge_intent_lines, charge_intent_source_facts and notice_receipts all
-- carry ON DELETE CASCADE (054:130, :150, :196). So one statement removed a
-- sealed document, the lines it was made of, the facts it was derived from,
-- and the carrier-verified notice evidence INV-005 rests on. A document that
-- can be deleted is not sealed; it is merely hard to edit. The trigger now
-- covers DELETE too, and a row that should not exist is answered the way
-- INV-014 answers it — with a superseding or correcting row.
--
-- Three consecutive supersessions missed it because the tuple has to be
-- maintained and nothing failed when it was not. A control whose correctness
-- depends on remembering it is a control that decays, so the fix is not a
-- longer tuple: it is to invert the default. This version compares the whole
-- row minus a NAMED mutable set. A column added tomorrow is frozen tomorrow,
-- with no migration and nothing to remember, and unfreezing one becomes a
-- deliberate edit to this list rather than an omission from a list.
--
-- The mutable set is the lifecycle, plus the one derived counter that lives
-- on a sealed row. state and state_changed_at have to move — an intent that
-- cannot advance is frozen solid, which 054 already says and its own test
-- asserts. reserved_micros is 056's source-capacity counter, incremented in
-- place by ReserveRemainder (internal/intent/store/receivable.go:73); it is
-- not inside computeDigest, so it is not part of the document.
--
-- Everything else is frozen, including created_at, which nothing updates.
--
-- DEFECT 2 — collects was digested but had nowhere to live.
--
-- ChargeIntent.collects is inside computeDigest (chargeintent.go:615) and
-- carries the receivable link CollectRemainderOf creates. There has never
-- been a column for it, no field on intent.Stored, and no restore in
-- Rehydrate. So any receivable written by SaveIntent and read back by
-- LoadIntent recomputes a digest taken over an EMPTY collects and fails
-- ErrDigestMismatch — the intent does not load, permanently.
--
-- It is latent: CollectRemainderOf has no non-test caller today, and
-- receivable_integration_test.go calls SaveIntent eight times and LoadIntent
-- never. Latent is not fixed. It is also the pattern the next sealed field
-- would be copied from, which is how one omission becomes four.
--
-- NULL, not NOT NULL: an intent that collects nothing states nothing, the
-- same shape supersedes_digest already uses. The self-reference is the same
-- too — a receivable names an intent in this table.
--
-- Production measured on 2026-08-31: charge_intents = 0 rows in every
-- environment. Nothing to backfill and no digest to invalidate.

ALTER TABLE ms_billing.charge_intents
    ADD COLUMN IF NOT EXISTS collects_digest TEXT NULL;

-- The foreign key is added SEPARATELY, and deliberately.
--
-- `ADD COLUMN IF NOT EXISTS ... REFERENCES` skips the WHOLE action when the
-- column already exists — the REFERENCES clause included — with only a NOTICE,
-- and the migration still reports success. A database where the column was
-- created by any other route would then carry it with no foreign key at all
-- and nothing would say so. Measured on PG 17.10.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'charge_intents_collects_digest_fkey'
    ) THEN
        ALTER TABLE ms_billing.charge_intents
            ADD CONSTRAINT charge_intents_collects_digest_fkey
            FOREIGN KEY (collects_digest) REFERENCES ms_billing.charge_intents(digest);
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION ms_billing.charge_intents_reject_sealed_update()
RETURNS TRIGGER AS $$
BEGIN
    -- Everything except the lifecycle. Listing what MAY change, rather than
    -- what may not, is the whole point: the set that must not change is the
    -- one that grows, and it now grows by itself.
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'charge_intents is sealed: a sealed intent is not deleted, it is superseded or corrected (INV-003)';
    END IF;
    IF (to_jsonb(NEW) - 'state' - 'state_changed_at' - 'reserved_micros')
       IS DISTINCT FROM
       (to_jsonb(OLD) - 'state' - 'state_changed_at' - 'reserved_micros')
    THEN
        RAISE EXCEPTION
            'charge_intents is sealed: supersede it instead of editing (INV-003)';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS charge_intents_sealed ON ms_billing.charge_intents;
CREATE TRIGGER charge_intents_sealed
    BEFORE UPDATE OR DELETE ON ms_billing.charge_intents
    FOR EACH ROW EXECUTE FUNCTION ms_billing.charge_intents_reject_sealed_update();

-- 🔴 TWO LIMITS OF ANY TRIGGER, MEASURED AND RECORDED RATHER THAN IMPLIED.
--
-- 1. `SET session_replication_role = replica` disables the trigger for the
--    session, and every write then lands unchecked. It is not grantable to
--    billing_svc or billing_ro, so it takes the owner/superuser role that
--    runs migrations — but "the seal holds against the migration role" is
--    false and should not be claimed. What holds against that role is
--    intent.Rehydrate, which recomputes the digest on every read and refuses
--    a row that no longer produces it.
--
-- 2. jsonb compares numbers by VALUE, so a scale-only change to a `numeric`
--    column (1000 -> 1000.0000) is invisible to this trigger. Every column
--    here is BIGINT or TEXT or TIMESTAMPTZ today, so nothing is exposed; a
--    future NUMERIC column would be, and this is the note that says so.
