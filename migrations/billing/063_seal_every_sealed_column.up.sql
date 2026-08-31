-- Migration 063: freeze every sealed column, by default, and store the one
-- sealed link that was digested but never written down.
--
-- 🔴 TWO DEFECTS, both introduced by this repository's own supersession work.
--
-- DEFECT 1 — the seal covered 17 of 22 columns.
--
-- 054:97-121 froze ms_billing.charge_intents by comparing a HARDCODED
-- 17-column tuple, which was every column the table had that day. Migrations
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
    ADD COLUMN IF NOT EXISTS collects_digest TEXT NULL
        REFERENCES ms_billing.charge_intents(digest);

CREATE OR REPLACE FUNCTION ms_billing.charge_intents_reject_sealed_update()
RETURNS TRIGGER AS $$
BEGIN
    -- Everything except the lifecycle. Listing what MAY change, rather than
    -- what may not, is the whole point: the set that must not change is the
    -- one that grows, and it now grows by itself.
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
