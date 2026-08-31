-- Migration 062: the sealed rail and the routing policy it was chosen under.
--
-- docs/DESIGN.md:1281-1283: "Before an intent seals, the engine freezes the
-- total, the FundingPlan, the rail, the merchant-account policy and the
-- routing-policy digest. A later rail change requires a replacement intent,
-- with a new digest and a new eligibility decision."
--
-- 🔴 WHY IT MUST BE SEALED, from :1030-1037: "A private caller must not select
-- a weaker adapter to bypass notice, authentication, tax, ceilings or
-- reconciliation." An unsealed rail is exactly that bypass — swap the adapter
-- after the customer accepted and the digest still verifies. In the digest,
-- the swap breaks it.
--
-- The merchant-account policy that same sentence names is NOT added here.
-- Nothing defines or produces one yet, and a column no writer fills is a
-- fiction the schema would then have to keep telling.
--
-- NOT NULL with no DEFAULT, for migrations 060 and 061's reason: a default
-- would let a row that never stated a rail read as though it had.
-- charge_intents is empty in every environment (production measured 0 on
-- 2026-08-31), so there is no backfill.
--
-- No CHECK on the rail's VALUE. There is no closed rail vocabulary —
-- docs/DESIGN.md:1031 says "your accepted authorization names permitted
-- rails", so the permitted set is per-authorization rather than global. The
-- predicate enforces the real rule (the sealed rail must equal the accepted
-- one, predicate.ClauseRailSupportsPlan); a CHECK here could only enforce a
-- vocabulary that does not exist, which would be a guard for a rule nobody
-- wrote.

ALTER TABLE ms_billing.charge_intents
    ADD COLUMN selected_rail           TEXT NOT NULL,
    ADD COLUMN routing_policy_revision TEXT NOT NULL;

-- Empty strings are refused for the same reason Seal refuses an unstated tax
-- verification class: an unstated value must not be storable as though it had
-- been chosen.
ALTER TABLE ms_billing.charge_intents
    ADD CONSTRAINT charge_intents_rail_stated
    CHECK (selected_rail <> '' AND routing_policy_revision <> '');
