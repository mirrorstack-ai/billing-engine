-- Migration 060: the sealed tax determination says HOW it was established.
--
-- 🔴 Before this column, a determination the engine recomputed and one a
-- vendor merely asserted were byte-identical once sealed. The execution
-- predicate has a clause for tax reproducibility, but the sealed document
-- carried no field naming the class, so the clause could not tell them apart
-- and neither could a customer re-deriving the digest offline.
--
-- The column is part of the canonical encoding (charge-intent/v2), so it is
-- inside the digest rather than beside it. A validated-but-undigested field
-- is one a customer cannot verify and an attacker can change without
-- breaking the seal.
--
-- NOT NULL with no DEFAULT is deliberate. A default would let a row that
-- never stated a class read as though it had, which is the exact substitution
-- the column exists to prevent — and INV-004's shape: an unstated input must
-- not silently acquire a value. charge_intents is EMPTY in every environment
-- (production measured 0 on 2026-08-31), so there is no backfill to write and
-- no existing row this can be wrong about.
--
-- ⚠️ If charge_intents is ever non-empty when this runs, it will fail loudly
-- rather than invent a provenance for rows that predate the field. That is
-- the correct failure: those rows genuinely do not say, and guessing on their
-- behalf is what this whole column forbids.

ALTER TABLE ms_billing.charge_intents
    ADD COLUMN tax_verification TEXT NOT NULL;

-- The closed set, mirroring intent.SealableTaxVerificationClasses(). A CHECK
-- rather than an enum type: the Go side is the authority on the vocabulary,
-- and a CHECK keeps the two in sync with one grep instead of a type the
-- migration would have to ALTER separately.
--
-- The empty string is absent on purpose. It is the Go zero value, Seal
-- refuses it, and it must not be storable either — otherwise the database
-- would accept a document the engine would not have produced.
ALTER TABLE ms_billing.charge_intents
    ADD CONSTRAINT charge_intents_tax_verification_known
    CHECK (tax_verification IN ('independently_reproducible', 'provider_attested', 'not_applicable'));
