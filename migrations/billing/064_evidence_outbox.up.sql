-- Migration 064: the billing-owned transactional evidence outbox (INV-014).
--
-- docs/DESIGN.md:386-399: "A signed, customer-encrypted evidence record must
-- commit through a billing-owned transactional outbox. The list of events is
-- closed: a sealed intent, a proof result, a notice or eligibility result, a
-- refusal, a nonterminal attempt state, a settlement, a revocation and a
-- correction. ... The outbox is worth building first anyway: it makes your
-- evidence a durable side effect of the money moving, rather than a report the
-- relay chooses to render."
--
-- The last clause is the whole design. A record is written INSIDE the
-- transaction that changes the billing state it attests, so there is no window
-- in which money moved and evidence did not, and no separate reporter that can
-- decline to run.
--
-- The kind CHECK is that closed list, all eight. Four have a writer in this
-- commit; four do not, and internal/architecture measures which rather than
-- taking this comment's word for it:
--
--   sealed_intent              ✅ internal/intent/proposer, in the same
--                                 transaction as SaveIntent
--   refusal                    ✅ internal/intent/executor, on a refused predicate
--   nonterminal_attempt_state  ✅ internal/intent/executor, in-progress and
--                                 unresolved branches
--   settlement                 ✅ internal/intent/executor, same transaction as
--                                 RecordOutcome
--   proof_result               ❌ CustomerProofStream is unbuilt (INV-013)
--   notice_eligibility_result  ❌ nothing delivers a notice yet
--   revocation                 ❌ no revocation path exists
--   correction                 ❌ no correction path exists
--
-- The four without writers are in the CHECK because the list is the design's
-- and adding a member later is a schema change to a table that by then holds
-- customer evidence. That is the opposite trade to migration 062's omitted
-- merchant-account policy column: an unfillable COLUMN is a fiction every row
-- has to keep telling, while an unused enum member costs one line and is the
-- published vocabulary a verifier reads.
--
-- 🔴 WHAT THIS TABLE DOES NOT DO
--
-- INV-014 asks for records that are signed AND customer-encrypted. These are
-- SIGNED and NOT ENCRYPTED, because encryption needs a customer key and
-- CustomerReadProof binds "an independently enrolled customer factor that does
-- not exist today" — §12 decision 16, answered as option C (build the
-- independence that needs no enrolled factor). So this is the half of INV-014
-- that is reachable, and internal/account/capabilities reports the other half
-- as unsupported rather than letting the table imply it.
--
-- There is no plaintext payload column, deliberately. Every fact a record
-- attests is a column, so the payload is RECONSTRUCTABLE and its digest
-- checkable, and nothing customer-written is duplicated here. Migration
-- 055:176-178 set the precedent — "deliberately omit metadata so malformed or
-- sensitive diagnostic payloads are never retained" — and it matters more
-- here, because 058/059's ALTER DEFAULT PRIVILEGES would otherwise hand every
-- future row to the billing_ro role.
--
-- checkpoint is monotonic and NOT gap-free. A rolled-back transaction consumes
-- a sequence value, so a reader can order records and spot a stale view but
-- CANNOT conclude that nothing was withheld. The gap-free per-payer stream
-- INV-013 asks for is CustomerProofStream, a separate unbuilt object whose
-- sequence is assigned under the claim lock. Nothing here provides it.

CREATE TABLE IF NOT EXISTS ms_billing.evidence_records (
    -- The outbox checkpoint. Monotonic, gappy; see the header.
    checkpoint      BIGSERIAL PRIMARY KEY,

    kind            TEXT NOT NULL CHECK (kind IN (
                        'sealed_intent',
                        'proof_result',
                        'notice_eligibility_result',
                        'refusal',
                        'nonterminal_attempt_state',
                        'settlement',
                        'revocation',
                        'correction'
                    )),

    -- Who the record is about. Same vocabulary as charge_intents.payer_kind:
    -- a record whose payer cannot be resolved is one no CustomerReadProof
    -- could ever scope.
    subject_kind    TEXT NOT NULL CHECK (subject_kind IN ('user', 'org', 'app')),
    subject_id      TEXT NOT NULL CHECK (subject_id <> ''),

    -- The intent the record concerns. NULL for kinds that are not about one
    -- (a revocation names an authorization); the FK means a record cannot
    -- name an intent that does not exist.
    intent_digest   TEXT NULL REFERENCES ms_billing.charge_intents(digest),

    -- Detail is the event's own closed-vocabulary content: refused clause
    -- names, an outcome, an attempt state. Enum-shaped, never customer prose,
    -- and inside payload_digest.
    detail          TEXT NOT NULL CHECK (detail <> ''),

    occurred_at     TIMESTAMPTZ NOT NULL,

    -- The identity of the signed payload. Exactly 32 bytes, as in migration
    -- 055's payload_fingerprint, so the column cannot quietly hold something
    -- that is not a digest.
    payload_digest  BYTEA NOT NULL CHECK (octet_length(payload_digest) = 32),

    -- The signed statement. key_id selects among keys a verifier has PINNED
    -- (docs/VERIFICATION.md:81 — the root must not come from the response),
    -- and the signature covers the payload digest together with the domain,
    -- issuer, audience, environment, schema, checkpoint and validity interval.
    signature         TEXT NOT NULL CHECK (signature <> ''),
    key_id            TEXT NOT NULL CHECK (key_id <> ''),
    signed_not_before TIMESTAMPTZ NOT NULL,
    signed_not_after  TIMESTAMPTZ NOT NULL,
    CONSTRAINT evidence_records_validity_ordered
        CHECK (signed_not_after >= signed_not_before),

    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Idempotency. A retried Execute must not append a second refusal for the
    -- same intent and the same reason: once a work loop exists the refusal
    -- path is the highest-frequency writer in the system, and "exactly one
    -- record per outcome" asserted by a test that runs each branch once is a
    -- coincidence, not a property.
    --
    -- payload_digest is in the key rather than detail, so uniqueness is over
    -- what was actually attested.
    CONSTRAINT evidence_records_one_per_outcome
        UNIQUE (kind, intent_digest, payload_digest)
);

CREATE INDEX IF NOT EXISTS evidence_records_subject_idx
    ON ms_billing.evidence_records (subject_kind, subject_id, checkpoint DESC);

CREATE INDEX IF NOT EXISTS evidence_records_intent_idx
    ON ms_billing.evidence_records (intent_digest)
    WHERE intent_digest IS NOT NULL;

-- Append-only, for UPDATE and DELETE alike.
--
-- An evidence record that can be edited is a report, not evidence — the same
-- argument INV-003 makes for the intent itself, and the DELETE half is the
-- gap migration 063 had to close on charge_intents. It is a blanket refusal
-- rather than a column comparison, because there is no column here anyone may
-- change.
CREATE OR REPLACE FUNCTION ms_billing.evidence_records_reject_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION
        'evidence_records is append-only: correct it with a correction record (INV-014)';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS evidence_records_append_only ON ms_billing.evidence_records;
CREATE TRIGGER evidence_records_append_only
    BEFORE UPDATE OR DELETE ON ms_billing.evidence_records
    FOR EACH ROW EXECUTE FUNCTION ms_billing.evidence_records_reject_mutation();

-- 🔴 REVOKE from the read-only ops role.
--
-- Migrations 058 and 059 run ALTER DEFAULT PRIVILEGES ... GRANT SELECT ON
-- TABLES TO billing_ro, so this table is readable by billing_ro the moment it
-- is created — including by cmd/intent-shadow, a Lambda that runs against
-- production. docs/DESIGN.md:392-394 is explicit that reads of these records
-- require a payer-bound CustomerReadProof and that "an api-platform identity
-- assertion, or possession of an object id, is not enough". A blanket SELECT
-- for an ops role is weaker than either.
--
-- Written as a REVOKE rather than by narrowing the default privilege, because
-- the default is correct for every other table in this schema and one
-- exception should read as an exception.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
        REVOKE ALL ON ms_billing.evidence_records FROM billing_ro;
    ELSE
        RAISE NOTICE 'billing_ro does not exist here; nothing to revoke';
    END IF;
END
$$;
