-- Migration 065: the engine-issued acceptance a standing authorization rests on.
--
-- §12 item 16 option C, piece 2 — the runtime half. The construction half
-- landed with AuthorizationDisclosure: Authorize now derives the digest of the
-- document a grant constitutes and refuses a grant whose acceptance names
-- anything else.
--
-- That check is enforced by the CONSTRUCTOR, and internal/intent/store's
-- LoadAuthorization re-runs Authorize on every read — so a predicate clause
-- re-checking it would be verifying what the constructor just guaranteed,
-- which is the "clause named for a check it does not perform" defect this
-- repository keeps finding. The predicate needs a fact the constructor CANNOT
-- know, and that is what this table holds:
--
--   * the engine ISSUED this challenge (it is a row here, not a string a
--     caller invented);
--   * the customer ANSWERED it (accepted_at is set);
--   * it has not EXPIRED;
--   * it has not been REVOKED.
--
-- 🔴 What this still does not do. INV-006: "the engine cannot tell a relayed
-- acceptance from an invented one." api-platform relays, and nothing here
-- changes that. What it removes is the ability to authorise recurring,
-- automatic collection with a string nobody issued — which is what
-- AcceptanceDigest was until this wave: predicate.authorityEvidenceBinds
-- returned true for any non-empty value, so one character was sufficient
-- evidence for a standing charge.
--
-- The nonce, audience and replay identity mirror predicate.AcceptanceReceipt's
-- customer-present fields, because the two gates are answering the same
-- question about different documents and a verifier should not have to learn
-- two shapes.
--
-- billing_authorizations measured 0 rows in production on 2026-08-31, so there
-- is nothing to backfill and no authorization is invalidated by requiring one.

CREATE TABLE IF NOT EXISTS ms_billing.authorization_acceptances (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- The authorization this challenge was issued for. It is issued BEFORE
    -- the authorization exists (the customer accepts, then it is minted), so
    -- this is deliberately NOT a foreign key: a challenge that was never
    -- answered has no authorization to point at, and dropping it would lose
    -- the record that the terms were shown at all.
    authorization_id   TEXT NOT NULL CHECK (authorization_id <> ''),

    -- The document. It must equal intent.DisclosureDigestFor(grant) for the
    -- authorization that names this challenge, which is what ties the two
    -- halves together.
    disclosure_digest  TEXT NOT NULL CHECK (disclosure_digest <> ''),

    -- Who was shown it. Same vocabulary as charge_intents.payer_kind: an
    -- acceptance whose payer cannot be resolved is one no charge can rest on.
    payer_kind         TEXT NOT NULL CHECK (payer_kind IN ('user', 'org', 'app')),
    payer_id           TEXT NOT NULL CHECK (payer_id <> ''),

    -- Freshness and replay, mirroring predicate.AcceptanceReceipt.
    nonce              TEXT NOT NULL CHECK (nonce <> ''),
    audience           TEXT NOT NULL CHECK (audience <> ''),
    replay_identity    TEXT NOT NULL CHECK (replay_identity <> ''),

    issued_at          TIMESTAMPTZ NOT NULL,
    expires_at         TIMESTAMPTZ NOT NULL,
    CONSTRAINT authorization_acceptances_window_ordered
        CHECK (expires_at > issued_at),

    -- NULL until the customer answers. An unanswered challenge is a document
    -- that was shown and not accepted, which is a real and different state
    -- from one that was never issued — and neither authorises anything.
    accepted_at        TIMESTAMPTZ NULL,

    -- Set when the customer withdraws. Separate from the authorization's own
    -- revoked_at: withdrawing consent to the DOCUMENT is not the same act as
    -- revoking the authorization, and either alone must stop collection.
    revoked_at         TIMESTAMPTZ NULL,

    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One live challenge per authorization per document. A second issue for
    -- the same terms is the same challenge; re-issuing under DIFFERENT terms
    -- produces a different digest and therefore a different row, which is
    -- exactly the distinction that matters.
    CONSTRAINT authorization_acceptances_one_per_document
        UNIQUE (authorization_id, disclosure_digest)
);

CREATE INDEX IF NOT EXISTS authorization_acceptances_authorization_idx
    ON ms_billing.authorization_acceptances (authorization_id)
    WHERE revoked_at IS NULL;

-- An answered challenge is evidence, and evidence is not edited.
--
-- accepted_at and revoked_at are the two things that legitimately arrive
-- later, and each may only be set ONCE and never cleared: an acceptance that
-- can be un-accepted, or a revocation that can be withdrawn, is a record
-- nobody can rely on. Everything else is frozen, by the same
-- whole-row-minus-a-named-set construction migration 063 uses on
-- charge_intents, so a column added later is frozen the day it exists.
CREATE OR REPLACE FUNCTION ms_billing.authorization_acceptances_reject_edit()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'authorization_acceptances is append-only: revoke the acceptance instead of deleting it';
    END IF;
    IF (to_jsonb(NEW) - 'accepted_at' - 'revoked_at')
       IS DISTINCT FROM
       (to_jsonb(OLD) - 'accepted_at' - 'revoked_at')
    THEN
        RAISE EXCEPTION
            'an issued acceptance is sealed: issue a new challenge instead of editing this one';
    END IF;
    IF OLD.accepted_at IS NOT NULL AND NEW.accepted_at IS DISTINCT FROM OLD.accepted_at THEN
        RAISE EXCEPTION 'an acceptance cannot be un-accepted or re-accepted';
    END IF;
    IF OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS DISTINCT FROM OLD.revoked_at THEN
        RAISE EXCEPTION 'a revocation cannot be withdrawn';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS authorization_acceptances_sealed ON ms_billing.authorization_acceptances;
CREATE TRIGGER authorization_acceptances_sealed
    BEFORE UPDATE OR DELETE ON ms_billing.authorization_acceptances
    FOR EACH ROW EXECUTE FUNCTION ms_billing.authorization_acceptances_reject_edit();
