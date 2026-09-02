-- Migration 054 — the intent core.
--
-- docs/DESIGN.md §1 lists five facts today's code keeps confusing with one
-- another and gives each its own record. This creates the durable homes for
-- three of them: the sealed proposal (charge_intents), the customer's
-- permission (billing_authorizations), and the delivery evidence that gates
-- automatic collection (notice_receipts). Plus the claim that makes
-- INV-008 -- "one intent settles at most once, across all providers" -- a
-- property of the database rather than of the code that talks to it.
--
-- Nothing reads or writes these tables yet. They land before the executor so
-- that the executor is written against a schema rather than the other way
-- round.
--
-- Spec: docs/DESIGN.md sections 3 and 4.

-- ---------------------------------------------------------------------------
-- charge_intents — the sealed proposal
-- ---------------------------------------------------------------------------
--
-- The digest is the primary key. It is the identity of the exact document a
-- customer was shown, computed over a length-prefixed canonical encoding
-- (internal/intent/canonical.go), so two different intents cannot share one.
-- Using it as the key means a duplicate seal of identical content is the same
-- row rather than a second proposal.
CREATE TABLE IF NOT EXISTS ms_billing.charge_intents (
    digest              TEXT PRIMARY KEY,

    payer_kind          TEXT NOT NULL CHECK (payer_kind IN ('user', 'org', 'app')),
    payer_id            TEXT NOT NULL,
    currency            TEXT NOT NULL,

    -- What this charge is for, from the closed catalog in DESIGN §6. It
    -- is sealed into the intent rather than supplied at authorization
    -- time, because it selects which rule of a standing authorization
    -- applies: a caller that chose it could pick the permission its
    -- charge happens to fit.
    kind                TEXT NOT NULL,

    price_book_revision TEXT NOT NULL,
    terms_revision      TEXT NOT NULL,
    notice_policy       TEXT NOT NULL,

    -- A determination that came out at zero is not the same as one never
    -- made, so the jurisdiction and rule revision are NOT NULL: a row here
    -- always carries a determination. INV-004 forbids an unknown input
    -- becoming zero, and tax is where that is most tempting.
    tax_jurisdiction    TEXT NOT NULL,
    tax_rule_revision   TEXT NOT NULL,
    tax_amount_micros   BIGINT NOT NULL CHECK (tax_amount_micros >= 0),

    subtotal_micros     BIGINT NOT NULL CHECK (subtotal_micros >= 0),
    total_micros        BIGINT NOT NULL CHECK (total_micros >= 0),

    authorization_id    TEXT NOT NULL,

    -- Half-open window. A charge executed outside it is one settled long
    -- after the customer stopped expecting it.
    execute_not_before  TIMESTAMPTZ NOT NULL,
    execute_not_after   TIMESTAMPTZ NOT NULL,
    CONSTRAINT charge_intents_window_ordered
        CHECK (execute_not_after >= execute_not_before),

    -- INV-003: a one-unit change creates a NEW intent that supersedes the
    -- old one. Editing is not offered, so a correction is a row pointing at
    -- the row it replaces.
    supersedes_digest   TEXT NULL REFERENCES ms_billing.charge_intents(digest),

    state               TEXT NOT NULL DEFAULT 'proposed'
                        CHECK (state IN (
                            'proposed', 'notice_pending', 'disclosed', 'eligible',
                            'executing', 'provider_in_progress',
                            'succeeded', 'voided', 'canceled', 'expired'
                        )),

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    state_changed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON COLUMN ms_billing.charge_intents.digest IS
    'Identity of the exact sealed document, over the canonical encoding in '
    'internal/intent/canonical.go. Also what a disclosure is bound to and what '
    'an acceptance receipt references.';

CREATE INDEX IF NOT EXISTS charge_intents_payer_idx
    ON ms_billing.charge_intents (payer_kind, payer_id, created_at DESC);
CREATE INDEX IF NOT EXISTS charge_intents_state_idx
    ON ms_billing.charge_intents (state, execute_not_after)
    WHERE state IN ('eligible', 'executing', 'provider_in_progress');

-- INV-003 enforced by the database, not by convention.
--
-- Everything except the lifecycle columns is frozen at insert. A service bug,
-- a migration, or a hand-run UPDATE cannot change what a customer was shown
-- and then collect against the new version — which is the whole reason the
-- design says "superseding is cheap; editing is unanswerable".
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

CREATE TRIGGER charge_intents_sealed
    BEFORE UPDATE ON ms_billing.charge_intents
    FOR EACH ROW EXECUTE FUNCTION ms_billing.charge_intents_reject_sealed_update();

-- ---------------------------------------------------------------------------
-- charge_intent_lines — what the total is made of
-- ---------------------------------------------------------------------------
--
-- amount_micros is stored even though it is quantity x unit price, because a
-- customer rechecking a charge offline reads the line they were shown rather
-- than recomputing it. The CHECK is what keeps the two from disagreeing.
CREATE TABLE IF NOT EXISTS ms_billing.charge_intent_lines (
    intent_digest     TEXT NOT NULL
                      REFERENCES ms_billing.charge_intents(digest) ON DELETE CASCADE,
    line_index        INT NOT NULL CHECK (line_index >= 0),

    meter             TEXT NOT NULL,
    module            TEXT NOT NULL,
    module_version    TEXT NOT NULL,
    quantity          BIGINT NOT NULL CHECK (quantity >= 0),
    unit_price_micros BIGINT NOT NULL CHECK (unit_price_micros >= 0),
    amount_micros     BIGINT NOT NULL CHECK (amount_micros >= 0),

    CONSTRAINT charge_intent_lines_amount_is_derived
        CHECK (amount_micros = quantity * unit_price_micros),

    PRIMARY KEY (intent_digest, line_index)
);

-- ---------------------------------------------------------------------------
-- charge_intent_source_facts — what the total was derived from
-- ---------------------------------------------------------------------------
--
-- Lets a reader walk back from an amount to the usage that was reported,
-- which is what makes a charge answerable rather than merely correct.
CREATE TABLE IF NOT EXISTS ms_billing.charge_intent_source_facts (
    intent_digest   TEXT NOT NULL
                    REFERENCES ms_billing.charge_intents(digest) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL,
    PRIMARY KEY (intent_digest, idempotency_key)
);

-- ---------------------------------------------------------------------------
-- billing_authorizations — the customer's permission
-- ---------------------------------------------------------------------------
--
-- 🔴 INV-006 is a trust assumption, not a control: api-platform holds the
-- session and this engine treats the subject id as opaque, so an acceptance
-- that never happened can still be asserted. What this table is for is
-- REPRODUCIBILITY — acceptance_digest binds the permission to the
-- engine-signed disclosure the customer was shown, so a fabricated acceptance
-- is something that can be pointed at afterwards.
CREATE TABLE IF NOT EXISTS ms_billing.billing_authorizations (
    id                        TEXT PRIMARY KEY,

    scope                     TEXT NOT NULL CHECK (scope IN ('one-time', 'standing')),
    subject_kind              TEXT NOT NULL CHECK (subject_kind IN ('user', 'org', 'app')),
    subject_id                TEXT NOT NULL,
    currency                  TEXT NOT NULL,

    -- A one-time authorization names the exact document it permits. One that
    -- names none is a standing authorization with worse paperwork.
    intent_digest             TEXT NULL,
    CONSTRAINT billing_authorizations_one_time_names_its_intent
        CHECK (scope <> 'one-time' OR intent_digest IS NOT NULL),

    -- A standing authorization declares what it permits and how far.
    charge_kinds              TEXT[] NOT NULL DEFAULT '{}',
    per_charge_ceiling_micros BIGINT NOT NULL DEFAULT 0
                              CHECK (per_charge_ceiling_micros >= 0),
    period_ceiling_micros     BIGINT NOT NULL DEFAULT 0
                              CHECK (period_ceiling_micros >= 0),
    CONSTRAINT billing_authorizations_standing_is_bounded
        CHECK (scope <> 'standing'
               OR (array_length(charge_kinds, 1) > 0 AND per_charge_ceiling_micros > 0)),

    terms_revision            TEXT NOT NULL,
    price_book_revision       TEXT NOT NULL,
    notice_policy             TEXT NOT NULL,

    effective_from            TIMESTAMPTZ NOT NULL,
    expires_at                TIMESTAMPTZ NOT NULL,
    CONSTRAINT billing_authorizations_window_ordered
        CHECK (expires_at >= effective_from),

    acceptance_digest         TEXT NOT NULL,
    revoked_at                TIMESTAMPTZ NULL,

    created_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS billing_authorizations_subject_idx
    ON ms_billing.billing_authorizations (subject_kind, subject_id, effective_from DESC);

-- ---------------------------------------------------------------------------
-- notice_receipts — delivery, not sending
-- ---------------------------------------------------------------------------
--
-- INV-005: automatic collection requires durable evidence that the sealed
-- intent was delivered BYTE-FOR-BYTE under its notice policy. delivered_digest
-- must equal the intent's own digest, which is what "the bytes you were sent
-- are the bytes that will be collected against" means in a schema.
--
-- terminal_status records what the carrier reported. Queue acceptance is not
-- delivery, so 'queued' and 'sent' are not in the allowed set: handing a
-- message to a queue proves only that we tried.
CREATE TABLE IF NOT EXISTS ms_billing.notice_receipts (
    intent_digest          TEXT PRIMARY KEY
                           REFERENCES ms_billing.charge_intents(digest) ON DELETE CASCADE,
    delivered_digest       TEXT NOT NULL,
    CONSTRAINT notice_receipts_delivered_what_will_be_charged
        CHECK (delivered_digest = intent_digest),

    policy                 TEXT NOT NULL,
    terminal_status        TEXT NOT NULL
                           CHECK (terminal_status IN ('delivered', 'relayed')),

    -- The wait runs from DELIVERY, not from sealing.
    eligibility_not_before TIMESTAMPTZ NOT NULL,

    -- A notice the customer cannot act on is a disclosure, not a control.
    revocation_path_fresh  BOOLEAN NOT NULL,

    recorded_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- intent_settlement_claims — INV-008, enforced by the database
-- ---------------------------------------------------------------------------
--
-- "One intent settles at most once, across all providers."
--
-- The intent digest is the PRIMARY KEY, so a second settlement is not a race
-- the code has to win — it is an integrity violation the database refuses. A
-- unique index is a control in a way a code path is not: it holds against a
-- concurrent executor, a replayed message, a second deployment, and a hand-run
-- statement alike.
CREATE TABLE IF NOT EXISTS ms_billing.intent_settlement_claims (
    intent_digest    TEXT PRIMARY KEY
                     REFERENCES ms_billing.charge_intents(digest),

    -- Who holds it, so a stuck claim can be attributed.
    claimed_by       TEXT NOT NULL,
    claimed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Set when the attempt reaches a terminal answer. A claim with no
    -- outcome is in flight; docs/DESIGN.md §4 is explicit that missing or
    -- ambiguous evidence RETAINS the claim rather than releasing it, because
    -- releasing it is what lets a second attempt begin.
    outcome          TEXT NULL
                     CHECK (outcome IN ('succeeded', 'voided', 'failed')),
    outcome_at       TIMESTAMPTZ NULL,
    CONSTRAINT intent_settlement_claims_outcome_is_timed
        CHECK ((outcome IS NULL) = (outcome_at IS NULL))
);

COMMENT ON TABLE ms_billing.intent_settlement_claims IS
    'INV-008 as a primary key: a second settlement of one intent is an '
    'integrity violation rather than a race the code must win.';
