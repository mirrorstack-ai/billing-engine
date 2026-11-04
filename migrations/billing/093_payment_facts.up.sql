-- Migration 093 — ms_billing.payment_facts, the rail-neutral payment-fact
-- intake (billing-engine#313, core-v2#1850).
--
-- Invoicing is a platform service, separate from every payment rail (D530).
-- Stripe, NewebPay, TapPay and manual bank transfer must each write ONE
-- neutral record for a settled payment, so the invoice layer (billing-engine
-- #286/#287) can issue off of something every rail produces, instead of
-- knowing which rail paid. This table IS that record. Nothing here calls a
-- vendor or exposes a route — see internal/shared/paymentfact for the writer
-- contract every rail's settlement step adopts, and docs/DESIGN.md's payment
-- fact note for how this differs from evidence.KindSettlement (an intent-rail
-- record, internal/intent/evidence) and a credit_ledger row (a wallet entry).
--
-- IDENTITY: (rail, rail_ref, mode) is UNIQUE. rail_ref is the rail's own
-- stable reference — a Stripe invoice/charge id, a NewebPay
-- MerchantID+MerchantOrderNo+TradeNo composite, a TapPay rec_trade_id, or a
-- bank-transfer statement id — never a billing-engine-minted id, because the
-- point is to be idempotent against a notice that arrives more than once
-- (NewebPay's notice has no event id at all; see billing-engine#296). A
-- replayed notice inserts nothing new; it finds the row the first delivery
-- wrote.
--
-- APPEND-ONLY, WITH ONE EXCEPTION: invoice_decision and decision_reason move
-- once, when the invoice worker (#287) reaches a verdict. Every other column
-- is sealed at insert — the trigger below enforces exactly that split, not a
-- blanket freeze.
--
-- seller_id and buyer_snapshot are forward references. Seller resolution
-- (#312) and the buyer invoice-profile snapshot (#285) are separate, not yet
-- merged pieces of this same tracker (core-v2#1850); both columns are
-- nullable today and NEITHER is backed by a foreign key yet, because neither
-- parent table exists in this tree. A fact written before #312/#285 land
-- carries NULL rather than a guess.
--
-- mode is copied from the payment at the instant this row is written and
-- never changes (billing-engine#297 is what will make every writer actually
-- set it to 'sandbox' for a test org; until then every caller in this tree
-- passes 'live', which is written here rather than silently defaulted so the
-- gap stays visible in every row instead of only in a comment).

CREATE TABLE IF NOT EXISTS ms_billing.payment_facts (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- The rail that settled this payment. Extensible: a new rail is a new
    -- value in this CHECK, not a new table.
    rail                 TEXT NOT NULL
                         CHECK (rail IN ('stripe', 'newebpay', 'tappay', 'bank_transfer')),

    -- The rail's own stable reference for the settled payment. See the header
    -- for what each rail supplies. Opaque to this table.
    rail_ref             TEXT NOT NULL CHECK (rail_ref <> ''),

    -- Copied from the payment at write time, immutable. D524: a sandbox
    -- payment must never produce a production invoice call, and this is the
    -- column the invoice worker gates that on.
    mode                 TEXT NOT NULL CHECK (mode IN ('sandbox', 'live')),

    -- What wrote this row. Not the rail (rail already says that) — this says
    -- WHICH step on that rail observed the payment: the engine's own settle
    -- step, a rail's async notice, an operator's manual paid-mark, or a
    -- finalized statement standing in for payment not yet received.
    source_kind          TEXT NOT NULL
                         CHECK (source_kind IN (
                             'engine_settlement', 'rail_notice',
                             'manual_paid_mark', 'statement'
                         )),

    -- The account that paid. A real FK: unlike seller_id/buyer_snapshot
    -- below, ms_billing.accounts exists today and every writer in this tree
    -- resolves one before it can settle anything.
    payer_account_id     UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,

    -- The issuing seller (billing-engine#312: MirrorStack-TW, a distributor,
    -- or later a customer app — D533). NULL while unresolved or while #312
    -- has not merged; no FK yet because the sellers table does not exist in
    -- this tree.
    seller_id            UUID NULL,

    currency             TEXT NOT NULL CHECK (currency <> ''),

    -- Integers, matching this schema's micro-dollar convention everywhere
    -- else (credit_ledger.amount_micros, charge_intents.total_micros). For a
    -- TWD invoice these are whole-NT$ values expressed in the same unit as
    -- every other money column here, not a second currency-specific scale.
    net_amount_micros    BIGINT NOT NULL CHECK (net_amount_micros >= 0),
    tax_amount_micros    BIGINT NOT NULL CHECK (tax_amount_micros >= 0),
    gross_amount_micros  BIGINT NOT NULL CHECK (gross_amount_micros >= 0),

    paid_at              TIMESTAMPTZ NOT NULL,

    -- The buyer invoice profile at payment time (billing-engine#285). NULL
    -- until that issue lands or until a buyer has not completed one.
    buyer_snapshot       JSONB NULL,

    -- The invoice worker's verdict (#287). 'pending' until it runs; this is
    -- the only state transition this table permits after insert.
    invoice_decision     TEXT NOT NULL DEFAULT 'pending'
                         CHECK (invoice_decision IN ('pending', 'not_applicable', 'deferred')),
    -- Present iff the decision is no longer 'pending' — a decided fact
    -- states why, and an undecided one states nothing yet.
    decision_reason      TEXT NULL
                         CHECK ((invoice_decision = 'pending') = (decision_reason IS NULL)),

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT payment_facts_gross_is_net_plus_tax
        CHECK (gross_amount_micros = net_amount_micros + tax_amount_micros)
);

-- The idempotency key the whole table exists to provide.
CREATE UNIQUE INDEX IF NOT EXISTS payment_facts_identity_uidx
    ON ms_billing.payment_facts (rail, rail_ref, mode);

CREATE INDEX IF NOT EXISTS payment_facts_payer_account_idx
    ON ms_billing.payment_facts (payer_account_id);

CREATE INDEX IF NOT EXISTS payment_facts_seller_idx
    ON ms_billing.payment_facts (seller_id) WHERE seller_id IS NOT NULL;

-- The invoice worker's (#287) work list: every fact still awaiting a
-- decision. Partial so the index stays small once most facts are decided.
CREATE INDEX IF NOT EXISTS payment_facts_pending_idx
    ON ms_billing.payment_facts (created_at) WHERE invoice_decision = 'pending';

-- Append-only except the decision pair. A column-by-column comparison rather
-- than a blanket refusal (071, 087) because this table, uniquely among this
-- schema's sealed tables, has one legitimate post-insert writer.
CREATE OR REPLACE FUNCTION ms_billing.payment_facts_reject_mutation()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'ms_billing.payment_facts is append-only: a payment fact is never deleted';
    END IF;

    IF NEW.id               IS DISTINCT FROM OLD.id
    OR NEW.rail              IS DISTINCT FROM OLD.rail
    OR NEW.rail_ref          IS DISTINCT FROM OLD.rail_ref
    OR NEW.mode              IS DISTINCT FROM OLD.mode
    OR NEW.source_kind       IS DISTINCT FROM OLD.source_kind
    OR NEW.payer_account_id  IS DISTINCT FROM OLD.payer_account_id
    OR NEW.seller_id         IS DISTINCT FROM OLD.seller_id
    OR NEW.currency          IS DISTINCT FROM OLD.currency
    OR NEW.net_amount_micros   IS DISTINCT FROM OLD.net_amount_micros
    OR NEW.tax_amount_micros   IS DISTINCT FROM OLD.tax_amount_micros
    OR NEW.gross_amount_micros IS DISTINCT FROM OLD.gross_amount_micros
    OR NEW.paid_at           IS DISTINCT FROM OLD.paid_at
    OR NEW.buyer_snapshot    IS DISTINCT FROM OLD.buyer_snapshot
    OR NEW.created_at        IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION
            'ms_billing.payment_facts is append-only except invoice_decision/decision_reason: refusing to change a sealed column of fact %', OLD.id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS payment_facts_append_only ON ms_billing.payment_facts;
CREATE TRIGGER payment_facts_append_only
    BEFORE UPDATE OR DELETE ON ms_billing.payment_facts
    FOR EACH ROW EXECUTE FUNCTION ms_billing.payment_facts_reject_mutation();

COMMENT ON TABLE ms_billing.payment_facts IS
    'One rail-neutral record per settled payment, written in the same transaction as the step that made the money real. Unique on (rail, rail_ref, mode). Append-only except invoice_decision/decision_reason, which the invoice worker (#287) sets once.';

-- Grants, the 071/087 convention: billing_svc gets SELECT/INSERT/UPDATE (the
-- UPDATE is real here, unlike 071/087's sealed tables — it is how the invoice
-- worker moves the decision pair) and loses DELETE; billing_ro gets SELECT.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_svc') THEN
        RAISE NOTICE 'migration 093: creating role billing_svc (db-bootstrap has not run here yet); it grants rds_iam separately and idempotently';
        CREATE ROLE billing_svc LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
        RAISE NOTICE 'migration 093: creating role billing_ro (db-bootstrap has not run here yet)';
        CREATE ROLE billing_ro LOGIN;
    END IF;
    EXECUTE 'GRANT SELECT, INSERT, UPDATE ON ms_billing.payment_facts TO billing_svc';
    EXECUTE 'REVOKE DELETE ON ms_billing.payment_facts FROM billing_svc';
    EXECUTE 'GRANT SELECT ON ms_billing.payment_facts TO billing_ro';
END
$$;

-- Prove the grants landed (058 was recorded applied having granted nothing).
DO $$
BEGIN
    IF NOT has_table_privilege('billing_svc', 'ms_billing.payment_facts', 'INSERT')
        OR NOT has_table_privilege('billing_svc', 'ms_billing.payment_facts', 'SELECT')
        OR NOT has_table_privilege('billing_svc', 'ms_billing.payment_facts', 'UPDATE') THEN
        RAISE EXCEPTION 'migration 093: billing_svc cannot SELECT/INSERT/UPDATE ms_billing.payment_facts after the grant ran';
    END IF;
    IF has_table_privilege('billing_svc', 'ms_billing.payment_facts', 'DELETE') THEN
        RAISE EXCEPTION 'migration 093: billing_svc still holds DELETE on ms_billing.payment_facts after the revoke ran; a payment fact is never deleted';
    END IF;
    IF NOT has_table_privilege('billing_ro', 'ms_billing.payment_facts', 'SELECT') THEN
        RAISE EXCEPTION 'migration 093: billing_ro cannot SELECT ms_billing.payment_facts after the grant ran';
    END IF;
    IF has_table_privilege('billing_ro', 'ms_billing.payment_facts', 'INSERT')
        OR has_table_privilege('billing_ro', 'ms_billing.payment_facts', 'UPDATE')
        OR has_table_privilege('billing_ro', 'ms_billing.payment_facts', 'DELETE') THEN
        RAISE EXCEPTION 'migration 093: billing_ro holds a write privilege on ms_billing.payment_facts; it is not a read-only role';
    END IF;
END
$$;
