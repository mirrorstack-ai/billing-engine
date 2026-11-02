-- Migration 093 — per-payer plan-fee price basis (billing-engine#258,
-- mirrorstack-core-v2#1589, billing plan key B6).
--
-- WHY: the first distributor buys plan fees and seats at 60% of list; usage
-- stays full price. That price exists only in contract text today — the
-- engine adds the LIST plan fee at every plan-fee line regardless of payer.
-- ms_billing.org_distributors (053) is attribution only ("who distributes
-- whom"), not "who pays what", and deliberately stays that way (053's own
-- header). This table is the missing "what" — a per-payer multiplier the
-- PlanFeeMicros function (usage package) applies to a list plan-fee / seat /
-- add-on amount. It is NEVER applied to usage (module/infra/AI metering).
--
-- 🔴 NO SEED ROW HERE. The 40% discount is a single first-customer contract
-- term (D8-wholesale-60), not a standard reseller price, and this migration
-- must not hardcode it, a default, an env value, or that customer's org id —
-- none of which a worker in a GitHub-only VM has authority to set. An
-- operator sets the first row after merge, through the SetPayerPriceBasis RPC
-- (cmd/account-api/main.go, internal-secret-gated). No row for a payer means
-- price_basis_bp = 10000 (list) — every existing amount is unchanged until an
-- operator writes a row.
--
-- KEYED BY OWNER, not by ms_billing.accounts.id — copying the EXACT
-- exactly-one-of shape accounts uses (001_init, owner_kind + owner_user_id
-- XOR owner_org_id). Keying on the owner (not the account row) lets an
-- operator price a payer BEFORE it card-activates and gets an account row —
-- the distributor's first customer is known well before that. A charge leg
-- reaches this table through its account's accounts.owner_kind /
-- owner_user_id / owner_org_id (see PriceBasisBPForAccount).
CREATE TABLE IF NOT EXISTS ms_billing.payer_price_basis (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    owner_kind      TEXT NOT NULL CHECK (owner_kind IN ('user', 'org')),
    owner_user_id   UUID NULL,    -- soft FK to ms_account.users.id
    owner_org_id    UUID NULL,    -- soft FK to ms_account.orgs.id

    -- Basis points off list: 10000 = list (100%), 1 = the floor (0.01% of
    -- list). Never above list — this function only ever discounts.
    price_basis_bp  INTEGER NOT NULL CHECK (price_basis_bp BETWEEN 1 AND 10000),

    -- Why this basis exists on this payer (e.g. "D8-wholesale-60: first
    -- distributor contract, 60% of list on plan fees and seats"). Free text,
    -- not a reason code — this is a hand-operated row, not a high-volume one.
    note            TEXT NOT NULL DEFAULT '',

    -- Operator identity (e.g. an email or admin user id), never a service
    -- principal — this RPC has no non-operator caller.
    set_by          TEXT NOT NULL,
    set_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT payer_price_basis_owner_check CHECK (
        (owner_kind = 'user' AND owner_user_id IS NOT NULL AND owner_org_id IS NULL)
        OR
        (owner_kind = 'org'  AND owner_org_id  IS NOT NULL AND owner_user_id IS NULL)
    )
);

-- Partial unique indexes, not a composite UNIQUE(owner_user_id, owner_org_id):
-- exactly one of the two columns is ever non-NULL on a row (the CHECK above),
-- and SQL UNIQUE treats NULLs as distinct, so a composite constraint would
-- never actually stop two rows naming the same user (both would carry
-- owner_org_id = NULL, which a UNIQUE constraint does not consider a clash).
CREATE UNIQUE INDEX IF NOT EXISTS payer_price_basis_user_idx
    ON ms_billing.payer_price_basis (owner_user_id) WHERE owner_user_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS payer_price_basis_org_idx
    ON ms_billing.payer_price_basis (owner_org_id) WHERE owner_org_id IS NOT NULL;

COMMENT ON TABLE ms_billing.payer_price_basis IS
    'Per-payer price basis (basis points off list, 10000 = list) applied to '
    'plan-fee / seat / add-on amounts only, never to usage. No row = 10000. '
    'Keyed by owner (user XOR org), same shape as accounts, so a basis can be '
    'set before the payer has an account row.';

-- Immutable audit trail for every SetPayerPriceBasis / ClearPayerPriceBasis
-- call (who, reason, old, new) — a hard requirement of the operator RPC, not
-- an afterthought: this table has no console UI and no public route, so this
-- log is the only record of who changed a customer's price and why.
CREATE TABLE IF NOT EXISTS ms_billing.payer_price_basis_audit (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_kind          TEXT NOT NULL CHECK (owner_kind IN ('user', 'org')),
    owner_user_id       UUID NULL,
    owner_org_id        UUID NULL,
    -- NULL = no row existed before this change (the payer was at the 10000
    -- default); NULL on new = the row was cleared (reverted to 10000).
    old_price_basis_bp  INTEGER NULL,
    new_price_basis_bp  INTEGER NULL,
    reason              TEXT NOT NULL,
    set_by              TEXT NOT NULL,
    set_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS payer_price_basis_audit_user_idx
    ON ms_billing.payer_price_basis_audit (owner_user_id, set_at);
CREATE INDEX IF NOT EXISTS payer_price_basis_audit_org_idx
    ON ms_billing.payer_price_basis_audit (owner_org_id, set_at);

COMMENT ON TABLE ms_billing.payer_price_basis_audit IS
    'Append-only log of every SetPayerPriceBasis / ClearPayerPriceBasis call. '
    'Never updated or deleted.';

-- Spec: mirrorstack-docs/db/ms_billing/tables.md (payer_price_basis,
-- payer_price_basis_audit).
