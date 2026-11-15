-- Queries backing the per-payer price basis (migration 093,
-- billing-engine#258): internal/account/usage.pgxStore and
-- internal/account/cycle.pgxStore read PriceBasisBPForAccount at every
-- plan-fee call site; internal/account/billing.pgxStore owns the
-- operator-only Set/Clear/Get RPC backing queries below it.

-- PriceBasisBPForAccount is what every charge/display leg actually calls: the
-- payer's basis points for the account that owns the plan being priced,
-- reached through accounts.owner_kind/owner_user_id/owner_org_id (CLAUDE.md:
-- "the charge legs reach the row through accounts.owner_*"). COALESCE to
-- 10000 (list) when the account has no payer_price_basis row, which is every
-- account until an operator sets one — so this query changes no existing
-- amount until that happens.
-- name: PriceBasisBPForAccount :one
SELECT COALESCE(pb.price_basis_bp, 10000)::int AS price_basis_bp
FROM ms_billing.accounts a
LEFT JOIN ms_billing.payer_price_basis pb
    ON (a.owner_kind = 'user' AND pb.owner_user_id = a.owner_user_id)
    OR (a.owner_kind = 'org'  AND pb.owner_org_id  = a.owner_org_id)
WHERE a.id = sqlc.arg(account_id)::uuid;

-- GetPriceBasisRow is the operator read (GetPayerPriceBasis RPC): the exact
-- row for one owner, or no rows when unset (the caller reports 10000/default).
-- name: GetPriceBasisRow :one
SELECT price_basis_bp, note, set_by, set_at
FROM ms_billing.payer_price_basis
WHERE owner_kind = sqlc.arg(owner_kind)::text
  AND owner_user_id IS NOT DISTINCT FROM sqlc.narg(owner_user_id)::uuid
  AND owner_org_id  IS NOT DISTINCT FROM sqlc.narg(owner_org_id)::uuid;

-- InsertPriceBasisRow creates a payer's first basis row. The caller
-- (SetPriceBasis) has already read GetPriceBasisRow in the same transaction
-- to decide insert-vs-update and to capture the audit "old" value, so this
-- never races a concurrent writer past the partial unique indexes without
-- surfacing a constraint error.
-- name: InsertPriceBasisRow :exec
INSERT INTO ms_billing.payer_price_basis (
    owner_kind, owner_user_id, owner_org_id, price_basis_bp, note, set_by
) VALUES (
    sqlc.arg(owner_kind)::text,
    sqlc.narg(owner_user_id)::uuid,
    sqlc.narg(owner_org_id)::uuid,
    sqlc.arg(price_basis_bp)::int,
    sqlc.arg(note)::text,
    sqlc.arg(set_by)::text
);

-- UpdatePriceBasisRow overwrites an existing payer's basis in place (one row
-- per owner, enforced by the partial unique indexes — never a second row).
-- name: UpdatePriceBasisRow :execrows
UPDATE ms_billing.payer_price_basis
SET price_basis_bp = sqlc.arg(price_basis_bp)::int,
    note            = sqlc.arg(note)::text,
    set_by          = sqlc.arg(set_by)::text,
    set_at          = now()
WHERE owner_kind = sqlc.arg(owner_kind)::text
  AND owner_user_id IS NOT DISTINCT FROM sqlc.narg(owner_user_id)::uuid
  AND owner_org_id  IS NOT DISTINCT FROM sqlc.narg(owner_org_id)::uuid;

-- DeletePriceBasisRow clears a payer's row (ClearPayerPriceBasis RPC),
-- reverting it to the 10000 default. Returns rows removed so the caller can
-- report "was never set" distinctly from "cleared".
-- name: DeletePriceBasisRow :execrows
DELETE FROM ms_billing.payer_price_basis
WHERE owner_kind = sqlc.arg(owner_kind)::text
  AND owner_user_id IS NOT DISTINCT FROM sqlc.narg(owner_user_id)::uuid
  AND owner_org_id  IS NOT DISTINCT FROM sqlc.narg(owner_org_id)::uuid;

-- InsertPriceBasisAudit records one SetPayerPriceBasis/ClearPayerPriceBasis
-- call (who, reason, old, new). Append-only; never updated.
-- name: InsertPriceBasisAudit :exec
INSERT INTO ms_billing.payer_price_basis_audit (
    owner_kind, owner_user_id, owner_org_id,
    old_price_basis_bp, new_price_basis_bp, reason, set_by
) VALUES (
    sqlc.arg(owner_kind)::text,
    sqlc.narg(owner_user_id)::uuid,
    sqlc.narg(owner_org_id)::uuid,
    sqlc.narg(old_price_basis_bp)::int,
    sqlc.narg(new_price_basis_bp)::int,
    sqlc.arg(reason)::text,
    sqlc.arg(set_by)::text
);
