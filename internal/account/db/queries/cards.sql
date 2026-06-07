-- Card-management queries: the request-id add-card flow (Start/Finish)
-- plus the ownership-gated detach / set-default target lookup.

-- PaymentMethodTarget resolves an active payment method owned by the
-- user, returning its Stripe PM id, the account's Stripe customer id,
-- and whether the row is currently the default. The JOIN on
-- owner_kind='user' AND owner_user_id is the sole ownership gate; no
-- match (wrong owner, unknown id, or soft-deleted) returns no rows.
-- name: PaymentMethodTarget :one
SELECT
    pmm.stripe_payment_method_id,
    COALESCE(a.stripe_customer_id, '')::text AS stripe_customer_id,
    pmm.is_default
FROM ms_billing.payment_methods_mirror pmm
JOIN ms_billing.accounts a ON a.id = pmm.account_id
WHERE a.owner_kind = 'user' AND a.owner_user_id = $1
  AND pmm.id = $2 AND pmm.deleted_at IS NULL;

-- InsertAddCardRequest creates a pending add_card_requests row for an
-- account and returns its id.
-- name: InsertAddCardRequest :one
INSERT INTO ms_billing.add_card_requests (account_id)
VALUES ($1)
RETURNING id;

-- SetAddCardRequestSetupIntent stamps the Stripe setup_intent_id onto a
-- still-pending request row. WHERE status='pending' is the idempotency
-- guard: a webhook may already have resolved the row. :execrows so the
-- store can log a debug no-op stamp when rows==0 (the row was already
-- resolved), matching SetStripeCustomer's RowsAffected pattern.
-- name: SetAddCardRequestSetupIntent :execrows
UPDATE ms_billing.add_card_requests
SET setup_intent_id = $2
WHERE id = $1 AND status = 'pending';

-- GetAddCardRequest returns the request's status plus the resolved
-- payment method via LEFT JOIN. pm.id is the nullable sentinel — NULL
-- until the webhook resolves the row. The COALESCE'd columns stay
-- non-null so they scan into the projection regardless of status.
-- Scoped to account_id so a user can only poll requests they own.
--
-- `AND pm.deleted_at IS NULL` is part of the LEFT JOIN condition (not a
-- WHERE clause) on purpose: a request that resolved to a card which was
-- later detached must not leak the now-soft-deleted card through the
-- status poll — the join simply yields NULLs (request still returns with
-- its status). Keeping it in the JOIN preserves the LEFT JOIN so the
-- request row still comes back when no/!active PM is attached.
-- name: GetAddCardRequest :one
SELECT
    r.status,
    pm.id AS payment_method_id,
    COALESCE(pm.stripe_payment_method_id, '')::text AS stripe_payment_method_id,
    COALESCE(pm.brand, '')::text AS brand,
    COALESCE(pm.last4, '')::text AS last4,
    COALESCE(pm.exp_month, 0)::int AS exp_month,
    COALESCE(pm.exp_year, 0)::int AS exp_year,
    COALESCE(pm.is_default, false)::boolean AS is_default
FROM ms_billing.add_card_requests r
LEFT JOIN ms_billing.payment_methods_mirror pm
    ON pm.id = r.payment_method_id AND pm.deleted_at IS NULL
WHERE r.id = $1 AND r.account_id = $2;
