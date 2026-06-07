-- Queries backing internal/account/billing.pgxStore (the account-api
-- Store interface). All operate on the ms_billing schema.

-- AcquireBillingAccountUserLock serializes concurrent EnsureAccount
-- calls per user. pg_advisory_xact_lock(namespace, key) is held for the
-- enclosing transaction; the Go layer derives namespace + hashtext(key).
-- name: AcquireBillingAccountUserLock :exec
SELECT pg_advisory_xact_lock($1::int, hashtext($2::text));

-- SelectAccountByUser returns the existing account row for a user, with
-- a non-null stripe_customer_id projection (COALESCE to '').
-- name: SelectAccountByUser :one
SELECT id, COALESCE(stripe_customer_id, '')::text AS stripe_customer_id
FROM ms_billing.accounts
WHERE owner_kind = 'user' AND owner_user_id = $1;

-- InsertUserAccount creates a fresh user-owned account and returns the
-- same projection as SelectAccountByUser (stripe_customer_id is '' on a
-- fresh insert).
-- name: InsertUserAccount :one
INSERT INTO ms_billing.accounts (owner_kind, owner_user_id)
VALUES ('user', $1)
RETURNING id, COALESCE(stripe_customer_id, '')::text AS stripe_customer_id;

-- SetStripeCustomer associates a Stripe Customer ID with an account.
-- :execrows so the caller can map 0 rows to ErrAccountNotFound.
-- name: SetStripeCustomer :execrows
UPDATE ms_billing.accounts SET stripe_customer_id = $2 WHERE id = $1;

-- AccountIDByUser returns just the account id for a user (read-only path
-- where a missing row is a normal "missing billing_account" outcome).
-- name: AccountIDByUser :one
SELECT id FROM ms_billing.accounts
WHERE owner_kind = 'user' AND owner_user_id = $1;

-- HasUsablePaymentMethod is the hot-path Ensure predicate: at least one
-- active (not soft-deleted) and not-expired mirror row on the account.
-- name: HasUsablePaymentMethod :one
SELECT EXISTS (
    SELECT 1
    FROM ms_billing.payment_methods_mirror
    WHERE account_id = $1
      AND deleted_at IS NULL
      AND (exp_year, exp_month) >= (
          EXTRACT(YEAR  FROM current_date)::INT,
          EXTRACT(MONTH FROM current_date)::INT
      )
) AS has;

-- ListPaymentMethods returns active payment methods for an account,
-- newest-first.
-- name: ListPaymentMethods :many
SELECT id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default
FROM ms_billing.payment_methods_mirror
WHERE account_id = $1 AND deleted_at IS NULL
ORDER BY attached_at DESC;
