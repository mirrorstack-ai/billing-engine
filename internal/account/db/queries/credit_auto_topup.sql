-- Durable automatic credit top-up execution (migration 049).
--
-- All mutating callers acquire LockAutoTopUpAccount first. That account-row
-- lock serializes policy evaluation and conflicts with the parent KEY SHARE
-- lock taken by new ledger inserts. LockAutoTopUpLedgerEntries then stabilizes
-- status transitions on existing journal rows for the transaction.

-- name: LockAutoTopUpAccount :one
SELECT billing_mode, credit_limit_micros
FROM ms_billing.accounts
WHERE id = sqlc.arg(account_id)::uuid
FOR UPDATE;

-- name: LockAutoTopUpLedgerEntries :many
SELECT id
FROM ms_billing.credit_ledger
WHERE account_id = sqlc.arg(account_id)::uuid
ORDER BY id
FOR UPDATE;

-- ReadAutoTopUpPolicy resolves the mutable policy and selected card only while
-- the account lock is held. payment_method_valid is false for a missing,
-- foreign, soft-deleted, fraud-blocked, or expired mirror row. The Stripe
-- customer comes from the same account, so the frozen customer/card pair
-- cannot cross an ownership boundary.
-- name: ReadAutoTopUpPolicy :one
SELECT
    config.enabled,
    config.threshold_micros,
    config.amount_micros,
    COALESCE(config.payment_method_id::text, '')::text AS payment_method_id,
    COALESCE(payment_method.stripe_payment_method_id, '')::text AS stripe_payment_method_id,
    COALESCE(account.stripe_customer_id, '')::text AS stripe_customer_id,
    (
        payment_method.id IS NOT NULL
        AND payment_method.deleted_at IS NULL
        AND NOT payment_method.fraud_blocked
        AND (payment_method.exp_year, payment_method.exp_month) >= (
            EXTRACT(YEAR FROM CURRENT_DATE)::int,
            EXTRACT(MONTH FROM CURRENT_DATE)::int
        )
        AND account.stripe_customer_id IS NOT NULL
    )::boolean AS payment_method_valid
FROM ms_billing.credit_auto_topup_configs config
JOIN ms_billing.accounts account ON account.id = config.account_id
LEFT JOIN ms_billing.payment_methods_mirror payment_method
       ON payment_method.id = config.payment_method_id
      AND payment_method.account_id = config.account_id
WHERE config.account_id = sqlc.arg(account_id)::uuid;

-- ReadAutoTopUpBalance returns both posted and currently spendable balances.
-- Spendable applies the exact migration-048 lot/expiry rule used by the wallet
-- gate: active positive lot remainder capped by posted balance after expired
-- remainder is removed.
-- name: ReadAutoTopUpBalance :one
WITH source_lots AS (
    SELECT
        source.expires_at,
        (
            source.amount_micros::numeric
            + COALESCE((
                SELECT SUM(draw.amount_micros)
                FROM ms_billing.credit_ledger draw
                WHERE draw.source_credit_id = source.id
                  AND draw.account_id = source.account_id
                  AND draw.status = 'settled'
                  AND draw.type IN ('usage_draw', 'subscription_draw')
            ), 0)
        ) AS remaining_micros
    FROM ms_billing.credit_ledger source
    WHERE source.account_id = sqlc.arg(account_id)::uuid
      AND source.status = 'settled'
      AND source.amount_micros > 0
      AND source.type IN (
          'grant', 'preallocation', 'refund', 'adjustment',
          'purchase', 'auto_topup'
      )
), balances AS (
    SELECT
        COALESCE((
            SELECT SUM(entry.amount_micros)
            FROM ms_billing.credit_ledger entry
            WHERE entry.account_id = sqlc.arg(account_id)::uuid
              AND entry.status = 'settled'
        ), 0) AS settled_micros,
        COALESCE((
            SELECT SUM(lot.remaining_micros)
            FROM source_lots lot
            WHERE lot.remaining_micros > 0
              AND (lot.expires_at IS NULL OR lot.expires_at > CURRENT_TIMESTAMP)
        ), 0) AS active_lot_micros,
        COALESCE((
            SELECT SUM(lot.remaining_micros)
            FROM source_lots lot
            WHERE lot.remaining_micros > 0
              AND lot.expires_at <= CURRENT_TIMESTAMP
        ), 0) AS expired_lot_micros
)
SELECT
    balances.settled_micros::bigint AS settled_balance_micros,
    GREATEST(
        LEAST(
            balances.active_lot_micros,
            GREATEST(
                balances.settled_micros - balances.expired_lot_micros,
                0
            )
        ),
        0
    )::bigint AS spendable_balance_micros
FROM balances;

-- name: LatestPendingAutoTopUp :one
SELECT
    id,
    account_id,
    amount_micros,
    status,
    balance_after_micros,
    COALESCE(idempotency_key, '')::text AS idempotency_key,
    COALESCE(stripe_invoice_id, '')::text AS stripe_invoice_id,
    COALESCE(receipt_url, '')::text AS receipt_url,
    COALESCE(attempt_payment_method_id::text, '')::text AS payment_method_id,
    COALESCE(attempt_stripe_payment_method_id, '')::text AS stripe_payment_method_id,
    COALESCE(attempt_stripe_customer_id, '')::text AS stripe_customer_id,
    attempt_expires_at,
    COALESCE(failure_code, '')::text AS failure_code,
    created_at
FROM ms_billing.credit_ledger
WHERE account_id = sqlc.arg(account_id)::uuid
  AND type = 'auto_topup'
  AND status = 'pending'
ORDER BY created_at DESC, id DESC
LIMIT 1
FOR UPDATE;

-- InsertPendingAutoTopUp appends the in-flight guard and every frozen payment
-- fact before Stripe is called. The partial pending unique index is the
-- relational backstop if a caller ever bypasses the account lock.
-- name: InsertPendingAutoTopUp :one
INSERT INTO ms_billing.credit_ledger (
    id,
    account_id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    actor,
    idempotency_key,
    attempt_payment_method_id,
    attempt_stripe_payment_method_id,
    attempt_stripe_customer_id,
    attempt_expires_at,
    created_at
) VALUES (
    sqlc.arg(attempt_id)::uuid,
    sqlc.arg(account_id)::uuid,
    sqlc.arg(amount_micros)::bigint,
    'auto_topup',
    'pending',
    sqlc.arg(balance_after_micros)::bigint,
    'system',
    sqlc.arg(idempotency_key)::text,
    sqlc.arg(payment_method_id)::uuid,
    sqlc.arg(stripe_payment_method_id)::text,
    sqlc.arg(stripe_customer_id)::text,
    sqlc.arg(attempt_expires_at)::timestamptz,
    sqlc.arg(created_at)::timestamptz
)
ON CONFLICT DO NOTHING
RETURNING
    id,
    account_id,
    amount_micros,
    status,
    balance_after_micros,
    COALESCE(idempotency_key, '')::text AS idempotency_key,
    COALESCE(stripe_invoice_id, '')::text AS stripe_invoice_id,
    COALESCE(receipt_url, '')::text AS receipt_url,
    COALESCE(attempt_payment_method_id::text, '')::text AS payment_method_id,
    COALESCE(attempt_stripe_payment_method_id, '')::text AS stripe_payment_method_id,
    COALESCE(attempt_stripe_customer_id, '')::text AS stripe_customer_id,
    attempt_expires_at,
    COALESCE(failure_code, '')::text AS failure_code,
    created_at;

-- name: GetAutoTopUpAttemptByID :one
SELECT
    id,
    account_id,
    amount_micros,
    status,
    balance_after_micros,
    COALESCE(idempotency_key, '')::text AS idempotency_key,
    COALESCE(stripe_invoice_id, '')::text AS stripe_invoice_id,
    COALESCE(receipt_url, '')::text AS receipt_url,
    COALESCE(attempt_payment_method_id::text, '')::text AS payment_method_id,
    COALESCE(attempt_stripe_payment_method_id, '')::text AS stripe_payment_method_id,
    COALESCE(attempt_stripe_customer_id, '')::text AS stripe_customer_id,
    attempt_expires_at,
    COALESCE(failure_code, '')::text AS failure_code,
    created_at
FROM ms_billing.credit_ledger
WHERE id = sqlc.arg(attempt_id)::uuid
  AND account_id = sqlc.arg(account_id)::uuid
  AND type = 'auto_topup';

-- GetAutoTopUpAttemptByStripeInvoice is the webhook reconciliation lookup.
-- It returns only auto-top-up rows: ordinary purchase failures remain owned by
-- their existing manual invoice lifecycle.
-- name: GetAutoTopUpAttemptByStripeInvoice :one
SELECT
    id,
    account_id,
    amount_micros,
    status,
    balance_after_micros,
    COALESCE(idempotency_key, '')::text AS idempotency_key,
    COALESCE(stripe_invoice_id, '')::text AS stripe_invoice_id,
    COALESCE(receipt_url, '')::text AS receipt_url,
    COALESCE(attempt_payment_method_id::text, '')::text AS payment_method_id,
    COALESCE(attempt_stripe_payment_method_id, '')::text AS stripe_payment_method_id,
    COALESCE(attempt_stripe_customer_id, '')::text AS stripe_customer_id,
    attempt_expires_at,
    COALESCE(failure_code, '')::text AS failure_code,
    created_at
FROM ms_billing.credit_ledger
WHERE stripe_invoice_id = sqlc.arg(stripe_invoice_id)::text
  AND type = 'auto_topup';

-- name: AttachAutoTopUpInvoice :one
UPDATE ms_billing.credit_ledger
SET stripe_invoice_id = sqlc.arg(stripe_invoice_id)::text,
    receipt_url = COALESCE(
        NULLIF(sqlc.arg(receipt_url)::text, ''),
        receipt_url
    )
WHERE id = sqlc.arg(attempt_id)::uuid
  AND account_id = sqlc.arg(account_id)::uuid
  AND type = 'auto_topup'
  AND status = 'pending'
  AND (
      stripe_invoice_id IS NULL
      OR stripe_invoice_id = sqlc.arg(stripe_invoice_id)::text
  )
RETURNING id;

-- FindCreditAttemptByStripeInvoice is the pre-lock account resolution used by
-- webhook and executor reconciliation. The transaction re-reads the full row
-- under LockCreditAttemptByStripeInvoice before changing money state.
-- name: FindCreditAttemptByStripeInvoice :one
SELECT account_id, id, type, status, amount_micros
FROM ms_billing.credit_ledger
WHERE stripe_invoice_id = sqlc.arg(stripe_invoice_id)::text
  AND type IN ('purchase', 'auto_topup');

-- name: LockCreditAttemptByStripeInvoice :one
SELECT
    id,
    account_id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    COALESCE(idempotency_key, '')::text AS idempotency_key,
    COALESCE(stripe_invoice_id, '')::text AS stripe_invoice_id,
    COALESCE(receipt_url, '')::text AS receipt_url,
    COALESCE(attempt_payment_method_id::text, '')::text AS payment_method_id,
    COALESCE(attempt_stripe_payment_method_id, '')::text AS stripe_payment_method_id,
    COALESCE(attempt_stripe_customer_id, '')::text AS stripe_customer_id,
    attempt_expires_at,
    COALESCE(failure_code, '')::text AS failure_code,
    created_at
FROM ms_billing.credit_ledger
WHERE stripe_invoice_id = sqlc.arg(stripe_invoice_id)::text
  AND type IN ('purchase', 'auto_topup')
FOR UPDATE;

-- name: LockAutoTopUpAttemptByID :one
SELECT
    id,
    account_id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    COALESCE(idempotency_key, '')::text AS idempotency_key,
    COALESCE(stripe_invoice_id, '')::text AS stripe_invoice_id,
    COALESCE(receipt_url, '')::text AS receipt_url,
    COALESCE(attempt_payment_method_id::text, '')::text AS payment_method_id,
    COALESCE(attempt_stripe_payment_method_id, '')::text AS stripe_payment_method_id,
    COALESCE(attempt_stripe_customer_id, '')::text AS stripe_customer_id,
    attempt_expires_at,
    COALESCE(failure_code, '')::text AS failure_code,
    created_at
FROM ms_billing.credit_ledger
WHERE id = sqlc.arg(attempt_id)::uuid
  AND account_id = sqlc.arg(account_id)::uuid
  AND type = 'auto_topup'
FOR UPDATE;

-- SettleCreditAttempt is deliberately allowed from failed as well as pending:
-- a verified paid invoice is the highest money truth. The account/row locks
-- and status predicate make pending|failed→settled happen exactly once.
-- name: SettleCreditAttempt :one
UPDATE ms_billing.credit_ledger
SET status = 'settled',
    balance_after_micros = sqlc.arg(balance_after_micros)::bigint,
    receipt_url = COALESCE(
        NULLIF(sqlc.arg(receipt_url)::text, ''),
        receipt_url
    ),
    failure_code = NULL
WHERE id = sqlc.arg(attempt_id)::uuid
  AND account_id = sqlc.arg(account_id)::uuid
  AND type IN ('purchase', 'auto_topup')
  AND status IN ('pending', 'failed')
RETURNING id;

-- name: FailAutoTopUpAttempt :one
UPDATE ms_billing.credit_ledger
SET status = 'failed',
    balance_after_micros = sqlc.arg(balance_after_micros)::bigint,
    receipt_url = COALESCE(
        NULLIF(sqlc.arg(receipt_url)::text, ''),
        receipt_url
    ),
    failure_code = sqlc.arg(failure_code)::text
WHERE id = sqlc.arg(attempt_id)::uuid
  AND account_id = sqlc.arg(account_id)::uuid
  AND type = 'auto_topup'
  AND status = 'pending'
RETURNING id;
