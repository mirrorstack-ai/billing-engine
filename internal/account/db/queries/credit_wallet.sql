-- Queries backing the universal credit-wallet draw at the account billing
-- boundary (migration 048). Draws are append-only settled ledger rows. The Go
-- store holds an account-row lock while using the transactional queries below,
-- which serializes same-account allocation and keeps balance_after_micros
-- snapshots in ledger order.

-- WalletCreditState is the cheap charge-path probe. Spendable credit is the
-- lesser of (a) active positive lot remainder and (b) the positive posted
-- balance after unused expired grant remainder is removed, so neither expiry
-- nor an account-level negative adjustment can be bypassed by allocating an
-- otherwise-positive lot. Expired grants remain in the immutable journal
-- balance but are excluded from spendable credit. Existing draws of both
-- boundary draw types make a reclaimed run re-enter the wallet path.
-- name: WalletCreditState :one
WITH source_lots AS (
    SELECT
        source.id,
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
        ), 0) AS lot_micros,
        COALESCE((
            SELECT SUM(lot.remaining_micros)
            FROM source_lots lot
            WHERE lot.remaining_micros > 0
              AND lot.expires_at <= CURRENT_TIMESTAMP
        ), 0) AS expired_lot_micros,
        COALESCE((
            SELECT -SUM(draw.amount_micros)
            FROM ms_billing.credit_ledger draw
            JOIN ms_billing.billing_periods period ON period.id = draw.period_id
            WHERE draw.account_id = sqlc.arg(account_id)::uuid
              AND period.account_id = sqlc.arg(account_id)::uuid
              AND period.period_start = sqlc.arg(period_start)::timestamptz
              AND period.period_end = sqlc.arg(period_end)::timestamptz
              AND draw.status = 'settled'
              AND draw.type IN ('usage_draw', 'subscription_draw')
        ), 0) AS period_drawn_micros
)
SELECT
    account.billing_mode,
    GREATEST(
        LEAST(
            balances.lot_micros,
            GREATEST(
                balances.settled_micros - balances.expired_lot_micros,
                0
            )
        ),
        0
    )::bigint AS spendable_balance_micros,
    balances.period_drawn_micros::bigint AS period_drawn_micros
FROM ms_billing.accounts account
CROSS JOIN balances
WHERE account.id = sqlc.arg(account_id)::uuid;

-- LockWalletAccount is the serialization point for a draw. The account FK on
-- credit_ledger also makes concurrent ledger inserts wait behind this lock.
-- name: LockWalletAccount :one
SELECT billing_mode
FROM ms_billing.accounts
WHERE id = sqlc.arg(account_id)::uuid
FOR UPDATE;

-- LockWalletLedgerEntries stabilizes status as well as row contents for the
-- allocation transaction. LockWalletAccount blocks concurrent child INSERTs
-- through the account FK, while these row locks block the explicitly allowed
-- pending-purchase status transition from changing the settled snapshot between
-- WalletSettledBalance and WalletSpendableLots.
-- name: LockWalletLedgerEntries :many
SELECT id
FROM ms_billing.credit_ledger
WHERE account_id = sqlc.arg(account_id)::uuid
ORDER BY id
FOR UPDATE;

-- WalletPeriodDraw resolves the exact first-class period and recovers any
-- already-durable boundary draw before the caller considers allowNew. Both draw
-- types are included for forward-compatible recovery, although the current
-- mixed boundary method emits usage_draw because its input has no category
-- split.
-- name: WalletPeriodDraw :one
SELECT
    period.id AS period_id,
    COALESCE(
        -SUM(draw.amount_micros) FILTER (
            WHERE draw.status = 'settled'
              AND draw.type IN ('usage_draw', 'subscription_draw')
        ),
        0
    )::bigint AS drawn_micros
FROM ms_billing.billing_periods period
LEFT JOIN ms_billing.credit_ledger draw
       ON draw.account_id = period.account_id
      AND draw.period_id = period.id
WHERE period.account_id = sqlc.arg(account_id)::uuid
  AND period.period_start = sqlc.arg(period_start)::timestamptz
  AND period.period_end = sqlc.arg(period_end)::timestamptz
GROUP BY period.id;

-- WalletSettledBalance is the posted balance immediately before a new draw.
-- Each inserted row subtracts its allocation from this value to populate the
-- next balance_after_micros snapshot.
-- name: WalletSettledBalance :one
SELECT COALESCE(SUM(amount_micros), 0)::bigint AS balance_micros
FROM ms_billing.credit_ledger
WHERE account_id = sqlc.arg(account_id)::uuid
  AND status = 'settled';

-- WalletExpiredCreditBalance is the unused remainder of grants whose expiry is
-- at or before this transaction's current time. It is subtracted from the
-- standard-mode posted-balance cap; otherwise an expired grant could offset a
-- later negative adjustment and make unrelated active credit look spendable.
-- name: WalletExpiredCreditBalance :one
SELECT COALESCE(SUM(expired.remaining_micros), 0)::bigint AS expired_micros
FROM (
    SELECT
        source.amount_micros::numeric
        + COALESCE((
            SELECT SUM(draw.amount_micros)
            FROM ms_billing.credit_ledger draw
            WHERE draw.source_credit_id = source.id
              AND draw.account_id = source.account_id
              AND draw.status = 'settled'
              AND draw.type IN ('usage_draw', 'subscription_draw')
        ), 0) AS remaining_micros
    FROM ms_billing.credit_ledger source
    WHERE source.account_id = sqlc.arg(account_id)::uuid
      AND source.status = 'settled'
      AND source.amount_micros > 0
      AND source.type IN (
          'grant', 'preallocation', 'refund', 'adjustment',
          'purchase', 'auto_topup'
      )
      AND source.expires_at <= CURRENT_TIMESTAMP
) expired
WHERE expired.remaining_micros > 0;

-- WalletSpendableLots returns active settled funding in the owner-decided
-- consumption order. A source's remainder is its positive amount plus all
-- settled negative draws that reference it. Positive refund/adjustment entries
-- share the non-expiring-grant tier, matching their non-purchased credit
-- semantics; negative entries affect the posted-balance cap instead.
-- name: WalletSpendableLots :many
SELECT
    source.id,
    remaining.remaining_micros::bigint AS remaining_micros
FROM ms_billing.credit_ledger source
CROSS JOIN LATERAL (
    SELECT
        source.amount_micros::numeric
        + COALESCE(SUM(draw.amount_micros), 0) AS remaining_micros
    FROM ms_billing.credit_ledger draw
    WHERE draw.source_credit_id = source.id
      AND draw.account_id = source.account_id
      AND draw.status = 'settled'
      AND draw.type IN ('usage_draw', 'subscription_draw')
) remaining
WHERE source.account_id = sqlc.arg(account_id)::uuid
  AND source.status = 'settled'
  AND source.amount_micros > 0
  AND source.type IN (
      'grant', 'preallocation', 'refund', 'adjustment',
      'purchase', 'auto_topup'
  )
  AND (source.expires_at IS NULL OR source.expires_at > CURRENT_TIMESTAMP)
  AND remaining.remaining_micros > 0
ORDER BY
    CASE
        WHEN source.type = 'grant' AND source.expires_at IS NOT NULL THEN 0
        WHEN source.type IN ('grant', 'preallocation', 'refund', 'adjustment') THEN 1
        ELSE 2
    END,
    source.expires_at ASC NULLS LAST,
    source.created_at ASC,
    source.id ASC
FOR UPDATE OF source;

-- InsertWalletDraw appends one signed allocation row. A multi-lot draw invokes
-- it once per source; credits mode may append one final NULL-source unsecured
-- row. idempotency_key is deterministic per account/period/type/source, while
-- migration 048's period/source indexes provide the matching relational guard.
-- name: InsertWalletDraw :exec
INSERT INTO ms_billing.credit_ledger (
    account_id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    actor,
    idempotency_key,
    period_id,
    source_credit_id
) VALUES (
    sqlc.arg(account_id)::uuid,
    -sqlc.arg(amount_micros)::bigint,
    'usage_draw',
    'settled',
    sqlc.arg(balance_after_micros)::bigint,
    'system',
    sqlc.arg(idempotency_key)::text,
    sqlc.arg(period_id)::uuid,
    sqlc.narg(source_credit_id)::uuid
);
