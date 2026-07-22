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

-- InsertCreationWalletDraw appends one signed creation-proration draw row.
-- Unlike InsertWalletDraw it carries NO period_id (period_id stays NULL): a
-- creation-proration charge is keyed per APP, not per billing period, so a
-- period-scoped guard would collide with the same period's boundary draw AND
-- with sibling apps' creation draws against the same funding lot. Its
-- idempotency is therefore the deterministic app-scoped idempotency_key alone
-- (migration 048's global idempotency_key unique index), which is what the
-- period/source relational guards can not express for a per-app debit. A
-- multi-lot draw invokes it once per source; credits mode may append one final
-- NULL-source unsecured row.
-- name: InsertCreationWalletDraw :exec
INSERT INTO ms_billing.credit_ledger (
    account_id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    actor,
    idempotency_key,
    source_credit_id
) VALUES (
    sqlc.arg(account_id)::uuid,
    -sqlc.arg(amount_micros)::bigint,
    'usage_draw',
    'settled',
    sqlc.arg(balance_after_micros)::bigint,
    'system',
    sqlc.arg(idempotency_key)::text,
    sqlc.narg(source_credit_id)::uuid
);

-- ---------------------------------------------------------------------------
-- Credit-wallet RPC reads and writes.
--
-- Mutating balance snapshots are serialized through LockCreditAccountBalance.
-- The account-row FOR UPDATE lock also conflicts with the KEY SHARE lock taken
-- by the credit_ledger account_id FK, so same-account journal INSERTs cannot
-- race the balance read. Callers that transition an existing pending purchase
-- additionally lock that ledger row through GetCreditPurchaseByID before using
-- FinalizeCreditPurchase.
-- ---------------------------------------------------------------------------

-- GetCreditStandingSnapshot returns the durable wallet policy, authoritative
-- posted balance, and optional auto-top-up config in one round-trip. The
-- explicit configured bit distinguishes a missing config from a disabled row
-- while keeping the remaining generated fields non-null and easy to map onto
-- the optional RPC object.
-- name: GetCreditStandingSnapshot :one
SELECT
    account.billing_mode,
    account.credit_limit_micros,
    COALESCE((
        SELECT SUM(entry.amount_micros)
        FROM ms_billing.credit_ledger entry
        WHERE entry.account_id = account.id
          AND entry.status = 'settled'
    ), 0)::bigint AS balance_micros,
    (auto_topup.account_id IS NOT NULL)::boolean AS auto_topup_configured,
    COALESCE(auto_topup.enabled, false)::boolean AS auto_topup_enabled,
    COALESCE(auto_topup.threshold_micros, 0)::bigint AS auto_topup_threshold_micros,
    COALESCE(auto_topup.amount_micros, 0)::bigint AS auto_topup_amount_micros,
    COALESCE(auto_topup.payment_method_id, '')::text AS auto_topup_payment_method_id
FROM ms_billing.accounts account
LEFT JOIN ms_billing.credit_auto_topup_configs auto_topup
       ON auto_topup.account_id = account.id
WHERE account.id = sqlc.arg(account_id)::uuid;

-- ListCreditLedgerPage is stable newest-first keyset pagination over the
-- migration-048 (account_id, created_at DESC, id DESC) index. A cursor is
-- either wholly absent or a complete (created_at,id) pair; a half cursor
-- deliberately matches no rows rather than silently restarting page one.
-- name: ListCreditLedgerPage :many
SELECT
    id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    actor,
    receipt_url,
    expires_at,
    created_at
FROM ms_billing.credit_ledger
WHERE account_id = sqlc.arg(account_id)::uuid
  AND (
      (
          sqlc.narg(cursor_created_at)::timestamptz IS NULL
          AND sqlc.narg(cursor_id)::uuid IS NULL
      )
      OR
      (
          sqlc.narg(cursor_created_at)::timestamptz IS NOT NULL
          AND sqlc.narg(cursor_id)::uuid IS NOT NULL
          AND (created_at, id) < (
              sqlc.narg(cursor_created_at)::timestamptz,
              sqlc.narg(cursor_id)::uuid
          )
      )
  )
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_limit)::int;

-- GetCreditLedgerEntryByIdempotencyKey resolves the migration-048 global
-- idempotency boundary. It intentionally returns ownership, amount, type,
-- actor, and expiry so callers can reject key reuse with different semantics
-- without disclosing or mutating the conflicting row.
-- name: GetCreditLedgerEntryByIdempotencyKey :one
SELECT
    id,
    account_id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    actor,
    COALESCE(idempotency_key, '')::text AS idempotency_key,
    COALESCE(stripe_invoice_id, '')::text AS stripe_invoice_id,
    COALESCE(receipt_url, '')::text AS receipt_url,
    expires_at,
    created_at
FROM ms_billing.credit_ledger
WHERE idempotency_key = sqlc.arg(idempotency_key)::text;

-- GetCreditPurchaseByID scopes the Finish RPC handle to its owning account.
-- FOR UPDATE stabilizes the pending status and presentment fields through a
-- transaction that may post the purchase into the settled balance.
-- name: GetCreditPurchaseByID :one
SELECT
    id,
    account_id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    actor,
    COALESCE(idempotency_key, '')::text AS idempotency_key,
    COALESCE(stripe_invoice_id, '')::text AS stripe_invoice_id,
    COALESCE(receipt_url, '')::text AS receipt_url,
    created_at
FROM ms_billing.credit_ledger
WHERE id = sqlc.arg(purchase_id)::uuid
  AND account_id = sqlc.arg(account_id)::uuid
  AND type = 'purchase'
FOR UPDATE;

-- LockCreditAccountBalance is the RPC write serialization point and the
-- authoritative posted balance immediately before inserting or settling a
-- journal entry. FOR UPDATE OF account avoids trying to lock aggregate rows.
-- name: LockCreditAccountBalance :one
SELECT
    account.billing_mode,
    account.credit_limit_micros,
    COALESCE((
        SELECT SUM(entry.amount_micros)
        FROM ms_billing.credit_ledger entry
        WHERE entry.account_id = account.id
          AND entry.status = 'settled'
    ), 0)::bigint AS balance_micros
FROM ms_billing.accounts account
WHERE account.id = sqlc.arg(account_id)::uuid
FOR UPDATE OF account;

-- InsertPendingCreditPurchase creates the durable handle before Stripe is
-- called. A global idempotency collision returns pgx.ErrNoRows; the caller then
-- resolves and validates it through GetCreditLedgerEntryByIdempotencyKey.
-- name: InsertPendingCreditPurchase :one
INSERT INTO ms_billing.credit_ledger (
    account_id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    actor,
    idempotency_key
) VALUES (
    sqlc.arg(account_id)::uuid,
    sqlc.arg(amount_micros)::bigint,
    'purchase',
    'pending',
    sqlc.arg(balance_after_micros)::bigint,
    'self',
    sqlc.arg(idempotency_key)::text
)
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING
    id,
    account_id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    actor,
    COALESCE(idempotency_key, '')::text AS idempotency_key,
    COALESCE(stripe_invoice_id, '')::text AS stripe_invoice_id,
    COALESCE(receipt_url, '')::text AS receipt_url,
    created_at;

-- AttachCreditPurchaseInvoice records Stripe's durable invoice identity and
-- hosted URL without allowing a retry to replace an already-attached invoice.
-- The Stripe call uses the same client idempotency key, so a legitimate retry
-- resolves the same invoice id.
-- name: AttachCreditPurchaseInvoice :one
UPDATE ms_billing.credit_ledger
SET stripe_invoice_id = sqlc.arg(stripe_invoice_id)::text,
    receipt_url = COALESCE(
        NULLIF(sqlc.arg(receipt_url)::text, ''),
        receipt_url
    )
WHERE id = sqlc.arg(purchase_id)::uuid
  AND account_id = sqlc.arg(account_id)::uuid
  AND type = 'purchase'
  AND status = 'pending'
  AND (
      stripe_invoice_id IS NULL
      OR stripe_invoice_id = sqlc.arg(stripe_invoice_id)::text
  )
RETURNING
    id,
    account_id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    actor,
    COALESCE(idempotency_key, '')::text AS idempotency_key,
    COALESCE(stripe_invoice_id, '')::text AS stripe_invoice_id,
    COALESCE(receipt_url, '')::text AS receipt_url,
    created_at;

-- FinalizeCreditPurchase is the sole purchase status transition primitive.
-- Only pending rows may move, and the target is constrained to the two terminal
-- outcomes accepted by the RPC. Retrying a terminal result is a read through
-- GetCreditPurchaseByID, never a second balance mutation.
-- name: FinalizeCreditPurchase :one
UPDATE ms_billing.credit_ledger
SET status = sqlc.arg(status)::text,
    balance_after_micros = sqlc.arg(balance_after_micros)::bigint,
    receipt_url = COALESCE(
        NULLIF(sqlc.arg(receipt_url)::text, ''),
        receipt_url
    )
WHERE id = sqlc.arg(purchase_id)::uuid
  AND account_id = sqlc.arg(account_id)::uuid
  AND type = 'purchase'
  AND status = 'pending'
  AND sqlc.arg(status)::text IN ('settled', 'failed')
RETURNING
    id,
    account_id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    actor,
    COALESCE(idempotency_key, '')::text AS idempotency_key,
    COALESCE(stripe_invoice_id, '')::text AS stripe_invoice_id,
    COALESCE(receipt_url, '')::text AS receipt_url,
    created_at;

-- UpsertCreditAutoTopUp owns the one-row-per-account mutable configuration.
-- The table CHECK enforces that enabled configs carry a payment method and
-- that amount stays within the same $5-$5,000 bounds as manual purchases.
-- name: UpsertCreditAutoTopUp :one
INSERT INTO ms_billing.credit_auto_topup_configs (
    account_id,
    enabled,
    threshold_micros,
    amount_micros,
    payment_method_id
) VALUES (
    sqlc.arg(account_id)::uuid,
    sqlc.arg(enabled)::boolean,
    sqlc.arg(threshold_micros)::bigint,
    sqlc.arg(amount_micros)::bigint,
    NULLIF(sqlc.arg(payment_method_id)::text, '')
)
ON CONFLICT (account_id) DO UPDATE SET
    enabled = EXCLUDED.enabled,
    threshold_micros = EXCLUDED.threshold_micros,
    amount_micros = EXCLUDED.amount_micros,
    payment_method_id = EXCLUDED.payment_method_id
RETURNING
    enabled,
    threshold_micros,
    amount_micros,
    COALESCE(payment_method_id, '')::text AS payment_method_id;

-- SetCreditAccountBillingMode applies a service-resolved concrete credit limit.
-- In particular, the service resolves an omitted credits-mode value to the
-- $5.00 wallet default before calling this query.
-- name: SetCreditAccountBillingMode :one
UPDATE ms_billing.accounts
SET billing_mode = sqlc.arg(billing_mode)::text,
    credit_limit_micros = sqlc.arg(credit_limit_micros)::bigint
WHERE id = sqlc.arg(account_id)::uuid
RETURNING billing_mode, credit_limit_micros;

-- GetDistributorCustomerAccount validates the distributor -> customer
-- relationship and returns the customer's wallet account in one lookup. A
-- distributor is represented by an org-owned account named by the customer's
-- sponsor_account_id; personal sponsorship never passes this join.
-- name: GetDistributorCustomerAccount :one
SELECT customer.id AS account_id
FROM ms_billing.org_billing_designations designation
JOIN ms_billing.accounts distributor
  ON distributor.id = designation.sponsor_account_id
 AND distributor.owner_kind = 'org'
 AND distributor.owner_org_id = sqlc.arg(distributor_org_id)::uuid
JOIN ms_billing.accounts customer
  ON customer.owner_kind = 'org'
 AND customer.owner_org_id = designation.org_id
WHERE designation.org_id = sqlc.arg(customer_org_id)::uuid
  AND designation.funding = 'sponsor';

-- ListDistributorCustomerSnapshots lists every customer whose designation
-- names the distributor's org account, including the wallet fields needed for
-- service-side ok/low/blocked classification.
-- name: ListDistributorCustomerSnapshots :many
SELECT
    designation.org_id AS customer_org_id,
    customer.id AS account_id,
    customer.billing_mode,
    customer.credit_limit_micros,
    COALESCE(balance.balance_micros, 0)::bigint AS balance_micros,
    (auto_topup.account_id IS NOT NULL)::boolean AS auto_topup_configured,
    COALESCE(auto_topup.enabled, false)::boolean AS auto_topup_enabled,
    COALESCE(auto_topup.threshold_micros, 0)::bigint AS auto_topup_threshold_micros,
    COALESCE(auto_topup.amount_micros, 0)::bigint AS auto_topup_amount_micros,
    COALESCE(auto_topup.payment_method_id, '')::text AS auto_topup_payment_method_id
FROM ms_billing.org_billing_designations designation
JOIN ms_billing.accounts distributor
  ON distributor.id = designation.sponsor_account_id
 AND distributor.owner_kind = 'org'
 AND distributor.owner_org_id = sqlc.arg(distributor_org_id)::uuid
JOIN ms_billing.accounts customer
  ON customer.owner_kind = 'org'
 AND customer.owner_org_id = designation.org_id
LEFT JOIN LATERAL (
    SELECT SUM(entry.amount_micros)::bigint AS balance_micros
    FROM ms_billing.credit_ledger entry
    WHERE entry.account_id = customer.id
      AND entry.status = 'settled'
) balance ON true
LEFT JOIN ms_billing.credit_auto_topup_configs auto_topup
       ON auto_topup.account_id = customer.id
WHERE designation.funding = 'sponsor'
ORDER BY designation.org_id;

-- InsertSettledCreditGrant appends a posted distributor/system grant. The
-- caller holds LockCreditAccountBalance and supplies the resulting snapshot.
-- A global idempotency collision returns pgx.ErrNoRows for validation through
-- GetCreditLedgerEntryByIdempotencyKey; no existing journal row is rewritten.
-- name: InsertSettledCreditGrant :one
INSERT INTO ms_billing.credit_ledger (
    account_id,
    amount_micros,
    type,
    status,
    balance_after_micros,
    actor,
    idempotency_key,
    expires_at
) VALUES (
    sqlc.arg(account_id)::uuid,
    sqlc.arg(amount_micros)::bigint,
    'grant',
    'settled',
    sqlc.arg(balance_after_micros)::bigint,
    sqlc.arg(actor)::text,
    sqlc.arg(idempotency_key)::text,
    sqlc.narg(expires_at)::timestamptz
)
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING id, balance_after_micros;
