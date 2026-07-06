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

-- AccountActivatedAt returns an account's billing-period ANCHOR instant
-- (migration 025): the UTC time it bound its first credit card, or NULL when it
-- never activated. The Go layer derives the anchor DAY-OF-MONTH from it
-- in-process (activated_at.UTC().Day()); a NULL falls back to anchor day 1 (the
-- UTC calendar month — the pre-025 window). Read once per RPC alongside the
-- resolved account id so every read/charge windows the account's own period.
-- name: AccountActivatedAt :one
SELECT activated_at FROM ms_billing.accounts WHERE id = $1;

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

-- AccountHasUnpaidInvoice is the delinquency predicate for Ensure: true when the
-- account has at least one invoice in an unpaid, collection-relevant state.
-- 'open' = finalized but payment not yet collected (a payment_failed leaves the
-- invoice 'open' for Stripe's smart retries); 'uncollectible' = Stripe gave up.
-- 'draft' is excluded (the charge spine hasn't finalized it; no collection
-- attempt has been made). 'paid'/'void' are clean. Delinquency is DERIVED from
-- invoice state, not a stored flag — the invoice mirror (reconciled by the
-- invoice.* webhooks) is the single source of truth. The ENFORCEMENT policy
-- (grace/suspend/prepaid) is PR #9; this only surfaces the signal.
-- name: AccountHasUnpaidInvoice :one
SELECT EXISTS (
    SELECT 1
    FROM ms_billing.invoices
    WHERE account_id = $1
      AND status IN ('open', 'uncollectible')
) AS has_unpaid;

-- ServiceBlockSignals reads, in ONE round-trip, the three inputs the
-- service-block eligibility gate (internal/account/eligibility) reasons over,
-- all scoped to one already-resolved account id:
--
--   usable_card_count   — active (deleted_at IS NULL), non-fraud
--                         (NOT fraud_blocked, migration 038), NOT-expired cards.
--                         Reuses HasUsablePaymentMethod's expiry predicate,
--                         COUNT instead of EXISTS. The gate blocks at 0.
--   failed_charge_streak — the account's consecutive failed-charge count,
--                         DERIVED at read time from the timestamped invoice
--                         mirror: the number of distinct FAILED invoices
--                         (ever_failed, migration 039, OR currently
--                         'uncollectible') created AFTER the account's most-
--                         recent PAID invoice. The gate blocks at >= 2.
--                         Deriving it (vs a stored counter mutated by two
--                         out-of-transaction webhook UPDATEs in Stripe DELIVERY
--                         order) makes it immune to at-least-once + out-of-order
--                         delivery: reorder the paid/failed events however you
--                         like, the count over the immutable (status, ever_failed,
--                         created_at) facts is the same. "Reset on the next
--                         successful charge" falls out for free — a newer paid
--                         invoice moves the cutoff forward, excluding older
--                         failures. A failed-then-paid invoice is excluded (its
--                         created_at is not AFTER its own paid instant).
--   first_charge_status  — the status of the account's EARLIEST real charge:
--                         the oldest invoice that is not 'draft' (never
--                         finalized) or 'void' (cancelled, never a real charge
--                         attempt), by (created_at, id) ASC. '' when the account
--                         has no such invoice yet (brand new — the gate graces
--                         it as long as a card is present). The Go layer maps
--                         '' / paid / open / uncollectible → the FirstChargeState
--                         enum (none / succeeded / pending / failed).
--
-- Scalar subqueries (not JOINs) so each signal is independent and a NULL card
-- count is impossible (COUNT is 0, not NULL); first_charge_status COALESCEs the
-- no-invoice case to ''. One row per account id (or none → caller maps to the
-- not-found verdict).
-- name: ServiceBlockSignals :one
SELECT
    (SELECT COUNT(*)
       FROM ms_billing.payment_methods_mirror pmm
       WHERE pmm.account_id = a.id
         AND pmm.deleted_at IS NULL
         AND NOT pmm.fraud_blocked
         AND (pmm.exp_year, pmm.exp_month) >= (
             EXTRACT(YEAR  FROM current_date)::INT,
             EXTRACT(MONTH FROM current_date)::INT
         ))::int AS usable_card_count,
    (SELECT COUNT(*)
       FROM ms_billing.invoices f
       WHERE f.account_id = a.id
         AND (f.ever_failed OR f.status = 'uncollectible')
         AND f.created_at > COALESCE(
             (SELECT MAX(p.created_at)
                FROM ms_billing.invoices p
                WHERE p.account_id = a.id AND p.status = 'paid'),
             '-infinity'::timestamptz
         ))::int AS failed_charge_streak,
    COALESCE((
        SELECT inv.status
        FROM ms_billing.invoices inv
        WHERE inv.account_id = a.id
          AND inv.status NOT IN ('draft', 'void')
        ORDER BY inv.created_at ASC, inv.id ASC
        LIMIT 1
    ), '')::text AS first_charge_status
FROM ms_billing.accounts a
WHERE a.id = $1;
