-- Owner-principal lookups for the serving-block standing notifier
-- (internal/account/standing): a webhook event carries a Stripe object id
-- (customer / invoice / payment method), and the notifier must resolve the
-- OWNER (user XOR org) whose standing may have transitioned so it can POST
-- the current blocked verdict to api-platform's /internal/apps/serving-block.
-- All three are read-only projections of the accounts row's owner columns;
-- exactly one of owner_user_id / owner_org_id is non-NULL per the accounts
-- polymorphic-owner CHECK (001/041).

-- name: AccountOwnerByStripeCustomer :one
SELECT owner_user_id, owner_org_id
FROM ms_billing.accounts
WHERE stripe_customer_id = $1;

-- name: AccountOwnerByStripeInvoice :one
SELECT account.owner_user_id, account.owner_org_id
FROM ms_billing.accounts account
JOIN ms_billing.invoices invoice ON invoice.account_id = account.id
WHERE invoice.stripe_invoice_id = $1;

-- Credit invoices are routed only after their exact trusted metadata has been
-- validated by the webhook handler. Keeping this lookup separate guarantees
-- an ordinary invoice notification never prepares or executes SQL that names
-- the credit ledger.
-- name: AccountOwnerByCreditInvoice :one
SELECT account.owner_user_id, account.owner_org_id
FROM ms_billing.accounts account
JOIN ms_billing.credit_ledger credit ON credit.account_id = account.id
WHERE credit.stripe_invoice_id = $1
  AND credit.type IN ('purchase', 'auto_topup');

-- name: AccountOwnerByStripePaymentMethod :one
SELECT a.owner_user_id, a.owner_org_id
FROM ms_billing.payment_methods_mirror pm
JOIN ms_billing.accounts a ON a.id = pm.account_id
WHERE pm.stripe_payment_method_id = $1;

-- The rollback restamp is deliberately migration-048-independent. It scans
-- every legacy account by immutable primary-key cursor and never filters on
-- billing_mode, rollout cohort, card state, or activation.
-- name: ListAccountOwnersForLegacyRestamp :many
SELECT id, owner_user_id, owner_org_id
FROM ms_billing.accounts
WHERE id > $1
ORDER BY id
LIMIT $2;

-- Snapshot cardinality is returned with every page. The protected workflow
-- accepts completion only when the first/terminal totals equal the aggregate
-- successful attempts; concurrent account creation therefore restarts from
-- the empty cursor instead of letting a lower random UUID be skipped.
-- name: CountAccountOwnersForLegacyRestamp :one
SELECT COUNT(*) FROM ms_billing.accounts;

-- Session-level singleton for the explicit rollback restamp. The caller must
-- execute lock and unlock on the same acquired pgxpool connection.
-- name: TryAcquireLegacyRestampMutex :one
SELECT pg_try_advisory_lock(1297306707, 1);

-- name: ReleaseLegacyRestampMutex :one
SELECT pg_advisory_unlock(1297306707, 1);
