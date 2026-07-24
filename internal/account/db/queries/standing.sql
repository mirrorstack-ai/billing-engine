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
WHERE account.id = COALESCE(
    (
        SELECT invoice.account_id
        FROM ms_billing.invoices invoice
        WHERE invoice.stripe_invoice_id = $1
    ),
    (
        SELECT credit.account_id
        FROM ms_billing.credit_ledger credit
        WHERE credit.stripe_invoice_id = $1
          AND credit.type IN ('purchase', 'auto_topup')
    )
);

-- name: AccountOwnerByStripePaymentMethod :one
SELECT a.owner_user_id, a.owner_org_id
FROM ms_billing.payment_methods_mirror pm
JOIN ms_billing.accounts a ON a.id = pm.account_id
WHERE pm.stripe_payment_method_id = $1;
