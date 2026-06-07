-- Queries backing internal/account/webhook.pgxStore (the Stripe webhook
-- side-effect surface). All operate on the ms_billing schema.

-- MarkEventProcessed records the event_id for idempotency. :execrows so
-- the caller can map 1 row → firstTime=true, 0 → duplicate.
-- name: MarkEventProcessed :execrows
INSERT INTO ms_billing.webhook_events_processed (event_id, event_type)
VALUES ($1, $2)
ON CONFLICT (event_id) DO NOTHING;

-- TouchAccountByStripeCustomer bumps updated_at for the account matching
-- a Stripe customer. :execrows → >0 means found.
-- name: TouchAccountByStripeCustomer :execrows
UPDATE ms_billing.accounts SET updated_at = now() WHERE stripe_customer_id = $1;

-- SetDefaultPaymentMethodByCustomer marks one PM as default and unmarks
-- all others for the account in a single UPDATE. Empty defaultStripePMID
-- ($2) clears the flag everywhere (no row equals '').
-- name: SetDefaultPaymentMethodByCustomer :exec
UPDATE ms_billing.payment_methods_mirror
SET is_default = (stripe_payment_method_id = $2)
WHERE account_id = (
    SELECT id FROM ms_billing.accounts WHERE stripe_customer_id = $1
)
AND deleted_at IS NULL;

-- InsertPaymentMethod mirrors a Stripe PM into payment_methods_mirror.
-- First active card on the account becomes the default (NOT EXISTS).
-- Skips when an active row already shares brand/last4/exp (best-effort
-- insert-time dedupe). fingerprint stored via NULLIF($7,''). No RETURNING
-- → :execrows; the Go layer disambiguates 0 rows via AccountExists.
-- name: InsertPaymentMethod :execrows
WITH acct AS (
    SELECT id FROM ms_billing.accounts WHERE stripe_customer_id = $1
)
INSERT INTO ms_billing.payment_methods_mirror
    (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default, fingerprint)
SELECT acct.id, $2, $3, $4, $5, $6,
    NOT EXISTS (
        SELECT 1 FROM ms_billing.payment_methods_mirror p
        WHERE p.account_id = acct.id AND p.deleted_at IS NULL
    ),
    NULLIF($7, '')
FROM acct
WHERE NOT EXISTS (
    SELECT 1 FROM ms_billing.payment_methods_mirror p2
    WHERE p2.account_id = acct.id
      AND p2.deleted_at IS NULL
      AND p2.brand = $3
      AND p2.last4 = $4
      AND p2.exp_month = $5
      AND p2.exp_year = $6
)
ON CONFLICT (stripe_payment_method_id) DO NOTHING;

-- AccountExistsByStripeCustomer disambiguates a 0-row InsertPaymentMethod
-- (drift vs ON CONFLICT/dedupe no-op).
-- name: AccountExistsByStripeCustomer :one
SELECT EXISTS (SELECT 1 FROM ms_billing.accounts WHERE stripe_customer_id = $1) AS account_exists;

-- SoftDeletePaymentMethod marks a mirror row deleted by Stripe PM id.
-- :execrows → >0 means found.
-- name: SoftDeletePaymentMethod :execrows
UPDATE ms_billing.payment_methods_mirror
SET deleted_at = now()
WHERE stripe_payment_method_id = $1 AND deleted_at IS NULL;

-- SetAddCardRequestStripePM stamps the resolved Stripe PM id onto a
-- still-pending request row matched by setup_intent_id.
-- name: SetAddCardRequestStripePM :exec
UPDATE ms_billing.add_card_requests
SET stripe_pm_id = $2
WHERE setup_intent_id = $1 AND status = 'pending';

-- MirrorRowByStripePM looks up a just-mirrored row by Stripe PM id, for
-- the resolve transaction (step 1). fingerprint is nullable.
-- name: MirrorRowByStripePM :one
SELECT id, account_id, fingerprint
FROM ms_billing.payment_methods_mirror
WHERE stripe_payment_method_id = $1;

-- DuplicateFingerprintPM probes for ANOTHER active mirror row on the same
-- account with the same fingerprint (resolve transaction step 2).
-- name: DuplicateFingerprintPM :one
SELECT id
FROM ms_billing.payment_methods_mirror
WHERE account_id = $1
  AND fingerprint = $2
  AND id <> $3
  AND deleted_at IS NULL
LIMIT 1;

-- SoftDeleteMirrorByID soft-deletes the just-mirrored duplicate row by id
-- (resolve transaction step 3).
-- name: SoftDeleteMirrorByID :exec
UPDATE ms_billing.payment_methods_mirror
SET deleted_at = now()
WHERE id = $1 AND deleted_at IS NULL;

-- ResolveAddCardRequest is the terminal resolve (step 4): set status +
-- payment_method_id + resolved_at on the still-pending request matched by
-- stripe_pm_id. $2 is cast to the enum type.
-- name: ResolveAddCardRequest :exec
UPDATE ms_billing.add_card_requests
SET status = $2::ms_billing.add_card_request_status,
    payment_method_id = $3,
    resolved_at = now()
WHERE stripe_pm_id = $1 AND status = 'pending';
