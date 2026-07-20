-- Queries backing the custom-domain mirror (migration 047). A domain has a
-- zero-length grace window: the activation-period sweep becomes eligible at
-- activated_at, while every subsequent full period is charged by the boundary
-- leg from immutable activation/removal timestamps.

-- InsertDomain records one custom-domain activation idempotently. The partial
-- unique index permits only one LIVE row per hostname; a retry never rewrites
-- the first activation's app/account/time. Historical removed rows do not
-- conflict, so a later re-activation gets a fresh charge identity.
-- name: InsertDomain :exec
INSERT INTO ms_billing.app_custom_domains
    (account_id, app_id, hostname, activated_at)
VALUES
    (@account_id::uuid, @app_id::uuid, @hostname, @activated_at::timestamptz)
ON CONFLICT (hostname) WHERE removed_at IS NULL DO NOTHING;

-- DomainByHostname reads the live activation when one exists, otherwise the
-- newest historical activation. RegisterDomain uses the live-conflict winner
-- after InsertDomain; the historical fallback preserves useful idempotent
-- visibility after removal.
-- name: DomainByHostname :one
SELECT id, account_id, app_id, hostname, activated_at, removed_at, created_at
FROM ms_billing.app_custom_domains
WHERE hostname = $1
ORDER BY (removed_at IS NULL) DESC, created_at DESC, id DESC
LIMIT 1;

-- RemoveDomain prospectively stops one app/hostname activation. The first
-- removal instant wins across retries; already-removed rows are untouched.
-- name: RemoveDomain :exec
UPDATE ms_billing.app_custom_domains
SET removed_at = @removed_at::timestamptz
WHERE app_id = @app_id::uuid
  AND hostname = @hostname
  AND removed_at IS NULL;

-- DomainsPendingCharge is the activation-period sweep work list. With no grace
-- window, a domain is eligible as soon as activated_at <= @at. Only activated
-- accounts participate (the spine's D1d gate); the account activation anchor is
-- returned so the caller can derive the anchored period containing activated_at.
-- charge_attempted_at drives recovery-before-fresh-charge on retries.
-- name: DomainsPendingCharge :many
SELECT d.id, d.account_id, d.app_id, d.hostname, d.activated_at,
       d.charge_attempted_at, a.activated_at AS account_activated_at
FROM ms_billing.app_custom_domains d
JOIN ms_billing.accounts a ON a.id = d.account_id
WHERE d.removed_at IS NULL
  AND d.charge_resolved = false
  AND d.activated_at <= @at::timestamptz
  AND a.activated_at IS NOT NULL
ORDER BY d.activated_at, d.id;

-- DomainStillPending re-verifies the work-list verdict immediately before the
-- sweep acts, so a concurrent removal/resolution cannot be charged from a stale
-- batch snapshot.
-- name: DomainStillPending :one
SELECT (removed_at IS NULL AND charge_resolved = false)::bool AS pending
FROM ms_billing.app_custom_domains
WHERE id = $1;

-- MarkDomainChargeAttempted stamps the first recovery marker before Stripe is
-- called. First-write-wins and is never cleared.
-- name: MarkDomainChargeAttempted :exec
UPDATE ms_billing.app_custom_domains
SET charge_attempted_at = $2
WHERE id = $1
  AND charge_attempted_at IS NULL;

-- MarkDomainChargeResolved terminally forgives an activation period under D1d
-- (the account activated at/after that period closed). No money moved.
-- name: MarkDomainChargeResolved :exec
UPDATE ms_billing.app_custom_domains
SET charge_resolved = true
WHERE id = $1
  AND charge_resolved = false;

-- MarkDomainCharged terminally records a successful activation-period charge
-- and its genuine Stripe object ids. The resolution predicate is the one-shot
-- DB guard; deterministic per-domain Stripe keys protect the network side.
-- name: MarkDomainCharged :exec
UPDATE ms_billing.app_custom_domains
SET charge_resolved        = true,
    charged_at             = @charged_at::timestamptz,
    charge_invoice_id      = @charge_invoice_id,
    charge_invoice_item_id = @charge_invoice_item_id
WHERE id = @domain_id::uuid
  AND charge_resolved = false;

-- CountLiveDomainsActivatedBefore is the boundary advance input for the NEW
-- period opening at @period_end: every still-live domain activated before that
-- boundary owes one full domain fee. charge_resolved is deliberately NOT read;
-- the activation sweep and boundary leg own disjoint periods, so depending on
-- mutable sweep state would create a cron-ordering gap.
-- name: CountLiveDomainsActivatedBefore :one
SELECT COALESCE(count(*), 0)::bigint AS live_count
FROM ms_billing.app_custom_domains
WHERE account_id = @account_id::uuid
  AND activated_at < @period_end::timestamptz
  AND removed_at IS NULL;

-- CountLiveDomainsForAccount is the current DISPLAY estimate input: one flat
-- domain fee per currently-live activation.
-- name: CountLiveDomainsForAccount :one
SELECT COALESCE(count(*), 0)::bigint AS live_count
FROM ms_billing.app_custom_domains
WHERE account_id = $1
  AND removed_at IS NULL;
