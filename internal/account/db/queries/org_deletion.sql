-- Durable organization billing retirement (migration 052).

-- StripeFundingAuthorization returns the rotating current funding authority
-- plus the PM/customer facts needed to arm a durable attempt. Callers persist
-- generation + funder in the same transaction before any Stripe request.
-- name: StripeFundingAuthorization :one
SELECT funding_auth.account_id,
       funding_auth.generation,
       funding_auth.funding_account_id,
       COALESCE(funding.stripe_customer_id, '')::text AS stripe_customer_id,
       EXISTS (
           SELECT 1
           FROM ms_billing.payment_methods_mirror payment_method
           WHERE payment_method.account_id = funding_auth.funding_account_id
             AND payment_method.deleted_at IS NULL
             AND (payment_method.exp_year, payment_method.exp_month) >= (
                 EXTRACT(YEAR FROM current_date)::INT,
                 EXTRACT(MONTH FROM current_date)::INT
             )
       ) AS has_usable_payment_method
FROM ms_billing.account_funding_authorizations funding_auth
JOIN ms_billing.accounts funding
  ON funding.id = funding_auth.funding_account_id
WHERE funding_auth.account_id = @account_id::uuid;

-- AcquireOrgBillingLifecycleLock serializes finalization with every trigger-
-- guarded future org-billing write.  The string namespace must remain byte-
-- identical to assert_org_billing_active in migration 052.
-- name: AcquireOrgBillingLifecycleLock :one
SELECT 1::bigint AS locked
FROM (
    SELECT pg_advisory_xact_lock(
        hashtextextended('ms_billing.org.lifecycle:' || CAST(@org_id::uuid AS text), 0)
    )
) AS lifecycle_lock;

-- AssertOrgBillingWriterPairActive acquires shared lifecycle locks in stable
-- UUID order and rejects either retired side. Distributor-authorized writes
-- call this and re-read the live designation in the same transaction.
-- name: AssertOrgBillingWriterPairActive :one
SELECT 1::bigint AS active
FROM (
    SELECT ms_billing.assert_org_billing_active_pair(
        @first_org_id::uuid,
        @second_org_id::uuid
    )
) AS lifecycle_guard;

-- name: GetOrgDeletionFinalizationForUpdate :one
SELECT org_id, operation_id, finalized_at
FROM ms_billing.org_deletion_finalizations
WHERE org_id = $1
FOR UPDATE;

-- Lock every historical account owned by the organization. The schema does
-- not enforce one-account-per-org, so finalization must never inspect only an
-- arbitrary account and miss a collectible obligation on another.
-- name: ListOrgAccountsForDeletion :many
SELECT id
FROM ms_billing.accounts
WHERE owner_kind = 'org' AND owner_org_id = $1
ORDER BY id
FOR UPDATE;

-- FinalCollectibleOrgInvoiceCount is the authoritative last check immediately
-- before retirement. A collectible invoice blocks both its customer owner and
-- the exact account whose Stripe Customer authorized collection. Pre-052
-- invoices lack that proof, so any still-collectible quarantined invoice is a
-- fail-closed global retirement barrier until operations settles or verifies
-- its historical payer.
-- name: FinalCollectibleOrgInvoiceCount :one
WITH direct_accounts AS MATERIALIZED (
    SELECT id
    FROM ms_billing.accounts
    WHERE owner_kind = 'org' AND owner_org_id = @org_id::uuid
)
SELECT count(*)::bigint AS collectible_count
FROM ms_billing.invoices invoice
WHERE invoice.status IN ('open', 'uncollectible')
  AND invoice.amount_due > 0
  AND (
        invoice.account_id IN (SELECT id FROM direct_accounts)
        OR invoice.charge_funding_account_id IN (SELECT id FROM direct_accounts)
        OR (
            invoice.charge_funding_legacy_unresolved
            AND EXISTS (SELECT 1 FROM direct_accounts)
        )
  );

-- A charge that has durably started but has not yet reached a terminal local
-- state can still create a collectible Stripe invoice.  Finalization waits for
-- recovery/reconciliation instead of racing that money movement.
-- name: OrgBillingInFlightCount :one
WITH direct_accounts AS MATERIALIZED (
    SELECT id
    FROM ms_billing.accounts
    WHERE owner_kind = 'org' AND owner_org_id = @org_id::uuid
),
charge_apps AS MATERIALIZED (
    SELECT app_id
    FROM ms_billing.apps
    WHERE account_id IN (SELECT id FROM direct_accounts)
)
SELECT (
    -- Every non-invoiced run is recoverable and owns retained usage and/or a
    -- frozen Stripe request. Deletion must not bury failed/skipped recovery.
    (SELECT count(*) FROM ms_billing.billing_runs
      WHERE (
            account_id IN (SELECT id FROM direct_accounts)
            OR charge_funding_account_id IN (SELECT id FROM direct_accounts)
            OR (
                charge_funding_legacy_unresolved
                AND EXISTS (SELECT 1 FROM direct_accounts)
            )
        )
        AND status <> 'invoiced')
  + (SELECT count(*) FROM ms_billing.app_combined_proration_attempts
      WHERE (
            account_id IN (SELECT id FROM direct_accounts)
            OR charge_funding_account_id IN (SELECT id FROM direct_accounts)
            OR (
                charge_funding_legacy_unresolved
                AND EXISTS (SELECT 1 FROM direct_accounts)
            )
        )
        AND resolved_at IS NULL)
  + (SELECT count(*) FROM ms_billing.app_custom_domains
      WHERE (
            app_id IN (SELECT app_id FROM charge_apps)
            OR charge_funding_account_id IN (SELECT id FROM direct_accounts)
            OR (
                charge_funding_legacy_unresolved
                AND EXISTS (SELECT 1 FROM direct_accounts)
            )
        )
        AND charge_attempted_at IS NOT NULL AND charge_resolved = false)
  + (SELECT count(*) FROM ms_billing.app_module_overage_timers
      WHERE (
            app_id IN (SELECT app_id FROM charge_apps)
            OR charge_funding_account_id IN (SELECT id FROM direct_accounts)
            OR (
                charge_funding_legacy_unresolved
                AND EXISTS (SELECT 1 FROM direct_accounts)
            )
        )
        AND charge_attempted_at IS NOT NULL AND grace_resolved = false)
  + (SELECT count(*) FROM ms_billing.credit_ledger
      WHERE type IN ('purchase', 'auto_topup')
        AND status = 'pending'
        AND (
            account_id IN (SELECT id FROM direct_accounts)
            OR (
                type = 'purchase'
                AND charge_funding_account_id IN (SELECT id FROM direct_accounts)
            )
            OR (
                type = 'purchase'
                AND charge_funding_legacy_unresolved
                AND EXISTS (SELECT 1 FROM direct_accounts)
            )
            OR (
                type = 'auto_topup'
                AND attempt_stripe_customer_id IN (
                    SELECT stripe_customer_id
                    FROM ms_billing.accounts
                    WHERE id IN (SELECT id FROM direct_accounts)
                      AND stripe_customer_id IS NOT NULL
                )
            )
        ))
)::bigint AS in_flight_count;

-- name: RetireOrgApps :execrows
UPDATE ms_billing.apps
SET deleted_at = COALESCE(deleted_at, @finalized_at::timestamptz),
    proration_skipped_at = CASE
        WHEN proration_invoice_id IS NULL
            THEN COALESCE(proration_skipped_at, @finalized_at::timestamptz)
        ELSE proration_skipped_at
    END
WHERE owner_org_id = @org_id::uuid;

-- name: RetireOrgModuleTimers :execrows
UPDATE ms_billing.app_module_overage_timers timer
SET removed_at = COALESCE(timer.removed_at, @finalized_at::timestamptz)
FROM ms_billing.apps app
WHERE timer.app_id = app.app_id
  AND app.owner_org_id = @org_id::uuid
  AND timer.removed_at IS NULL;

-- name: RetireOrgDomains :execrows
UPDATE ms_billing.app_custom_domains domain_row
SET removed_at = COALESCE(domain_row.removed_at, @finalized_at::timestamptz)
FROM ms_billing.apps app
WHERE domain_row.app_id = app.app_id
  AND app.owner_org_id = @org_id::uuid
  AND domain_row.removed_at IS NULL;

-- Funding methods are retained as soft-deleted audit rows.  Sponsor-owned
-- cards are on the sponsor's user account and are intentionally untouched.
-- name: RetireOrgPaymentMethods :execrows
UPDATE ms_billing.payment_methods_mirror payment_method
SET deleted_at = COALESCE(payment_method.deleted_at, @finalized_at::timestamptz),
    is_default = false
FROM ms_billing.accounts account
WHERE payment_method.account_id = account.id
  AND account.owner_kind = 'org'
  AND account.owner_org_id = @org_id::uuid
  AND payment_method.deleted_at IS NULL;

-- name: DisableOrgAutoTopUp :execrows
UPDATE ms_billing.credit_auto_topup_configs config
SET enabled = false,
    payment_method_id = NULL
FROM ms_billing.accounts account
WHERE config.account_id = account.id
  AND account.owner_kind = 'org'
  AND account.owner_org_id = @org_id::uuid;

-- A pending SetupIntent may still deliver webhooks later.  Making its local
-- request terminal prevents that replay from resolving a deleted org's card.
-- name: FailOrgPendingAddCardRequests :execrows
UPDATE ms_billing.add_card_requests request
SET status = 'failed',
    resolved_at = COALESCE(request.resolved_at, @finalized_at::timestamptz)
FROM ms_billing.accounts account
WHERE request.account_id = account.id
  AND account.owner_kind = 'org'
  AND account.owner_org_id = @org_id::uuid
  AND request.status = 'pending';

-- Retired accounts remain immutable history but are forced onto the
-- no-off-session posture as an additional defence behind the tombstone.
-- name: RetireOrgAccountCollection :execrows
UPDATE ms_billing.accounts
SET usage_billing_mode = 'prepaid',
    credit_limit_micros = 0,
    spend_ceiling_micros = 0
WHERE owner_kind = 'org' AND owner_org_id = @org_id::uuid;

-- name: DeleteOrgDesignationForFinalization :execrows
DELETE FROM ms_billing.org_billing_designations
WHERE org_id = $1;

-- A first-party/distributor org account can sponsor other organizations.
-- Retiring it must remove those future funding edges too; the customer org
-- accounts and every historical financial row remain intact.
-- name: DeleteOrgOutboundSponsorships :execrows
DELETE FROM ms_billing.org_billing_designations
WHERE sponsor_account_id IN (
    SELECT id
    FROM ms_billing.accounts
    WHERE owner_kind = 'org' AND owner_org_id = @org_id::uuid
);

-- Retain the exact outbound sponsor edges before removing their live
-- designations. The finalization FK is deferred because the immutable
-- tombstone is deliberately inserted last in the transaction.
-- name: RetainOrgOutboundSponsorships :execrows
INSERT INTO ms_billing.org_deletion_retired_sponsorships (
    retired_sponsor_org_id,
    customer_org_id,
    sponsor_account_id,
    operation_id,
    retired_at
)
SELECT
    @org_id::uuid,
    designation.org_id,
    designation.sponsor_account_id,
    @operation_id::uuid,
    @retired_at::timestamptz
FROM ms_billing.org_billing_designations designation
WHERE designation.funding = 'sponsor'
  AND designation.sponsor_account_id IN (
      SELECT id
      FROM ms_billing.accounts
      WHERE owner_kind = 'org' AND owner_org_id = @org_id::uuid
  )
ON CONFLICT (retired_sponsor_org_id, customer_org_id) DO NOTHING;

-- Inserted last, after all retirement writes.  The transaction-scoped
-- lifecycle lock is still held, so no guarded writer can interleave.
-- name: InsertOrgDeletionFinalization :exec
INSERT INTO ms_billing.org_deletion_finalizations
    (org_id, operation_id, finalized_at)
VALUES
    (@org_id::uuid, @operation_id::uuid, @finalized_at::timestamptz);
