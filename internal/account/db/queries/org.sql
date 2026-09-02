-- Queries backing the org-billing substrate (migration 041, design D1):
-- the org account get-or-create, the funding designation, and the
-- RepointOrgUsage attach/backfill sweep. All operate on ms_billing.

-- SelectAccountByOrg returns the existing org-owned account row, with the
-- same non-null stripe_customer_id projection as SelectAccountByUser.
-- name: SelectAccountByOrg :one
SELECT id, COALESCE(stripe_customer_id, '')::text AS stripe_customer_id
FROM ms_billing.accounts
WHERE owner_kind = 'org' AND owner_org_id = $1
  AND NOT EXISTS (
      SELECT 1 FROM ms_billing.org_deletion_finalizations f
      WHERE f.org_id = $1
  );

-- InsertOrgAccount creates a fresh org-owned account (the org leg of the
-- advisory-locked get-or-create — the lock, namespace 'lbto', is the
-- uniqueness guard exactly like the user leg's 'lbta').
-- name: InsertOrgAccount :one
INSERT INTO ms_billing.accounts (owner_kind, owner_org_id)
VALUES ('org', $1)
RETURNING id, COALESCE(stripe_customer_id, '')::text AS stripe_customer_id;

-- GetOrgDesignation reads the org's funding designation row verbatim.
-- name: GetOrgDesignation :one
SELECT org_id, funding, sponsor_account_id, sponsor_user_id,
       disclosed_backlog_micros, updated_by, updated_at
FROM ms_billing.org_billing_designations
WHERE org_id = $1;

-- UpsertOrgDesignation writes the org's funding choice. A re-designation
-- overwrites in place (funding switches change only which instrument future
-- invoice finalization charges — attribution never moves, design D1).
-- name: UpsertOrgDesignation :exec
INSERT INTO ms_billing.org_billing_designations
    (org_id, funding, sponsor_account_id, sponsor_user_id,
     disclosed_backlog_micros, updated_by)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (org_id) DO UPDATE SET
    funding                  = EXCLUDED.funding,
    sponsor_account_id       = EXCLUDED.sponsor_account_id,
    sponsor_user_id          = EXCLUDED.sponsor_user_id,
    disclosed_backlog_micros = EXCLUDED.disclosed_backlog_micros,
    updated_by               = EXCLUDED.updated_by;

-- DeleteOrgDesignation is the sponsor self-revoke: the org drops back to
-- unbilled (resolution finds no designation) until re-designation. Roster
-- rows KEEP their account_id — frozen attribution never rewrites; only new
-- events record NULL until the org designates again.
-- name: DeleteOrgDesignation :execrows
DELETE FROM ms_billing.org_billing_designations WHERE org_id = $1;

-- ResolveOrgFundedAccount is THE org account resolution (ingest, reads,
-- Ensure): the org's own account, gated on a designation row existing AND the
-- account being activated. Sponsor designation activates immediately (a
-- usable instrument exists); funding='org' activates at card bind — so the
-- single activated_at gate implements "the pointer never flips to an
-- unfunded account" for both modes. No row → the org is unbilled (lazy
-- NULL-account events), which callers treat exactly like a missing user
-- account.
-- name: ResolveOrgFundedAccount :one
SELECT a.id
FROM ms_billing.org_billing_designations d
JOIN ms_billing.accounts a
    ON a.owner_kind = 'org' AND a.owner_org_id = d.org_id
WHERE d.org_id = $1
  AND a.activated_at IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM ms_billing.org_deletion_finalizations f
      WHERE f.org_id = d.org_id
  );

-- ChargeFundingAccount maps an account to the account whose Stripe customer /
-- default PM pays its invoices: itself, unless it is an org account whose
-- designation says a sponsor lends the card. The charge legs resolve their
-- customer + PM gate through this exactly once, at charge time — a designation
-- switch between runs re-routes only future charges (design D1).
-- name: ChargeFundingAccount :one
SELECT COALESCE(d.sponsor_account_id, a.id)::uuid AS funding_account_id
FROM ms_billing.accounts a
LEFT JOIN ms_billing.org_billing_designations d
    ON a.owner_kind = 'org'
   AND d.org_id = a.owner_org_id
   AND d.funding = 'sponsor'
WHERE a.id = $1
  AND NOT EXISTS (
      SELECT 1 FROM ms_billing.org_deletion_finalizations f
      WHERE a.owner_kind = 'org' AND f.org_id = a.owner_org_id
  );

-- ActivateAccountIfUnset stamps the ADR-0006 activation anchor when the org
-- account activates by SPONSOR designation (its anchor = designation day; the
-- card-bind webhook stamps the funding='org' case). Idempotent — the anchor
-- is immutable once set.
-- name: ActivateAccountIfUnset :execrows
UPDATE ms_billing.accounts
SET activated_at = $2
WHERE id = $1 AND activated_at IS NULL
  AND NOT EXISTS (
      SELECT 1
      FROM ms_billing.org_deletion_finalizations f
      WHERE f.org_id = ms_billing.accounts.owner_org_id
  );

-- OrgUnbilledBacklogMicros estimates the org's pre-designation unbilled
-- backlog: every NULL-account event attributable to the org (through its
-- roster rows' owner_org_id), priced exactly like the live bill display
-- (AppBillLines' live branch: declared price ×1 for custom metrics, ×12/10
-- for reserved infra.*/platform.*). It is the DISCLOSURE estimate shown
-- before the sponsor confirms — the authoritative charge happens later,
-- through the normal rollup, once the sweep re-points the events.
-- name: OrgUnbilledBacklogMicros :one
WITH base_events AS (
    SELECT
        e.app_id, e.module_id, e.metric, e.aggregation_key, e.subject,
        COALESCE(e.model, '') AS model,
        COALESCE(e.module_version, '') AS module_version,
        e.value
    FROM ms_billing.usage_events e
    WHERE e.account_id IS NULL
      AND e.app_id IN (SELECT app_id FROM ms_billing.apps WHERE owner_org_id = $1)
),
billable_events AS (
    SELECT app_id, module_id, metric, model, module_version, value AS billable_value
    FROM base_events
    WHERE aggregation_key IS DISTINCT FROM 'subject'
    UNION ALL
    -- The same opaque subject may legitimately exist in multiple apps. Take
    -- its peak inside each authoritative app/meter/price scope, then let the
    -- outer disclosure sum those independently billable app contributions.
    SELECT
        app_id, module_id, metric, model, module_version,
        MAX(value)::numeric AS billable_value
    FROM base_events
    WHERE aggregation_key = 'subject'
    GROUP BY app_id, module_id, metric, model, module_version, subject
)
SELECT COALESCE(SUM(
    CASE
        WHEN e.metric LIKE 'infra.%' OR e.metric LIKE 'platform.%'
            THEN e.billable_value * COALESCE(md.unit_price_micros, 0) * 12 / 10
        ELSE e.billable_value * COALESCE(md.unit_price_micros, 0)
    END), 0)::numeric AS backlog_micros
FROM billable_events e
LEFT JOIN ms_billing.metric_definitions md
    ON md.module_id = e.module_id AND md.metric = e.metric;

-- AttachOrgAppsToAccount backfills account_id onto the org's unbilled roster
-- rows — the roster half of the RepointOrgUsage sweep. Attached rows enter
-- the base-fee machinery prospectively: created_at is untouched (the D1d
-- no-retroactive-catch-up rule permanently skips any creation period that
-- closed before activation), and timers are synthesized fresh by the caller.
-- name: AttachOrgAppsToAccount :execrows
UPDATE ms_billing.apps
SET account_id = $2
WHERE owner_org_id = $1 AND account_id IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM ms_billing.org_deletion_finalizations f
      WHERE f.org_id = $1
  );

-- RepointOrgNullAccountEvents folds the org's pre-designation NULL-account
-- events into its funded account — the events half of the sweep. The rollup
-- sweep CLAMPS its billing time to the current open window — "backfilled
-- events bill in the first period that closes after designation" (decision
-- 1). recorded_at/repointed_from retain their migration-041 behavior for v1;
-- billable_at is set for EVERY swept row so v2 can clamp occurred_at without
-- mutating that original-occurrence audit field. Scoped through the
-- roster's owner_org_id so lazy USER events are never swept. Idempotent:
-- account_id IS NULL never matches a swept row again.
-- name: RepointOrgNullAccountEvents :execrows
UPDATE ms_billing.usage_events
SET account_id              = @account_id::uuid,
    billable_at             = GREATEST(
        COALESCE(occurred_at, recorded_at),
        @window_start::timestamptz
    ),
    occurrence_policy       = CASE
        WHEN observation_version = 2
         AND occurred_at < @window_start::timestamptz
        THEN 'first_funded'
        ELSE occurrence_policy
    END,
    repointed_from          = CASE WHEN recorded_at < @window_start::timestamptz
                                   THEN recorded_at ELSE repointed_from END,
    recorded_at             = GREATEST(recorded_at, @window_start::timestamptz)
WHERE account_id IS NULL
  AND app_id IN (SELECT app_id FROM ms_billing.apps WHERE owner_org_id = @org_id::uuid)
  AND NOT EXISTS (
      SELECT 1 FROM ms_billing.org_deletion_finalizations f
      WHERE f.org_id = @org_id::uuid
  );

-- ClosedBillingPeriodEndAtStart lets the repoint transaction advance a lazy
-- backlog past a period whose rollup barrier won the advisory-lock race.
-- name: ClosedBillingPeriodEndAtStart :one
SELECT period_end
FROM ms_billing.billing_periods
WHERE account_id = @account_id::uuid
  AND period_start = @period_start::timestamptz
  AND status IN ('closing', 'invoiced');

-- OrgLiveAppIDs lists the org's live roster rows — the timer-synthesis loop
-- of the RepointOrgUsage sweep reconciles each one after attach.
-- name: OrgLiveAppIDs :many
SELECT app_id
FROM ms_billing.apps
WHERE owner_org_id = $1 AND deleted_at IS NULL;

-- PaymentMethodTargetForOrg is PaymentMethodTarget's org twin: resolves an
-- active payment method owned by the ORG account for detach / set-default.
-- name: PaymentMethodTargetForOrg :one
SELECT
    pmm.stripe_payment_method_id,
    COALESCE(a.stripe_customer_id, '')::text AS stripe_customer_id,
    pmm.is_default
FROM ms_billing.payment_methods_mirror pmm
JOIN ms_billing.accounts a ON a.id = pmm.account_id
WHERE a.owner_kind = 'org' AND a.owner_org_id = $1
  AND pmm.id = $2 AND pmm.deleted_at IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM ms_billing.org_deletion_finalizations f
      WHERE f.org_id = a.owner_org_id
  );

-- ListSponsoredOrgIDs lists the orgs a user sponsors (org-billing W1, the /me
-- sponsored-orgs read). funding='sponsor' means the sponsor pair is the acting
-- user's OWN account (migration 041), so filtering on sponsor_user_id yields
-- exactly the orgs this user pays for. The activated_at gate mirrors
-- ResolveOrgFundedAccount — an org whose account never activated is unbilled
-- and carries no total, so it is excluded from the sponsored roster. Uses the
-- funding='sponsor' partial index on sponsor_user_id (migration 043).
-- name: ListSponsoredOrgIDs :many
SELECT d.org_id
FROM ms_billing.org_billing_designations d
JOIN ms_billing.accounts a ON a.owner_kind = 'org' AND a.owner_org_id = d.org_id
WHERE d.funding = 'sponsor' AND d.sponsor_user_id = $1 AND a.activated_at IS NOT NULL
ORDER BY d.org_id;

-- OrgsWithUnsweptUsage is the self-healing work list for funded, activated
-- orgs whose roster or retained usage still lacks the funded account id. Both
-- predicates become false after a successful attach sweep, which prevents the
-- daily driver from repeatedly resetting live-app overage grace timers.
-- name: OrgsWithUnsweptUsage :many
SELECT d.org_id
FROM ms_billing.org_billing_designations d
JOIN ms_billing.accounts a
  ON a.owner_kind = 'org' AND a.owner_org_id = d.org_id
 AND a.activated_at IS NOT NULL
WHERE EXISTS (
        SELECT 1 FROM ms_billing.apps ap
        WHERE ap.owner_org_id = d.org_id AND ap.account_id IS NULL)
   OR EXISTS (
        SELECT 1
        FROM ms_billing.usage_events e
        JOIN ms_billing.apps ap ON ap.app_id = e.app_id
        WHERE ap.owner_org_id = d.org_id AND e.account_id IS NULL)
ORDER BY d.org_id;

-- UpsertOrgDistributor binds customer org C to distributor org B (migration
-- 053). PK on customer_org_id makes a re-bind an UPDATE, never a second row,
-- so "who distributes C" stays a single unambiguous fact.
--
-- source is written on INSERT and PRESERVED on re-bind unless it changes from
-- 'registration' to 'manual': an operator overriding a registration-derived
-- link should be visible as manual, but a later registration-time write must
-- not silently downgrade a deliberate manual override back to 'registration'.
-- name: UpsertOrgDistributor :one
INSERT INTO ms_billing.org_distributors (
    customer_org_id, distributor_org_id, source
) VALUES (
    sqlc.arg(customer_org_id)::uuid,
    sqlc.arg(distributor_org_id)::uuid,
    sqlc.arg(source)::text
)
ON CONFLICT (customer_org_id) DO UPDATE SET
    distributor_org_id = EXCLUDED.distributor_org_id,
    source             = CASE
                             WHEN ms_billing.org_distributors.source = 'manual'
                             THEN 'manual'
                             ELSE EXCLUDED.source
                         END,
    updated_at         = now()
RETURNING customer_org_id, distributor_org_id, source;

-- DeleteOrgDistributor clears C's distributor link. Returns the rows removed
-- so the caller can distinguish "unlinked" from "was never linked" rather
-- than reporting success for a no-op.
-- name: DeleteOrgDistributor :execrows
DELETE FROM ms_billing.org_distributors
WHERE customer_org_id = sqlc.arg(customer_org_id)::uuid;

-- GetOrgDistributor returns the distributor org that distributes C, if any.
-- name: GetOrgDistributor :one
SELECT distributor_org_id, source
FROM ms_billing.org_distributors
WHERE customer_org_id = sqlc.arg(customer_org_id)::uuid;

-- OrgIsDistributor derives is_distributor: an org IS a distributor iff it
-- distributes at least one customer. Deliberately DERIVED rather than stored
-- as a flag, so the answer can never drift from the links themselves.
-- Backed by org_distributors_distributor_idx.
-- name: OrgIsDistributor :one
SELECT EXISTS (
    SELECT 1 FROM ms_billing.org_distributors
    WHERE distributor_org_id = sqlc.arg(distributor_org_id)::uuid
)::boolean AS is_distributor;
