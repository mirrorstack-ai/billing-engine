-- Queries backing the org-billing substrate (migration 041, design D1):
-- the org account get-or-create, the funding designation, and the
-- RepointOrgUsage attach/backfill sweep. All operate on ms_billing.

-- SelectAccountByOrg returns the existing org-owned account row, with the
-- same non-null stripe_customer_id projection as SelectAccountByUser.
-- name: SelectAccountByOrg :one
SELECT id, COALESCE(stripe_customer_id, '')::text AS stripe_customer_id
FROM ms_billing.accounts
WHERE owner_kind = 'org' AND owner_org_id = $1;

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
  AND a.activated_at IS NOT NULL;

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
WHERE a.id = $1;

-- ActivateAccountIfUnset stamps the ADR-0006 activation anchor when the org
-- account activates by SPONSOR designation (its anchor = designation day; the
-- card-bind webhook stamps the funding='org' case). Idempotent — the anchor
-- is immutable once set.
-- name: ActivateAccountIfUnset :execrows
UPDATE ms_billing.accounts
SET activated_at = $2
WHERE id = $1 AND activated_at IS NULL;

-- OrgUnbilledBacklogMicros estimates the org's pre-designation unbilled
-- backlog: every NULL-account event attributable to the org (through its
-- roster rows' owner_org_id), priced exactly like the live bill display
-- (AppBillLines' live branch: declared price ×1 for custom metrics, ×12/10
-- for reserved infra.*/platform.*). It is the DISCLOSURE estimate shown
-- before the sponsor confirms — the authoritative charge happens later,
-- through the normal rollup, once the sweep re-points the events.
-- name: OrgUnbilledBacklogMicros :one
SELECT COALESCE(SUM(
    CASE
        WHEN e.metric LIKE 'infra.%' OR e.metric LIKE 'platform.%'
            THEN e.value * COALESCE(md.unit_price_micros, 0) * 12 / 10
        ELSE e.value * COALESCE(md.unit_price_micros, 0)
    END), 0)::numeric AS backlog_micros
FROM ms_billing.usage_events e
LEFT JOIN ms_billing.metric_definitions md
    ON md.module_id = e.module_id AND md.metric = e.metric
WHERE e.account_id IS NULL
  AND e.app_id IN (SELECT app_id FROM ms_billing.apps WHERE owner_org_id = $1);

-- AttachOrgAppsToAccount backfills account_id onto the org's unbilled roster
-- rows — the roster half of the RepointOrgUsage sweep. Attached rows enter
-- the base-fee machinery prospectively: created_at is untouched (the D1d
-- no-retroactive-catch-up rule permanently skips any creation period that
-- closed before activation), and timers are synthesized fresh by the caller.
-- name: AttachOrgAppsToAccount :execrows
UPDATE ms_billing.apps
SET account_id = $2
WHERE owner_org_id = $1 AND account_id IS NULL;

-- RepointOrgNullAccountEvents folds the org's pre-designation NULL-account
-- events into its funded account — the events half of the sweep. The rollup
-- windows events by recorded_at, so an event older than the account's current
-- open window (@window_start) would never fall inside ANY future window: the
-- sweep CLAMPS its recorded_at to the window start — "backfilled events bill
-- in the first period that closes after designation" (decision 1) — and
-- preserves the original instant in repointed_from (migration 041 audit
-- column). Scoped through the roster's owner_org_id so lazy USER events are
-- never swept. Idempotent: account_id IS NULL never matches a swept row again.
-- name: RepointOrgNullAccountEvents :execrows
UPDATE ms_billing.usage_events
SET account_id     = @account_id::uuid,
    repointed_from = CASE WHEN recorded_at < @window_start::timestamptz
                          THEN recorded_at ELSE repointed_from END,
    recorded_at    = GREATEST(recorded_at, @window_start::timestamptz)
WHERE account_id IS NULL
  AND app_id IN (SELECT app_id FROM ms_billing.apps WHERE owner_org_id = @org_id::uuid);

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
  AND pmm.id = $2 AND pmm.deleted_at IS NULL;
