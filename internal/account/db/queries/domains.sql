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
       d.charge_attempted_at, d.charge_funding_account_id,
       d.charge_funding_generation,
       a.activated_at AS account_activated_at
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

-- ArmDomainStripeCharge is the atomic funding/marker boundary. It row-locks
-- the candidate, chooses the existing first-write claim or the account's
-- current rotating authorization, verifies a usable PM on that exact funder,
-- and persists the claim before returning the Stripe customer. A designation
-- change racing this statement linearizes wholly before or after the arm.
-- name: ArmDomainStripeCharge :one
WITH target AS MATERIALIZED (
    SELECT account_id, charge_attempted_at,
           charge_funding_account_id, charge_funding_generation
    FROM ms_billing.app_custom_domains
    WHERE id = @domain_id::uuid
      AND removed_at IS NULL
      AND charge_resolved = false
      AND NOT charge_funding_legacy_unresolved
    FOR UPDATE
), selected AS MATERIALIZED (
    SELECT target.account_id,
           COALESCE(target.charge_funding_account_id,
                    funding_auth.funding_account_id) AS funding_account_id,
           COALESCE(target.charge_funding_generation,
                    funding_auth.generation) AS funding_generation
    FROM target
    JOIN ms_billing.account_funding_authorizations funding_auth
      ON funding_auth.account_id = target.account_id
), funding AS MATERIALIZED (
    SELECT selected.account_id,
           selected.funding_account_id,
           selected.funding_generation,
           COALESCE(account.stripe_customer_id, '')::text AS stripe_customer_id,
           EXISTS (
               SELECT 1
               FROM ms_billing.payment_methods_mirror payment_method
               WHERE payment_method.account_id = selected.funding_account_id
                 AND payment_method.deleted_at IS NULL
                 AND (payment_method.exp_year, payment_method.exp_month) >= (
                     EXTRACT(YEAR FROM current_date)::INT,
                     EXTRACT(MONTH FROM current_date)::INT
                 )
           ) AS has_usable_payment_method
    FROM selected
    JOIN ms_billing.accounts account
      ON account.id = selected.funding_account_id
), armed AS (
    UPDATE ms_billing.app_custom_domains domain_row
    SET charge_attempted_at = COALESCE(domain_row.charge_attempted_at,
                                       @attempted_at::timestamptz),
        charge_funding_account_id = COALESCE(
            domain_row.charge_funding_account_id,
            funding.funding_account_id
        ),
        charge_funding_generation = COALESCE(
            domain_row.charge_funding_generation,
            funding.funding_generation
        )
    FROM funding
    WHERE domain_row.id = @domain_id::uuid
      AND funding.has_usable_payment_method
    RETURNING domain_row.id
)
SELECT funding.account_id,
       funding.funding_account_id,
       funding.funding_generation,
       funding.stripe_customer_id,
       funding.has_usable_payment_method,
       EXISTS (SELECT 1 FROM armed)::boolean AS armed
FROM funding;

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

-- ActivatedRecurringFeeShares is the CURRENT next-period recurring-base input,
-- decomposed BY APP. Membership mirrors WHAT THE BOUNDARY WILL ACTUALLY BILL,
-- per entity type — never a mutable "was it charged yet" marker. Gating on the
-- marker is what let three classes of live entity forecast as $0 while the
-- boundary billed them every period:
--   * app: creation-proration guard armed, a legacy advance snapshot, or a D1d
--     terminal skip — all three mean the boundary advance leg owns it now;
--   * module overage: current account-FIFO over row that is durably RESOLVED,
--     charged or D1d-forgiven;
--   * custom domain: still live and activated before the next boundary, which
--     is CountLiveDomainsActivatedBefore's predicate exactly.
-- Pending creations stay solely in the one-time projection. Recurring timing
-- depends only on activation timing, so a settled one-time row may legitimately
-- overlap a recurring row in the same period.
--
-- This SUPERSEDES the scalar ActivatedRecurringFeeCounts. The account totals
-- are now SUMS of these rows rather than three sibling counts, so the bill's
-- per-app decomposition and its account line can never disagree: they are the
-- same rows added up two ways, not two queries that must be kept in step.
--
-- The row set is the UNION of the three sources, not just activated apps: an
-- app whose creation charge is still pending can already own a live domain
-- or a charged over-timer, and that money is in the account forecast, so it
-- needs an app to land on. `activated` is therefore a per-row FLAG, not a
-- filter — an app can contribute surcharges while contributing no base fee.
--
-- The FIFO window is account-wide by design (migration 033: ONE included pool
-- for the whole account, ordered installed_at ASC, id ASC across every app), so
-- WHICH app owns the free five is decided globally here and the per-app
-- over-count falls out of that one ordering.
-- name: ActivatedRecurringFeeShares :many
WITH live_timer_fifo AS (
    SELECT app_id,
           grace_charged_at,
           grace_resolved,
           row_number() OVER (ORDER BY installed_at, id) AS fifo_position
    FROM ms_billing.app_module_overage_timers
    WHERE account_id = @account_id::uuid
      AND removed_at IS NULL
),
activated_apps AS (
    SELECT app.app_id
    FROM ms_billing.apps app
    WHERE app.account_id = @account_id::uuid
      AND app.deleted_at IS NULL
      AND (
          app.proration_invoice_id IS NOT NULL
          -- D1d terminal no-charge verdict: the creation proration was
          -- permanently skipped (app created before the account activated), so
          -- no invoice id will ever be written. LiveAppModuleCountsCreatedBefore
          -- reads no proration state and bills the full recurring base, and
          -- UnresolvedOneTimeCharges excludes a skipped-never-attempted app —
          -- so without this arm the app is in NEITHER projection.
          OR app.proration_skipped_at IS NOT NULL
          OR EXISTS (
              SELECT 1
              FROM ms_billing.app_base_snapshots snap
              WHERE snap.app_id = app.app_id
                AND snap.source = 'advance'
          )
      )
),
over_timers AS (
    SELECT app_id, count(*)::bigint AS over_count
    FROM live_timer_fifo
    WHERE fifo_position > @included_modules::int
      -- Durably RESOLVED, not merely charged. CountOngoingOverModuleTimers (the
      -- boundary precharge input) reads no resolution state at all, so a timer
      -- resolved with no charge under D1d is billed at every subsequent boundary
      -- while the charged-marker gate kept it out of the forecast forever. A
      -- resolved row ranked inside the allowance cannot leak in: FIFO rank is
      -- stable, so it fails fifo_position.
      AND (grace_charged_at IS NOT NULL OR grace_resolved)
    GROUP BY app_id
),
live_domains AS (
    -- CountLiveDomainsActivatedBefore's predicate, verbatim: every still-live
    -- domain activated before the next boundary owes one full domain fee, and
    -- charge state is deliberately NOT read. charged_at is written only by
    -- MarkDomainCharged, which four terminal no-charge paths never reach —
    -- prepaid/credits accounts never reach it at all — so the customer was
    -- billed $2/domain while this forecast said $0.
    SELECT app_id, count(*)::bigint AS domain_count
    FROM ms_billing.app_custom_domains
    WHERE account_id = @account_id::uuid
      AND removed_at IS NULL
      AND activated_at < @period_end::timestamptz
    GROUP BY app_id
),
app_universe AS (
    SELECT app_id FROM activated_apps
    UNION
    SELECT app_id FROM over_timers
    UNION
    SELECT app_id FROM live_domains
)
SELECT universe.app_id,
       EXISTS (
           SELECT 1 FROM activated_apps
           WHERE activated_apps.app_id = universe.app_id
       ) AS activated,
       COALESCE(over_timers.over_count, 0)::bigint AS over_module_count,
       COALESCE(live_domains.domain_count, 0)::bigint AS custom_domain_count
FROM app_universe universe
LEFT JOIN over_timers ON over_timers.app_id = universe.app_id
LEFT JOIN live_domains ON live_domains.app_id = universe.app_id
ORDER BY universe.app_id;

-- SettledDomainCreationCharges feeds 本期新建立 with custom-domain activation
-- charges. Membership follows charged_at (the actual settlement instant), so a
-- delayed charge appears in the period that collected it. The service derives
-- the exact charged line amount from activated_at and the account anchor; the
-- invoice total is intentionally not used because customer credits can reduce
-- amount_due to zero without erasing the paid domain line.
-- name: SettledDomainCreationCharges :many
SELECT domain_row.id,
       domain_row.app_id,
       domain_row.hostname,
       domain_row.activated_at,
       domain_row.charged_at,
       invoice.id AS invoice_id,
       invoice.number
FROM ms_billing.app_custom_domains domain_row
JOIN ms_billing.invoices invoice
  ON invoice.stripe_invoice_id = domain_row.charge_invoice_id
WHERE domain_row.account_id = @account_id::uuid
  AND domain_row.charged_at >= @period_start::timestamptz
  AND domain_row.charged_at < @period_end::timestamptz
  AND invoice.status <> 'void'
ORDER BY domain_row.charged_at DESC, domain_row.id;
