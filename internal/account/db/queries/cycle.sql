-- Queries backing the Milestone D PR #6 charge spine (the Stripe charge +
-- cmd/billing-cycle binary). All operate on the ms_billing schema.
--
-- The rollup/settlement queries (OpenPeriodForAccount, RollupSumKinds, …) live
-- in rollup.sql (PR #5); this file ships ONLY the charge-cycle queries:
--   AccountsWithUsageEvents    accounts with raw usage_events in a closed period
--                              window (the ROLLUP-phase work list — phase 1 of
--                              cmd/billing-cycle)
--   AccountsWithUnbilledUsage  accounts with priced usage in a closed period
--                              that have no SUCCESSFUL billing_run yet (the
--                              CHARGE-phase work list — phase 2)
--   PeriodChargedTotal         Σ usage_aggregates.charged_micros per (account,
--                              period) — the arrears input
--   InsertBillingRun           the idempotency gate (ON CONFLICT reclaiming a
--                              non-terminal run; only an 'invoiced' run blocks)
--   MarkBillingRun             terminal status + stripe_invoice_id + total
--   UpsertInvoice              the Stripe invoice mirror
--   HasUsableDefaultPM         the no-PM gate (reuses payment_methods_mirror)
--   AccountStripeCustomer      resolve the account's stripe_customer_id for the
--                              charge (NULL until the first Stripe Customer)
--
-- SCOPE (PR #6): the USAGE (arrears) leg only. The advance leg (seats × price
-- + apps × $20), the tiers table, and tier-sourced allowance are DEFERRED to a
-- dedicated subscription/tier PR — they need tier pricing + per-account
-- seat/app counts that do not exist in billing yet. allowanceMicros is an
-- INPUT to the Go service (0 for v1), not a column read here.
--
-- Money is micro-dollar BIGINT in usage_aggregates; the Go service converts
-- micros → whole cents (round-half-up) at the Stripe boundary. The invoice
-- mirror stores NUMERIC cents.

-- ActivatedAccounts returns every account that has bound a card (activated_at IS
-- NOT NULL — migration 025), with its anchor instant, for cmd/billing-cycle's
-- per-account close driver. Un-activated accounts (NULL) have no card and nothing
-- to bill, so they are excluded. The cycle derives each account's anchor day from
-- activated_at and closes THAT account's just-ended anchored period — every close
-- day differs, so the batch can no longer share one window.
-- name: ActivatedAccounts :many
SELECT id, activated_at
FROM ms_billing.accounts
WHERE activated_at IS NOT NULL;

-- LatestClosedPeriodEnd returns the newest billing_periods.period_end for an
-- account; pgx.ErrNoRows when it has no period yet (the Go store maps that to
-- "none"). cmd/billing-cycle uses it to STRADDLE-CLAMP the first anchored run at
-- cutover: if a computed anchored window starts before the last calendar-month
-- period already ended, the run starts at that end instead, producing one clean
-- bridge period (calendar → anchor) with no overlap, gap, or duplicate
-- (account_id, period_start) key. ORDER BY … LIMIT 1 (not MAX) so the result
-- keeps the NOT NULL period_end column type instead of a nullable aggregate.
-- name: LatestClosedPeriodEnd :one
SELECT period_end
FROM ms_billing.billing_periods
WHERE account_id = $1
ORDER BY period_end DESC
LIMIT 1;

-- AccountsWithUsageEvents returns the distinct accounts with at least one raw
-- usage_events row in the closed window [period_start, period_end) — the ROLLUP
-- (phase 1) work list. cmd/billing-cycle rolls each of these up
-- (RollupPeriod → usage_aggregates) BEFORE the charge phase, so the charge
-- phase's PeriodChargedTotal reads a populated aggregate set rather than 0.
-- account_id is NULLABLE on usage_events (lazy-account metering); IS NOT NULL
-- excludes pre-attribution events that have no account to bill. Half-open
-- [start, end): recorded_at >= start AND recorded_at < end, matching the rollup
-- SELECTs in rollup.sql.
-- name: AccountsWithUsageEvents :many
SELECT DISTINCT account_id::uuid AS account_id
FROM ms_billing.usage_events
WHERE account_id  IS NOT NULL
  AND recorded_at >= $1
  AND recorded_at <  $2;

-- AccountsWithUnbilledUsage returns the distinct accounts that have at least
-- one usage_aggregates row for the EXACT closed window [period_start,
-- period_end) and do NOT already have a SUCCESSFUL (invoiced) billing_run for
-- it. The window is matched by equality on the billing_periods row (each window
-- is one period row), so two adjacent months never both match a single run.
--
-- The anti-join surfaces an account when EITHER it has no run row
-- (br.id IS NULL) OR its run is non-terminal. 'invoiced' is the ONLY terminal
-- (excluding) status; every other status re-surfaces the account so the next
-- cycle re-attempts (usage is RETAINED, never abandoned):
--   'pending'         a prior run died mid-flight, before MarkBillingRun
--   'failed'          the charge errored
--   'skipped_no_pm'   no usable payment method last cycle
--   'skipped_prepaid' the account was in / tightened to prepaid mode
--   'skipped_ceiling' the arrears breached the per-cycle spend ceiling
-- InsertBillingRun's ON CONFLICT (WHERE status <> 'invoiced') reclaims the SAME
-- non-terminal row, and the deterministic Stripe Idempotency-Keys
-- (ii-<run>/inv-<run>, stable per run id) make a pending re-attempt reuse the
-- same Stripe objects — no double charge.
--
-- RE-CHARGE AFTER RELAX (a deliberate design decision): retained usage is
-- DEFERRED, never FORGIVEN. If an account was tightened to prepaid in cycle N
-- (its period-N run marked skipped_prepaid) and is later RELAXED back to arrears
-- (invoice.paid clears the delinquency — see RelaxCollectionOnPaidInvoice), the
-- period-N row is still non-terminal here, so it re-surfaces and the next cycle
-- charges that retained period-N usage. That is correct: the customer always owes
-- the metered usage; prepaid only postpones the off-session collection, it does
-- not waive the debt. Forgiveness would require an explicit credit, which v1 does
-- not do.
-- name: AccountsWithUnbilledUsage :many
SELECT DISTINCT ua.account_id AS account_id
FROM ms_billing.usage_aggregates ua
JOIN ms_billing.billing_periods bp ON bp.id = ua.period_id
LEFT JOIN ms_billing.billing_runs br
       ON br.account_id   = ua.account_id
      AND br.period_start = bp.period_start
      AND br.period_end   = bp.period_end
WHERE bp.period_start = $1
  AND bp.period_end   = $2
  AND (br.id IS NULL OR br.status <> 'invoiced');

-- PeriodChargedTotal sums charged_micros across an account's usage_aggregates
-- for a period window — the customer-billable arrears total (before
-- allowance-netting, which the Go service applies). Joins billing_periods so
-- the window is matched on the period row, not a raw usage_aggregates column.
-- name: PeriodChargedTotal :one
SELECT COALESCE(SUM(ua.charged_micros), 0)::bigint AS total_micros
FROM ms_billing.usage_aggregates ua
JOIN ms_billing.billing_periods bp ON bp.id = ua.period_id
WHERE ua.account_id   = $1
  AND bp.period_start = $2
  AND bp.period_end   = $3;

-- InsertBillingRun is the FIRST idempotency layer: one run row per
-- (account, period window). It inserts a 'pending' row; on conflict it RECLAIMS
-- the existing row for a fresh attempt — but ONLY when that row is non-terminal
-- ('pending' from a run that died mid-flight, 'skipped_no_pm', or 'failed'). The
-- DO UPDATE resets the reclaimed row to 'pending' (clearing any stale invoice
-- id / total) and RETURNING fires, so the Go store sees shouldCharge=true.
--
-- When the existing row is 'invoiced' (terminal success) the DO UPDATE's WHERE
-- excludes it: no row is updated, RETURNING yields nothing, and the store maps
-- pgx.ErrNoRows to shouldCharge=false — the account was already charged for this
-- window, so the cycle does NOT re-charge.
--
-- Reclaiming the SAME row (not inserting a new one) preserves the run id, so the
-- deterministic Stripe Idempotency-Keys (ii-<run>/inv-<run>) stay identical
-- across attempts: a 'pending' re-attempt whose prior run already created the
-- Stripe invoice reuses that exact invoice (no double charge), and UpsertInvoice
-- is idempotent on stripe_invoice_id. UNIQUE(account, period) still holds —
-- there is never more than one run row per window.
-- name: InsertBillingRun :one
INSERT INTO ms_billing.billing_runs (account_id, period_start, period_end, status)
VALUES ($1, $2, $3, 'pending')
ON CONFLICT (account_id, period_start, period_end) DO UPDATE
    SET status            = 'pending',
        stripe_invoice_id = NULL,
        total_amount      = 0
    WHERE ms_billing.billing_runs.status <> 'invoiced'
RETURNING id;

-- MarkBillingRun sets a run's terminal status, the Stripe invoice id (NULL for
-- zero-arrears / skipped runs), and the charged total. Scoped to the run id so
-- only the row this cycle inserted is updated.
-- name: MarkBillingRun :exec
UPDATE ms_billing.billing_runs
SET status            = $2,
    stripe_invoice_id = $3,
    total_amount      = $4
WHERE id = $1;

-- UpsertInvoice mirrors a Stripe invoice into ms_billing.invoices, keyed on the
-- UNIQUE stripe_invoice_id so a re-run (deterministic Stripe Idempotency-Key
-- returns the same invoice) upserts the same row rather than duplicating it.
-- Webhook reconciliation (PR #7) later updates status + amount_paid in place.
-- name: UpsertInvoice :exec
INSERT INTO ms_billing.invoices (
    account_id, stripe_invoice_id, status,
    amount_due, amount_paid, currency,
    period_start, period_end
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
)
ON CONFLICT (stripe_invoice_id)
DO UPDATE SET
    status       = EXCLUDED.status,
    amount_due   = EXCLUDED.amount_due,
    amount_paid  = EXCLUDED.amount_paid,
    currency     = EXCLUDED.currency,
    period_start = EXCLUDED.period_start,
    period_end   = EXCLUDED.period_end;

-- HasUsableDefaultPM is the no-PM charge gate: true iff the account has at
-- least one active (not soft-deleted), not-expired payment_methods_mirror row.
-- Mirrors billing.sql HasUsablePaymentMethod (the authoritative usable-PM
-- predicate) so the charge spine and the Ensure hot-path agree. A default flag
-- isn't required — Stripe charges the Customer's invoice-settings default PM,
-- which the account sets when attaching; "usable PM exists" is the gate.
-- name: HasUsableDefaultPM :one
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

-- AccountStripeCustomer resolves the account's Stripe Customer id for the
-- charge. COALESCE to '' so the Go layer distinguishes "no Customer yet" (empty)
-- from a real id without a nullable round-trip. A charge never auto-creates a
-- Customer — an account reaching the charge leg with no Customer is an
-- anomaly the service surfaces, never a silent Stripe Customer create.
-- name: AccountStripeCustomer :one
SELECT COALESCE(stripe_customer_id, '')::text AS stripe_customer_id
FROM ms_billing.accounts
WHERE id = $1;

-- AccountCollectionFields loads the risk-graded collection controls for the
-- charge gate (PR #9): the off-session arrears leg reads these to decide whether
-- to charge, skip-prepaid, cap at the spend ceiling, or tighten over the credit
-- limit. created_at is returned so the risk-judge can compute account tenure
-- WITHOUT a cross-schema read into ms_account (billing-engine never reads
-- ms_account tables). spend_ceiling_micros is NULL when no ceiling is set; the
-- Go layer carries it as a nullable.
-- name: AccountCollectionFields :one
SELECT
    usage_billing_mode,
    credit_limit_micros,
    spend_ceiling_micros,
    created_at
FROM ms_billing.accounts
WHERE id = $1;

-- UpdateAccountCollection persists a risk-judge mode transition (and, when the
-- trust-ramp recomputes, the credit limit + spend ceiling). :execrows so the Go
-- store maps RowsAffected == 0 to "account not found". updated_at is bumped by
-- the accounts_set_updated_at trigger automatically.
-- name: UpdateAccountCollection :execrows
UPDATE ms_billing.accounts
SET usage_billing_mode   = $2,
    credit_limit_micros  = $3,
    spend_ceiling_micros = $4
WHERE id = $1;
