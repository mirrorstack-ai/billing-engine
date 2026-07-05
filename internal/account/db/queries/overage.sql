-- Queries backing the account-wide POOLED module overage (migration 032, owner
-- spec 2026-07-05). The pool is SUM(module_count) over the account's LIVE apps;
-- overage = $3 × max(0, pool − 5), charged once per account per period through
-- the account_overage_snapshots ledger (the double-charge guard). These queries
-- serve BOTH the charge side (cycle package: the recompute + the mid-period
-- grace sweep + the boundary leg) and the display side (usage package:
-- GetAccountBill's pooled-overage line). Money never lives on the apps mirror;
-- the pool is derived on read.

-- SumLiveModuleCount returns the account-wide POOLED installed-module count:
-- SUM(module_count) over the account's LIVE (deleted_at IS NULL) apps. This is
-- the single source of the pool both the overage timer recompute and the charge
-- legs tier on: overage applies to max(0, this − IncludedModules). COALESCE to 0
-- so an account with no live apps sums to 0 (never over). ::bigint keeps the
-- aggregate a non-nullable scalar.
-- name: SumLiveModuleCount :one
SELECT COALESCE(SUM(module_count), 0)::bigint AS pooled_count
FROM ms_billing.apps
WHERE account_id = $1
  AND deleted_at IS NULL;

-- StartAccountOverage arms the account's grace timer: it stamps overage_since
-- the FIRST time the pool crosses 5. WHERE overage_since IS NULL makes it
-- first-crossing-wins (idempotent — a later recompute that finds the pool still
-- over affects 0 rows and never moves the anchor), exactly like activated_at's
-- first-bind-wins. :execrows so the caller can observe (and tolerate) the
-- already-armed no-op.
-- name: StartAccountOverage :execrows
UPDATE ms_billing.accounts
SET overage_since = $2
WHERE id = $1
  AND overage_since IS NULL;

-- ClearAccountOverage disarms the timer when the pool drops back to ≤5:
-- overage_since → NULL. WHERE overage_since IS NOT NULL keeps it idempotent (a
-- recompute on an already-under account affects 0 rows). No refund is issued
-- (D1e) — clearing only stops FUTURE accrual; overage already charged this
-- period stays billed via its account_overage_snapshots row.
-- name: ClearAccountOverage :execrows
UPDATE ms_billing.accounts
SET overage_since = NULL
WHERE id = $1
  AND overage_since IS NOT NULL;

-- AccountsInOverageGrace returns every account whose grace timer has EXPIRED as
-- of the cutoff (overage_since <= $1 = now − 3 days) and that can be charged
-- (activated_at IS NOT NULL — same activation gate as the spine). The
-- mid-period sweep iterates these, derives each account's current anchored
-- period from activated_at, and charges the pooled overage once per period
-- (guarded by account_overage_snapshots). overage_since is returned so the
-- sweep prorates from grace-end (overage_since + 3d) to the period end.
-- name: AccountsInOverageGrace :many
SELECT id, overage_since, activated_at
FROM ms_billing.accounts
WHERE overage_since IS NOT NULL
  AND overage_since <= $1
  AND activated_at  IS NOT NULL;

-- SelectAccountOverageSnapshot reads the frozen pooled overage a charge leg
-- claimed/billed for ONE (account, period) — the double-charge guard for the
-- charge side (a row means "this period's pooled overage is claimed — pending
-- or charged — skip/resume it, never independently charge it") AND the
-- authoritative display value for GetAccountBill's pooled-overage line. Exact
-- period_start match (both the grace sweep and the boundary leg key on the
-- anchored window start); no row → never claimed → the caller charges it
-- (charge side) or falls back to the live pooled estimate (display side).
-- name: SelectAccountOverageSnapshot :one
SELECT over_count, charged_micros, source, status
FROM ms_billing.account_overage_snapshots
WHERE account_id  = $1
  AND period_start = $2;

-- InsertAccountOverageSnapshot claims ONE (account, period)'s pooled overage
-- charge for a leg — status='pending' is written BEFORE the leg calls Stripe
-- (see migration 032's status column comment); the leg later calls
-- MarkAccountOverageSnapshotCharged once Stripe actually succeeds. ON CONFLICT
-- (account_id, period_start) DO NOTHING: an existing row — a prior grace claim,
-- or a prior reclaimed boundary attempt's own row — wins, so a re-run never
-- rewrites what was already claimed. :execrows so the caller can tell whether
-- ITS insert won the race (rows=1) or lost to a concurrent claim (rows=0) and
-- must re-read + defer to the winner instead of proceeding to charge Stripe.
-- name: InsertAccountOverageSnapshot :execrows
INSERT INTO ms_billing.account_overage_snapshots
    (account_id, period_start, period_end, over_count, charged_micros, source, status, invoice_item_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (account_id, period_start) DO NOTHING;

-- MarkAccountOverageSnapshotCharged flips a claimed row to 'charged' once
-- Stripe actually created the invoice item/invoice, recording the GENUINE
-- Stripe invoice item id (finding #4 — never the idempotency-key string).
-- Unconditional on status (not just WHERE status='pending'): a retry that
-- re-enters this leg after already flipping the row to 'charged' (e.g. the
-- Stripe call succeeded, the write here committed, but a LATER step in the
-- caller failed) must still be able to re-affirm the row harmlessly — the
-- caller only ever calls this with the Stripe values it just confirmed, so
-- overwriting an already-'charged' row with the same (idempotent) values is
-- safe by construction (deterministic per-(account,period) Idempotency-Keys
-- guarantee Stripe returns the SAME object on every re-call).
-- name: MarkAccountOverageSnapshotCharged :exec
UPDATE ms_billing.account_overage_snapshots
SET status          = 'charged',
    invoice_item_id = $3
WHERE account_id  = $1
  AND period_start = $2;

-- TopUpAccountOverageSnapshot records an INCREMENTAL charge against an already-
-- 'charged' period whose pool grew further before the period closed (finding
-- #3 — a judgment call, see cycle/overage.go's topUpGraceOverage doc). Only
-- fires from a 'charged' row (a 'pending' row is a resume-in-progress, never a
-- top-up target); over_count/charged_micros are overwritten with the NEW
-- cumulative totals so the ledger + the display always show what was actually
-- billed in total for the period.
-- name: TopUpAccountOverageSnapshot :exec
UPDATE ms_billing.account_overage_snapshots
SET over_count      = $3,
    charged_micros  = $4,
    invoice_item_id = $5
WHERE account_id  = $1
  AND period_start = $2
  AND status       = 'charged';
