-- Migration 030 — account-wide POOLED module overage (owner spec 2026-07-05,
-- confirmed reversal of the per-app overage tier).
--
-- Base-fee v1 shipped module overage PER-APP: each app's base was
--   BaseFee + $3 × max(0, module_count − 5)
-- with 5 included modules PER APP. This migration moves the overage to an
-- ACCOUNT-WIDE POOL: one allowance of 5 included modules for the ENTIRE
-- account, charged $3/month for each module beyond the pooled 5 across ALL of
-- the account's live (non-deleted) apps. The FLAT per-app base fee ($20/app)
-- is UNCHANGED and stays per-app — only the $3/module overage math moves from
-- per-app to pooled.
--
-- Two schema additions carry the pool + its grace timer:
--
--   1. accounts.overage_since — the UTC instant the account's pooled
--      SUM(module_count) over live apps FIRST crossed the included 5. NULL
--      means the account is not currently over the pool. It arms ONE grace
--      timer per ACCOUNT (not per app, not per module): when the pool first
--      exceeds 5 the timer starts; if the pool drops back to ≤5 before it
--      expires the timer is CLEARED (overage_since → NULL, no charge); if the
--      pool is still >5 after 3 days the mid-period sweep charges the pooled
--      overage prorated from grace-end to the period end. The timer is
--      recomputed by RegisterApp / SyncAppModules after every module_count
--      write (SUM(module_count) WHERE account_id = ? AND deleted_at IS NULL).
--
--   2. account_overage_snapshots — the ACCOUNT-scoped analogue of
--      app_base_snapshots (migration 028): one row per (account, period) that
--      FREEZES the pooled overage a charge leg actually billed for that
--      period, so the mid-period grace charge and the boundary advance leg can
--      never double-charge the same period (both consult / write this ledger,
--      keyed (account_id, period_start), the same double-charge guard
--      app_base_snapshots is for the per-app base). source records which leg
--      billed it: 'grace' (the mid-period sweep, prorated from grace-end) or
--      'advance' (the boundary, full pooled overage for a period no sweep
--      caught) — mirroring app_base_snapshots' 'proration' vs 'advance'.
--
-- No refunds (D1e philosophy): removing a module that drops the pool back
-- under 5 clears overage_since (stops FUTURE accrual) but never refunds
-- overage already charged this period.
--
-- Born clean at slot 030. Companion docs update pending in
-- mirrorstack-docs/db/ms_billing/ (tables.md#account_overage_snapshots +
-- the accounts.overage_since column).

ALTER TABLE ms_billing.accounts
    ADD COLUMN overage_since TIMESTAMPTZ NULL;

COMMENT ON COLUMN ms_billing.accounts.overage_since IS
    'UTC instant the account-wide pooled SUM(module_count) over live apps first crossed the included 5 '
    '(account-wide overage grace anchor, owner spec 2026-07-05). NULL = not currently over the pool. '
    'Recomputed by RegisterApp / SyncAppModules; arms one 3-day grace timer per account.';

CREATE TABLE IF NOT EXISTS ms_billing.account_overage_snapshots (
    -- The billing account whose pooled overage this row froze. Cascade:
    -- dropping the account drops its overage charge history with it.
    account_id       UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,

    -- The FULL anchored billing-period window this overage charge covers.
    -- period_start is the display + double-charge lookup key (exact match from
    -- both the mid-period sweep and the boundary leg); a grace row keys on the
    -- full window's start even though its charged_micros covers only
    -- [grace-end, period_end).
    period_start     TIMESTAMPTZ NOT NULL,
    period_end       TIMESTAMPTZ NOT NULL,

    -- The pooled over-count the charge tiered on: SUM(module_count) over the
    -- account's live apps MINUS the included 5, snapshotted at charge time.
    over_count       INT NOT NULL CHECK (over_count >= 0),

    -- The pooled overage actually billed for this account-period, integer
    -- micro-dollars (NEVER float).
    charged_micros   BIGINT NOT NULL CHECK (charged_micros >= 0),

    -- Which leg billed it: the mid-period grace sweep ('grace', prorated from
    -- grace-end) or the boundary advance leg ('advance', full pooled overage).
    source           TEXT NOT NULL CHECK (source IN ('grace', 'advance')),

    -- CRASH-SAFE claim marker (PR #47 review fix — the cross-leg double-charge
    -- finding). 'pending' is written BEFORE the leg calls Stripe (the row is the
    -- durable "I am about to charge this period's overage" claim); 'charged' is
    -- written only AFTER Stripe actually created the invoice item/invoice. The
    -- OTHER leg (grace vs boundary) treats ANY row for the period — pending or
    -- charged — as claimed and must never independently charge it, closing the
    -- crash window where the ORIGINAL code only wrote this row AFTER Stripe
    -- succeeded (a crash between the two let the other leg see "no row" and
    -- double-charge under a disjoint Idempotency-Key namespace).
    status           TEXT NOT NULL CHECK (status IN ('pending', 'charged')),

    -- The Stripe invoice item id of the overage charge (empty/NULL while
    -- status='pending', or for a 0-cent rounded charge that recorded nothing)
    -- — audit trail only.
    invoice_item_id  TEXT NULL,

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One pooled-overage charge per account-period — the "charged exactly
    -- once" invariant, enforced at the ledger (the same guard shape as
    -- app_base_snapshots' PRIMARY KEY (app_id, period_start)).
    PRIMARY KEY (account_id, period_start)
);
