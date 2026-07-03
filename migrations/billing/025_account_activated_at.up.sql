-- Migration 025 — account billing-period ANCHOR (card-binding day).
--
-- Adds `ms_billing.accounts.activated_at`: the UTC instant an account bound its
-- FIRST credit card (its billing-account activation). This is the anchor for the
-- account's billing period — periods run from the activated_at DAY-OF-MONTH, not
-- the UTC calendar month and not the signup date (ADR 0005). Bound 6/2 → periods
-- [2nd 00:00 UTC, next month's 2nd 00:00 UTC).
--
-- Why a NEW column (not a reuse):
--   * accounts.created_at is WRONG — the row is inserted at the START of the
--     add-card flow (EnsureAccount), before Stripe confirms the card, and
--     persists even if the user abandons checkout.
--   * payment_methods_mirror.attached_at is EPHEMERAL — soft-deleted on detach;
--     a re-add mints a fresh pm_* with a fresh attached_at, so MIN(attached_at)
--     over active cards MOVES when the first card is swapped. A period anchor
--     must be immutable for the life of the relationship.
--   * ms_billing.subscriptions does not exist in v1 (no current_period_start to
--     anchor on).
--
-- Semantics: immutable, first-bind-wins. The webhook stamps it in the
-- payment_method.attached handler with `WHERE activated_at IS NULL` so it is set
-- exactly once and never regresses on detach/re-add. NULL = never activated (no
-- card ever bound) → skipped by cmd/billing-cycle. The billing-period anchor day
-- is derived in-process as activated_at.UTC().Day() ∈ [1..31]; a read that finds
-- NULL falls back to anchor day 1 (the UTC calendar month — the pre-025 behavior).
--
-- Born-clean: a pure additive nullable column (Postgres fast-default metadata
-- change, no table rewrite, no data lock) plus a one-time, forward-only backfill.
--
-- Spec: mirrorstack-docs/adr/0005-billing-period-anchor.md,
--       db/ms_billing/{README,tables,migrations}.md.

ALTER TABLE ms_billing.accounts
    ADD COLUMN activated_at TIMESTAMPTZ NULL;

COMMENT ON COLUMN ms_billing.accounts.activated_at IS
    'UTC instant the account bound its FIRST credit card (billing-account activation). '
    'Immutable, first-bind-wins; billing-period anchor day = activated_at day-of-month (ADR 0005). '
    'NULL = never activated -> skipped by cmd/billing-cycle.';

-- One-time, forward-only backfill: freeze each existing account's anchor from the
-- FIRST card it ever bound (MIN(attached_at) over the mirror). MIN(attached_at) is
-- only fragile as a LIVE anchor (it moves when the first card is swapped); frozen
-- ONCE into activated_at it becomes immutable, which is exactly the anchor
-- contract. Accounts with zero cards stay NULL (correctly un-activated). The
-- WHERE activated_at IS NULL guard keeps this idempotent under a down/up re-apply.
UPDATE ms_billing.accounts a
   SET activated_at = sub.first_attached
  FROM (
        SELECT account_id, MIN(attached_at) AS first_attached
          FROM ms_billing.payment_methods_mirror
         GROUP BY account_id
       ) sub
 WHERE a.id = sub.account_id
   AND a.activated_at IS NULL;
