-- 076: the plan-change ledger (core-v2#1412, billing-engine#202 PR-2b).
--
-- Migration 075 records WHICH plan an app is on. This table records every
-- CHANGE of plan and what it cost, because a change is a money event with two
-- shapes the owner decided on 2026-09-13:
--
--   * an UPGRADE takes effect at once and charges the DIFFERENCE between the
--     two bases for the remaining days of the current period — collected at
--     once on the account's rail (a wallet draw of what the wallet holds in
--     credits mode, the remainder on the card; refused only with no usable
--     card). Inside the creation grace it charges nothing separately: the
--     creation charge, when it runs, prices each day at the plan in force
--     that day (folded_into_creation);
--   * a DOWNGRADE is scheduled for the period boundary, cancellable for free
--     until then, with no refund; the boundary leg reads the plan in force at
--     the boundary.
--
-- 🔴 THE ROW IS THE IDEMPOTENCY KEY. cycle.SetAppPlan is a fire-and-forget RPC
-- api-platform retries; a retry finds the open row for the app and resumes
-- it rather than charging twice. There is at most ONE open change per app
-- (app_plan_changes_open_uidx). The wallet leg's ledger rows carry an
-- idempotency_key derived from this row's id, and the intent the card leg
-- seals has a digest derived from this row's amount and instant, so every
-- money step of a change is replayable against the same document.
--
-- Nothing here is a price. The amount is what the reviewed constants in
-- internal/account/usage/plans.go derived at the instant of the change, and it
-- is recorded so the customer's statement and this ledger agree even after a
-- later price change.
--
-- Expand-only; no existing row is touched.
CREATE TABLE IF NOT EXISTS ms_billing.app_plan_changes (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    app_id               UUID NOT NULL REFERENCES ms_billing.apps(app_id) ON DELETE CASCADE,
    -- The account that pays for the change: the app's account at the instant
    -- the change was opened. An app transferred later keeps this row's history
    -- on the account that was charged.
    account_id           UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,

    from_plan            TEXT NOT NULL
                         CONSTRAINT app_plan_changes_from_known CHECK (from_plan IN ('free', 'pro', 'business')),
    to_plan              TEXT NOT NULL
                         CONSTRAINT app_plan_changes_to_known CHECK (to_plan IN ('free', 'pro', 'business')),
    kind                 TEXT NOT NULL
                         CONSTRAINT app_plan_changes_kind_known CHECK (kind IN ('upgrade', 'downgrade')),

    -- When the owner asked, and when the plan (does / did) take effect: the
    -- request instant for an upgrade, the next period boundary for a
    -- downgrade.
    requested_at         TIMESTAMPTZ NOT NULL,
    effective_at         TIMESTAMPTZ NOT NULL,

    -- The anchored period the change was priced against: for an upgrade the
    -- period containing requested_at (the remaining days are its
    -- [requested day, period_end)); for a downgrade the period that ends at
    -- effective_at.
    period_start         TIMESTAMPTZ NOT NULL,
    period_end           TIMESTAMPTZ NOT NULL,

    -- An upgrade inside the creation grace charges nothing here: the creation
    -- charge prices the window split by day (usage.SegmentedProratedBaseMicros
    -- over this table's folded rows). Such a row is settled at 0 on insert.
    folded_into_creation BOOLEAN NOT NULL DEFAULT false,

    -- What the change costs (derived micros), and how each rail took it.
    -- wallet_micros is what the credit wallet actually drew (exact micros);
    -- card_micros is the whole-cent remainder sealed for the card
    -- (collectableMicros), so the two need not sum to amount_micros to the
    -- micro — the card leg rounds once, like every other leg.
    amount_micros        BIGINT NOT NULL CONSTRAINT app_plan_changes_amount_nonneg CHECK (amount_micros >= 0),
    wallet_micros        BIGINT NOT NULL DEFAULT 0 CONSTRAINT app_plan_changes_wallet_nonneg CHECK (wallet_micros >= 0),
    card_micros          BIGINT NOT NULL DEFAULT 0 CONSTRAINT app_plan_changes_card_nonneg CHECK (card_micros >= 0),
    -- The wallet decision is made ONCE (wallet_decided_at), even when it drew
    -- 0: the card intent's digest depends on the wallet allocation, so a
    -- reconciler that re-decided the wallet would seal a second document for
    -- the same change.
    wallet_decided_at    TIMESTAMPTZ NULL,
    -- 'intent:<digest>' once the card remainder is sealed on the intent rail.
    -- Never a provider invoice id: this leg holds no write port.
    card_ref             TEXT NULL,

    -- upgrade:   pending  → settled
    -- downgrade: scheduled → applied | cancelled
    -- A settled upgrade has every rail recorded; a pending one is what the
    -- reconciler (cycle.SweepPendingPlanChanges) finishes after a crash.
    status               TEXT NOT NULL
                         CONSTRAINT app_plan_changes_status_known
                         CHECK (status IN ('pending', 'settled', 'scheduled', 'applied', 'cancelled')),
    CONSTRAINT app_plan_changes_kind_status CHECK (
        (kind = 'upgrade'   AND status IN ('pending', 'settled'))
     OR (kind = 'downgrade' AND status IN ('scheduled', 'applied', 'cancelled'))
    ),
    settled_at           TIMESTAMPTZ NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE ms_billing.app_plan_changes IS
    'One row per plan change of one app (migration 076). An upgrade is charged its '
    'prorated difference at once (pending → settled); a downgrade waits for the period '
    'boundary (scheduled → applied | cancelled). The row is the idempotency key of every '
    'money step of the change.';

-- At most one OPEN change per app: a pending upgrade still being settled, or a
-- scheduled downgrade awaiting its boundary. A retry of SetAppPlan finds it;
-- a second, different request while one is open is refused.
CREATE UNIQUE INDEX IF NOT EXISTS app_plan_changes_open_uidx
    ON ms_billing.app_plan_changes (app_id)
    WHERE status IN ('pending', 'scheduled');

-- The boundary apply's work list: scheduled downgrades due at or before an
-- instant, per account.
CREATE INDEX IF NOT EXISTS app_plan_changes_due_idx
    ON ms_billing.app_plan_changes (account_id, effective_at)
    WHERE status = 'scheduled';

-- The reconciler's work list: upgrades whose money steps did not all commit.
CREATE INDEX IF NOT EXISTS app_plan_changes_pending_idx
    ON ms_billing.app_plan_changes (requested_at)
    WHERE status = 'pending';

-- The creation charge's read: this app's folded changes, in effect order.
CREATE INDEX IF NOT EXISTS app_plan_changes_app_effective_idx
    ON ms_billing.app_plan_changes (app_id, effective_at);

-- The permanent skip marker (migration 031) now also marks a creation window
-- that priced to NOTHING — a Free app's $0 base with no co-created overage —
-- so the sweep stops re-selecting it and co-created timers stop deferring to
-- it. Same column, same terminal meaning ("this app's creation period will
-- never be charged"), one more reason to reach it.
COMMENT ON COLUMN ms_billing.apps.proration_skipped_at IS
    'Set once (never unset) when ChargeCreationProration determines this app''s '
    'creation period will never be charged: the account only activated at/after the '
    'anchored creation period had already closed (a would-be retroactive catch-up, '
    'D1d), or the combined creation charge priced to nothing (a Free app with no '
    'co-created module overage, migration 076). The app is permanently excluded from '
    'the proration sweep from then on.';
