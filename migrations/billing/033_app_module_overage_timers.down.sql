-- Down for 033 — drop the per-module-instance overage timers and RESTORE
-- migration 032's account-wide pooled overage schema (overage_since column +
-- account_overage_snapshots ledger), so an up/down/up round-trip against 032 is
-- clean. The restored definitions are copied verbatim from 032's up.

DROP TABLE IF EXISTS ms_billing.app_module_overage_timers;

ALTER TABLE ms_billing.accounts
    ADD COLUMN IF NOT EXISTS overage_since TIMESTAMPTZ NULL;

COMMENT ON COLUMN ms_billing.accounts.overage_since IS
    'UTC instant the account-wide pooled SUM(module_count) over live apps first crossed the included 5 '
    '(account-wide overage grace anchor, owner spec 2026-07-05). NULL = not currently over the pool. '
    'Recomputed by RegisterApp / SyncAppModules; arms one 3-day grace timer per account.';

CREATE TABLE IF NOT EXISTS ms_billing.account_overage_snapshots (
    account_id       UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,
    period_start     TIMESTAMPTZ NOT NULL,
    period_end       TIMESTAMPTZ NOT NULL,
    over_count       INT NOT NULL CHECK (over_count >= 0),
    charged_micros   BIGINT NOT NULL CHECK (charged_micros >= 0),
    source           TEXT NOT NULL CHECK (source IN ('grace', 'advance')),
    status           TEXT NOT NULL CHECK (status IN ('pending', 'charged')),
    invoice_item_id  TEXT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, period_start)
);
