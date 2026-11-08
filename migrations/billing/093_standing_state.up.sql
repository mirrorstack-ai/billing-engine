-- 093: last observed standing verdict per owner principal (billing-engine#331).
--
-- The standing notifier (internal/account/standing) fires on EVERY standing
-- event and stores no previous verdict, so it cannot tell a transition from a
-- repeat. This table is that memory: one row per owner principal, written by
-- the notifier BEFORE it pushes, so "eligible -> blocked" is detected exactly
-- once even when two webhooks race (the notifier row-locks the owner's row).
--
-- owner_key is 'user:<uuid>' or 'org:<uuid>' (the soft-FK principal, like
-- accounts.owner_user_id / owner_org_id: nothing outside ms_billing is read).
-- changed_at moves ONLY when blocked flips; a reason change while still
-- blocked updates reason and updated_at but is not a new transition.
CREATE TABLE ms_billing.standing_state (
    owner_key  text        PRIMARY KEY,
    blocked    boolean     NOT NULL,
    reason     text        NOT NULL,
    changed_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT standing_state_owner_key_shape
        CHECK (owner_key ~ '^(user|org):[0-9a-f-]{36}$')
);
