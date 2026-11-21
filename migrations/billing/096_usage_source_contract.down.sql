-- 096 down: back to migration 095's closed set (NULL or sandbox_trial). Added NOT
-- VALID, exactly as 095 left it: the CHECK refuses every new row outside the
-- narrower set, and a usage event already stored with another tag keeps its row
-- (it is ordinary usage, so it stays chargeable; the ledger is never rewritten).
ALTER TABLE ms_billing.usage_events
    DROP CONSTRAINT IF EXISTS usage_events_source_known;
ALTER TABLE ms_billing.usage_events
    ADD CONSTRAINT usage_events_source_known
        CHECK (source IS NULL OR source = 'sandbox_trial') NOT VALID;

COMMENT ON COLUMN ms_billing.usage_events.source IS
    'Migration 095: NULL (ordinary usage) or sandbox_trial (a capped trial run MirrorStack pays for, D441). Recorded and priced, never charged: the CHECK forces dev_served, so every charging filter excludes it, and only the sandbox budget counts it.';
