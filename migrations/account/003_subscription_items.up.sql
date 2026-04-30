-- Subscription items: one row per Stripe SubscriptionItem.
--
-- Two flavors:
--   * kind = 'seat'  → quantity NOT NULL (per-seat licensing)
--   * kind = 'meter' → metric NOT NULL  (usage-based, quantity reported via meter events)
--
-- The CHECK constraint enforces the kind/shape pairing at the DB level so
-- a malformed row from a webhook handler can't slip through.
CREATE TABLE IF NOT EXISTS ms_billing_account.billing_subscription_items (
    id                            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id               UUID NOT NULL REFERENCES ms_billing_account.billing_subscriptions(id) ON DELETE CASCADE,
    kind                          TEXT NOT NULL CHECK (kind IN ('seat', 'meter')),
    stripe_subscription_item_id   TEXT NOT NULL UNIQUE,
    module_id                     TEXT,
    metric                        TEXT,
    quantity                      INT,
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT billing_subscription_items_kind_shape CHECK (
        (kind = 'seat'  AND quantity IS NOT NULL) OR
        (kind = 'meter' AND metric   IS NOT NULL)
    )
);

CREATE INDEX idx_billing_subscription_items_subscription ON ms_billing_account.billing_subscription_items (subscription_id);
CREATE INDEX idx_billing_subscription_items_module ON ms_billing_account.billing_subscription_items (module_id) WHERE module_id IS NOT NULL;
