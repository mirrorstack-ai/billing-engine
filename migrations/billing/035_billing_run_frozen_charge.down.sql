-- Down migration 035 — drop the boundary-charge freeze columns. Both are pure
-- additive nullable columns (frozen per billing_run before its first Stripe
-- charge), so dropping them fully reverts the migration; up/down/up round-trips
-- cleanly.

ALTER TABLE ms_billing.billing_runs
    DROP COLUMN IF EXISTS frozen_charge_cents,
    DROP COLUMN IF EXISTS frozen_charge_with_base;
