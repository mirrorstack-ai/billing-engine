-- Down 042 — reverse the AI roster refresh: re-activate Sonnet 4.6, drop Sonnet 5.
--
-- Mirrors 018's down style: undo ONLY what 042 up did.
--   * re-activate the Sonnet 4.6 rows the up deactivated,
--   * DELETE the Sonnet 5 rows the up seeded.
-- Leaves Haiku 4.5 and every other pre-042 row untouched.

-- 1) Restore the pre-042 Sonnet 4.6 price rows to active.
UPDATE ms_billing.metric_model_prices
SET active = true
WHERE model = 'anthropic.claude-sonnet-4-6';

-- 2) Remove the Sonnet 5 seed rows.
DELETE FROM ms_billing.metric_model_prices
WHERE model = 'anthropic.claude-sonnet-5';
