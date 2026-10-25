-- Down 092 — remove ONLY the 5.x roster price rows the up seeded.
--
-- Mirrors 042's down style: undo ONLY what 092 up did. Haiku 4.5, Sonnet 4.6,
-- Sonnet 5 and every other row are untouched.
DELETE FROM ms_billing.metric_model_prices
WHERE model IN (
    'anthropic.claude-haiku-5-5',
    'anthropic.claude-sonnet-5-5',
    'anthropic.claude-opus-5-5',
    'anthropic.claude-fable-5-1'
);
