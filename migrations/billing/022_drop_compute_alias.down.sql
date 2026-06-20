-- Down 022 — restore the deprecated infra.compute.ms alias row.
--
-- Re-inserts the sentinel-keyed alias as migration 019.up step (2) created it
-- (kind=sum, unit=millisecond, placeholder price 1 µ$, active) and pins the
-- display_group migration 021 backfilled onto it ('compute') in the same
-- statement, so down/up round-trips to the exact pre-022 catalog state (021
-- grouped both compute.ms and walltime.ms under 'compute'). display_group is
-- safe to set inline here: the column was added by 021, which is already
-- applied whenever 022 runs in either direction. UNIQUE(module_id, metric)
-- holds — 022.up DELETEd this slot, so the INSERT fills a now-empty slot
-- alongside the authoritative infra.compute.walltime.ms row (untouched by 022).
-- ON CONFLICT DO NOTHING keeps the down idempotent if the row already exists.
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, unit, unit_price_micros, active, display_group
) VALUES
    ('00000000-0000-0000-0000-000000000000', 'infra.compute.ms', 'sum', 'millisecond', 1, true, 'compute')
ON CONFLICT (module_id, metric) DO NOTHING;
