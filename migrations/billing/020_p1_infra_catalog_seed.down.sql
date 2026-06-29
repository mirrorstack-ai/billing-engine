-- Down 020 — reverse the P1 infra catalog seed.
--
-- Deletes ONLY the nine sentinel-keyed metric_definitions rows seeded by 020.up
-- under the all-zero platform-infra module_id. Leaves the 017/018/019 rows
-- (compute/egress/AI) and every other row untouched. No schema change to revert.
--
-- A finance edit to a seeded row's price does NOT block this DELETE: down is a
-- full reversal of the seed by (module_id, metric), price-independent — the same
-- born-clean delete-by-key the 018 down uses for its AI rows.
DELETE FROM ms_billing.metric_definitions
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN (
      'infra.request.count',
      'infra.mcp.tool_call.count',
      'infra.cron.count',
      'infra.event.count',
      'infra.event.bytes',
      'infra.egress.api.bytes',
      'infra.storage.put.count',
      'infra.storage.list.count',
      'infra.storage.gib_hours'
  );
