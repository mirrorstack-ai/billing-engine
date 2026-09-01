-- Migration 069: record WHICH provider object settled an intent.
--
-- 🔴 THE EXECUTOR RETURNS THE REFERENCE AND WRITES IT NOWHERE.
--
-- internal/intent/executor/executor.go returns Outcome{Reference:
-- result.Reference} — the Stripe invoice id the money moved through — and
-- RecordOutcomeWithEvidence stores only the outcome and its timestamp. The
-- evidence event's Detail is the literal string "succeeded". So after a
-- successful collection nothing in this database maps the provider's object
-- back to the sealed document that authorised it.
--
-- That is not only a reconciliation gap. It is the concrete blocker for §6's
-- `collect_receivable`: a receivable is CollectRemainderOf(source) and links to
-- a SOURCE INTENT, so retrying an unpaid invoice requires knowing which intent
-- raised it. Without this column that question has no answer, and the
-- unpaid-retry leg cannot be routed at all.
--
-- Nullable, because it is genuinely absent in two legitimate states: a claim in
-- flight has no outcome yet, and a refusal never reached a provider. A NOT NULL
-- with a sentinel would make "no provider object exists" indistinguishable from
-- "we failed to record one".
ALTER TABLE ms_billing.intent_settlement_claims
    ADD COLUMN IF NOT EXISTS provider_reference TEXT;

-- A settled claim must name what settled it. Enforced as a CHECK rather than
-- left to the writer, because the writer is exactly what got this wrong: the
-- reference was already flowing through the call and simply not persisted.
--
-- 'voided' and 'failed' are exempt: a void may precede any provider object, and
-- a failure may be a refusal at the rail rather than an answer from it.
ALTER TABLE ms_billing.intent_settlement_claims
    DROP CONSTRAINT IF EXISTS intent_settlement_claims_succeeded_names_its_object;

ALTER TABLE ms_billing.intent_settlement_claims
    ADD CONSTRAINT intent_settlement_claims_succeeded_names_its_object
        CHECK (outcome IS DISTINCT FROM 'succeeded' OR provider_reference IS NOT NULL)
        NOT VALID;

-- NOT VALID, deliberately. Rows settled before this migration have no
-- reference and never will — the information was discarded at the time and
-- cannot be recovered from this side. Validating would fail the migration on
-- history nobody can fix. New rows are checked; old ones are left as the
-- honest record that this was not being written down.
--
-- 🔴 Do NOT "fix" this by backfilling a placeholder. A row claiming a provider
-- object that does not exist is worse than a row admitting it has none.

CREATE INDEX IF NOT EXISTS intent_settlement_claims_provider_reference_idx
    ON ms_billing.intent_settlement_claims (provider_reference)
    WHERE provider_reference IS NOT NULL;

COMMENT ON COLUMN ms_billing.intent_settlement_claims.provider_reference IS
    'The provider object the money moved through, so a receivable can link to the intent that raised an unpaid invoice and a reconciler can walk provider -> document.';
