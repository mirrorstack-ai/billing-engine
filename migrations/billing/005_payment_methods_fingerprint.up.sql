-- Migration 005 — fingerprint column on payment_methods_mirror.
--
-- Stripe issues a fresh `pm_*` ID for every setup_intent confirm — even
-- when the customer enters the same card a second time. The mirror's
-- previous "same stripe_payment_method_id?" duplicate check therefore
-- never fired in practice and every re-add looked like a brand-new card.
--
-- `card.fingerprint` is Stripe's canonical "same card" identifier across
-- PaymentMethod IDs: distinct cards have distinct fingerprints; the same
-- card re-attached produces the same fingerprint. Storing it on the
-- mirror lets `ResolvePendingAddCardRequest` decide between
-- 'completed' (no other active row with this fingerprint on the account)
-- and 'duplicate' (an existing active row already covers this card).
--
-- Nullable on purpose: pre-migration rows have no fingerprint and must
-- stay queryable; the dedupe predicate filters `fingerprint IS NOT NULL`
-- so legacy rows never collide with new attempts.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#payment_methods_mirror

ALTER TABLE ms_billing.payment_methods_mirror
    ADD COLUMN IF NOT EXISTS fingerprint TEXT NULL;

-- Hot-path predicate for dedupe lookup: "is there another active card on
-- this account with the same fingerprint?" The partial index ignores
-- soft-deleted rows and legacy (fingerprint IS NULL) rows.
CREATE INDEX IF NOT EXISTS pmm_account_fingerprint_active_idx
    ON ms_billing.payment_methods_mirror (account_id, fingerprint)
    WHERE deleted_at IS NULL AND fingerprint IS NOT NULL;
