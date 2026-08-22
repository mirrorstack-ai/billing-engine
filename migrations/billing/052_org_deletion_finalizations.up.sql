-- Migration 052 — immutable organization billing retirement.
--
-- Organization deletion is finalized by billing-engine only after the
-- control plane has torn down applications and domains.  Financial history
-- stays where it is (accounts, invoices, usage, payments and ledger rows),
-- while this tombstone permanently prevents a late retry from re-creating
-- funding or billable resources for the deleted organization.

CREATE TABLE IF NOT EXISTS ms_billing.org_deletion_finalizations (
    org_id       UUID PRIMARY KEY,
    operation_id UUID NOT NULL,
    finalized_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT org_deletion_finalizations_operation_key UNIQUE (operation_id),
    CONSTRAINT org_deletion_finalizations_org_operation_key
        UNIQUE (org_id, operation_id)
);

COMMENT ON TABLE ms_billing.org_deletion_finalizations IS
    'Immutable operation-keyed tombstone for a fully retired organization billing principal; financial history is retained in its original tables.';

-- Outbound sponsorship is active state, so finalization removes the live
-- designation. Keep the exact former edge as immutable history. Besides audit,
-- this closes the stale-candidate gap: a charge worker that resolved the old
-- sponsor before deletion cannot silently fall back to the customer account
-- after the live edge disappears. A new live designation supersedes this
-- retired edge for future work.
CREATE TABLE IF NOT EXISTS ms_billing.org_deletion_retired_sponsorships (
    retired_sponsor_org_id UUID NOT NULL,
    customer_org_id        UUID NOT NULL,
    sponsor_account_id     UUID NOT NULL,
    operation_id           UUID NOT NULL,
    retired_at             TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (retired_sponsor_org_id, customer_org_id),
    CONSTRAINT org_deletion_retired_sponsorships_operation_fkey
        FOREIGN KEY (retired_sponsor_org_id, operation_id)
        REFERENCES ms_billing.org_deletion_finalizations(org_id, operation_id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX org_deletion_retired_sponsorships_customer_idx
    ON ms_billing.org_deletion_retired_sponsorships (customer_org_id);

COMMENT ON TABLE ms_billing.org_deletion_retired_sponsorships IS
    'Immutable audit of live outbound sponsor designations removed by organization billing retirement. Charge authority is carried by the generation-pinned attempt markers below, never inferred from this history.';

-- One rotating authorization per billing account.  It is deliberately
-- separate from org_billing_designations: deleting a designation rotates an
-- org account back to self-funding instead of erasing the generation needed
-- to reject an old in-memory charge candidate.  A charge arm reads this row
-- and writes the exact (generation, funding account) onto its durable
-- pre-Stripe marker in one statement.
CREATE TABLE IF NOT EXISTS ms_billing.account_funding_authorizations (
    account_id         UUID PRIMARY KEY
                       REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,
    generation         UUID NOT NULL DEFAULT gen_random_uuid(),
    funding_account_id UUID NOT NULL
                       REFERENCES ms_billing.accounts(id) ON DELETE RESTRICT,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX account_funding_authorizations_funder_idx
    ON ms_billing.account_funding_authorizations (funding_account_id, account_id);

COMMENT ON TABLE ms_billing.account_funding_authorizations IS
    'Rotating funding authority for atomic pre-Stripe charge arms. Every designation mutation creates a new generation; durable attempts retain the exact generation and funder they armed under.';

INSERT INTO ms_billing.account_funding_authorizations (
    account_id,
    funding_account_id
)
SELECT account.id,
       CASE
           WHEN account.owner_kind = 'org'
                AND designation.funding = 'sponsor'
               THEN designation.sponsor_account_id
           ELSE account.id
       END
FROM ms_billing.accounts account
LEFT JOIN ms_billing.org_billing_designations designation
  ON account.owner_kind = 'org'
 AND designation.org_id = account.owner_org_id
ON CONFLICT (account_id) DO NOTHING;

CREATE OR REPLACE FUNCTION ms_billing.sync_account_funding_authorization()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_funding_account_id UUID;
BEGIN
    IF NEW.owner_kind = 'org' THEN
        SELECT CASE
                   WHEN designation.funding = 'sponsor'
                       THEN designation.sponsor_account_id
                   ELSE NEW.id
               END
          INTO v_funding_account_id
          FROM (SELECT 1) singleton
          LEFT JOIN ms_billing.org_billing_designations designation
            ON designation.org_id = NEW.owner_org_id;
    ELSE
        v_funding_account_id := NEW.id;
    END IF;

    INSERT INTO ms_billing.account_funding_authorizations (
        account_id,
        generation,
        funding_account_id,
        updated_at
    ) VALUES (
        NEW.id,
        gen_random_uuid(),
        COALESCE(v_funding_account_id, NEW.id),
        now()
    )
    ON CONFLICT (account_id) DO UPDATE SET
        generation = gen_random_uuid(),
        funding_account_id = EXCLUDED.funding_account_id,
        updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER accounts_sync_funding_authorization
AFTER INSERT OR UPDATE OF owner_kind, owner_org_id ON ms_billing.accounts
FOR EACH ROW EXECUTE FUNCTION ms_billing.sync_account_funding_authorization();

CREATE OR REPLACE FUNCTION ms_billing.sync_org_designation_funding_authorization()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_org_id UUID;
    v_funding_account_id UUID;
BEGIN
    v_org_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.org_id ELSE NEW.org_id END;
    IF TG_OP = 'DELETE' THEN
        v_funding_account_id := NULL;
    ELSIF NEW.funding <> 'sponsor' THEN
        v_funding_account_id := NULL;
    ELSE
        v_funding_account_id := NEW.sponsor_account_id;
    END IF;

    INSERT INTO ms_billing.account_funding_authorizations (
        account_id,
        generation,
        funding_account_id,
        updated_at
    )
    SELECT account.id,
           gen_random_uuid(),
           COALESCE(v_funding_account_id, account.id),
           now()
    FROM ms_billing.accounts account
    WHERE account.owner_kind = 'org'
      AND account.owner_org_id = v_org_id
    ON CONFLICT (account_id) DO UPDATE SET
        generation = EXCLUDED.generation,
        funding_account_id = EXCLUDED.funding_account_id,
        updated_at = EXCLUDED.updated_at;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER org_designations_sync_funding_authorization
AFTER INSERT OR UPDATE OR DELETE ON ms_billing.org_billing_designations
FOR EACH ROW EXECUTE FUNCTION ms_billing.sync_org_designation_funding_authorization();

-- Persist the exact funder on every durable Stripe-attempt marker.  Recovery
-- never follows the mutable live designation again.  A sponsor finalizer can
-- therefore count the claims that really authorize its Stripe customer while
-- an unarmed customer candidate is free to move to a new designation.
ALTER TABLE ms_billing.billing_runs
    ADD COLUMN IF NOT EXISTS charge_funding_account_id UUID NULL
        REFERENCES ms_billing.accounts(id) ON DELETE RESTRICT,
    ADD COLUMN IF NOT EXISTS charge_funding_generation UUID NULL,
    ADD COLUMN IF NOT EXISTS charge_funding_legacy_unresolved BOOLEAN NOT NULL
        DEFAULT false;

ALTER TABLE ms_billing.app_combined_proration_attempts
    ADD COLUMN IF NOT EXISTS charge_funding_account_id UUID NULL
        REFERENCES ms_billing.accounts(id) ON DELETE RESTRICT,
    ADD COLUMN IF NOT EXISTS charge_funding_generation UUID NULL,
    ADD COLUMN IF NOT EXISTS charge_funding_legacy_unresolved BOOLEAN NOT NULL
        DEFAULT false;

ALTER TABLE ms_billing.app_custom_domains
    ADD COLUMN IF NOT EXISTS charge_funding_account_id UUID NULL
        REFERENCES ms_billing.accounts(id) ON DELETE RESTRICT,
    ADD COLUMN IF NOT EXISTS charge_funding_generation UUID NULL,
    ADD COLUMN IF NOT EXISTS charge_funding_legacy_unresolved BOOLEAN NOT NULL
        DEFAULT false;

ALTER TABLE ms_billing.app_module_overage_timers
    ADD COLUMN IF NOT EXISTS charge_funding_account_id UUID NULL
        REFERENCES ms_billing.accounts(id) ON DELETE RESTRICT,
    ADD COLUMN IF NOT EXISTS charge_funding_generation UUID NULL,
    ADD COLUMN IF NOT EXISTS charge_funding_legacy_unresolved BOOLEAN NOT NULL
        DEFAULT false;

-- Terminal invoice mirrors retain the same payer provenance as their durable
-- attempt. Stripe can keep retrying an open charge_automatically invoice after
-- our local attempt resolves, so sponsor retirement must continue to count the
-- exact funder until that invoice is no longer collectible.
ALTER TABLE ms_billing.invoices
    ADD COLUMN IF NOT EXISTS charge_funding_account_id UUID NULL
        REFERENCES ms_billing.accounts(id) ON DELETE RESTRICT,
    ADD COLUMN IF NOT EXISTS charge_funding_generation UUID NULL,
    ADD COLUMN IF NOT EXISTS charge_funding_legacy_unresolved BOOLEAN NOT NULL
        DEFAULT false;

-- Manual wallet purchases are another Stripe invoice rail.  The pre-052
-- schema allowed their durable ledger row to be created before the payer was
-- selected, and kept the Stripe customer only in process memory.  Preserve
-- the exact payer generation and customer before the first Stripe request.
-- Auto-top-ups already freeze their Stripe customer/payment method when their
-- row is inserted, so these authorization columns intentionally belong only
-- to type='purchase'.
ALTER TABLE ms_billing.credit_ledger
    ADD COLUMN IF NOT EXISTS charge_funding_account_id UUID NULL
        REFERENCES ms_billing.accounts(id) ON DELETE RESTRICT,
    ADD COLUMN IF NOT EXISTS charge_funding_generation UUID NULL,
    ADD COLUMN IF NOT EXISTS charge_funding_legacy_unresolved BOOLEAN NOT NULL
        DEFAULT false;

-- Migration 049 reserved attempt_stripe_customer_id exclusively for automatic
-- top-ups.  Manual purchases now use the same immutable customer snapshot,
-- while the PM and expiry snapshots remain auto-top-up-only.
ALTER TABLE ms_billing.credit_ledger
    DROP CONSTRAINT IF EXISTS credit_ledger_auto_topup_attempt_fields_check;

ALTER TABLE ms_billing.credit_ledger
    ADD CONSTRAINT credit_ledger_auto_topup_attempt_fields_check CHECK (
        (
            type = 'auto_topup'
            AND attempt_payment_method_id IS NOT NULL
            AND NULLIF(BTRIM(attempt_stripe_payment_method_id), '') IS NOT NULL
            AND NULLIF(BTRIM(attempt_stripe_customer_id), '') IS NOT NULL
            AND attempt_expires_at IS NOT NULL
        )
        OR (
            type = 'purchase'
            AND attempt_payment_method_id IS NULL
            AND attempt_stripe_payment_method_id IS NULL
            AND attempt_expires_at IS NULL
            AND failure_code IS NULL
        )
        OR (
            type NOT IN ('auto_topup', 'purchase')
            AND attempt_payment_method_id IS NULL
            AND attempt_stripe_payment_method_id IS NULL
            AND attempt_stripe_customer_id IS NULL
            AND attempt_expires_at IS NULL
            AND failure_code IS NULL
        )
    );

-- An already-attached pre-052 purchase may have crossed Stripe, but the local
-- row cannot prove which historical designation supplied its Customer. Never
-- backfill that from today's designation: quarantine it for reconciliation.
UPDATE ms_billing.credit_ledger
SET charge_funding_legacy_unresolved = true
WHERE type = 'purchase'
  AND stripe_invoice_id IS NOT NULL
  AND charge_funding_account_id IS NULL;

ALTER TABLE ms_billing.credit_ledger
    ADD CONSTRAINT credit_ledger_purchase_funding_arm_check CHECK (
        (
            type = 'purchase'
            AND charge_funding_legacy_unresolved
            AND stripe_invoice_id IS NOT NULL
            AND charge_funding_account_id IS NULL
            AND charge_funding_generation IS NULL
            AND attempt_stripe_customer_id IS NULL
        )
        OR (
            type = 'purchase'
            AND NOT charge_funding_legacy_unresolved
            AND (
                (
                    stripe_invoice_id IS NULL
                    AND charge_funding_account_id IS NULL
                    AND charge_funding_generation IS NULL
                    AND attempt_stripe_customer_id IS NULL
                )
                OR (
                    charge_funding_account_id IS NOT NULL
                    AND charge_funding_generation IS NOT NULL
                    AND NULLIF(BTRIM(attempt_stripe_customer_id), '') IS NOT NULL
                )
            )
        )
        OR (
            type <> 'purchase'
            AND NOT charge_funding_legacy_unresolved
            AND charge_funding_account_id IS NULL
            AND charge_funding_generation IS NULL
        )
    );

-- Pre-052 invoices do not carry Stripe Customer provenance. Quarantine rather
-- than guessing from today's designation; org finalization treats a collectible
-- quarantined invoice as a global legacy barrier until operations settles it or
-- verifies/stamps its historical payer.
UPDATE ms_billing.invoices
SET charge_funding_legacy_unresolved = true
WHERE charge_funding_account_id IS NULL;

ALTER TABLE ms_billing.invoices
    ADD CONSTRAINT invoices_funding_provenance_check CHECK (
        (
            charge_funding_legacy_unresolved
            AND charge_funding_account_id IS NULL
            AND charge_funding_generation IS NULL
        )
        OR
        (
            NOT charge_funding_legacy_unresolved
            AND charge_funding_account_id IS NOT NULL
            AND charge_funding_generation IS NOT NULL
        )
    );

-- A pre-052 marker does not prove which designation generation actually
-- crossed Stripe. Never invent that provenance from the current live edge:
-- the customer may have changed sponsors since the marker was written. Keep
-- these rows reachable but explicitly quarantined; every recovery reader fails
-- closed on the absent pinned funder until operations verifies/resolves it.
UPDATE ms_billing.billing_runs
SET charge_funding_legacy_unresolved = true
WHERE frozen_charge_cents IS NOT NULL
  AND charge_funding_account_id IS NULL;

UPDATE ms_billing.app_combined_proration_attempts
SET charge_funding_legacy_unresolved = true
WHERE charge_funding_account_id IS NULL;

UPDATE ms_billing.app_custom_domains
SET charge_funding_legacy_unresolved = true
WHERE charge_attempted_at IS NOT NULL
  AND charge_funding_account_id IS NULL;

UPDATE ms_billing.app_module_overage_timers
SET charge_funding_legacy_unresolved = true
WHERE charge_attempted_at IS NOT NULL
  AND charge_funding_account_id IS NULL;

ALTER TABLE ms_billing.billing_runs
    ADD CONSTRAINT billing_runs_funding_arm_check CHECK (
        (
            charge_funding_legacy_unresolved
            AND frozen_charge_cents IS NOT NULL
            AND charge_funding_account_id IS NULL
            AND charge_funding_generation IS NULL
        )
        OR
        (
            NOT charge_funding_legacy_unresolved
            AND (
                (charge_funding_account_id IS NULL AND charge_funding_generation IS NULL)
                OR
                (charge_funding_account_id IS NOT NULL AND charge_funding_generation IS NOT NULL)
            )
        )
    ),
    ADD CONSTRAINT billing_runs_frozen_funding_check CHECK (
        frozen_charge_cents IS NULL
        OR charge_funding_account_id IS NOT NULL
        OR charge_funding_legacy_unresolved
    );

ALTER TABLE ms_billing.app_combined_proration_attempts
    ADD CONSTRAINT combined_proration_funding_arm_check CHECK (
        (
            charge_funding_legacy_unresolved
            AND charge_funding_account_id IS NULL
            AND charge_funding_generation IS NULL
        )
        OR
        (
            NOT charge_funding_legacy_unresolved
            AND charge_funding_account_id IS NOT NULL
            AND charge_funding_generation IS NOT NULL
        )
    );

ALTER TABLE ms_billing.app_custom_domains
    ADD CONSTRAINT custom_domains_funding_arm_check CHECK (
        (
            charge_funding_legacy_unresolved
            AND charge_attempted_at IS NOT NULL
            AND charge_funding_account_id IS NULL
            AND charge_funding_generation IS NULL
        )
        OR
        (
            NOT charge_funding_legacy_unresolved
            AND (
                (charge_funding_account_id IS NULL AND charge_funding_generation IS NULL)
                OR
                (charge_funding_account_id IS NOT NULL AND charge_funding_generation IS NOT NULL)
            )
        )
    ),
    ADD CONSTRAINT custom_domains_attempt_funding_check CHECK (
        charge_attempted_at IS NULL
        OR charge_funding_account_id IS NOT NULL
        OR charge_funding_legacy_unresolved
    );

ALTER TABLE ms_billing.app_module_overage_timers
    ADD CONSTRAINT module_timers_funding_arm_check CHECK (
        (
            charge_funding_legacy_unresolved
            AND charge_attempted_at IS NOT NULL
            AND charge_funding_account_id IS NULL
            AND charge_funding_generation IS NULL
        )
        OR
        (
            NOT charge_funding_legacy_unresolved
            AND (
                (charge_funding_account_id IS NULL AND charge_funding_generation IS NULL)
                OR
                (charge_funding_account_id IS NOT NULL AND charge_funding_generation IS NOT NULL)
            )
        )
    ),
    ADD CONSTRAINT module_timers_attempt_funding_check CHECK (
        charge_attempted_at IS NULL
        OR charge_funding_account_id IS NOT NULL
        OR charge_funding_legacy_unresolved
    );

CREATE INDEX billing_runs_unresolved_funder_idx
    ON ms_billing.billing_runs (charge_funding_account_id)
    WHERE charge_funding_account_id IS NOT NULL AND status <> 'invoiced';
CREATE INDEX combined_proration_unresolved_funder_idx
    ON ms_billing.app_combined_proration_attempts (charge_funding_account_id)
    WHERE resolved_at IS NULL;
CREATE INDEX custom_domains_unresolved_funder_idx
    ON ms_billing.app_custom_domains (charge_funding_account_id)
    WHERE charge_attempted_at IS NOT NULL AND charge_resolved = false;
CREATE INDEX module_timers_unresolved_funder_idx
    ON ms_billing.app_module_overage_timers (charge_funding_account_id)
    WHERE charge_attempted_at IS NOT NULL AND grace_resolved = false;
CREATE INDEX invoices_collectible_funder_idx
    ON ms_billing.invoices (charge_funding_account_id)
    WHERE status IN ('open', 'uncollectible') AND amount_due > 0;
CREATE INDEX credit_ledger_pending_purchase_funder_idx
    ON ms_billing.credit_ledger (charge_funding_account_id)
    WHERE type = 'purchase' AND status = 'pending';

CREATE OR REPLACE FUNCTION ms_billing.reject_org_deletion_finalization_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'organization deletion finalizations are immutable'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER org_deletion_finalizations_immutable
BEFORE UPDATE OR DELETE ON ms_billing.org_deletion_finalizations
FOR EACH ROW EXECUTE FUNCTION ms_billing.reject_org_deletion_finalization_mutation();

CREATE TRIGGER org_deletion_retired_sponsorships_immutable
BEFORE UPDATE OR DELETE ON ms_billing.org_deletion_retired_sponsorships
FOR EACH ROW EXECUTE FUNCTION ms_billing.reject_org_deletion_finalization_mutation();

-- Writers take a SHARED transaction-scoped lifecycle lock. Independent
-- metering and reconciliation for the same organization remain concurrent,
-- while FinalizeOrgDeletion takes the EXCLUSIVE form of the same key and waits
-- for every earlier writer before its final obligation checks.
CREATE OR REPLACE FUNCTION ms_billing.lock_org_billing_lifecycle_shared_many(
    p_org_ids UUID[]
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_org_id UUID;
BEGIN
    FOR v_org_id IN
        SELECT DISTINCT org_id
        FROM unnest(p_org_ids) AS requested(org_id)
        WHERE org_id IS NOT NULL
        ORDER BY org_id
    LOOP
        PERFORM pg_advisory_xact_lock_shared(
            hashtextextended('ms_billing.org.lifecycle:' || v_org_id::text, 0)
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION ms_billing.assert_org_billing_active_many(
    p_org_ids UUID[]
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_retired_org_id UUID;
BEGIN
    PERFORM ms_billing.lock_org_billing_lifecycle_shared_many(p_org_ids);

    SELECT finalization.org_id
      INTO v_retired_org_id
        FROM ms_billing.org_deletion_finalizations finalization
       WHERE finalization.org_id = ANY(p_org_ids)
       ORDER BY finalization.org_id
       LIMIT 1;
    IF v_retired_org_id IS NOT NULL THEN
        RAISE EXCEPTION 'organization billing principal % is retired', v_retired_org_id
            USING ERRCODE = '55000';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION ms_billing.assert_org_billing_active(p_org_id UUID)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM ms_billing.assert_org_billing_active_many(ARRAY[p_org_id]);
END;
$$;

CREATE OR REPLACE FUNCTION ms_billing.assert_org_billing_active_pair(
    p_first_org_id UUID,
    p_second_org_id UUID
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM ms_billing.assert_org_billing_active_many(
        ARRAY[p_first_org_id, p_second_org_id]
    );
END;
$$;

-- Resolve every organization whose lifecycle authorizes writes for an
-- account. The account owner is always included. The rotating authorization
-- contributes the exact current funding account's organization. Historical
-- retired edges are audit only: treating them as present authority both blocks
-- deletion of the customer and becomes bypassable after re-designation.
CREATE OR REPLACE FUNCTION ms_billing.account_org_billing_lifecycle_orgs(
    p_account_id UUID
)
RETURNS UUID[]
LANGUAGE sql
STABLE
AS $$
    SELECT ARRAY(
        SELECT DISTINCT lifecycle_org_id
        FROM (
            SELECT account.owner_org_id AS lifecycle_org_id
            FROM ms_billing.accounts account
            WHERE account.id = p_account_id
              AND account.owner_kind = 'org'

            UNION ALL

            SELECT funding.owner_org_id
            FROM ms_billing.accounts account
            JOIN ms_billing.account_funding_authorizations funding_auth
              ON funding_auth.account_id = account.id
            JOIN ms_billing.accounts funding
              ON funding.id = funding_auth.funding_account_id
             AND funding.owner_kind = 'org'
            WHERE account.id = p_account_id
        ) lifecycle
        WHERE lifecycle_org_id IS NOT NULL
        ORDER BY lifecycle_org_id
    );
$$;

CREATE OR REPLACE FUNCTION ms_billing.lock_account_org_billing_lifecycle_shared(
    p_account_id UUID
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM ms_billing.lock_org_billing_lifecycle_shared_many(
        ms_billing.account_org_billing_lifecycle_orgs(p_account_id)
    );
END;
$$;

CREATE OR REPLACE FUNCTION ms_billing.assert_account_org_billing_active(p_account_id UUID)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM ms_billing.assert_org_billing_active_many(
        ms_billing.account_org_billing_lifecycle_orgs(p_account_id)
    );
END;
$$;

CREATE OR REPLACE FUNCTION ms_billing.assert_funding_account_org_billing_active(
    p_funding_account_id UUID
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_funding_org_id UUID;
BEGIN
    SELECT owner_org_id
      INTO v_funding_org_id
      FROM ms_billing.accounts
     WHERE id = p_funding_account_id
       AND owner_kind = 'org';
    IF v_funding_org_id IS NOT NULL THEN
        PERFORM ms_billing.assert_org_billing_active(v_funding_org_id);
    END IF;
END;
$$;

-- Attempt recovery can retain an exact funder that is no longer the account's
-- current designation. Lock the owner, current authorization, and durable
-- funder as one sorted set so concurrent sponsor/customer finalizers cannot
-- deadlock on different acquisition orders.
CREATE OR REPLACE FUNCTION ms_billing.account_and_funding_lifecycle_orgs(
    p_account_id UUID,
    p_funding_account_id UUID
)
RETURNS UUID[]
LANGUAGE sql
STABLE
AS $$
    SELECT ARRAY(
        SELECT DISTINCT lifecycle_org_id
        FROM (
            SELECT unnest(
                ms_billing.account_org_billing_lifecycle_orgs(p_account_id)
            ) AS lifecycle_org_id

            UNION ALL

            SELECT funding.owner_org_id
            FROM ms_billing.accounts funding
            WHERE funding.id = p_funding_account_id
              AND funding.owner_kind = 'org'
        ) lifecycle
        WHERE lifecycle_org_id IS NOT NULL
        ORDER BY lifecycle_org_id
    );
$$;

CREATE OR REPLACE FUNCTION ms_billing.lock_account_and_funding_lifecycle_shared(
    p_account_id UUID,
    p_funding_account_id UUID
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM ms_billing.lock_org_billing_lifecycle_shared_many(
        ms_billing.account_and_funding_lifecycle_orgs(
            p_account_id,
            p_funding_account_id
        )
    );
END;
$$;

CREATE OR REPLACE FUNCTION ms_billing.assert_account_and_funding_billing_active(
    p_account_id UUID,
    p_funding_account_id UUID
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM ms_billing.assert_org_billing_active_many(
        ms_billing.account_and_funding_lifecycle_orgs(
            p_account_id,
            p_funding_account_id
        )
    );
END;
$$;

-- Reconciliation callers that must become a retired no-op (rather than an
-- error) use this predicate. It waits on the same shared barrier, then reads
-- the post-wait tombstone state from inside this VOLATILE function.
CREATE OR REPLACE FUNCTION ms_billing.account_org_billing_active_after_shared_lock(
    p_account_id UUID
)
RETURNS BOOLEAN
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_org_ids UUID[];
BEGIN
    v_org_ids := ms_billing.account_org_billing_lifecycle_orgs(p_account_id);
    PERFORM ms_billing.lock_org_billing_lifecycle_shared_many(v_org_ids);
    RETURN NOT EXISTS (
        SELECT 1
        FROM ms_billing.org_deletion_finalizations finalization
        WHERE finalization.org_id = ANY(v_org_ids)
    );
END;
$$;

-- Funding designation writes are the primary resurrection path.
CREATE OR REPLACE FUNCTION ms_billing.guard_org_designation_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_old_sponsor_org_id UUID;
    v_new_sponsor_org_id UUID;
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.sponsor_account_id IS NOT NULL THEN
        SELECT owner_org_id INTO v_old_sponsor_org_id
        FROM ms_billing.accounts
        WHERE id = OLD.sponsor_account_id AND owner_kind = 'org';
    END IF;
    IF NEW.sponsor_account_id IS NOT NULL THEN
        SELECT owner_org_id INTO v_new_sponsor_org_id
        FROM ms_billing.accounts
        WHERE id = NEW.sponsor_account_id AND owner_kind = 'org';
    END IF;

    PERFORM ms_billing.assert_org_billing_active_many(ARRAY[
        CASE WHEN TG_OP = 'UPDATE' THEN OLD.org_id ELSE NULL END,
        NEW.org_id,
        v_old_sponsor_org_id,
        v_new_sponsor_org_id
    ]);
    RETURN NEW;
END;
$$;

CREATE TRIGGER org_designations_reject_retired
BEFORE INSERT OR UPDATE ON ms_billing.org_billing_designations
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_org_designation_write();

-- A deleted org may keep its account for history, but it must never gain a
-- fresh Stripe identity or activation anchor after retirement.  Harmless
-- bookkeeping updates (including webhook-owned updated_at changes) remain
-- legal so retained history can continue reconciling.
CREATE OR REPLACE FUNCTION ms_billing.guard_org_account_reactivation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.owner_kind = 'org' THEN
            PERFORM ms_billing.assert_org_billing_active(NEW.owner_org_id);
        END IF;
    ELSIF NEW.owner_kind IS DISTINCT FROM OLD.owner_kind
        OR NEW.owner_user_id IS DISTINCT FROM OLD.owner_user_id
        OR NEW.owner_org_id IS DISTINCT FROM OLD.owner_org_id THEN
        -- Ownership changes are not an ordinary write path, but lock/check
        -- both sides so a retired account cannot be moved out and revived or
        -- an active account moved into a retired organization.
        IF OLD.owner_kind = 'org' THEN
            PERFORM ms_billing.assert_account_org_billing_active(OLD.id);
        END IF;
        IF NEW.owner_kind = 'org' THEN
            PERFORM ms_billing.assert_org_billing_active(NEW.owner_org_id);
        END IF;
    ELSIF NEW.owner_kind = 'org' AND (
        NEW.activated_at IS DISTINCT FROM OLD.activated_at
        OR NEW.stripe_customer_id IS DISTINCT FROM OLD.stripe_customer_id
        OR NEW.billing_mode IS DISTINCT FROM OLD.billing_mode
        OR NEW.usage_billing_mode IS DISTINCT FROM OLD.usage_billing_mode
        OR NEW.credit_limit_micros IS DISTINCT FROM OLD.credit_limit_micros
        OR NEW.spend_ceiling_micros IS DISTINCT FROM OLD.spend_ceiling_micros
    ) THEN
        PERFORM ms_billing.assert_account_org_billing_active(NEW.id);
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER accounts_reject_retired_org_reactivation
BEFORE INSERT OR UPDATE ON ms_billing.accounts
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_org_account_reactivation();

-- New live roster/domain/timer rows and charge-attempt transitions are
-- billable entitlement mutations.  Soft-removal performed by finalization is
-- deliberately allowed.
CREATE OR REPLACE FUNCTION ms_billing.guard_org_app_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT'
        OR NEW.deleted_at IS NULL
        OR NEW.proration_attempted_at IS DISTINCT FROM OLD.proration_attempted_at
        OR NEW.proration_invoice_id IS DISTINCT FROM OLD.proration_invoice_id
        OR NEW.proration_skipped_at IS DISTINCT FROM OLD.proration_skipped_at THEN
        -- Org apps always carry owner_org_id, including sponsor-funded apps.
        -- Check account_id as well so malformed/legacy mirrors cannot bypass
        -- retirement merely by omitting the denormalized owner column.
        IF NEW.account_id IS NOT NULL THEN
            PERFORM ms_billing.assert_account_org_billing_active(NEW.account_id);
        END IF;
        IF NEW.owner_org_id IS NOT NULL THEN
            PERFORM ms_billing.assert_org_billing_active(NEW.owner_org_id);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER apps_reject_retired_org_write
BEFORE INSERT OR UPDATE ON ms_billing.apps
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_org_app_write();

CREATE OR REPLACE FUNCTION ms_billing.guard_org_timer_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_org_id UUID;
BEGIN
    IF TG_OP = 'INSERT'
        OR NEW.removed_at IS NULL
        OR NEW.charge_attempted_at IS DISTINCT FROM OLD.charge_attempted_at
        OR NEW.grace_resolved IS DISTINCT FROM OLD.grace_resolved
        OR NEW.grace_charged_at IS DISTINCT FROM OLD.grace_charged_at
        OR NEW.grace_invoice_id IS DISTINCT FROM OLD.grace_invoice_id
        OR NEW.grace_invoice_item_id IS DISTINCT FROM OLD.grace_invoice_item_id THEN
        PERFORM ms_billing.assert_account_and_funding_billing_active(
            NEW.account_id,
            NEW.charge_funding_account_id
        );
        SELECT owner_org_id INTO v_org_id
          FROM ms_billing.apps
         WHERE app_id = NEW.app_id;
        IF v_org_id IS NOT NULL THEN
            PERFORM ms_billing.assert_org_billing_active(v_org_id);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER module_timers_reject_retired_org_write
BEFORE INSERT OR UPDATE ON ms_billing.app_module_overage_timers
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_org_timer_write();

CREATE OR REPLACE FUNCTION ms_billing.guard_org_domain_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_org_id UUID;
BEGIN
    IF TG_OP = 'INSERT'
        OR NEW.removed_at IS NULL
        OR NEW.charge_attempted_at IS DISTINCT FROM OLD.charge_attempted_at
        OR NEW.charge_resolved IS DISTINCT FROM OLD.charge_resolved
        OR NEW.charged_at IS DISTINCT FROM OLD.charged_at
        OR NEW.charge_invoice_id IS DISTINCT FROM OLD.charge_invoice_id
        OR NEW.charge_invoice_item_id IS DISTINCT FROM OLD.charge_invoice_item_id THEN
        PERFORM ms_billing.assert_account_and_funding_billing_active(
            NEW.account_id,
            NEW.charge_funding_account_id
        );
        SELECT owner_org_id INTO v_org_id
          FROM ms_billing.apps
         WHERE app_id = NEW.app_id;
        IF v_org_id IS NOT NULL THEN
            PERFORM ms_billing.assert_org_billing_active(v_org_id);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER custom_domains_reject_retired_org_write
BEFORE INSERT OR UPDATE ON ms_billing.app_custom_domains
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_org_domain_write();

-- Metering for a retired org is rejected whether attribution arrived through
-- account_id or through a lazy org-owned app.  Historical usage rows remain
-- untouched and readable.
CREATE OR REPLACE FUNCTION ms_billing.guard_retired_org_usage_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_org_id UUID;
BEGIN
    IF NEW.account_id IS NOT NULL THEN
        SELECT owner_org_id INTO v_org_id
          FROM ms_billing.accounts
         WHERE id = NEW.account_id AND owner_kind = 'org';
    END IF;
    IF v_org_id IS NULL THEN
        SELECT owner_org_id INTO v_org_id
          FROM ms_billing.apps
         WHERE app_id = NEW.app_id;
    END IF;
    IF v_org_id IS NOT NULL THEN
        PERFORM ms_billing.assert_org_billing_active(v_org_id);
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER usage_events_reject_retired_org_insert
BEFORE INSERT ON ms_billing.usage_events
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_retired_org_usage_insert();

-- Every invoice mutation shares the lifecycle barrier with finalization.
-- Existing rows remain mutable after retirement so Stripe webhooks can finish
-- reconciliation. A genuinely new invoice must additionally prove that every
-- organization authorizing its account is still active.
CREATE OR REPLACE FUNCTION ms_billing.guard_retired_org_invoice_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        PERFORM ms_billing.lock_account_and_funding_lifecycle_shared(
            OLD.account_id,
            OLD.charge_funding_account_id
        );
        IF NEW.account_id IS DISTINCT FROM OLD.account_id
           OR NEW.charge_funding_account_id IS DISTINCT FROM OLD.charge_funding_account_id THEN
            PERFORM ms_billing.assert_account_and_funding_billing_active(
                NEW.account_id,
                NEW.charge_funding_account_id
            );
        END IF;
        RETURN NEW;
    END IF;

    PERFORM ms_billing.lock_account_and_funding_lifecycle_shared(
        NEW.account_id,
        NEW.charge_funding_account_id
    );

    -- UpsertInvoice is also the retained webhook reconciliation path. Let an
    -- INSERT ... ON CONFLICT for the exact existing (Stripe invoice, account)
    -- reach its DO UPDATE arm; only a genuinely new invoice is a post-retire
    -- charge and must acquire/check the lifecycle guard.
    IF EXISTS (
        SELECT 1
        FROM ms_billing.invoices invoice
        WHERE invoice.stripe_invoice_id = NEW.stripe_invoice_id
          AND invoice.account_id = NEW.account_id
    ) THEN
        RETURN NEW;
    END IF;
    PERFORM ms_billing.assert_account_and_funding_billing_active(
        NEW.account_id,
        NEW.charge_funding_account_id
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER invoices_reject_retired_org_insert
BEFORE INSERT OR UPDATE ON ms_billing.invoices
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_retired_org_invoice_write();

-- The durable pre-Stripe attempt markers must acquire the lifecycle lock too.
-- Otherwise finalization could observe no invoice while an uncommitted charge
-- attempt is about to cross the network boundary.
CREATE OR REPLACE FUNCTION ms_billing.guard_retired_org_account_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM ms_billing.assert_account_and_funding_billing_active(
        NEW.account_id,
        NEW.charge_funding_account_id
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER billing_runs_reject_retired_org_write
BEFORE INSERT OR UPDATE ON ms_billing.billing_runs
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_retired_org_account_write();

CREATE TRIGGER combined_proration_reject_retired_org_write
BEFORE INSERT OR UPDATE ON ms_billing.app_combined_proration_attempts
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_retired_org_account_write();

CREATE OR REPLACE FUNCTION ms_billing.guard_retired_org_credit_ledger_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Refunds/adjustments remain legal financial reconciliation. Every other
    -- new ledger fact funds or charges future service and is forbidden after
    -- retirement. A pending purchase/auto-top-up update is guarded as well.
    IF TG_OP = 'INSERT' AND NEW.type NOT IN ('refund', 'adjustment') THEN
        PERFORM ms_billing.assert_account_and_funding_billing_active(
            NEW.account_id,
            NEW.charge_funding_account_id
        );
    ELSIF TG_OP = 'UPDATE'
        AND NEW.type IN ('purchase', 'auto_topup')
        AND NEW.status = 'pending' THEN
        PERFORM ms_billing.assert_account_and_funding_billing_active(
            NEW.account_id,
            NEW.charge_funding_account_id
        );
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER credit_ledger_reject_retired_org_future_write
BEFORE INSERT OR UPDATE ON ms_billing.credit_ledger
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_retired_org_credit_ledger_write();

-- Late Stripe attachment events must not restore a usable card.  Soft-delete
-- and fraud/history reconciliation remain legal because they do not make the
-- row active.
CREATE OR REPLACE FUNCTION ms_billing.guard_retired_org_payment_method_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.deleted_at IS NULL THEN
        PERFORM ms_billing.assert_account_org_billing_active(NEW.account_id);
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER payment_methods_reject_retired_org_write
BEFORE INSERT OR UPDATE ON ms_billing.payment_methods_mirror
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_retired_org_payment_method_write();

CREATE OR REPLACE FUNCTION ms_billing.guard_retired_org_auto_top_up_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.enabled THEN
        PERFORM ms_billing.assert_account_org_billing_active(NEW.account_id);
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER auto_top_up_reject_retired_org_write
BEFORE INSERT OR UPDATE ON ms_billing.credit_auto_topup_configs
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_retired_org_auto_top_up_write();

-- Add-card Start and SetupIntent correlation are future-funding writes. They
-- share the lifecycle barrier. The finalizer's exact pending→failed transition
-- is the only bypass: it cancels an already-created external SetupIntent and
-- never makes a funding method usable.
CREATE OR REPLACE FUNCTION ms_billing.guard_retired_org_add_card_request_write()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE'
        AND OLD.status = 'pending'
        AND NEW.status = 'failed'
        AND NEW.account_id = OLD.account_id THEN
        RETURN NEW;
    END IF;

    PERFORM ms_billing.assert_account_org_billing_active(NEW.account_id);
    RETURN NEW;
END;
$$;

CREATE TRIGGER add_card_requests_reject_retired_org_write
BEFORE INSERT OR UPDATE ON ms_billing.add_card_requests
FOR EACH ROW EXECUTE FUNCTION ms_billing.guard_retired_org_add_card_request_write();
