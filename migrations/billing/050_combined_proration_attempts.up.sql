-- Migration 050 — durable ownership for the combined app-creation charge.
--
-- The creation-proration Stripe leg can fold co-created FIFO-over module
-- timers onto the app's invoice. Before this migration it persisted only the
-- app-level attempted marker, then recomputed the timer set from mutable live
-- FIFO state on every retry. A crash after attaching one or more Stripe items
-- therefore lost which timers the attempt owned; an uninstall or rank
-- improvement could make recovery omit an already-pinned line, while treating
-- every co-created timer as owned would falsely charge included timers.
--
-- DEPLOYMENT INTERLOCK: migration 050 must be applied before atomically moving
-- the non-canary billing-cycle Lambda alias to the matching binary. Do not run
-- an older billing-cycle version after a new worker can create one of these
-- headers. Keep the scheduled/manual billing-cycle invocation drained from the
-- legacy-row preflight below through that alias flip. After the first new
-- worker invocation, use forward recovery rather than rolling the charge worker
-- back: an old binary cannot understand the new exact-item metadata contract.
-- The guards remain expand-only for the pre-cutover binary: rows with no new
-- header retain their old behavior.
--
-- One immutable header now freezes the exact Stripe request shape before the
-- first network call. Header presence is also the durable "set is known" bit:
-- a header with zero child rows means the attempt intentionally owned no
-- timers, whereas apps.proration_attempted_at without a header is legacy /
-- incomplete state that the service must fail closed. Child rows retain the
-- exact timer identities even after soft removal or FIFO-rank changes.

-- A pre-050 attempted/non-terminal row can make an old retry skip its guarded
-- marker UPDATE (WHERE proration_attempted_at IS NULL), then reach Stripe
-- without migration-050 metadata. Refuse the deploy until those rows have been
-- reconciled to a genuine invoice terminal. A skip marker alone is not proof:
-- old code could stamp it after an attempted crash. This is intentionally the
-- first executable statement so even non-transactional psql runners leave no
-- partial migration when the preflight fails. The deploy must keep
-- billing-cycle idle after this check through the atomic alias cutover.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM ms_billing.apps app
        WHERE app.proration_attempted_at IS NOT NULL
          AND app.proration_invoice_id IS NULL
    ) THEN
        RAISE EXCEPTION
            'migration 050 requires all legacy attempted creation prorations to be terminal'
            USING ERRCODE = '23514',
                  HINT = 'reconcile or resolve every app with proration_attempted_at set and no invoice guard before retrying the deploy; a later skip marker cannot prove that Stripe did not move money';
    END IF;
END;
$$;

CREATE TABLE ms_billing.app_combined_proration_attempts (
    app_id                   UUID PRIMARY KEY
                             REFERENCES ms_billing.apps(app_id) ON DELETE CASCADE,
    account_id               UUID NOT NULL
                             REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,
    attempted_at             TIMESTAMPTZ NOT NULL,

    -- Exact Stripe line shape. Raw micro-dollars feed unresolved projections;
    -- whole cents are the immutable Stripe request amounts.
    currency                 TEXT NOT NULL,
    base_charge_micros       BIGINT NOT NULL CHECK (base_charge_micros > 0),
    base_charge_cents        BIGINT NOT NULL CHECK (base_charge_cents > 0),
    module_charge_micros     BIGINT NOT NULL CHECK (module_charge_micros >= 0),
    module_charge_cents      BIGINT NOT NULL CHECK (module_charge_cents >= 0),
    timer_count              INT NOT NULL CHECK (timer_count >= 0),
    coverage_start           TIMESTAMPTZ NOT NULL,
    coverage_end             TIMESTAMPTZ NOT NULL,
    base_description         TEXT NOT NULL,
    module_description       TEXT NOT NULL,

    -- Exact display snapshots the terminal persistence transaction writes.
    snapshot_period_start    TIMESTAMPTZ NOT NULL,
    snapshot_period_end      TIMESTAMPTZ NOT NULL,
    snapshot_base_micros     BIGINT NOT NULL CHECK (snapshot_base_micros >= 0),
    snapshot_module_count    INT NOT NULL CHECK (snapshot_module_count >= 0),
    straddle_period_start    TIMESTAMPTZ NULL,
    straddle_period_end      TIMESTAMPTZ NULL,
    straddle_base_micros     BIGINT NULL,

    -- Written only in the same transaction that arms the app guard and marks
    -- every frozen timer terminal. The header remains afterwards as audit.
    resolved_at              TIMESTAMPTZ NULL,
    resolved_invoice_id      TEXT NULL,

    CONSTRAINT app_combined_proration_attempts_currency_check
        CHECK (currency = LOWER(currency) AND NULLIF(BTRIM(currency), '') IS NOT NULL),
    CONSTRAINT app_combined_proration_attempts_coverage_check
        CHECK (coverage_end > coverage_start),
    CONSTRAINT app_combined_proration_attempts_snapshot_check
        CHECK (snapshot_period_end > snapshot_period_start),
    CONSTRAINT app_combined_proration_attempts_descriptions_check
        CHECK (
            NULLIF(BTRIM(base_description), '') IS NOT NULL
            AND NULLIF(BTRIM(module_description), '') IS NOT NULL
        ),
    CONSTRAINT app_combined_proration_attempts_timer_amount_check
        CHECK (
            (timer_count = 0)
            OR (module_charge_micros > 0 AND module_charge_cents > 0)
        ),
    CONSTRAINT app_combined_proration_attempts_straddle_check
        CHECK (
            (
                straddle_period_start IS NULL
                AND straddle_period_end IS NULL
                AND straddle_base_micros IS NULL
            )
            OR (
                straddle_period_start IS NOT NULL
                AND straddle_period_end IS NOT NULL
                AND straddle_period_end > straddle_period_start
                AND straddle_base_micros IS NOT NULL
                AND straddle_base_micros > 0
            )
        ),
    CONSTRAINT app_combined_proration_attempts_terminal_check
        CHECK (
            (resolved_at IS NULL AND resolved_invoice_id IS NULL)
            OR (
                resolved_at IS NOT NULL
                AND NULLIF(BTRIM(resolved_invoice_id), '') IS NOT NULL
            )
        )
);

CREATE TABLE ms_billing.app_combined_proration_attempt_timers (
    app_id    UUID NOT NULL
              REFERENCES ms_billing.app_combined_proration_attempts(app_id)
              ON DELETE CASCADE,
    timer_id  UUID NOT NULL
              REFERENCES ms_billing.app_module_overage_timers(id)
              ON DELETE RESTRICT,
    PRIMARY KEY (app_id, timer_id),
    UNIQUE (timer_id)
);

-- The strict bill / runtime projection scans only unresolved attempts by
-- account. timer_count lets the reader reject an incomplete/corrupt child set
-- instead of silently understating money.
CREATE INDEX app_combined_proration_attempts_unresolved_account_idx
    ON ms_billing.app_combined_proration_attempts (account_id, app_id)
    WHERE resolved_at IS NULL;

CREATE UNIQUE INDEX app_combined_proration_attempts_invoice_uidx
    ON ms_billing.app_combined_proration_attempts (resolved_invoice_id)
    WHERE resolved_invoice_id IS NOT NULL;

-- Mixed-version safety. During a rolling deploy an old worker understands only
-- apps.proration_attempted_at and can otherwise arm the app/timer terminal
-- guards by recomputing mutable FIFO state, silently burying the exact frozen
-- header. Reject those legacy terminal writes while the header is unresolved.
--
-- The new terminal transaction resolves the header FIRST, then arms the app and
-- every exact child. No concurrent transaction observes the resolved header
-- until commit; if an app/timer affected-row assertion fails, the entire
-- transaction (including that internal header resolve) rolls back.
CREATE FUNCTION ms_billing.guard_unresolved_combined_proration_app_terminal()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (
        NEW.proration_invoice_id IS DISTINCT FROM OLD.proration_invoice_id
        OR NEW.proration_skipped_at IS DISTINCT FROM OLD.proration_skipped_at
    ) AND EXISTS (
        SELECT 1
        FROM ms_billing.app_combined_proration_attempts attempt
        WHERE attempt.app_id = OLD.app_id
          AND attempt.resolved_at IS NULL
    ) THEN
        RAISE EXCEPTION
            'app % has an unresolved combined proration attempt', OLD.app_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER apps_unresolved_combined_proration_terminal_guard
BEFORE UPDATE OF proration_invoice_id,
                 proration_skipped_at
ON ms_billing.apps
FOR EACH ROW
EXECUTE FUNCTION ms_billing.guard_unresolved_combined_proration_app_terminal();

CREATE FUNCTION ms_billing.guard_unresolved_combined_proration_timer_terminal()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (
        NEW.charge_attempted_at IS DISTINCT FROM OLD.charge_attempted_at
        OR NEW.grace_resolved IS DISTINCT FROM OLD.grace_resolved
        OR NEW.grace_charged_at IS DISTINCT FROM OLD.grace_charged_at
        OR NEW.grace_invoice_id IS DISTINCT FROM OLD.grace_invoice_id
        OR NEW.grace_invoice_item_id IS DISTINCT FROM OLD.grace_invoice_item_id
    ) AND EXISTS (
        SELECT 1
        FROM ms_billing.app_combined_proration_attempt_timers owned
        JOIN ms_billing.app_combined_proration_attempts attempt
          ON attempt.app_id = owned.app_id
        WHERE owned.timer_id = OLD.id
          AND attempt.resolved_at IS NULL
    ) THEN
        RAISE EXCEPTION
            'module timer % has an unresolved combined proration owner', OLD.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER app_module_timers_unresolved_combined_proration_terminal_guard
BEFORE UPDATE OF charge_attempted_at,
                 grace_resolved,
                 grace_charged_at,
                 grace_invoice_id,
                 grace_invoice_item_id
ON ms_billing.app_module_overage_timers
FOR EACH ROW
EXECUTE FUNCTION ms_billing.guard_unresolved_combined_proration_timer_terminal();
