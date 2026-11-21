-- Migration 094 — cross-rail revenue views (billing-engine#311, owner D548).
--
-- Plain SQL views over this schema's own ledger, read through billing_ro.
-- Reporting only: no Stripe call, no Lambda, no new role, no write path, and
-- nothing here moves money. docs/VERIFICATION.md and mirrorstack-docs-internal
-- #131 are the places this view list is meant to be copied into.
--
-- PART 1 ONLY (the Stripe/USD rail). Part 2 (billing-engine#294's
-- ms_billing.newebpay_payments arm) is a follow-up migration once that table
-- exists; every view below documents where its NewebPay arm will attach.
--
-- 🔴 A CONFIRMED GAP, FOUND WHILE BUILDING THIS, NOT ASSUMED AWAY.
--
-- Design point 4 of this issue asks for "rail of a wallet-funded part = the
-- rail of its funding lot via source_credit_id" on a RECOGNIZED-REVENUE line.
-- There is no such join available: charge_intents.wallet_allocation_micros
-- (migration 061) names an amount, never a credit_ledger row, and no boundary
-- or plan-change leg writes the intent's digest (or any other back-reference)
-- onto the credit_ledger draw row(s) it consumed
-- (internal/account/cycle/store.go's wallet-draw idempotency keys are
-- "wallet-draw:<account/app>:...:usage_draw:<lot>" — account/app/lot, never an
-- intent digest). So "the rail of its funding lot" cannot be resolved for a
-- charge intent's wallet-funded share; rev_recognized_monthly below tags that
-- share 'wallet:unattributed' always, not merely when a join happens to miss.
-- Filed as the open question on billing-engine#311 rather than papered over
-- with an invented join.
--
-- 🔴 A SECOND VERIFIED FACT THAT CHANGES THE DESIGN: payer_kind = 'app' IS
-- UNREACHABLE TODAY. charge_intents.payer_kind's CHECK allows ('user', 'org',
-- 'app'), but every charge is proposed against an ACCOUNT id and the proposer
-- (internal/intent/proposer/proposer.go) resolves it through
-- internal/intent/store/payer.go's PayerForAccount, which returns only
-- intent.Subject{Kind: "user"} or {Kind: "org"} — never "app". The resolution
-- through ms_billing.apps this issue asks to "verify" is therefore handled
-- defensively below (so a future 'app' payer does not silently miscount) but
-- exercises zero rows against this commit.
--
-- Everything here is additive and reporting-only; the down migration drops it
-- cleanly, leaving charge_intents, credit_ledger, app_plan_changes etc.
-- untouched.

-- ---------------------------------------------------------------------------
-- rev_fx_rates — the one FX rate table every view reads
-- ---------------------------------------------------------------------------
--
-- Source: the Central Bank of the Republic of China (Taiwan)'s published daily
-- USD/NTD rate (owner decision D526). Seeded with NO rows here — this worker
-- has no sourced rate to attest to, and an empty table is valid: USD needs no
-- row, and every view's fx_missing column says so for anything else. Rows are
-- added through a later, reviewable migration, never by application code.
--
-- 🔴 NP-D4: no Go charge path may read this table. It is for reporting only —
-- every price in this codebase is quoted and charged in its native currency
-- (chargeCurrency = "usd" throughout internal/account/cycle; NewebPay's arm,
-- billing-engine#294, prices in NT$ directly). The guard test
-- (internal/architecture/rev_fx_guard_test.go) greps for that.
CREATE TABLE IF NOT EXISTS ms_billing.rev_fx_rates (
    currency     TEXT NOT NULL,
    rate_date    DATE NOT NULL,
    usd_per_unit NUMERIC(20, 10) NOT NULL CHECK (usd_per_unit > 0),
    source       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (currency, rate_date)
);

COMMENT ON TABLE ms_billing.rev_fx_rates IS
    'Reporting-only USD conversion table (D526). USD needs no row; a currency '
    'with no rate on or before a settlement date reports NULL and fx_missing, '
    'never 0. NP-D4: no Go charge path may read this table — every charge '
    'prices and collects in its native currency, never through an FX figure.';

-- ---------------------------------------------------------------------------
-- rev_plan_price_reference — the ONE place a plan's base fee is priced for
-- reporting, pinned against internal/account/usage/plans.go
-- ---------------------------------------------------------------------------
--
-- Not a price authority: internal/account/usage/plans.go (planTerms) is, and
-- stays, the only place a plan's base fee is charged from. This table is a
-- reporting-side COPY of those three figures, at the commit this migration
-- landed (owner 2026-09-13/14 price points cited in migration 076's header).
-- internal/architecture's integration test reads both sides and fails the
-- build the day they disagree, so a price change in plans.go cannot drift
-- silently out of MRR movement reporting.
CREATE TABLE IF NOT EXISTS ms_billing.rev_plan_price_reference (
    plan            TEXT PRIMARY KEY CHECK (plan IN ('free', 'pro', 'business')),
    base_fee_micros BIGINT NOT NULL CHECK (base_fee_micros >= 0)
);

COMMENT ON TABLE ms_billing.rev_plan_price_reference IS
    'Reporting copy of internal/account/usage/plans.go''s planTerms base fee, '
    'used only to price rev_mrr_movements_monthly. Pinned by a Go test against '
    'the real source of truth; never read by a charge path.';

INSERT INTO ms_billing.rev_plan_price_reference (plan, base_fee_micros) VALUES
    ('free', 0),
    ('pro', 20000000),
    ('business', 50000000)
ON CONFLICT (plan) DO NOTHING;

-- ---------------------------------------------------------------------------
-- rev_normalize_rail — rail normalisation in the one place design point 3
-- asks for
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ms_billing.rev_normalize_rail(raw TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE lower(btrim(raw))
               WHEN 'stripe'   THEN 'stripe'
               WHEN 'newebpay' THEN 'newebpay'
               WHEN 'tappay'   THEN 'tappay'
               ELSE 'other:' || lower(btrim(raw))
           END;
$$;

COMMENT ON FUNCTION ms_billing.rev_normalize_rail(TEXT) IS
    'The one place a raw selected_rail string becomes a reporting rail. '
    'stripe / newebpay / tappay normalise case- and whitespace-insensitively; '
    'anything else becomes other:<raw lowercased> and is shown, never dropped.';

-- ---------------------------------------------------------------------------
-- rev_settled_lines — one row per line of a settled intent
-- ---------------------------------------------------------------------------
--
-- Settled = intent_settlement_claims.outcome = 'succeeded' (voided, failed and
-- unsettled intents never appear here — there is no row at all for them in
-- this view). A superseded intent (charge_intents.supersedes_digest) that
-- later settled is a DIFFERENT digest from the one it replaced, and only the
-- digest that actually carries a succeeded claim contributes a row — the
-- superseded one has none — so nothing here counts an intent twice.
--
-- Tax is NOT a charge_intent_lines row today: it is sealed as
-- charge_intents.tax_amount_micros, a column on the intent, and
-- internal/intent/catalog.go's KindTax is declared but never produced by any
-- cut-over leg. So a synthetic row is added per settled intent with
-- tax_amount_micros > 0, sentinel line_index -1, class 'tax' — visible, but
-- never collided with a real (intent_digest, line_index) pair (every real
-- line_index is >= 0 per charge_intent_lines' own CHECK).
CREATE OR REPLACE VIEW ms_billing.rev_settled_lines AS
WITH real_lines AS (
    SELECT
        ci.digest                                          AS intent_digest,
        cl.line_index                                       AS line_index,
        sc.outcome_at                                        AS settled_at,
        ci.payer_kind                                        AS payer_kind,
        ci.payer_id                                          AS payer_id,
        ci.currency                                          AS currency,
        ci.selected_rail                                     AS raw_rail,
        ci.kind                                              AS intent_kind,
        cl.module                                            AS source_ref,
        cl.amount_micros                                     AS native_amount_micros
    FROM ms_billing.charge_intents ci
    JOIN ms_billing.intent_settlement_claims sc
      ON sc.intent_digest = ci.digest
    JOIN ms_billing.charge_intent_lines cl
      ON cl.intent_digest = ci.digest
    WHERE sc.outcome = 'succeeded'
),
tax_lines AS (
    SELECT
        ci.digest                                           AS intent_digest,
        -1                                                    AS line_index,
        sc.outcome_at                                         AS settled_at,
        ci.payer_kind                                         AS payer_kind,
        ci.payer_id                                           AS payer_id,
        ci.currency                                           AS currency,
        ci.selected_rail                                      AS raw_rail,
        'tax'                                                 AS intent_kind,
        'tax'                                                 AS source_ref,
        ci.tax_amount_micros                                  AS native_amount_micros
    FROM ms_billing.charge_intents ci
    JOIN ms_billing.intent_settlement_claims sc
      ON sc.intent_digest = ci.digest
    WHERE sc.outcome = 'succeeded'
      AND ci.tax_amount_micros > 0
),
lines AS (
    SELECT * FROM real_lines
    UNION ALL
    SELECT * FROM tax_lines
)
SELECT
    l.intent_digest,
    l.line_index,
    l.settled_at,
    -- Payer resolution. 'user' and 'org' are already the resolved owner
    -- (PayerForAccount never produces anything else today); 'app' is handled
    -- defensively — see the header note — by resolving payer_id as an app id
    -- through ms_billing.apps to its account's owner.
    COALESCE(acc.owner_kind, l.payer_kind)                         AS resolved_payer_kind,
    COALESCE(acc.owner_user_id::text, acc.owner_org_id::text, l.payer_id)
                                                                     AS resolved_payer_id,
    od.distributor_org_id                                           AS distributor_org_id,
    ms_billing.rev_normalize_rail(l.raw_rail)                       AS rail,
    l.currency                                                      AS currency,
    CASE
        WHEN l.intent_kind = 'module_usage' AND l.source_ref LIKE 'arrears:%'
            THEN 'module_usage'
        WHEN l.intent_kind = 'platform_base' AND l.source_ref = 'advance:base'
            THEN 'base_plan'
        WHEN l.intent_kind = 'platform_base'
             AND (l.source_ref IN ('advance:capacity', 'advance:domains', 'members:closed')
                  OR l.source_ref LIKE 'domain:%')
            THEN 'plan_extras'
        WHEN l.intent_kind = 'platform_base'
             AND (l.source_ref LIKE 'plan-change:%' OR l.source_ref = 'base')
            THEN 'proration'
        -- timer:<uuid> is written both by the standalone module-overage leg
        -- (overage.go moduleOverageChargeRef) and by the creation combined-
        -- proration leg's per-timer lines (proration.go
        -- combinedProrationTimerItemKey) — the two produce an IDENTICAL
        -- source-ref shape, so they are not distinguishable from this column
        -- alone. Both are prorated module overage, so both land here.
        WHEN l.intent_kind = 'platform_base' AND l.source_ref LIKE 'timer:%'
            THEN 'module_overage'
        WHEN l.intent_kind IN ('credit_purchase', 'auto_topup')
            THEN 'credit_purchase'
        WHEN l.intent_kind = 'tax'
            THEN 'tax'
        ELSE 'other'
    END                                                              AS class,
    l.native_amount_micros                                          AS native_amount_micros
FROM lines l
LEFT JOIN ms_billing.apps rp_app
       ON l.payer_kind = 'app' AND rp_app.app_id = l.payer_id::uuid
LEFT JOIN ms_billing.accounts acc
       ON acc.id = rp_app.account_id
LEFT JOIN ms_billing.org_distributors od
       ON od.customer_org_id = COALESCE(acc.owner_org_id,
                                         CASE WHEN l.payer_kind = 'org' THEN l.payer_id::uuid END);

COMMENT ON VIEW ms_billing.rev_settled_lines IS
    'One row per line of a settled charge intent (intent_settlement_claims.outcome '
    '= succeeded only; voided/failed/unsettled intents never appear, and a '
    'superseded digest contributes nothing unless IT settled). Tax is sealed as '
    'charge_intents.tax_amount_micros rather than a line, so it is added here as a '
    'synthetic row (line_index -1, class tax) per settled intent with a positive '
    'tax amount. class is a closed mapping from (kind, source ref); an unmapped '
    'ref is class other and stays visible rather than being dropped. rail is the '
    'INTENT''s nominal selected_rail, normalised — see rev_recognized_monthly for '
    'the wallet-vs-provider split, which this view does not perform.';

-- ---------------------------------------------------------------------------
-- rev_recognized_monthly — recognized revenue, net of tax, wallet-funded
-- counted once
-- ---------------------------------------------------------------------------
--
-- Counts platform_base- and module_usage-kind lines only (base_plan,
-- plan_extras, proration, module_overage, module_usage classes) — never
-- credit_purchase or tax. A line is split pro rata, in integer micros, between
-- its provider-collected share and its wallet-funded share, using the parent
-- intent's wallet_allocation_micros / total_micros ratio; the remainder of
-- that integer division is given to the PROVIDER share (stated in the view
-- body below), so the two shares always sum to the line's native amount
-- exactly. The provider share is tagged with the intent's own (normalised)
-- rail; the wallet share is tagged 'wallet:unattributed' always — see the
-- header note on why no finer rail is derivable from this schema today.
CREATE OR REPLACE VIEW ms_billing.rev_recognized_monthly AS
WITH revenue_lines AS (
    SELECT
        sl.intent_digest,
        sl.settled_at,
        sl.resolved_payer_kind,
        sl.resolved_payer_id,
        sl.distributor_org_id,
        sl.rail,
        sl.currency,
        sl.class,
        sl.native_amount_micros,
        ci.wallet_allocation_micros,
        ci.total_micros
    FROM ms_billing.rev_settled_lines sl
    JOIN ms_billing.charge_intents ci ON ci.digest = sl.intent_digest
    WHERE sl.class IN ('base_plan', 'plan_extras', 'proration', 'module_overage', 'module_usage')
),
split AS (
    SELECT
        date_trunc('month', settled_at)                                       AS month,
        settled_at,
        resolved_payer_kind,
        resolved_payer_id,
        distributor_org_id,
        currency,
        class,
        rail                                                                   AS provider_rail,
        -- Integer pro-rata wallet share; the remainder of the division stays
        -- on the provider side, per the view comment above.
        CASE WHEN total_micros = 0 THEN 0
             ELSE (native_amount_micros * wallet_allocation_micros) / total_micros
        END                                                                    AS wallet_share_micros,
        native_amount_micros
            - CASE WHEN total_micros = 0 THEN 0
                   ELSE (native_amount_micros * wallet_allocation_micros) / total_micros
              END                                                              AS provider_share_micros
    FROM revenue_lines
),
unioned AS (
    SELECT month, settled_at, resolved_payer_kind, resolved_payer_id, distributor_org_id,
           currency, class, provider_rail AS rail, provider_share_micros AS amount_micros
      FROM split
     WHERE provider_share_micros <> 0
    UNION ALL
    SELECT month, settled_at, resolved_payer_kind, resolved_payer_id, distributor_org_id,
           currency, class, 'wallet:unattributed' AS rail, wallet_share_micros AS amount_micros
      FROM split
     WHERE wallet_share_micros <> 0
),
-- FX is resolved per ROW, against that row's actual settlement date — never
-- the aggregate month — so a rate published mid-month is never applied to a
-- settlement that preceded it and never missed for one that followed it.
converted AS (
    SELECT
        u.*,
        fxr.usd_per_unit,
        fxr.rate_date,
        CASE WHEN upper(u.currency) = 'USD' THEN u.amount_micros
             WHEN fxr.usd_per_unit IS NULL THEN NULL
             ELSE ROUND(u.amount_micros * fxr.usd_per_unit)
        END AS usd_amount_micros
    FROM unioned u
    LEFT JOIN LATERAL (
        SELECT fx.usd_per_unit, fx.rate_date
          FROM ms_billing.rev_fx_rates fx
         WHERE fx.currency = upper(u.currency)
           AND fx.rate_date <= (u.settled_at AT TIME ZONE 'UTC')::date
         ORDER BY fx.rate_date DESC
         LIMIT 1
    ) fxr ON upper(u.currency) <> 'USD'
)
SELECT
    month,
    resolved_payer_kind,
    resolved_payer_id,
    distributor_org_id,
    rail,
    currency,
    class,
    SUM(amount_micros)                                                        AS native_amount_micros,
    -- NULL, not a partial sum, the moment any underlying row is missing a
    -- rate: a partial USD total with no flag would be read as complete.
    CASE WHEN bool_or(upper(currency) <> 'USD' AND usd_per_unit IS NULL) THEN NULL
         ELSE SUM(usd_amount_micros)
    END                                                                        AS usd_reporting_micros,
    MAX(rate_date)                                                             AS fx_rate_date,
    bool_or(upper(currency) <> 'USD' AND usd_per_unit IS NULL)                 AS fx_missing
FROM converted
GROUP BY month, resolved_payer_kind, resolved_payer_id, distributor_org_id,
         rail, currency, class;

COMMENT ON VIEW ms_billing.rev_recognized_monthly IS
    'Recognized revenue by month/payer/distributor/rail/currency/class, net of '
    'tax (tax is its own class elsewhere and excluded here by rev_settled_lines '
    'class filter). A wallet-funded share of a line counts exactly once, split '
    'pro rata off the parent intent''s wallet_allocation_micros in integer '
    'micros with the remainder kept on the provider share. The wallet share''s '
    'rail is always wallet:unattributed: no column links a charge_intent to the '
    'credit_ledger draw row(s) that funded it (filed on billing-engine#311). '
    'USD columns are converted per underlying settlement, against the latest '
    'rev_fx_rates row on or before THAT settlement''s date, then summed — never '
    'looked up once for the aggregate month, so a rate published mid-month is '
    'applied only to the settlements that followed it. fx_missing is true, '
    'never usd_reporting_micros = 0, when no rate applies to every row summed.';

-- ---------------------------------------------------------------------------
-- rev_cash_collected_monthly — cash by rail, net of tax collection timing
-- ---------------------------------------------------------------------------
--
-- provider_remainder_micros of every succeeded intent (any kind EXCEPT
-- credit_purchase/auto_topup — those are added from credit_ledger instead, to
-- avoid double-counting the same cash the day a collector exists for them;
-- today neither kind ever reaches intent_settlement_claims, since the only
-- collector live is cmd/intent-executor for the three legacy-draining paths),
-- plus settled credit_ledger purchase/auto_topup rows. Credit purchases are
-- cash and deferred credit here, never revenue — see rev_recognized_monthly,
-- which never reads credit_ledger.
--
-- merchant_org is 'mirrorstack' for every Part-1 (Stripe) row: MirrorStack's
-- own account received the money. Part 2 (billing-engine#294's NewebPay arm)
-- adds rows merchant_org'd to the distributor instead, because NewebPay money
-- lands with the distributor, not MirrorStack.
CREATE OR REPLACE VIEW ms_billing.rev_cash_collected_monthly AS
WITH intent_cash AS (
    SELECT
        date_trunc('month', sc.outcome_at)       AS month,
        sc.outcome_at                              AS settled_at,
        'mirrorstack'                             AS merchant_org,
        ms_billing.rev_normalize_rail(ci.selected_rail) AS rail,
        ci.currency                               AS currency,
        ci.provider_remainder_micros               AS amount_micros
    FROM ms_billing.charge_intents ci
    JOIN ms_billing.intent_settlement_claims sc ON sc.intent_digest = ci.digest
    WHERE sc.outcome = 'succeeded'
      AND ci.kind NOT IN ('credit_purchase', 'auto_topup')
),
ledger_cash AS (
    SELECT
        date_trunc('month', cl.created_at)        AS month,
        cl.created_at                              AS settled_at,
        'mirrorstack'                              AS merchant_org,
        CASE WHEN cl.stripe_invoice_id IS NOT NULL THEN 'stripe'
             ELSE 'wallet:unattributed'
        END                                        AS rail,
        'usd'                                       AS currency,
        cl.amount_micros                            AS amount_micros
    FROM ms_billing.credit_ledger cl
    WHERE cl.status = 'settled'
      AND cl.type IN ('purchase', 'auto_topup')
),
unioned AS (
    SELECT month, settled_at, merchant_org, rail, currency, amount_micros FROM intent_cash
    UNION ALL
    SELECT month, settled_at, merchant_org, rail, currency, amount_micros FROM ledger_cash
),
-- Same per-row FX resolution as rev_recognized_monthly: against the actual
-- settlement instant, never the aggregate month.
converted AS (
    SELECT
        u.*,
        fxr.usd_per_unit,
        fxr.rate_date,
        CASE WHEN upper(u.currency) = 'USD' THEN u.amount_micros
             WHEN fxr.usd_per_unit IS NULL THEN NULL
             ELSE ROUND(u.amount_micros * fxr.usd_per_unit)
        END AS usd_amount_micros
    FROM unioned u
    LEFT JOIN LATERAL (
        SELECT fx.usd_per_unit, fx.rate_date
          FROM ms_billing.rev_fx_rates fx
         WHERE fx.currency = upper(u.currency)
           AND fx.rate_date <= (u.settled_at AT TIME ZONE 'UTC')::date
         ORDER BY fx.rate_date DESC
         LIMIT 1
    ) fxr ON upper(u.currency) <> 'USD'
)
SELECT
    month,
    merchant_org,
    rail,
    currency,
    SUM(amount_micros)                                                       AS native_amount_micros,
    CASE WHEN bool_or(upper(currency) <> 'USD' AND usd_per_unit IS NULL) THEN NULL
         ELSE SUM(usd_amount_micros)
    END                                                                       AS usd_reporting_micros,
    MAX(rate_date)                                                           AS fx_rate_date,
    bool_or(upper(currency) <> 'USD' AND usd_per_unit IS NULL)               AS fx_missing
FROM converted
GROUP BY month, merchant_org, rail, currency;

COMMENT ON VIEW ms_billing.rev_cash_collected_monthly IS
    'Cash collected by month/merchant/rail/currency: provider_remainder_micros '
    'of succeeded intents (excluding credit_purchase/auto_topup, added instead '
    'from settled credit_ledger rows to avoid double counting the day a '
    'collector exists for those kinds) plus settled credit_ledger '
    'purchase/auto_topup rows. merchant_org is mirrorstack for every row here; '
    'billing-engine#294''s NewebPay arm merchant_org''s its rows to the '
    'distributor instead. Credit purchases are cash and deferred credit, never '
    'revenue.';

-- ---------------------------------------------------------------------------
-- rev_credits_deferred — wallet balance and movement per account and rail of
-- the lot
-- ---------------------------------------------------------------------------
--
-- Every settled credit_ledger row's rail: a purchase/auto_topup row with a
-- Stripe invoice is 'stripe'; a grant/preallocation (never collected through
-- any rail) is 'wallet:granted'; a draw (usage_draw/subscription_draw) takes
-- the rail of the lot it names via source_credit_id, or 'wallet:unattributed'
-- when it draws the credits-mode unsecured remainder (source_credit_id NULL);
-- a refund or adjustment is 'wallet:unattributed' — the schema's
-- credit_ledger_source_check forbids source_credit_id on those two types, so
-- which lot a refund reverses is not recorded anywhere this view can read.
CREATE OR REPLACE VIEW ms_billing.rev_credits_deferred AS
SELECT
    cl.account_id                                                           AS account_id,
    date_trunc('month', cl.created_at)                                      AS month,
    CASE
        WHEN cl.type IN ('purchase', 'auto_topup') AND cl.stripe_invoice_id IS NOT NULL
            THEN 'stripe'
        WHEN cl.type IN ('grant', 'preallocation')
            THEN 'wallet:granted'
        WHEN cl.type IN ('usage_draw', 'subscription_draw') AND cl.source_credit_id IS NOT NULL
            THEN COALESCE(
                CASE WHEN lot.type IN ('purchase', 'auto_topup') AND lot.stripe_invoice_id IS NOT NULL
                     THEN 'stripe'
                     WHEN lot.type IN ('grant', 'preallocation')
                     THEN 'wallet:granted'
                END,
                'wallet:unattributed')
        ELSE 'wallet:unattributed'
    END                                                                      AS rail,
    cl.type                                                                  AS movement_type,
    cl.status                                                                AS status,
    SUM(cl.amount_micros)                                                   AS amount_micros,
    COUNT(*)                                                                AS row_count
FROM ms_billing.credit_ledger cl
LEFT JOIN ms_billing.credit_ledger lot ON lot.id = cl.source_credit_id
GROUP BY cl.account_id, date_trunc('month', cl.created_at), rail, cl.type, cl.status;

COMMENT ON VIEW ms_billing.rev_credits_deferred IS
    'Wallet balance movement per account, month, rail of the funding lot and '
    'ledger type/status. amount_micros sums credit_ledger.amount_micros as '
    'signed (positive in, negative out), so the posted balance for an account '
    'is the sum of its settled rows, matching migration 048''s own invariant. '
    'Wallet is USD micros only; this view carries no currency column.';

-- ---------------------------------------------------------------------------
-- rev_mrr_monthly — MRR per month/payer/rail/currency
-- ---------------------------------------------------------------------------
--
-- Settled advance:base lines only (class base_plan), whatever funded them —
-- unlike rev_recognized_monthly this does NOT split wallet from provider,
-- because MRR asks only "how much base-plan revenue repeats," not who it
-- cleared through. Every settled advance:base line already covers exactly one
-- month (boundary_charges.go bills one period's advance at a time), so no
-- period-length normalisation is needed beyond truncating outcome_at to its
-- month.
CREATE OR REPLACE VIEW ms_billing.rev_mrr_monthly AS
SELECT
    date_trunc('month', sl.settled_at)   AS month,
    sl.resolved_payer_kind               AS payer_kind,
    sl.resolved_payer_id                 AS payer_id,
    sl.rail                              AS rail,
    sl.currency                          AS currency,
    SUM(sl.native_amount_micros)         AS mrr_micros
FROM ms_billing.rev_settled_lines sl
WHERE sl.class = 'base_plan'
GROUP BY date_trunc('month', sl.settled_at), sl.resolved_payer_kind, sl.resolved_payer_id,
         sl.rail, sl.currency;

COMMENT ON VIEW ms_billing.rev_mrr_monthly IS
    'MRR per month/payer/rail/currency: settled advance:base lines only '
    '(class base_plan in rev_settled_lines), whatever rail or wallet share '
    'funded them. The 2027-01-01 org seat leg (core-v2#1589) is out of scope '
    'for v1 and intentionally absent; adding it later only needs a new class '
    'in rev_settled_lines''s mapping, kept in that one place for this reason.';

-- ---------------------------------------------------------------------------
-- rev_mrr_movements_monthly — new, expansion, contraction, churn
-- ---------------------------------------------------------------------------
--
-- Counts a settled upgrade (status = 'settled') or an APPLIED downgrade
-- (status = 'applied') only — a scheduled downgrade has not happened yet and
-- a cancelled one never will, so neither moves MRR. Priced through
-- rev_plan_price_reference, pinned to internal/account/usage/plans.go by a Go
-- test (see the table's own comment). All current plan prices are USD-only.
CREATE OR REPLACE VIEW ms_billing.rev_mrr_movements_monthly AS
SELECT
    date_trunc('month', apc.effective_at)                                  AS month,
    CASE
        WHEN apc.from_plan = 'free' AND apc.to_plan <> 'free' THEN 'new'
        WHEN apc.to_plan = 'free' AND apc.from_plan <> 'free' THEN 'churn'
        WHEN apc.kind = 'upgrade'                              THEN 'expansion'
        ELSE 'contraction'
    END                                                                     AS movement_type,
    acc.owner_kind                                                         AS payer_kind,
    COALESCE(acc.owner_user_id::text, acc.owner_org_id::text)              AS payer_id,
    'usd'                                                                  AS currency,
    COUNT(*)                                                               AS change_count,
    SUM(to_price.base_fee_micros - from_price.base_fee_micros)             AS mrr_delta_micros
FROM ms_billing.app_plan_changes apc
JOIN ms_billing.accounts acc ON acc.id = apc.account_id
JOIN ms_billing.rev_plan_price_reference from_price ON from_price.plan = apc.from_plan
JOIN ms_billing.rev_plan_price_reference to_price ON to_price.plan = apc.to_plan
WHERE (apc.kind = 'upgrade' AND apc.status = 'settled')
   OR (apc.kind = 'downgrade' AND apc.status = 'applied')
GROUP BY date_trunc('month', apc.effective_at), movement_type, acc.owner_kind,
         COALESCE(acc.owner_user_id::text, acc.owner_org_id::text);

COMMENT ON VIEW ms_billing.rev_mrr_movements_monthly IS
    'MRR movements (new/expansion/contraction/churn) from app_plan_changes, '
    'priced through rev_plan_price_reference. Only a settled upgrade or an '
    'applied downgrade counts; scheduled and cancelled downgrades never do, '
    'which is what makes a cancelled downgrade change nothing. new = free -> '
    'paid, churn = paid -> free, expansion/contraction = a move between paid '
    'tiers priced up or down.';

-- ---------------------------------------------------------------------------
-- Grants — explicit, 068-style: verify the role, grant, then assert the grant
-- took. Default privileges are not relied on for these objects.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
        RAISE NOTICE 'migration 094: creating role billing_ro (db-bootstrap has not run here yet)';
        CREATE ROLE billing_ro LOGIN;
    END IF;

    EXECUTE 'GRANT SELECT ON '
        'ms_billing.rev_fx_rates, '
        'ms_billing.rev_plan_price_reference, '
        'ms_billing.rev_settled_lines, '
        'ms_billing.rev_recognized_monthly, '
        'ms_billing.rev_cash_collected_monthly, '
        'ms_billing.rev_credits_deferred, '
        'ms_billing.rev_mrr_monthly, '
        'ms_billing.rev_mrr_movements_monthly '
        'TO billing_ro';
END
$$;

DO $$
DECLARE
    tbl TEXT;
BEGIN
    FOREACH tbl IN ARRAY ARRAY[
        'ms_billing.rev_fx_rates',
        'ms_billing.rev_plan_price_reference',
        'ms_billing.rev_settled_lines',
        'ms_billing.rev_recognized_monthly',
        'ms_billing.rev_cash_collected_monthly',
        'ms_billing.rev_credits_deferred',
        'ms_billing.rev_mrr_monthly',
        'ms_billing.rev_mrr_movements_monthly'
    ]
    LOOP
        IF NOT has_table_privilege('billing_ro', tbl, 'SELECT') THEN
            RAISE EXCEPTION 'migration 094: billing_ro cannot SELECT %, the cross-rail revenue '
                'views are unreadable by the only role meant to read them', tbl;
        END IF;
    END LOOP;

    -- Belt and suspenders: this migration never touches evidence_records, but
    -- it asserts the INV-014 outbox protection is still standing anyway,
    -- since that is the one privilege regression that must never recur.
    IF has_table_privilege('billing_ro', 'ms_billing.evidence_records', 'SELECT') THEN
        RAISE EXCEPTION 'migration 094: billing_ro can read ms_billing.evidence_records; '
            'the INV-014 outbox protection regressed';
    END IF;
END
$$;
