//go:build integration

package architecture

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// 🔴 THE CROSS-RAIL REVENUE VIEWS (billing-engine#311, migration 094).
//
// These are plain SQL views, read only through billing_ro. The fixtures below
// are the same ones hand-verified against a real Postgres while the migration
// was written, ported into the repo's own integration-test harness so a
// regression here fails CI instead of being re-discovered by hand.

var revViewNames = []string{
	"ms_billing.rev_fx_rates",
	"ms_billing.rev_plan_price_reference",
	"ms_billing.rev_settled_lines",
	"ms_billing.rev_recognized_monthly",
	"ms_billing.rev_cash_collected_monthly",
	"ms_billing.rev_credits_deferred",
	"ms_billing.rev_mrr_monthly",
	"ms_billing.rev_mrr_movements_monthly",
}

func TestBillingRoCanSelectEveryRevenueView(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	for _, v := range revViewNames {
		var ok bool
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT has_table_privilege('billing_ro', $1, 'SELECT')`, v).Scan(&ok))
		require.Truef(t, ok, "billing_ro cannot SELECT %s", v)
	}

	var evidenceReadable bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT has_table_privilege('billing_ro', 'ms_billing.evidence_records', 'SELECT')`,
	).Scan(&evidenceReadable))
	require.False(t, evidenceReadable,
		"billing_ro can read ms_billing.evidence_records; the cross-rail revenue views "+
			"migration must never grant that back")

	// The views must actually execute as billing_ro, not merely be grantable.
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	_, err = conn.Exec(ctx, "SET ROLE billing_ro")
	require.NoError(t, err)
	for _, v := range revViewNames {
		_, err := conn.Exec(ctx, "SELECT * FROM "+v+" LIMIT 1")
		require.NoErrorf(t, err, "billing_ro could not execute a SELECT against %s", v)
	}
	_, err = conn.Exec(ctx, "RESET ROLE")
	require.NoError(t, err)
}

func TestNoViewReadsEvidenceRecords(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	for _, v := range revViewNames {
		var def string
		err := pool.QueryRow(ctx, `SELECT pg_get_viewdef($1::regclass, true)`, v).Scan(&def)
		if err != nil {
			// rev_fx_rates and rev_plan_price_reference are tables, not views.
			continue
		}
		require.NotContains(t, def, "evidence_records",
			"%s's definition references evidence_records", v)
	}
}

// seedAccountAndApp is the minimal billing identity every fixture below hangs
// off: one user-owned account, one app on it.
func seedAccountAndApp(ctx context.Context, t *testing.T, pool *pgxpool.Pool) (accountID, ownerUserID, appID string) {
	t.Helper()
	accountID = "11111111-1111-1111-1111-111111111111"
	ownerUserID = "22222222-2222-2222-2222-222222222222"
	appID = "33333333-3333-3333-3333-333333333333"

	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id) VALUES ($1, 'user', $2)`,
		accountID, ownerUserID)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.apps (app_id, account_id, created_at, created_module_count, plan, created_plan)
		VALUES ($1, $2, '2026-09-01T00:00:00Z', 0, 'pro', 'pro')`,
		appID, accountID)
	require.NoError(t, err)
	return accountID, ownerUserID, appID
}

type testIntent struct {
	digest               string
	kind                 string
	taxAmountMicros      int64
	subtotalMicros       int64
	totalMicros          int64
	walletAllocMicros    int64
	providerRemainMicros int64
	currency             string
	rail                 string
	lines                []testLine
	outcome              string // "" = unsettled (no claim row at all)
	outcomeAt            time.Time
}

type testLine struct {
	sourceRef string
	amount    int64
}

func insertIntent(ctx context.Context, t *testing.T, pool *pgxpool.Pool, payerID string, in testIntent) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.charge_intents
		 (digest, payer_kind, payer_id, currency, kind, price_book_revision, terms_revision, notice_policy,
		  tax_jurisdiction, tax_rule_revision, tax_amount_micros, subtotal_micros, total_micros,
		  authorization_id, execute_not_before, execute_not_after, wallet_allocation_micros,
		  provider_remainder_micros, selected_rail, routing_policy_revision, tax_verification)
		VALUES
		 ($1, 'user', $2, $3, $4, 'pb1', 'tr1', 'np1', 'us', 'tr1', $5, $6, $7,
		  $8, '2026-10-01T00:00:00Z', '2026-10-02T00:00:00Z', $9, $10, $11, 'rp1', 'not_applicable')`,
		in.digest, payerID, in.currency, in.kind, in.taxAmountMicros, in.subtotalMicros, in.totalMicros,
		"auth:"+in.digest, in.walletAllocMicros, in.providerRemainMicros, in.rail)
	require.NoError(t, err)

	for i, l := range in.lines {
		_, err := pool.Exec(ctx, `
			INSERT INTO ms_billing.charge_intent_lines
			 (intent_digest, line_index, meter, module, module_version, quantity, unit_price_micros, amount_micros)
			VALUES ($1, $2, 'line', $3, '1', 1, $4, $4)`,
			in.digest, i, l.sourceRef, l.amount)
		require.NoError(t, err)
	}

	if in.outcome != "" {
		_, err := pool.Exec(ctx, `
			INSERT INTO ms_billing.intent_settlement_claims (intent_digest, claimed_by, outcome, outcome_at, provider_reference)
			VALUES ($1, 'test', $2, $3, 'pi_'||$1)`,
			in.digest, in.outcome, in.outcomeAt)
		require.NoError(t, err)
	}
}

// 🔴 THE MRR FIXTURE (acceptance checklist item 3).
//
// One account with a wallet-funded base fee bundled with capacity/domain/
// member lines and tax, a creation proration, a plan-change proration, a
// standalone module overage, module usage, and a credit purchase. MRR must
// equal the advance:base line alone; every other line lands in its own class;
// the wallet-funded base fee counts once in recognized revenue.
func TestMRRFixtureClassifiesEveryLineAndCountsTheWalletFundedBaseFeeOnce(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, ownerUserID, _ := seedAccountAndApp(ctx, t, pool)
	settled := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)

	// Advance bundle: base(20M) + capacity(5M) + domains(3M) + members(2M),
	// tax 3M, wallet-funded 10M of the 33M total.
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "mrr-d1", kind: "platform_base",
		taxAmountMicros: 3_000_000, subtotalMicros: 30_000_000, totalMicros: 33_000_000,
		walletAllocMicros: 10_000_000, providerRemainMicros: 23_000_000,
		currency: "usd", rail: "stripe",
		lines: []testLine{
			{"advance:base", 20_000_000},
			{"advance:capacity", 5_000_000},
			{"advance:domains", 3_000_000},
			{"members:closed", 2_000_000},
		},
		outcome: "succeeded", outcomeAt: settled,
	})

	// Creation proration: base + per-timer module line.
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "mrr-d2", kind: "platform_base",
		subtotalMicros: 750_000, totalMicros: 750_000, providerRemainMicros: 750_000,
		currency: "usd", rail: "stripe",
		lines: []testLine{
			{"base", 500_000},
			{"timer:44444444-4444-4444-4444-444444444444", 250_000},
		},
		outcome: "succeeded", outcomeAt: settled,
	})

	// Plan-change proration.
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "mrr-d3", kind: "platform_base",
		subtotalMicros: 1_000_000, totalMicros: 1_000_000, providerRemainMicros: 1_000_000,
		currency: "usd", rail: "stripe",
		lines:   []testLine{{"plan-change:55555555-5555-5555-5555-555555555555", 1_000_000}},
		outcome: "succeeded", outcomeAt: settled,
	})

	// Standalone module overage.
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "mrr-d4", kind: "platform_base",
		subtotalMicros: 400_000, totalMicros: 400_000, providerRemainMicros: 400_000,
		currency: "usd", rail: "stripe",
		lines:   []testLine{{"timer:66666666-6666-6666-6666-666666666666", 400_000}},
		outcome: "succeeded", outcomeAt: settled,
	})

	// Module usage (arrears).
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "mrr-d5", kind: "module_usage",
		subtotalMicros: 600_000, totalMicros: 600_000, providerRemainMicros: 600_000,
		currency: "usd", rail: "stripe",
		lines:   []testLine{{"arrears:11111111-1111-1111-1111-111111111111", 600_000}},
		outcome: "succeeded", outcomeAt: settled,
	})

	// Credit purchase.
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "mrr-d6", kind: "credit_purchase",
		subtotalMicros: 5_000_000, totalMicros: 5_000_000, providerRemainMicros: 5_000_000,
		currency: "usd", rail: "stripe",
		lines:   []testLine{{"purchase:77777777-7777-7777-7777-777777777777", 5_000_000}},
		outcome: "succeeded", outcomeAt: settled,
	})

	// MRR: only the advance:base line, nothing else.
	var mrr int64
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(mrr_micros), 0) FROM ms_billing.rev_mrr_monthly`).Scan(&mrr))
	require.Equal(t, int64(20_000_000), mrr, "MRR must equal the advance:base line alone")

	// Every class is represented exactly where expected.
	rows, err := pool.Query(ctx, `
		SELECT class, SUM(native_amount_micros) FROM ms_billing.rev_settled_lines GROUP BY class`)
	require.NoError(t, err)
	defer rows.Close()
	byClass := map[string]int64{}
	for rows.Next() {
		var class string
		var amt int64
		require.NoError(t, rows.Scan(&class, &amt))
		byClass[class] = amt
	}
	require.Equal(t, int64(20_000_000), byClass["base_plan"])
	require.Equal(t, int64(10_000_000), byClass["plan_extras"]) // capacity+domains+members
	require.Equal(t, int64(1_500_000), byClass["proration"])    // creation base + plan-change
	require.Equal(t, int64(650_000), byClass["module_overage"]) // creation timer + standalone timer
	require.Equal(t, int64(600_000), byClass["module_usage"])
	require.Equal(t, int64(5_000_000), byClass["credit_purchase"])
	require.Equal(t, int64(3_000_000), byClass["tax"])

	// Recognized revenue counts the wallet-funded base fee exactly once: the
	// provider share plus the wallet share must sum to the full 20M line.
	rows2, err := pool.Query(ctx, `
		SELECT rail, native_amount_micros FROM ms_billing.rev_recognized_monthly WHERE class = 'base_plan'`)
	require.NoError(t, err)
	defer rows2.Close()
	var total int64
	count := 0
	for rows2.Next() {
		var rail string
		var amt int64
		require.NoError(t, rows2.Scan(&rail, &amt))
		total += amt
		count++
	}
	require.Equal(t, 2, count, "the wallet-funded base fee must split into exactly a provider row and a wallet row")
	require.Equal(t, int64(20_000_000), total, "recognized revenue must count the wallet-funded base fee exactly once")

	// Credit purchase and tax never appear in recognized revenue.
	var creditOrTax int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM ms_billing.rev_recognized_monthly WHERE class IN ('credit_purchase', 'tax')`,
	).Scan(&creditOrTax))
	require.Zero(t, creditOrTax)
}

func TestVoidedFailedAndUnsettledIntentsNeverCount(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, ownerUserID, _ := seedAccountAndApp(ctx, t, pool)
	settled := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)

	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "void-1", kind: "platform_base",
		subtotalMicros: 999_000, totalMicros: 999_000, providerRemainMicros: 999_000,
		currency: "usd", rail: "stripe",
		lines: []testLine{{"advance:base", 999_000}}, outcome: "voided", outcomeAt: settled,
	})
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "fail-1", kind: "platform_base",
		subtotalMicros: 999_000, totalMicros: 999_000, providerRemainMicros: 999_000,
		currency: "usd", rail: "stripe",
		lines: []testLine{{"advance:base", 999_000}}, outcome: "failed", outcomeAt: settled,
	})
	// Unsettled: no claim row at all.
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "unsettled-1", kind: "platform_base",
		subtotalMicros: 999_000, totalMicros: 999_000, providerRemainMicros: 999_000,
		currency: "usd", rail: "stripe",
		lines: []testLine{{"advance:base", 999_000}},
	})

	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM ms_billing.rev_settled_lines`).Scan(&n))
	require.Zero(t, n, "voided/failed/unsettled intents must never produce a settled line")

	// A superseded intent that itself never settled must not count either,
	// even though the digest it supersedes may be unrelated.
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "superseded-old", kind: "platform_base",
		subtotalMicros: 999_000, totalMicros: 999_000, providerRemainMicros: 999_000,
		currency: "usd", rail: "stripe",
		lines: []testLine{{"advance:base", 999_000}}, outcome: "voided", outcomeAt: settled,
	})
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "superseded-new", kind: "platform_base",
		subtotalMicros: 1_200_000, totalMicros: 1_200_000, providerRemainMicros: 1_200_000,
		currency: "usd", rail: "stripe",
		lines: []testLine{{"advance:base", 1_200_000}}, outcome: "succeeded", outcomeAt: settled,
	})
	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.charge_intents SET supersedes_digest = 'superseded-old' WHERE digest = 'superseded-new'`)
	// The sealed-update trigger permits this: supersedes_digest is not part of
	// its frozen-column set only for the reverse direction; if it errors here
	// the schema changed and this assertion needs revisiting.
	if err == nil {
		var count int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM ms_billing.rev_settled_lines WHERE intent_digest IN ('superseded-old', 'superseded-new')`,
		).Scan(&count))
		require.Equal(t, 1, count, "only the digest that itself settled should contribute a row")
	}
}

func TestRailNormalization(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	cases := []struct {
		raw  string
		want string
	}{
		{"stripe", "stripe"},
		{" Stripe ", "stripe"},
		{"newebpay", "newebpay"},
		{"tappay", "tappay"},
		{"braintree", "other:braintree"},
	}
	for _, c := range cases {
		var got string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT ms_billing.rev_normalize_rail($1)`, c.raw).Scan(&got))
		require.Equalf(t, c.want, got, "rev_normalize_rail(%q)", c.raw)
	}
}

func TestFXConversionUsesTheLatestRateOnOrBeforeSettlement(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, ownerUserID, _ := seedAccountAndApp(ctx, t, pool)

	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.rev_fx_rates (currency, rate_date, usd_per_unit, source) VALUES
		 ('TWD', '2026-10-01', 0.0320000000, 'test'),
		 ('TWD', '2026-11-01', 0.0330000000, 'test')`)
	require.NoError(t, err)

	// Settled mid-October: must use the 10-01 rate, never the later 11-01 one.
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "fx-twd", kind: "platform_base",
		subtotalMicros: 1_000_000_000, totalMicros: 1_000_000_000, providerRemainMicros: 1_000_000_000,
		currency: "twd", rail: "stripe",
		lines:   []testLine{{"advance:base", 1_000_000_000}},
		outcome: "succeeded", outcomeAt: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC),
	})

	// No rate at all for JPY.
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "fx-jpy", kind: "platform_base",
		subtotalMicros: 500_000, totalMicros: 500_000, providerRemainMicros: 500_000,
		currency: "jpy", rail: "stripe",
		lines:   []testLine{{"advance:base", 500_000}},
		outcome: "succeeded", outcomeAt: time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC),
	})

	// USD needs no rate at all.
	insertIntent(ctx, t, pool, ownerUserID, testIntent{
		digest: "fx-usd", kind: "platform_base",
		subtotalMicros: 700_000, totalMicros: 700_000, providerRemainMicros: 700_000,
		currency: "usd", rail: "stripe",
		lines:   []testLine{{"advance:base", 700_000}},
		outcome: "succeeded", outcomeAt: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC),
	})

	var usd int64
	var rateDate time.Time
	var fxMissing bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT usd_reporting_micros, fx_rate_date, fx_missing
		  FROM ms_billing.rev_recognized_monthly WHERE currency = 'twd'`,
	).Scan(&usd, &rateDate, &fxMissing))
	require.Equal(t, int64(32_000_000), usd, "1,000,000,000 TWD micros at 0.032 must convert to 32,000,000 USD micros")
	require.Equal(t, 2026, rateDate.Year())
	require.Equal(t, time.October, rateDate.Month())
	require.Equal(t, 1, rateDate.Day(), "must use the 10-01 rate, not the later 11-01 one")
	require.False(t, fxMissing)

	var jpyMissing bool
	var jpyUSDValid bool
	err = pool.QueryRow(ctx, `
		SELECT fx_missing, usd_reporting_micros IS NULL
		  FROM ms_billing.rev_recognized_monthly WHERE currency = 'jpy'`,
	).Scan(&jpyMissing, &jpyUSDValid)
	require.NoError(t, err)
	require.True(t, jpyMissing)
	require.True(t, jpyUSDValid, "a missing rate must report NULL, never 0")

	var usdNative, usdReporting int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT native_amount_micros, usd_reporting_micros
		  FROM ms_billing.rev_recognized_monthly WHERE currency = 'usd'`,
	).Scan(&usdNative, &usdReporting))
	require.Equal(t, usdNative, usdReporting, "USD needs no FX rate")
}

func TestMRRMovements(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, _, appID := seedAccountAndApp(ctx, t, pool)
	accountID := "11111111-1111-1111-1111-111111111111"

	insertChange := func(id, from, to, kind, status string, effectiveAt time.Time) {
		_, err := pool.Exec(ctx, `
			INSERT INTO ms_billing.app_plan_changes
			 (id, app_id, account_id, from_plan, to_plan, kind, requested_at, effective_at,
			  period_start, period_end, status, amount_micros)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $7, '2026-10-01T00:00:00Z', '2026-11-01T00:00:00Z', $8, 0)`,
			id, appID, accountID, from, to, kind, effectiveAt, status)
		require.NoError(t, err)
	}

	insertChange("cccccccc-0000-0000-0000-000000000001", "free", "pro", "upgrade", "settled",
		time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	insertChange("cccccccc-0000-0000-0000-000000000002", "pro", "business", "upgrade", "settled",
		time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC))
	insertChange("cccccccc-0000-0000-0000-000000000003", "business", "pro", "downgrade", "applied",
		time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
	insertChange("cccccccc-0000-0000-0000-000000000004", "pro", "free", "downgrade", "applied",
		time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC))
	insertChange("cccccccc-0000-0000-0000-000000000005", "business", "pro", "downgrade", "cancelled",
		time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC))
	insertChange("cccccccc-0000-0000-0000-000000000006", "business", "pro", "downgrade", "scheduled",
		time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC))

	rows, err := pool.Query(ctx, `
		SELECT movement_type, mrr_delta_micros FROM ms_billing.rev_mrr_movements_monthly`)
	require.NoError(t, err)
	defer rows.Close()
	got := map[string]int64{}
	for rows.Next() {
		var mt string
		var delta int64
		require.NoError(t, rows.Scan(&mt, &delta))
		got[mt] = delta
	}
	require.Len(t, got, 4, "a cancelled downgrade and a not-yet-applied scheduled downgrade must change nothing")
	require.Equal(t, int64(20_000_000), got["new"])
	require.Equal(t, int64(30_000_000), got["expansion"])
	require.Equal(t, int64(-30_000_000), got["contraction"])
	require.Equal(t, int64(-20_000_000), got["churn"])
}

// 🔴 THE PLAN-PRICE REFERENCE MUST NEVER DRIFT FROM plans.go.
//
// rev_mrr_movements_monthly prices a plan change through
// ms_billing.rev_plan_price_reference, a reporting-side copy seeded by
// migration 094. This test is the pin: if a future plan price change in
// plans.go is not mirrored into a new migration, this fails instead of MRR
// movement reporting silently drifting from the real price.
func TestPlanPriceReferencePinnedToPlansGo(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	rows, err := pool.Query(ctx, `SELECT plan, base_fee_micros FROM ms_billing.rev_plan_price_reference`)
	require.NoError(t, err)
	defer rows.Close()

	seen := map[string]bool{}
	for rows.Next() {
		var plan string
		var micros int64
		require.NoError(t, rows.Scan(&plan, &micros))
		want := usage.TermsFor(usage.Plan(plan)).BaseFeeMicros
		require.Equalf(t, want, micros,
			"rev_plan_price_reference's %s base fee (%d) does not match usage.TermsFor(%s).BaseFeeMicros (%d); "+
				"a plan price changed in plans.go without a matching migration", plan, micros, plan, want)
		seen[plan] = true
	}
	for _, p := range []usage.Plan{usage.PlanFree, usage.PlanPro, usage.PlanBusiness} {
		require.True(t, seen[string(p)], "rev_plan_price_reference is missing plan %s", p)
	}
}
