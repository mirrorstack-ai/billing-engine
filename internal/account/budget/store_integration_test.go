//go:build integration

package budget_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/budget"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

const (
	haiku  = "anthropic.claude-haiku-4-5-20251001-v1:0" // 018: in 1000 / out 5000 µ$ per 1k
	sonnet = "anthropic.claude-sonnet-4-6"              // 018: in 3000 / out 15000 µ$ per 1k
)

func seedAccountRow(t *testing.T, pool *pgxpool.Pool, ownerKind string, ownerID uuid.UUID) uuid.UUID {
	t.Helper()
	id := uuid.New()
	col := "owner_user_id"
	if ownerKind == "org" {
		col = "owner_org_id"
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.accounts (id, owner_kind, `+col+`) VALUES ($1, $2, $3)`,
		id.String(), ownerKind, ownerID.String())
	require.NoError(t, err)
	return id
}

// seedAIEvent inserts one infra.ai.* event (module = the platform sentinel, the
// shape RecordInfraUsage writes) with an optional template_key (migration 084).
func seedAIEvent(t *testing.T, pool *pgxpool.Pool, acct, app uuid.UUID, metric string, thousandsOfTokens float64, model, templateKey string, at time.Time, devServed bool) {
	t.Helper()
	var tpl any
	if templateKey != "" {
		tpl = templateKey
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.usage_events
		   (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at, model, template_key, dev_served)
		 VALUES ($1, $2, $3, '00000000-0000-0000-0000-000000000000', $4, 'sum', $5, $6, $7, $8, $9)`,
		uuid.NewString(), acct.String(), app.String(), metric, thousandsOfTokens, at, model, tpl, devServed)
	require.NoError(t, err)
}

// TestPgxStore_AISpend_TemplateAndAccountScopes pins the migration-084 spend
// queries against a real Postgres: a template filter narrows the app's AI
// spend to that template's events; the app-wide read sums every template and
// the untemplated ones; the account read sums the account across apps; the
// per-model price layer is applied (Sonnet ≠ Haiku); dev-served and non-AI
// events never count.
func TestPgxStore_AISpend_TemplateAndAccountScopes(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := budget.NewStore(pool)
	ctx := context.Background()
	acct := seedAccountRow(t, pool, "user", uuid.New())
	app, otherApp := uuid.New(), uuid.New()
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	at := start.Add(48 * time.Hour)

	seedAIEvent(t, pool, acct, app, "infra.ai.input.tokens", 10, haiku, "member-help", at, false)  // 10k × 1000 = 10,000 µ$
	seedAIEvent(t, pool, acct, app, "infra.ai.output.tokens", 2, sonnet, "member-help", at, false) // 2k × 15000 = 30,000 µ$
	seedAIEvent(t, pool, acct, app, "infra.ai.input.tokens", 5, haiku, "faq", at, false)           // 5,000 µ$
	seedAIEvent(t, pool, acct, app, "infra.ai.input.tokens", 7, haiku, "", at, false)              // 7,000 µ$ (console turn, no template)
	seedAIEvent(t, pool, acct, app, "infra.ai.input.tokens", 100, haiku, "member-help", at, true)  // dev-served: never money
	seedAIEvent(t, pool, acct, otherApp, "infra.ai.input.tokens", 3, haiku, "", at, false)         // 3,000 µ$ on the account's other app
	seedAIEvent(t, pool, acct, app, "infra.ai.input.tokens", 9, haiku, "member-help", end, false)  // next period: out of window
	_, err := pool.Exec(ctx, `INSERT INTO ms_billing.usage_events (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, $2, $3, '00000000-0000-0000-0000-000000000000', 'infra.compute.walltime.ms', 'sum', 1000000, $4)`,
		uuid.NewString(), acct.String(), app.String(), at) // 1,000,000 µ$ of compute: not AI
	require.NoError(t, err)

	got, err := store.AppPeriodSpendMicros(ctx, app, budget.CategoryAI, "member-help", start, end)
	require.NoError(t, err)
	require.EqualValues(t, 40_000, got, "the template's own events only, Sonnet priced at its own rate")

	got, err = store.AppPeriodSpendMicros(ctx, app, budget.CategoryAI, "faq", start, end)
	require.NoError(t, err)
	require.EqualValues(t, 5_000, got)

	got, err = store.AppPeriodSpendMicros(ctx, app, budget.CategoryAI, "", start, end)
	require.NoError(t, err)
	require.EqualValues(t, 52_000, got, "the app-wide AI row sums every template and the untemplated events; compute and dev-served excluded")

	// Category 'all' is the pre-084 budget query: it includes the compute line
	// but prices infra.ai.* at the sentinel catalog rate (Sonnet's 2k output
	// tokens at 5,000/1k = 10,000, not the model's 30,000) — it never joined
	// metric_model_prices. Pinned as observed; the discrepancy is recorded on
	// core-v2#1486 (pricing consistency), not changed in this PR.
	got, err = store.AppPeriodSpendMicros(ctx, app, budget.CategoryAll, "", start, end)
	require.NoError(t, err)
	require.EqualValues(t, 1_032_000, got, "category 'all' includes the compute line and prices AI at the sentinel rate")

	got, err = store.AccountPeriodAISpendMicros(ctx, acct, start, end)
	require.NoError(t, err)
	require.EqualValues(t, 55_000, got, "the account read spans both apps")
}

// TestPgxStore_Budgets_TemplateRowsAndOrgResolution pins the migration-084
// budget key (scope, scope_id, category, template_key), the new flags, and the
// org → billing-account resolution an org-scoped cap reads through.
func TestPgxStore_Budgets_TemplateRowsAndOrgResolution(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := budget.NewStore(pool)
	ctx := context.Background()
	app, org := uuid.New(), uuid.New()

	wide, err := store.UpsertBudget(ctx, budget.Budget{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI, LimitMicros: 10_000_000, AlertPercents: []int{80, 100}, Active: true, HardCap: true})
	require.NoError(t, err)
	tpl, err := store.UpsertBudget(ctx, budget.Budget{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI, TemplateKey: "member-help", LimitMicros: 2_000_000, AlertPercents: []int{80, 100}, Active: true, HardCap: true, AllowOverage: true})
	require.NoError(t, err)
	require.NotEqual(t, wide.ID, tpl.ID, "the template row is its own row")
	all, err := store.UpsertBudget(ctx, budget.Budget{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAll, LimitMicros: 50_000_000, AlertPercents: []int{100}, Active: true})
	require.NoError(t, err)
	require.NotEqual(t, wide.ID, all.ID)

	got, found, err := store.GetBudget(ctx, budget.ScopeApp, app, budget.CategoryAI, "member-help")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, tpl.ID, got.ID)
	require.True(t, got.HardCap)
	require.True(t, got.AllowOverage)
	require.Equal(t, "member-help", got.TemplateKey)
	_, found, err = store.GetBudget(ctx, budget.ScopeApp, app, budget.CategoryAI, "faq")
	require.NoError(t, err)
	require.False(t, found, "an unknown template has no row of its own (the service falls back to the AI-wide row)")

	// A re-set updates in place on the full key.
	again, err := store.UpsertBudget(ctx, budget.Budget{Scope: budget.ScopeApp, ScopeID: app, Category: budget.CategoryAI, TemplateKey: "member-help", LimitMicros: 3_000_000, AlertPercents: []int{100}, Active: true, HardCap: false})
	require.NoError(t, err)
	require.Equal(t, tpl.ID, again.ID)
	require.EqualValues(t, 3_000_000, again.LimitMicros)
	require.False(t, again.HardCap)

	// The DB refuses a template row outside an app's AI cap and a malformed key.
	_, err = pool.Exec(ctx, `INSERT INTO ms_billing.budgets (scope, scope_id, category, template_key, limit_micros) VALUES ('org', $1, 'ai', 'member-help', 1)`, org.String())
	require.Error(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO ms_billing.budgets (scope, scope_id, category, template_key, limit_micros) VALUES ('app', $1, 'ai', 'Member Help', 1)`, app.String())
	require.Error(t, err)

	// Org resolution: none until the org has an account.
	_, found, err = store.OrgAccountID(ctx, org)
	require.NoError(t, err)
	require.False(t, found)
	orgAcct := seedAccountRow(t, pool, "org", org)
	id, found, err := store.OrgAccountID(ctx, org)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, orgAcct, id)
}

// TestPgxStore_ExposurePoolReads pins the migration-085 reads: the curve row
// is seeded; the account's whole-period spend counts every metric (AI + compute)
// and still excludes dev-served and the next period; the signals read sees the
// billing mode, a usable card and the paid-invoice count; an app resolves to
// its payer through its attributed events.
func TestPgxStore_ExposurePoolReads(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := budget.NewStore(pool)
	ctx := context.Background()
	acct := seedAccountRow(t, pool, "user", uuid.New())
	app := uuid.New()
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	at := start.Add(72 * time.Hour)

	cfg, err := store.RiskRampConfig(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 5_000_000, cfg.NoCardMicros)
	require.EqualValues(t, 10_000_000, cfg.CardBaseMicros)
	require.EqualValues(t, 200_000_000, cfg.CeilingMicros)
	require.InDelta(t, 0.5, cfg.Exponent, 1e-9, "the first seed is the square root")
	require.True(t, cfg.DelinquentFloor)
	require.Equal(t, 2, cfg.LatePenaltyK)
	require.False(t, cfg.EnforcementPaused, "the kill-switch ships off")
	paused, err := store.SetAIEnforcementPaused(ctx, true, "incident", "ops:owner")
	require.NoError(t, err)
	require.True(t, paused)
	cfg, err = store.RiskRampConfig(ctx)
	require.NoError(t, err)
	require.True(t, cfg.EnforcementPaused, "one row, read per verdict")
	require.Equal(t, "incident", cfg.PausedReason)
	require.Equal(t, "ops:owner", cfg.PausedBy)
	require.False(t, cfg.PausedAt.IsZero())
	_, err = store.SetAIEnforcementPaused(ctx, false, "", "")
	require.NoError(t, err)
	cfg, err = store.RiskRampConfig(ctx)
	require.NoError(t, err)
	require.False(t, cfg.EnforcementPaused)
	require.Empty(t, cfg.PausedBy, "resume clears the provenance")
	require.True(t, cfg.PausedAt.IsZero())

	_, found, err := store.AppPayerAccountID(ctx, app)
	require.NoError(t, err)
	require.False(t, found, "no attributed event yet")

	seedAIEvent(t, pool, acct, app, "infra.ai.input.tokens", 10, haiku, "member-help", at, false) // 10,000 µ$
	_, err = pool.Exec(ctx, `INSERT INTO ms_billing.usage_events (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, $2, $3, '00000000-0000-0000-0000-000000000000', 'infra.compute.walltime.ms', 'sum', 1000000, $4)`,
		uuid.NewString(), acct.String(), app.String(), at) // 1,000,000 µ$ (1 µ$/ms)
	require.NoError(t, err)
	seedAIEvent(t, pool, acct, app, "infra.ai.input.tokens", 100, haiku, "", at, true)   // dev-served
	seedAIEvent(t, pool, acct, app, "infra.ai.input.tokens", 100, haiku, "", end, false) // next period

	got, err := store.AccountPeriodSpendMicros(ctx, acct, start, end)
	require.NoError(t, err)
	require.EqualValues(t, 1_010_000, got, "every metric counts toward the pool; dev-served and the next period do not")

	payer, found, err := store.AppPayerAccountID(ctx, app)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, acct, payer)

	sig, err := store.ExposureSignals(ctx, acct)
	require.NoError(t, err)
	require.Equal(t, "standard", sig.BillingMode, "accounts default to PaaS")
	require.False(t, sig.HasUsableCard)
	require.Zero(t, sig.PaidInvoices)

	_, err = pool.Exec(ctx, `INSERT INTO ms_billing.payment_methods_mirror (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year)
		VALUES ($1, 'pm_exposure', 'visa', '4242', 12, 2099)`, acct.String())
	require.NoError(t, err)
	for i, st := range []string{"paid", "paid", "open"} {
		// charge_funding_legacy_unresolved = true is the pre-052 provenance shape
		// (no funding account pinned); the CHECK requires one of the two shapes.
		_, err = pool.Exec(ctx, `INSERT INTO ms_billing.invoices (account_id, stripe_invoice_id, status, amount_due, amount_paid, currency, charge_funding_legacy_unresolved)
			VALUES ($1, $2, $3, 100, 100, 'usd', true)`, acct.String(), "in_exposure_"+string(rune('a'+i)), st)
		require.NoError(t, err)
	}
	sig, err = store.ExposureSignals(ctx, acct)
	require.NoError(t, err)
	require.True(t, sig.HasUsableCard)
	require.Equal(t, 2, sig.PaidInvoices, "open invoices do not count as paid")
	require.True(t, sig.DelinquentNow, "an open invoice with a balance is delinquent now")
	require.Zero(t, sig.LateCount)
	require.EqualValues(t, 5_000_000, budget.ExposureLimitMicros(cfg, sig), "delinquent: floored regardless of the card and 2 paid invoices")
	_, err = pool.Exec(ctx, `UPDATE ms_billing.invoices SET ever_failed = true WHERE stripe_invoice_id = 'in_exposure_a'`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE ms_billing.invoices SET status = 'paid', amount_due = 0 WHERE stripe_invoice_id = 'in_exposure_c'`)
	require.NoError(t, err)
	sig, err = store.ExposureSignals(ctx, acct)
	require.NoError(t, err)
	require.False(t, sig.DelinquentNow, "settled: nothing open with a balance")
	require.Equal(t, 1, sig.LateCount, "a paid invoice that once failed is remembered as late")
	require.Equal(t, 3, sig.PaidInvoices)
	require.EqualValues(t, 14_142_136, budget.ExposureLimitMicros(cfg, sig), "3 paid − 2×1 late = k 1 → $10×√2")

	// The system exposure row is storable and re-upserts in place.
	first, err := store.UpsertBudget(ctx, budget.Budget{Scope: budget.ScopeAccount, ScopeID: acct, Category: budget.CategoryExposure, AccountID: acct, LimitMicros: 17_320_508, AlertPercents: []int{80, 100}, Active: true, HardCap: true})
	require.NoError(t, err)
	again, err := store.UpsertBudget(ctx, budget.Budget{Scope: budget.ScopeAccount, ScopeID: acct, Category: budget.CategoryExposure, AccountID: acct, LimitMicros: 20_000_000, AlertPercents: []int{80, 100}, Active: true, HardCap: true})
	require.NoError(t, err)
	require.Equal(t, first.ID, again.ID, "the alert key (budget_id) survives the curve moving")
	require.EqualValues(t, 20_000_000, again.LimitMicros)
}
