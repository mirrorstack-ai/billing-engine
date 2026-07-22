//go:build integration

package usage_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// These exercise GetAccountBill's two app-roster enumeration queries
// (AppIDsWithUsage / MirroredAppIDsOverlappingWindow) against a real Postgres.
// They verify what the fake-store unit tests can't: the rolled ∪ live UNION
// dedup, the account gate, the window bounds on the live half, and the mirror's
// half-open [created_at, deleted_at) overlap arithmetic (both boundary edges).
// Reuses the seed helpers + appPeriodStart/End constants from
// app_usage_integration_test.go (same package).

// seedMirrorApp inserts a ms_billing.apps roster row (migration 027) directly;
// deletedAt == "" seeds a live row (NULL deleted_at).
func seedMirrorApp(t *testing.T, pool *pgxpool.Pool, acct, app uuid.UUID, createdAt, deletedAt string) {
	t.Helper()
	var del any
	if deletedAt != "" {
		del = deletedAt
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.apps (app_id, account_id, module_count, created_module_count, created_at, deleted_at)
		 VALUES ($1, $2, 0, 0, $3, $4)`,
		app.String(), acct.String(), createdAt, del)
	require.NoError(t, err)
}

// TestAppIDsWithUsage_Integration: the usage half enumerates the UNION of
// rolled (usage_aggregates for the period) and live (usage_events in the
// window) app_ids, deduped, account-gated, window-bounded on the live half,
// with the zero-UUID account-agent sentinel excluded from BOTH ledgers.
func TestAppIDsWithUsage_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	foreign := appSeedAccount(t, pool)
	appLive, appRolled, appBoth := uuid.New(), uuid.New(), uuid.New()
	appForeign, appOutside := uuid.New(), uuid.New()
	mod := uuid.New()
	appSeedMetricDef(t, pool, mod, "orders.placed", usage.KindCount, 100)

	// Live-only app: raw events in the window.
	appSeedEvent(t, pool, acct, appLive, mod, "orders.placed", usage.KindCount, 4, "2026-06-05T00:00:00Z", "", "")
	// Account-agent live usage: the zero UUID must not enter the app roster.
	appSeedEvent(t, pool, acct, uuid.Nil, mod, "orders.placed", usage.KindCount, 1, "2026-06-08T00:00:00Z", "", "")
	// Rolled-only app: a frozen aggregate row for the period, no live events.
	periodID := appSeedPeriod(t, pool, acct, appPeriodStart, appPeriodEnd)
	appSeedAggregate(t, pool, periodID, acct, appRolled, mod, "orders.placed", usage.KindCount, "", "", 10, 100, 1000, 1000)
	// Historical account-agent usage: filtering only live events would leak this
	// frozen zero-UUID aggregate into the app roster and its base-fee path.
	appSeedAggregate(t, pool, periodID, acct, uuid.Nil, mod, "orders.placed", usage.KindCount, "", "", 3, 100, 300, 300)
	// Both halves: must dedup to ONE entry.
	appSeedEvent(t, pool, acct, appBoth, mod, "orders.placed", usage.KindCount, 1, "2026-06-06T00:00:00Z", "", "")
	appSeedAggregate(t, pool, periodID, acct, appBoth, mod, "orders.placed", usage.KindCount, "", "", 2, 100, 200, 200)
	// Make the aggregate-bearing window a genuinely closed/frozen period.
	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.billing_periods SET status = 'invoiced' WHERE id = $1`,
		periodID.String())
	require.NoError(t, err)
	// Another account's event in the window: the account gate must drop it.
	appSeedEvent(t, pool, foreign, appForeign, mod, "orders.placed", usage.KindCount, 9, "2026-06-07T00:00:00Z", "", "")
	// Same account, event OUTSIDE the window: the live bounds must drop it.
	appSeedEvent(t, pool, acct, appOutside, mod, "orders.placed", usage.KindCount, 9, "2026-07-05T00:00:00Z", "", "")

	ids, err := store.AppIDsWithUsage(ctx, acct,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.ElementsMatch(t, []uuid.UUID{appLive, appRolled, appBoth}, ids)
	require.NotContains(t, ids, uuid.Nil,
		"the account-agent sentinel is excluded from both live and historical roster branches")
}

// TestMirroredAppIDs_Integration: the mirror half enumerates rows whose
// [created_at, deleted_at) overlaps the half-open window — created-inside and
// deleted-inside rows are in; rows created AT/after period_end or deleted
// AT/before period_start are out (both edges exclusive per half-open math);
// other accounts' rows never appear.
func TestMirroredAppIDs_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	foreign := appSeedAccount(t, pool)

	longLived, createdInside, deletedInside := uuid.New(), uuid.New(), uuid.New()
	createdAtEnd, deletedAtStart, foreignApp := uuid.New(), uuid.New(), uuid.New()
	seedMirrorApp(t, pool, acct, longLived, "2026-01-10T00:00:00Z", "")                         // spans the window → in
	seedMirrorApp(t, pool, acct, createdInside, "2026-06-22T14:30:00Z", "")                     // created mid-window → in
	seedMirrorApp(t, pool, acct, deletedInside, "2026-01-10T00:00:00Z", "2026-06-20T00:00:00Z") // deleted mid-window → in (spent base)
	seedMirrorApp(t, pool, acct, createdAtEnd, appPeriodEnd, "")                                // created exactly at period_end → out
	seedMirrorApp(t, pool, acct, deletedAtStart, "2026-01-10T00:00:00Z", appPeriodStart)        // deleted exactly at period_start → out
	seedMirrorApp(t, pool, foreign, foreignApp, "2026-06-05T00:00:00Z", "")                     // other account → out

	ids, err := store.MirroredAppIDs(ctx, acct,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.ElementsMatch(t, []uuid.UUID{longLived, createdInside, deletedInside}, ids)
}

// TestSettledNewCreationCharges_Integration_WalletDraw proves a creation settled
// without a Stripe invoice remains visible in 本期新建立. The synthetic wallet
// guard supplies the display identity, while only settled creation usage-draw
// ledger rows contribute to the base amount.
func TestSettledNewCreationCharges_Integration_WalletDraw(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStoreWithCreditWallet(pool, true)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	appID := uuid.New()
	const amountMicros = int64(3_250_123)
	walletRef := "wallet:app-proration:" + appID.String()
	recordedAt := appMustTime(t, "2026-06-18T12:30:00Z")

	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.apps
		   (app_id, account_id, module_count, created_module_count, name, created_at,
		    proration_invoice_id, updated_at)
		 VALUES ($1, $2, 3, 3, 'wallet app', $3, $4, $5)`,
		appID.String(), acct.String(), "2026-06-15T00:00:00Z", walletRef, recordedAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.app_base_snapshots
		   (app_id, period_start, period_end, module_count, base_micros, source)
		 VALUES ($1, $2, $3, 3, $4, 'proration')`,
		appID.String(), appPeriodStart, appPeriodEnd, amountMicros)
	require.NoError(t, err)

	keyPrefix := "wallet-draw:app-creation:" + appID.String() + ":"
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.credit_ledger
		   (account_id, amount_micros, type, status, balance_after_micros, actor,
		    idempotency_key, created_at)
		 VALUES
		   ($1, $2, 'usage_draw', 'settled', 0, 'system', $3, $4),
		   ($1, -9000000, 'usage_draw', 'failed', 0, 'system', $5, $4),
		   ($1, -7000000, 'subscription_draw', 'settled', 0, 'system', $6, $4)`,
		acct.String(), -amountMicros, keyPrefix+"usage_draw:base",
		recordedAt, keyPrefix+"usage_draw:failed", keyPrefix+"subscription_draw:distractor")
	require.NoError(t, err)

	rows, err := store.SettledNewCreationCharges(ctx, acct,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	row := rows[0]
	require.Equal(t, appID, row.AppID)
	require.Equal(t, appID, row.InvoiceID, "the internal UUID fallback remains stable")
	require.Equal(t, walletRef, row.Number, "the wallet guard is the public charge identity")
	require.Equal(t, amountMicros, row.AmountDueMicros)
	require.Equal(t, amountMicros, row.BaseMicros)
	// pgx decodes timestamptz into time.Local; compare the instant, not the
	// Location (recordedAt is UTC), so the assertion is timezone-independent.
	require.Equal(t, recordedAt, row.RecordedAt.UTC())
}

// TestListNewCreationCharges_Integration_PendingAddonUsesAccountFIFO proves the
// pending creation preview counts the exact co-created timer rows the creation
// sweep charges. Three older timers consume three of the account's five bundled
// FIFO slots, so five of appA's seven co-created timers are over; the per-app
// heuristic (7 - 5 = 2) would materially under-project the combined invoice.
func TestListNewCreationCharges_Integration_PendingAddonUsesAccountFIFO(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	owner, acct := uuid.New(), uuid.New()
	activatedAt := appMustTime(t, "2026-05-04T00:00:00Z")
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, activated_at)
		 VALUES ($1, 'user', $2, $3)`,
		acct.String(), owner.String(), activatedAt)
	require.NoError(t, err)

	appA, appB := uuid.New(), uuid.New()
	createdAt := appMustTime(t, "2026-06-19T00:00:00Z")
	seedMirrorApp(t, pool, acct, appA, createdAt.Format(time.RFC3339), "")
	seedMirrorApp(t, pool, acct, appB, "2026-05-20T00:00:00Z", "")
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.apps
		 SET module_count = 7, created_module_count = 7, name = 'FIFO app A'
		 WHERE app_id = $1`, appA.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.apps
		 SET module_count = 3, created_module_count = 3, name = 'earlier app B'
		 WHERE app_id = $1`, appB.String())
	require.NoError(t, err)

	seedTimers := func(appID uuid.UUID, installedAt time.Time, count int) {
		t.Helper()
		_, seedErr := pool.Exec(ctx,
			`INSERT INTO ms_billing.app_module_overage_timers
			   (account_id, app_id, installed_at, grace_expires_at)
			 SELECT $1, $2, $3, $4
			 FROM generate_series(1, $5::int)`,
			acct.String(), appID.String(), installedAt, usage.GraceExpiry(installedAt), count)
		require.NoError(t, seedErr)
	}

	// Account FIFO ranks 1-3 are appB's older live timers. appA's seven
	// co-created timers occupy ranks 4-10, so ranks 6-10 are the five over rows.
	seedTimers(appB, appMustTime(t, "2026-06-01T00:00:00Z"), 3)
	seedTimers(appA, createdAt, 7)

	const (
		wantOverCount      = 5
		perAppOverCount    = 7 - usage.IncludedModules
		testMicrosPerCent  = int64(10_000)
		wantPerTimerMicros = int64(1_500_000)
	)
	overCount, err := store.CoCreatedOverModuleTimerCount(ctx, acct, appA, createdAt, usage.IncludedModules)
	require.NoError(t, err)
	require.Equal(t, wantOverCount, overCount,
		"the shared account-FIFO query returns five over timers, not the per-app heuristic's two")
	require.Equal(t, 2, perAppOverCount)
	require.NotEqual(t, perAppOverCount, overCount)

	// The real account read derives anchor day 4 from activated_at. Both the
	// service's current window (anchored from now) and the sweep's creation
	// window (anchored from created_at) therefore resolve to the same 30 days.
	anchorDay, err := store.AccountAnchorDay(ctx, acct)
	require.NoError(t, err)
	require.Equal(t, 4, anchorDay)
	now := appMustTime(t, "2026-06-21T00:00:00Z") // still inside appA's creation grace
	periodStart, periodEnd := billingperiod.AnchoredPeriodWindow(now, anchorDay)
	sweepStart, sweepEnd := billingperiod.AnchoredPeriodWindow(createdAt, billingperiod.AnchorDay(activatedAt))
	require.Equal(t, appMustTime(t, "2026-06-04T00:00:00Z"), periodStart)
	require.Equal(t, appMustTime(t, "2026-07-04T00:00:00Z"), periodEnd)
	require.Equal(t, periodStart, sweepStart)
	require.Equal(t, periodEnd, sweepEnd)

	perTimerMicros := usage.CreationChargeOverageMicros(createdAt, periodStart, periodEnd)
	require.Equal(t, wantPerTimerMicros, perTimerMicros, "$3 x 15/30 = $1.50 per timer")
	wantProjectedMicros := int64(wantOverCount) * perTimerMicros
	perAppProjectedMicros := int64(perAppOverCount) * perTimerMicros
	require.Equal(t, int64(7_500_000), wantProjectedMicros)
	require.Equal(t, int64(3_000_000), perAppProjectedMicros)
	require.NotEqual(t, perAppProjectedMicros, wantProjectedMicros,
		"a regression to created_module_count - IncludedModules must fail")

	resp, err := usage.NewService(store).WithNow(func() time.Time { return now }).ListNewCreationCharges(ctx, usage.ListNewCreationChargesRequest{
		OwnerUserID: owner,
	})
	require.NoError(t, err)
	require.Len(t, resp.Charges, 1)
	charge := resp.Charges[0]
	require.Equal(t, appA, charge.AppID)
	require.Equal(t, usage.NewCreationChargeStatusPending, charge.Status)
	require.Equal(t, usage.CreationChargeBaseMicros(createdAt, periodStart, periodEnd), charge.AmountMicros)
	require.Equal(t, charge.AmountMicros, charge.BaseFeeMicros)
	require.Equal(t, perAppOverCount, charge.AddonModuleCount,
		"the existing frozen per-app count surface remains unchanged")
	require.Zero(t, charge.AddonMicros)
	require.Equal(t, wantProjectedMicros, charge.ProjectedAddonMicros)

	// Reproduce cycle.centsFromMicros' non-negative round-half-up boundary on
	// the full per-timer amount, then compare it with the preview's cents.
	sweepPerTimerMicros := usage.ProratedBaseMicros(usage.ModuleOverageFeeMicros, createdAt, periodStart, periodEnd)
	if !usage.GraceExpiry(createdAt.UTC()).Before(periodEnd) {
		sweepPerTimerMicros += usage.ModuleOverageFeeMicros
	}
	sweepPerTimerCents := (sweepPerTimerMicros + testMicrosPerCent/2) / testMicrosPerCent
	require.Equal(t, int64(150), sweepPerTimerCents)
	require.Equal(t, sweepPerTimerCents, perTimerMicros/testMicrosPerCent,
		"preview rounds one timer to cents exactly where the sweep does")
}

// TestGetAccountBill_Integration_RolledAgentModelsUseFrozenCharges proves the
// account-agent model decomposition reads the authoritative charged_micros from
// a frozen usage_aggregates period. A model-less custom row remains in the
// agent total as the consumer-computed "Other" residual, never as a model row.
func TestGetAccountBill_Integration_RolledAgentModelsUseFrozenCharges(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	owner := uuid.New()
	acct := uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id) VALUES ($1, 'user', $2)`,
		acct.String(), owner.String())
	require.NoError(t, err)
	appID := uuid.New()
	seedMirrorApp(t, pool, acct, appID, "2026-05-01T00:00:00Z", "")
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.app_custom_domains
		   (account_id, app_id, hostname, activated_at, removed_at)
		 VALUES
		   ($1, $2, 'one.example.test',   '2026-05-10T00:00:00Z', NULL),
		   ($1, $2, 'two.example.test',   '2026-05-15T00:00:00Z', NULL),
		   ($1, $2, 'gone.example.test',  '2026-05-05T00:00:00Z', '2026-05-20T00:00:00Z')`,
		acct.String(), appID.String())
	require.NoError(t, err)

	periodID := appSeedPeriod(t, pool, acct, appPeriodStart, appPeriodEnd)
	sentinel := usage.PlatformInfraModuleID()
	const (
		haiku  = "anthropic.claude-haiku-4-5-20251001-v1:0"
		sonnet = "anthropic.claude-sonnet-4-6"
	)

	// These charged amounts are already frozen by rollup and must be forwarded
	// exactly, without looking up or re-applying current model prices.
	appSeedAggregate(t, pool, periodID, acct, uuid.Nil, sentinel,
		"infra.ai.input.tokens", usage.KindSum, haiku, "", 2, 1000, 2000, 2400)
	appSeedAggregate(t, pool, periodID, acct, uuid.Nil, sentinel,
		"infra.ai.output.tokens", usage.KindSum, sonnet, "", 1, 15000, 15000, 18000)
	// Model-less agent spend contributes to Agent.TotalMicros only; web-account
	// derives it as the positive residual instead of receiving an "other" row.
	appSeedAggregate(t, pool, periodID, acct, uuid.Nil, uuid.New(),
		"agent.work.units", usage.KindCount, "", "", 5, 100, 500, 500)

	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.billing_periods SET status = 'invoiced' WHERE id = $1`,
		periodID.String())
	require.NoError(t, err)

	resp, err := usage.NewService(store).GetAccountBill(ctx, usage.GetAccountBillRequest{
		OwnerUserID: owner,
		PeriodID:    periodID.String(),
	})
	require.NoError(t, err)

	require.Equal(t, []usage.AgentModelUsage{
		{Model: sonnet, BillableQuantity: 1, ChargedMicros: 18000},
		{Model: haiku, BillableQuantity: 2, ChargedMicros: 2400},
	}, resp.Agent.Models, "models sort by charged_micros descending")

	modelMicros := sumModelCharges(resp.Agent.Models)
	require.LessOrEqual(t, modelMicros, resp.Agent.TotalMicros)
	require.EqualValues(t, 20400, modelMicros)
	require.EqualValues(t, 500, resp.Agent.ModuleUsageMicros)
	require.EqualValues(t, 20400, resp.Agent.InfraMicros)
	require.EqualValues(t, 20900, resp.Agent.TotalMicros,
		"model-less spend remains in the agent total as the consumer's residual")
	require.EqualValues(t, 2*usage.DomainFeeMicros, resp.CustomDomainsMicros,
		"only currently-live custom domains contribute to the account line")
	require.Equal(t,
		resp.BaseFeeTotalMicros+resp.ModuleUsageTotalMicros+resp.InfraTotalMicros+
			resp.AccountOverageMicros+resp.CustomDomainsMicros+
			resp.Agent.TotalMicros-resp.PaasCreditMicros,
		resp.TotalMicros,
		"all account lines reconcile exactly into total_micros")
}
