//go:build integration

package usage_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit/rollout"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
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

func TestLiveOverModuleTimerCountForApp_Integration_AttributesAccountFIFO(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	appA, appB := uuid.New(), uuid.New()
	seedMirrorApp(t, pool, acct, appA, "2026-06-01T00:00:00Z", "")
	seedMirrorApp(t, pool, acct, appB, "2026-06-02T00:00:00Z", "")

	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.app_module_overage_timers
			(account_id, app_id, installed_at, grace_expires_at)
		SELECT $1, $2, '2026-06-01T00:00:00Z'::timestamptz,
		       '2026-06-04T00:00:00Z'::timestamptz
		FROM generate_series(1, 4)`, acct.String(), appA.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.app_module_overage_timers
			(account_id, app_id, installed_at, grace_expires_at)
		SELECT $1, $2, '2026-06-02T00:00:00Z'::timestamptz,
		       '2026-06-05T00:00:00Z'::timestamptz
		FROM generate_series(1, 5)`, acct.String(), appB.String())
	require.NoError(t, err)

	overA, err := store.LiveOverModuleTimerCountForApp(ctx, acct, appA, usage.IncludedModules)
	require.NoError(t, err)
	overB, err := store.LiveOverModuleTimerCountForApp(ctx, acct, appB, usage.IncludedModules)
	require.NoError(t, err)
	require.Zero(t, overA, "the first four account-FIFO installs stay included")
	require.Equal(t, 4, overB, "only one of app B's five timers occupies the final included slot")
}

func TestActivatedRecurringFeeCountsAndSettledDomains_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	accountID := appSeedAccount(t, pool)
	chargedApp, legacyApp, pendingApp := uuid.New(), uuid.New(), uuid.New()
	seedMirrorApp(t, pool, accountID, chargedApp, "2026-06-01T00:00:00Z", "")
	seedMirrorApp(t, pool, accountID, legacyApp, "2026-06-02T00:00:00Z", "")
	seedMirrorApp(t, pool, accountID, pendingApp, "2026-06-03T00:00:00Z", "")

	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.apps SET proration_invoice_id = 'in_app' WHERE app_id = $1`,
		chargedApp)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.app_base_snapshots
		   (app_id, period_start, period_end, module_count, base_micros, source)
		 VALUES ($1, $2, $3, 0, 20000000, 'advance')`,
		legacyApp, appPeriodStart, appPeriodEnd)
	require.NoError(t, err)

	for i := 0; i < 9; i++ {
		installedAt := appMustTime(t, "2026-06-04T00:00:00Z").Add(time.Duration(i) * time.Minute)
		var chargedAt any
		if i == 5 || i == 6 {
			chargedAt = installedAt.Add(usage.GraceDays * 24 * time.Hour)
		}
		_, err = pool.Exec(ctx,
			`INSERT INTO ms_billing.app_module_overage_timers
			   (account_id, app_id, installed_at, grace_expires_at, grace_charged_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			accountID, chargedApp, installedAt,
			installedAt.Add(usage.GraceDays*24*time.Hour), chargedAt)
		require.NoError(t, err)
	}

	invoiceID := uuid.New()
	chargedAt := appMustTime(t, "2026-06-18T12:30:00Z")
	periodStart := appMustTime(t, appPeriodStart)
	periodEnd := appMustTime(t, appPeriodEnd)
	seedInvoiceMirror(t, pool, accountID, invoiceID, "in_domain", "paid", 200, 200,
		chargedAt, "MS-0042", "", "", false)
	chargedDomainID, periodClosedDomainID, zeroCentsDomainID, skippedPrepaidDomainID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.app_custom_domains
		   (id, account_id, app_id, hostname, activated_at, charged_at,
		    charge_resolved, charge_invoice_id)
		 VALUES
		   ($1, $2, $3, 'mirrorstack.ai', $4, $5, true, 'in_domain'),
		   ($6, $2, $3, 'closed.example', $4, NULL, true, NULL),
		   ($7, $2, $3, 'zero.example', $4, NULL, true, NULL),
		   ($8, $2, $3, 'prepaid.example', $4, NULL, false, NULL)`,
		chargedDomainID, accountID, chargedApp,
		appMustTime(t, "2026-06-15T00:00:00Z"), chargedAt,
		periodClosedDomainID, zeroCentsDomainID, skippedPrepaidDomainID)
	require.NoError(t, err)

	readStore, ok := store.(interface {
		ActivatedRecurringFeeCounts(context.Context, uuid.UUID, int, time.Time) (usage.RecurringFeeCounts, error)
		SettledDomainCreationCharges(context.Context, uuid.UUID, time.Time, time.Time) ([]usage.SettledDomainCreationChargeRaw, error)
	})
	require.True(t, ok)

	counts, err := readStore.ActivatedRecurringFeeCounts(ctx, accountID, usage.IncludedModules, periodEnd)
	require.NoError(t, err)
	require.Equal(t, usage.RecurringFeeCounts{Apps: 2, ModuleOverages: 2, CustomDomains: 4}, counts,
		"any live domain activated before the boundary joins the recurring forecast, including terminal rows that do not write charged_at")

	domains, err := readStore.SettledDomainCreationCharges(ctx, accountID,
		periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, domains, 1)
	require.Equal(t, chargedDomainID, domains[0].ID)
	require.Equal(t, "mirrorstack.ai", domains[0].Hostname)
	require.True(t, chargedAt.Equal(domains[0].ChargedAt))
	require.Equal(t, invoiceID, domains[0].InvoiceID)
	require.Equal(t, "MS-0042", domains[0].Number)
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

// TestSettledNewCreationCharges_Integration_ShadowSelectedWalletDraw proves a
// creation settled without a Stripe invoice remains visible to the dedicated
// selected-account shadow projection after an enforce→shadow rollback. The
// public/excluded store remains on the legacy query, while the shadow-only
// access reads durable wallet truth without authorizing a mutation or changing
// a public response.
func TestSettledNewCreationCharges_Integration_ShadowSelectedWalletDraw(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`,
		acct,
	)
	require.NoError(t, err)
	appID := uuid.New()
	const amountMicros = int64(3_250_123)
	walletRef := "wallet:app-proration:" + appID.String()
	recordedAt := appMustTime(t, "2026-06-18T12:30:00Z")
	laterAppUpdate := appMustTime(t, "2026-07-10T09:00:00Z")

	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.apps
		   (app_id, account_id, module_count, created_module_count, name, created_at,
		    proration_invoice_id, updated_at)
		 VALUES ($1, $2, 3, 3, 'wallet app', $3, $4, $5)`,
		appID.String(), acct.String(), "2026-05-30T00:00:00Z", walletRef, laterAppUpdate)
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

	legacyRows, err := usage.NewStoreWithCreditAccess(
		pool,
		func(uuid.UUID) bool { return false },
	).SettledNewCreationCharges(
		ctx,
		acct,
		appMustTime(t, appPeriodStart),
		appMustTime(t, appPeriodEnd),
	)
	require.NoError(t, err)
	require.Empty(t, legacyRows,
		"an excluded/public legacy read must not enter the wallet query")

	allowlistDigest := sha256.Sum256([]byte(acct.String()))
	controller := rollout.NewController(rollout.Parse(rollout.Config{
		MasterEnabled:   true,
		SchemaReady:     true,
		Component:       rollout.ComponentAPI,
		Mode:            string(rollout.ModeShadow),
		BasisPoints:     "0",
		Allowlist:       acct.String(),
		AllowlistSHA256: hex.EncodeToString(allowlistDigest[:]),
		CoreManifestSHA: "1111111111111111111111111111111111111111",
		BillingSHA:      "2222222222222222222222222222222222222222",
	}), nil)
	store := usage.NewStoreWithCreditAccess(
		pool,
		rollout.ReadOnlySelectedAccess(controller),
	)
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

func TestUnresolvedOneTimeCharges_Integration_CreationTerminalsAndD11(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	activatedAt := appMustTime(t, "2026-06-11T09:00:00Z")
	accountID := uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts
		   (id, owner_kind, owner_user_id, activated_at)
		 VALUES ($1, 'user', $2, $3)`,
		accountID, uuid.New(), activatedAt)
	require.NoError(t, err)

	foreignAccount := uuid.New()
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts
		   (id, owner_kind, owner_user_id, activated_at)
		 VALUES ($1, 'user', $2, $3)`,
		foreignAccount, uuid.New(), activatedAt)
	require.NoError(t, err)
	unactivatedAccount := appSeedAccount(t, pool)

	type appFixture struct {
		id        uuid.UUID
		accountID uuid.UUID
		createdAt time.Time
		deletedAt *time.Time
		guard     string
		skippedAt *time.Time
	}
	insertApp := func(f appFixture) {
		t.Helper()
		var deletedAt, skippedAt any
		if f.deletedAt != nil {
			deletedAt = *f.deletedAt
		}
		if f.skippedAt != nil {
			skippedAt = *f.skippedAt
		}
		var guard any
		if f.guard != "" {
			guard = f.guard
		}
		_, insertErr := pool.Exec(ctx,
			`INSERT INTO ms_billing.apps
			   (app_id, account_id, module_count, created_module_count, created_at,
			    deleted_at, proration_invoice_id, proration_skipped_at)
			 VALUES ($1, $2, 0, 0, $3, $4, $5, $6)`,
			f.id, f.accountID, f.createdAt, deletedAt, guard, skippedAt)
		require.NoError(t, insertErr)
	}

	inGrace, postETA := uuid.New(), uuid.New()
	guardArmed, skipArmed := uuid.New(), uuid.New()
	deletedBefore, deletedExactly, deletedAfter := uuid.New(), uuid.New(), uuid.New()
	foreignApp, unactivatedApp := uuid.New(), uuid.New()
	createdAt := appMustTime(t, "2026-07-15T11:00:00Z")
	graceExpiry := usage.GraceExpiry(createdAt)
	oneSecondBefore := graceExpiry.Add(-time.Second)
	oneSecondAfter := graceExpiry.Add(time.Second)
	skippedAt := graceExpiry.Add(time.Hour)

	for _, fixture := range []appFixture{
		{id: inGrace, accountID: accountID, createdAt: appMustTime(t, "2026-07-25T11:00:00Z")},
		{id: postETA, accountID: accountID, createdAt: appMustTime(t, "2026-07-01T11:00:00Z")},
		{id: guardArmed, accountID: accountID, createdAt: createdAt, guard: "in_armed"},
		{id: skipArmed, accountID: accountID, createdAt: createdAt, skippedAt: &skippedAt},
		{id: deletedBefore, accountID: accountID, createdAt: createdAt, deletedAt: &oneSecondBefore},
		{id: deletedExactly, accountID: accountID, createdAt: createdAt, deletedAt: &graceExpiry},
		{id: deletedAfter, accountID: accountID, createdAt: createdAt, deletedAt: &oneSecondAfter},
		{id: foreignApp, accountID: foreignAccount, createdAt: createdAt},
		{id: unactivatedApp, accountID: unactivatedAccount, createdAt: createdAt},
	} {
		insertApp(fixture)
	}

	rows, err := store.UnresolvedOneTimeCharges(
		ctx, accountID, usage.IncludedModules, usage.GraceDays*24,
	)
	require.NoError(t, err)
	require.Len(t, rows, 4)

	got := make(map[uuid.UUID]usage.UnresolvedOneTimeChargeRaw, len(rows))
	for _, row := range rows {
		require.Equal(t, usage.UnresolvedOneTimeChargeCreationBase, row.Kind)
		got[row.ChargeID] = row
	}
	require.ElementsMatch(t,
		[]uuid.UUID{inGrace, postETA, deletedExactly, deletedAfter},
		[]uuid.UUID{
			got[inGrace].ChargeID,
			got[postETA].ChargeID,
			got[deletedExactly].ChargeID,
			got[deletedAfter].ChargeID,
		},
	)
	require.False(t, got[inGrace].CountsTowardRecurring,
		"an unresolved creation is one-time exposure, not recurring base")
	require.False(t, got[postETA].CountsTowardRecurring,
		"the ETA-to-sweep gap stays one-time until the durable charge guard arms")
	require.False(t, got[deletedExactly].CountsTowardRecurring)
	require.False(t, got[deletedAfter].CountsTowardRecurring)
	require.Equal(t, usage.GraceExpiry(got[inGrace].ChargeAt), got[inGrace].GraceExpiresAt)
	require.Equal(t, activatedAt, got[inGrace].ActivatedAt.UTC())
	require.NotContains(t, got, guardArmed)
	require.NotContains(t, got, skipArmed)
	require.NotContains(t, got, deletedBefore,
		"D11 deletion before grace is free; deletion exactly at grace still owes")
	require.NotContains(t, got, foreignApp)
	require.NotContains(t, got, unactivatedApp)
}

func TestUnresolvedOneTimeCharges_Integration_TimerFIFORecoveryAndTerminals(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()
	activatedAt := appMustTime(t, "2026-06-11T09:00:00Z")

	seedAccount := func() uuid.UUID {
		t.Helper()
		accountID := uuid.New()
		_, err := pool.Exec(ctx,
			`INSERT INTO ms_billing.accounts
			   (id, owner_kind, owner_user_id, activated_at)
			 VALUES ($1, 'user', $2, $3)`,
			accountID, uuid.New(), activatedAt)
		require.NoError(t, err)
		return accountID
	}
	seedApp := func(accountID uuid.UUID, createdAt time.Time, guard string, attemptedAt *time.Time) uuid.UUID {
		t.Helper()
		appID := uuid.New()
		var guardArg, attemptedArg any
		if guard != "" {
			guardArg = guard
		}
		if attemptedAt != nil {
			attemptedArg = *attemptedAt
		}
		_, err := pool.Exec(ctx,
			`INSERT INTO ms_billing.apps
			   (app_id, account_id, module_count, created_module_count, created_at,
			    proration_invoice_id, proration_attempted_at)
			 VALUES ($1, $2, 0, 0, $3, $4, $5)`,
			appID, accountID, createdAt, guardArg, attemptedArg)
		require.NoError(t, err)
		return appID
	}
	seedTimer := func(accountID, appID uuid.UUID, installedAt, graceExpiresAt time.Time) uuid.UUID {
		t.Helper()
		timerID := uuid.New()
		_, err := pool.Exec(ctx,
			`INSERT INTO ms_billing.app_module_overage_timers
			   (id, account_id, app_id, installed_at, grace_expires_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			timerID, accountID, appID, installedAt, graceExpiresAt)
		require.NoError(t, err)
		return timerID
	}
	rowsFor := func(accountID uuid.UUID) []usage.UnresolvedOneTimeChargeRaw {
		t.Helper()
		rows, err := store.UnresolvedOneTimeCharges(
			ctx, accountID, usage.IncludedModules, usage.GraceDays*24,
		)
		require.NoError(t, err)
		return rows
	}
	moduleRows := func(rows []usage.UnresolvedOneTimeChargeRaw) []usage.UnresolvedOneTimeChargeRaw {
		t.Helper()
		var modules []usage.UnresolvedOneTimeChargeRaw
		for _, row := range rows {
			if row.Kind == usage.UnresolvedOneTimeChargeModuleTimer {
				modules = append(modules, row)
			}
		}
		return modules
	}

	t.Run("legacy app attempt without frozen ownership fails closed", func(t *testing.T) {
		accountID := seedAccount()
		createdAt := appMustTime(t, "2026-07-15T11:00:00Z")
		attemptedAt := usage.GraceExpiry(createdAt)
		appID := seedApp(accountID, createdAt, "", &attemptedAt)

		for i := 0; i < usage.IncludedModules+2; i++ {
			seedTimer(accountID, appID, createdAt, usage.GraceExpiry(createdAt))
		}
		_, err := store.UnresolvedOneTimeCharges(
			ctx, accountID, usage.IncludedModules, usage.GraceDays*24,
		)
		require.ErrorIs(t, err, billing.ErrCombinedProrationAttemptUnknown)
	})

	t.Run("timer attempt survives over to included rank improvement", func(t *testing.T) {
		accountID := seedAccount()
		appID := seedApp(
			accountID,
			appMustTime(t, "2026-06-01T00:00:00Z"),
			"creation:settled",
			nil,
		)
		var timers []uuid.UUID
		for i := 0; i < usage.IncludedModules+1; i++ {
			installedAt := appMustTime(t, "2026-07-01T00:00:00Z").Add(time.Duration(i) * time.Hour)
			timers = append(timers, seedTimer(
				accountID, appID, installedAt, usage.GraceExpiry(installedAt),
			))
		}
		attemptedTimer := timers[len(timers)-1]
		_, err := pool.Exec(ctx,
			`UPDATE ms_billing.app_module_overage_timers
			 SET charge_attempted_at = $2
			 WHERE id = $1`,
			attemptedTimer, appMustTime(t, "2026-07-05T00:00:00Z"))
		require.NoError(t, err)
		_, err = pool.Exec(ctx,
			`UPDATE ms_billing.app_module_overage_timers
			 SET removed_at = $2
			 WHERE id = $1`,
			timers[0], appMustTime(t, "2026-07-06T00:00:00Z"))
		require.NoError(t, err)

		modules := moduleRows(rowsFor(accountID))
		require.Len(t, modules, 1)
		require.Equal(t, attemptedTimer, modules[0].ChargeID)
		require.False(t, modules[0].CountsTowardRecurring,
			"after the earlier removal it is currently included, but recovery still owns it")

		_, err = pool.Exec(ctx,
			`UPDATE ms_billing.app_module_overage_timers
			 SET removed_at = $2
			 WHERE id = $1`,
			attemptedTimer, appMustTime(t, "2026-07-07T00:00:00Z"))
		require.NoError(t, err)
		modules = moduleRows(rowsFor(accountID))
		require.Len(t, modules, 1)
		require.Equal(t, attemptedTimer, modules[0].ChargeID)
		require.False(t, modules[0].CountsTowardRecurring,
			"removal cannot erase a timer whose standalone Stripe recovery marker is unresolved")

		// The exposure the projection holds must have a reachable exit: the sweep
		// work list carves removal out for an attempted timer (mirroring
		// AppsPendingProration), so the crashed attempt converges on its terminal
		// guard instead of being projected forever.
		cycleStore := cycle.NewStore(pool)
		cands, err := cycleStore.ModuleOverageTimersPastGrace(
			ctx, appMustTime(t, "2026-07-08T00:00:00Z"),
		)
		require.NoError(t, err)
		swept := make(map[uuid.UUID]bool, len(cands))
		for _, cand := range cands {
			swept[cand.ID] = true
		}
		require.True(t, swept[attemptedTimer],
			"the sweep must recover a removed timer after its Stripe attempt marker was stamped")
		require.False(t, swept[timers[0]],
			"a removal with no attempt marker still drops out of the sweep")

		pending, err := cycleStore.ModuleTimerStillPending(ctx, attemptedTimer)
		require.NoError(t, err)
		require.True(t, pending,
			"charge-time re-verification must preserve attempted recovery ownership")
		require.NoError(t, cycleStore.MarkModuleTimerIncluded(ctx, attemptedTimer))
		require.Empty(t, moduleRows(rowsFor(accountID)),
			"the durable timer terminal removes attempted recovery exposure exactly once")
	})

	t.Run("ETA does not filter and resolved or removed timers do", func(t *testing.T) {
		accountID := seedAccount()
		appID := seedApp(
			accountID,
			appMustTime(t, "2026-06-01T00:00:00Z"),
			"creation:settled",
			nil,
		)
		baseInstall := appMustTime(t, "2026-07-01T00:00:00Z")
		for i := 0; i < usage.IncludedModules; i++ {
			installedAt := baseInstall.Add(time.Duration(i) * time.Hour)
			seedTimer(accountID, appID, installedAt, usage.GraceExpiry(installedAt))
		}
		pastETAInstall := baseInstall.Add(24 * time.Hour)
		futureETAInstall := baseInstall.Add(48 * time.Hour)
		pastETA := seedTimer(
			accountID, appID, pastETAInstall,
			appMustTime(t, "2026-07-02T00:00:00Z"),
		)
		futureETA := seedTimer(
			accountID, appID, futureETAInstall,
			appMustTime(t, "2027-07-02T00:00:00Z"),
		)

		modules := moduleRows(rowsFor(accountID))
		require.Len(t, modules, 2)
		require.ElementsMatch(t, []uuid.UUID{pastETA, futureETA},
			[]uuid.UUID{modules[0].ChargeID, modules[1].ChargeID})

		_, err := pool.Exec(ctx,
			`UPDATE ms_billing.app_module_overage_timers
			 SET grace_resolved = true
			 WHERE id = $1`, pastETA)
		require.NoError(t, err)
		_, err = pool.Exec(ctx,
			`UPDATE ms_billing.app_module_overage_timers
			 SET removed_at = now()
			 WHERE id = $1`, futureETA)
		require.NoError(t, err)
		require.Empty(t, moduleRows(rowsFor(accountID)))
	})
}

func TestUnresolvedOneTimeCharges_Integration_FrozenCombinedOwnership(t *testing.T) {
	type frozenFixture struct {
		pool      *pgxpool.Pool
		accountID uuid.UUID
		appID     uuid.UUID
		createdAt time.Time
		timerIDs  []uuid.UUID
		attempt   cycle.CombinedProrationAttempt
	}
	seedFrozen := func(t *testing.T) frozenFixture {
		t.Helper()
		pool := testutil.NewTestDB(t)
		ctx := context.Background()
		activatedAt := appMustTime(t, "2026-06-11T09:00:00Z")
		accountID := uuid.New()
		_, err := pool.Exec(ctx,
			`INSERT INTO ms_billing.accounts
			   (id, owner_kind, owner_user_id, activated_at)
			 VALUES ($1, 'user', $2, $3)`,
			accountID, uuid.New(), activatedAt)
		require.NoError(t, err)

		appID := uuid.New()
		createdAt := appMustTime(t, "2026-07-25T11:00:00Z")
		_, err = pool.Exec(ctx,
			`INSERT INTO ms_billing.apps
			   (app_id, account_id, module_count, created_module_count, created_at)
			 VALUES ($1, $2, 7, 7, $3)`,
			appID, accountID, createdAt)
		require.NoError(t, err)

		timerIDs := make([]uuid.UUID, 0, usage.IncludedModules+2)
		for i := 0; i < usage.IncludedModules+2; i++ {
			timerID := seqUUID(byte(100 + i))
			timerIDs = append(timerIDs, timerID)
			_, err = pool.Exec(ctx,
				`INSERT INTO ms_billing.app_module_overage_timers
				   (id, account_id, app_id, installed_at, grace_expires_at)
				 VALUES ($1, $2, $3, $4, $5)`,
				timerID, accountID, appID, createdAt, usage.GraceExpiry(createdAt))
			require.NoError(t, err)
		}

		periodStart := appMustTime(t, "2026-07-11T00:00:00Z")
		periodEnd := appMustTime(t, "2026-08-11T00:00:00Z")
		attempt, outcome, err := cycle.NewStore(pool).FreezeCombinedProrationAttempt(
			ctx,
			appID,
			usage.GraceExpiry(createdAt),
			cycle.CombinedProrationChargeShape{
				AccountID:          accountID,
				Currency:           "usd",
				BaseChargeMicros:   10_967_742,
				BaseChargeCents:    1_097,
				ModuleChargeMicros: 1_645_161,
				ModuleChargeCents:  165,
				CoverageStart:      createdAt,
				CoverageEnd:        periodEnd,
				BaseDescription:    "frozen app base",
				ModuleDescription:  "frozen module unit",
				Snapshot: cycle.AppBaseSnapshot{
					AppID:       appID,
					PeriodStart: periodStart,
					PeriodEnd:   periodEnd,
					ModuleCount: usage.IncludedModules + 2,
					BaseMicros:  10_967_742,
				},
			},
			false,
		)
		require.NoError(t, err)
		require.Equal(t, cycle.StripeRailClaimed, outcome)
		require.Len(t, attempt.TimerIDs, 2)
		return frozenFixture{
			pool:      pool,
			accountID: accountID,
			appID:     appID,
			createdAt: createdAt,
			timerIDs:  timerIDs,
			attempt:   attempt,
		}
	}

	t.Run("exact rows survive mutation and disappear only at terminal commit", func(t *testing.T) {
		f := seedFrozen(t)
		ctx := context.Background()
		store := usage.NewStore(f.pool)
		rows, err := store.UnresolvedOneTimeCharges(
			ctx, f.accountID, usage.IncludedModules, usage.GraceDays*24,
		)
		require.NoError(t, err)
		require.Len(t, rows, 3)

		byID := make(map[uuid.UUID]usage.UnresolvedOneTimeChargeRaw, len(rows))
		for _, row := range rows {
			require.True(t, row.Frozen)
			byID[row.ChargeID] = row
		}
		require.Equal(t, int64(10_967_742), byID[f.appID].FrozenAmountMicros)
		for _, timerID := range f.attempt.TimerIDs {
			require.Equal(t, int64(1_645_161), byID[timerID].FrozenAmountMicros)
		}

		// Remove an earlier included timer (FIFO improvement), remove one exact
		// child, and soft-delete the app before grace. None may rewrite or erase
		// the immutable ownership set; only recurring-membership flags change.
		_, err = f.pool.Exec(ctx,
			`UPDATE ms_billing.app_module_overage_timers
			 SET removed_at = $2
			 WHERE id = $1`,
			f.timerIDs[0], f.createdAt.Add(time.Hour))
		require.NoError(t, err)
		_, err = f.pool.Exec(ctx,
			`UPDATE ms_billing.app_module_overage_timers
			 SET removed_at = $2
			 WHERE id = $1`,
			f.attempt.TimerIDs[1], f.createdAt.Add(2*time.Hour))
		require.NoError(t, err)
		_, err = f.pool.Exec(ctx,
			`UPDATE ms_billing.apps
			 SET deleted_at = $2
			 WHERE app_id = $1`,
			f.appID, f.createdAt.Add(time.Hour))
		require.NoError(t, err)

		rows, err = store.UnresolvedOneTimeCharges(
			ctx, f.accountID, usage.IncludedModules, usage.GraceDays*24,
		)
		require.NoError(t, err)
		require.Len(t, rows, 3)
		for _, row := range rows {
			require.False(t, row.CountsTowardRecurring)
		}

		// A missing frozen child must fail the strict projection rather than
		// understate. Restore it afterwards so the terminal path can be tested.
		missingChild := f.attempt.TimerIDs[0]
		_, err = f.pool.Exec(ctx,
			`DELETE FROM ms_billing.app_combined_proration_attempt_timers
			 WHERE app_id = $1 AND timer_id = $2`,
			f.appID, missingChild)
		require.NoError(t, err)
		_, err = store.UnresolvedOneTimeCharges(
			ctx, f.accountID, usage.IncludedModules, usage.GraceDays*24,
		)
		require.ErrorContains(t, err, "child count mismatch")
		_, err = f.pool.Exec(ctx,
			`INSERT INTO ms_billing.app_combined_proration_attempt_timers
			   (app_id, timer_id)
			 VALUES ($1, $2)`,
			f.appID, missingChild)
		require.NoError(t, err)

		tx, err := f.pool.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx,
			`UPDATE ms_billing.app_combined_proration_attempts
			 SET resolved_at = $2, resolved_invoice_id = 'in_frozen_terminal'
			 WHERE app_id = $1`,
			f.appID, f.createdAt.Add(4*time.Hour))
		require.NoError(t, err)
		_, err = tx.Exec(ctx,
			`UPDATE ms_billing.apps
			 SET proration_invoice_id = 'in_frozen_terminal'
			 WHERE app_id = $1`,
			f.appID)
		require.NoError(t, err)
		_, err = tx.Exec(ctx,
			`UPDATE ms_billing.app_module_overage_timers
			 SET grace_resolved = true,
			     grace_charged_at = $2,
			     grace_invoice_id = 'in_frozen_terminal',
			     grace_invoice_item_id = 'ii_frozen_terminal'
			 WHERE id = ANY($1::uuid[])`,
			f.attempt.TimerIDs, f.createdAt.Add(4*time.Hour))
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		rows, err = store.UnresolvedOneTimeCharges(
			ctx, f.accountID, usage.IncludedModules, usage.GraceDays*24,
		)
		require.NoError(t, err)
		require.Empty(t, rows,
			"the pending amount is removed only after header, app, and exact children commit terminal")
	})

	t.Run("resolved header with unresolved exact children fails closed", func(t *testing.T) {
		f := seedFrozen(t)
		ctx := context.Background()
		tx, err := f.pool.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx,
			`UPDATE ms_billing.app_combined_proration_attempts
			 SET resolved_at = $2, resolved_invoice_id = 'in_split'
			 WHERE app_id = $1`,
			f.appID, f.createdAt.Add(4*time.Hour))
		require.NoError(t, err)
		_, err = tx.Exec(ctx,
			`UPDATE ms_billing.apps
			 SET proration_invoice_id = 'in_split'
			 WHERE app_id = $1`,
			f.appID)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		_, err = usage.NewStore(f.pool).UnresolvedOneTimeCharges(
			ctx, f.accountID, usage.IncludedModules, usage.GraceDays*24,
		)
		require.ErrorIs(t, err, billing.ErrCombinedProrationAttemptUnknown)
	})
}

func TestUnresolvedOneTimeCharges_Integration_MVCCWalletGuardHandoff(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	accountID, ownerID, appID := uuid.New(), uuid.New(), uuid.New()
	activatedAt := appMustTime(t, "2026-06-11T09:00:00Z")
	createdAt := appMustTime(t, "2026-07-25T11:00:00Z")
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts
		   (id, owner_kind, owner_user_id, activated_at)
		 VALUES ($1, 'user', $2, $3)`,
		accountID, ownerID, activatedAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.apps
		   (app_id, account_id, module_count, created_module_count, created_at)
		 VALUES ($1, $2, 7, 7, $3)`,
		appID, accountID, createdAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.app_module_overage_timers
		   (account_id, app_id, installed_at, grace_expires_at)
		 SELECT $1, $2, $3, $4
		 FROM generate_series(1, 7)`,
		accountID, appID, createdAt, usage.GraceExpiry(createdAt))
	require.NoError(t, err)

	type projectionSet struct {
		creationIDs map[uuid.UUID]bool
		timerIDs    map[uuid.UUID]bool
	}
	readProjection := func() projectionSet {
		t.Helper()
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		rows, readErr := store.UnresolvedOneTimeCharges(
			readCtx, accountID, usage.IncludedModules, usage.GraceDays*24,
		)
		require.NoError(t, readErr)
		set := projectionSet{
			creationIDs: make(map[uuid.UUID]bool),
			timerIDs:    make(map[uuid.UUID]bool),
		}
		seen := make(map[uuid.UUID]bool, len(rows))
		for _, row := range rows {
			require.False(t, seen[row.ChargeID],
				"one unified snapshot must never emit an unresolved unit twice")
			seen[row.ChargeID] = true
			switch row.Kind {
			case usage.UnresolvedOneTimeChargeCreationBase:
				set.creationIDs[row.ChargeID] = true
			case usage.UnresolvedOneTimeChargeModuleTimer:
				set.timerIDs[row.ChargeID] = true
			default:
				t.Fatalf("unexpected charge kind %q", row.Kind)
			}
		}
		return set
	}
	assertPreHandoff := func(set projectionSet) {
		t.Helper()
		require.Equal(t, map[uuid.UUID]bool{appID: true}, set.creationIDs)
		require.Len(t, set.timerIDs, 2,
			"seven co-created timers expose exactly the two FIFO-over units")
	}

	before := readProjection()
	assertPreHandoff(before)

	// Hold the wallet base settlement's guard update uncommitted. The unified
	// SELECT runs from another pool connection and must observe one coherent
	// pre-commit MVCC snapshot: base + the same two timer units, without a lock
	// wait, transient omission, or ownership double count.
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx,
		`UPDATE ms_billing.apps
		 SET proration_invoice_id = $2
		 WHERE app_id = $1`,
		appID, "wallet:app-proration:"+appID.String())
	require.NoError(t, err)

	during := readProjection()
	assertPreHandoff(during)
	require.Equal(t, before.timerIDs, during.timerIDs)

	require.NoError(t, tx.Commit(ctx))

	// After commit, the base guard is terminal and only the wallet rail's
	// deliberately unresolved module units remain. Their identity is unchanged
	// and each still appears exactly once.
	after := readProjection()
	require.Empty(t, after.creationIDs)
	require.Equal(t, before.timerIDs, after.timerIDs)
	require.Len(t, after.timerIDs, 2)
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
