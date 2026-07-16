//go:build integration

package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// These exercise the generated sqlc queries against a real Postgres (gated by
// the `integration` build tag; run via `make test-integration`, skipped when
// Docker is unavailable). They verify the SQL the unit tests can't: the
// time-weighted integral, the rollup upsert idempotency, and the settlement
// income aggregation read back through usage_aggregates.

const (
	pStart = "2026-06-01T00:00:00Z"
	pEnd   = "2026-07-01T00:00:00Z"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	require.NoError(t, err)
	return ts
}

// seedAccount inserts a user-owned billing account and returns its id.
func seedAccount(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id) VALUES ($1, 'user', $2)`,
		id.String(), uuid.New().String())
	require.NoError(t, err)
	return id
}

func seedMetricDef(t *testing.T, pool *pgxpool.Pool, moduleID uuid.UUID, metric string, kind usage.Kind, priceMicros int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.metric_definitions (module_id, metric, kind, unit_price_micros) VALUES ($1,$2,$3,$4)`,
		moduleID.String(), metric, string(kind), priceMicros)
	require.NoError(t, err)
}

func seedEvent(t *testing.T, pool *pgxpool.Pool, acct, app, mod uuid.UUID, metric string, kind usage.Kind, value float64, at string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.usage_events (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		uuid.NewString(), acct.String(), app.String(), mod.String(), metric, string(kind), value, at)
	require.NoError(t, err)
}

func TestRollupPeriod_Integration_SumAndTimeWeighted(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	svc := cycle.NewService(store, nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindSum, 50_000)
	seedMetricDef(t, pool, mod, "myapp.bytes", usage.KindTimeWeighted, 3)

	// sum → 4 + 6 = 10 orders.
	seedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 4, "2026-06-01T00:00:00Z")
	seedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 6, "2026-06-02T00:00:00Z")
	// time_weighted: 100 held 1h, then 200 held to period_end (06-01 03:00 sample window
	// extends to month end, so the second sample holds for the rest of the month).
	seedEvent(t, pool, acct, app, mod, "myapp.bytes", usage.KindTimeWeighted, 100, "2026-06-01T00:00:00Z")
	seedEvent(t, pool, acct, app, mod, "myapp.bytes", usage.KindTimeWeighted, 200, "2026-06-01T01:00:00Z")

	resp, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)

	got := map[string]cycle.MetricAggregate{}
	for _, a := range resp.Aggregates {
		got[a.Metric] = a
	}
	require.Equal(t, "10", got["orders.placed"].BillableQuantity)
	require.EqualValues(t, 500_000, got["orders.placed"].ChargedMicros) // 10×50_000, no markup

	// 100 byte held 1h = 100 byte-hours; 200 byte held the rest of the month =
	// 200 × 719h = 143_800 byte-hours; total 143_900 byte-hours. Price 3/unit →
	// raw_cost = round_half_up(143_900 × 3) = 431_700 micros. Assert the exact
	// integral so a SQL regression that computes a non-zero-but-wrong value
	// fails (not just > 0).
	require.Equal(t, usage.KindTimeWeighted, got["myapp.bytes"].Kind)
	require.Equal(t, "143900", got["myapp.bytes"].BillableQuantity)
	require.EqualValues(t, 431_700, got["myapp.bytes"].RawCostMicros)
	require.EqualValues(t, 431_700, got["myapp.bytes"].ChargedMicros)
}

func TestRollupPeriod_Integration_Idempotent(t *testing.T) {
	pool := testutil.NewTestDB(t)
	svc := cycle.NewService(cycle.NewStore(pool), nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindCount, 1_000)
	seedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindCount, 3, "2026-06-05T00:00:00Z")

	_, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	_, err = svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)

	var count int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.usage_aggregates WHERE account_id=$1`, acct.String()).Scan(&count))
	require.Equal(t, 1, count, "re-running the rollup upserts, never duplicates")

	var periods int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.billing_periods WHERE account_id=$1`, acct.String()).Scan(&periods))
	require.Equal(t, 1, periods, "OpenPeriodForAccount is idempotent")
}

func TestSettleDevelopers_Integration_FromAggregates(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	svc := cycle.NewService(store, nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindSum, 50_000)
	seedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 20, "2026-06-10T00:00:00Z")
	// published module → 15% platform take.
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.module_visibility (module_id, visibility) VALUES ($1,'published')`, mod.String())
	require.NoError(t, err)

	roll, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	require.EqualValues(t, 1_000_000, roll.TotalChargedMicros) // 20×50_000

	set, err := svc.SettleDevelopers(ctx, acct, roll.PeriodID)
	require.NoError(t, err)
	require.Len(t, set.Settlements, 1)
	require.EqualValues(t, 150_000, set.Settlements[0].PlatformTakeMicros)
	require.EqualValues(t, 850_000, set.Settlements[0].DeveloperOwedMicros)

	// developer_id persists as NULL; status accrued.
	var devNull bool
	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT developer_id IS NULL, status FROM ms_billing.developer_settlements WHERE period_id=$1 AND module_id=$2`,
		roll.PeriodID.String(), mod.String()).Scan(&devNull, &status))
	require.True(t, devNull, "developer_id is NULL until a module→developer sync exists")
	require.Equal(t, "accrued", status)
}

func TestSettleDevelopers_Integration_UnknownVisibilityDefaultsPrivate(t *testing.T) {
	pool := testutil.NewTestDB(t)
	svc := cycle.NewService(cycle.NewStore(pool), nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindSum, 50_000)
	seedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 20, "2026-06-10T00:00:00Z")
	// no module_visibility row → default private 30%.

	roll, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	set, err := svc.SettleDevelopers(ctx, acct, roll.PeriodID)
	require.NoError(t, err)
	require.EqualValues(t, 300_000, set.Settlements[0].PlatformTakeMicros) // 30%
	require.Equal(t, usage.VisibilityPrivate, set.Settlements[0].MarginShareClass)
}

// --- charge-cycle SQL (PR #6) ---------------------------------------------

// TestAccountsWithUsageEvents_Integration verifies the rollup-phase work list:
// accounts with raw usage_events in the half-open window, excluding NULL-account
// (lazy) events and events outside the window.
func TestAccountsWithUsageEvents_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	in := seedAccount(t, pool)
	out := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindSum, 50_000)

	seedEvent(t, pool, in, app, mod, "orders.placed", usage.KindSum, 1, "2026-06-10T00:00:00Z")  // inside
	seedEvent(t, pool, out, app, mod, "orders.placed", usage.KindSum, 1, "2026-07-05T00:00:00Z") // after end

	got, err := store.AccountsWithUsageEvents(ctx, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	require.Contains(t, got, in)
	require.NotContains(t, got, out, "events outside the half-open window are excluded")
}

// TestChargeCycleSQL_ReclaimAndExactWindow verifies the two blocking fixes at
// the SQL layer: (1) AccountsWithUnbilledUsage surfaces an account whose only
// run is non-terminal and hides it once invoiced, matched on the EXACT window;
// (2) InsertBillingRun reclaims a non-terminal run (same id) and refuses an
// invoiced one.
func TestChargeCycleSQL_ReclaimAndExactWindow(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	svc := cycle.NewService(store, nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindSum, 50_000)
	seedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 20, "2026-06-10T00:00:00Z")

	start, end := mustTime(t, pStart), mustTime(t, pEnd)
	_, err := svc.RollupPeriod(ctx, acct, start, end)
	require.NoError(t, err)

	// No run yet → account appears in the charge work list.
	unbilled, err := store.AccountsWithUnbilledUsage(ctx, start, end)
	require.NoError(t, err)
	require.Contains(t, unbilled, acct)

	// First InsertBillingRun creates the row (shouldCharge=true). Mark it
	// skipped_no_pm (a non-terminal outcome).
	run1, should1, err := store.InsertBillingRun(ctx, acct, start, end)
	require.NoError(t, err)
	require.True(t, should1)
	require.NoError(t, store.MarkBillingRun(ctx, run1, cycle.RunStatusSkippedNoPM, "", 0))

	// A skipped run still surfaces in the work list (RETAINED, re-attempt).
	unbilled, err = store.AccountsWithUnbilledUsage(ctx, start, end)
	require.NoError(t, err)
	require.Contains(t, unbilled, acct, "skipped_no_pm must re-appear for retry")

	// InsertBillingRun reclaims the SAME run row for a fresh attempt.
	run2, should2, err := store.InsertBillingRun(ctx, acct, start, end)
	require.NoError(t, err)
	require.True(t, should2, "a skipped run is reclaimed")
	require.Equal(t, run1, run2, "reclaim reuses the same run id (stable idem-keys)")

	// Mark invoiced → terminal. Now it disappears + reclaim refuses.
	require.NoError(t, store.MarkBillingRun(ctx, run2, cycle.RunStatusInvoiced, "in_test_x", 123))
	unbilled, err = store.AccountsWithUnbilledUsage(ctx, start, end)
	require.NoError(t, err)
	require.NotContains(t, unbilled, acct, "invoiced run excludes the account")

	_, should3, err := store.InsertBillingRun(ctx, acct, start, end)
	require.NoError(t, err)
	require.False(t, should3, "an invoiced run blocks re-charge")

	// Exact-window match: a different window must not collide with this run.
	otherStart := mustTime(t, "2026-07-01T00:00:00Z")
	otherEnd := mustTime(t, "2026-08-01T00:00:00Z")
	_, shouldOther, err := store.InsertBillingRun(ctx, acct, otherStart, otherEnd)
	require.NoError(t, err)
	require.True(t, shouldOther, "a different window is a distinct run")
}

func TestUpsertInvoice_Integration_EverFailedIsSticky(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	acct := seedAccount(t, pool)

	open := cycle.InvoiceMirror{
		AccountID: acct, StripeInvoiceID: "in_failed", Status: "open",
		AmountDueCents: 200, Currency: "usd", EverFailed: true,
	}
	paid := cycle.InvoiceMirror{
		AccountID: acct, StripeInvoiceID: "in_paid", Status: "paid",
		AmountDueCents: 200, AmountPaidCents: 200, Currency: "usd",
	}
	require.NoError(t, store.UpsertInvoice(ctx, open))
	require.NoError(t, store.UpsertInvoice(ctx, paid))

	var failedStatus string
	var everFailed bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status, ever_failed FROM ms_billing.invoices WHERE stripe_invoice_id = $1`,
		open.StripeInvoiceID).Scan(&failedStatus, &everFailed))
	require.Equal(t, "open", failedStatus)
	require.True(t, everFailed)

	require.NoError(t, pool.QueryRow(ctx,
		`SELECT ever_failed FROM ms_billing.invoices WHERE stripe_invoice_id = $1`,
		paid.StripeInvoiceID).Scan(&everFailed))
	require.False(t, everFailed)

	open.Status = "paid"
	open.AmountPaidCents = open.AmountDueCents
	open.EverFailed = false
	require.NoError(t, store.UpsertInvoice(ctx, open))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status, ever_failed FROM ms_billing.invoices WHERE stripe_invoice_id = $1`,
		open.StripeInvoiceID).Scan(&failedStatus, &everFailed))
	require.Equal(t, "paid", failedStatus)
	require.True(t, everFailed, "a later paid mirror must not clear a latched failure")
}

// TestAccountCollection_Integration_DefaultsAndUpdate verifies migration 016's
// born-clean column defaults (arrears mode, $25 credit floor, NULL ceiling) and
// the UpdateAccountCollection round-trip (mode + ceiling persisted and read
// back). PR #9.
func TestAccountCollection_Integration_DefaultsAndUpdate(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)

	// Migration 016 defaults: arrears mode, $25 credit floor, no ceiling.
	got, err := store.AccountCollection(ctx, acct)
	require.NoError(t, err)
	require.Equal(t, cycle.BillingModeArrears, got.Mode)
	require.EqualValues(t, 25_000_000, got.CreditLimitMicros)
	require.False(t, got.HasSpendCeiling)
	require.False(t, got.CreatedAt.IsZero(), "created_at populated for tenure")

	// Update: flip to prepaid + set a ceiling; read it back.
	require.NoError(t, store.UpdateAccountCollection(ctx, acct, cycle.AccountCollection{
		Mode:               cycle.BillingModePrepaid,
		CreditLimitMicros:  25_000_000,
		HasSpendCeiling:    true,
		SpendCeilingMicros: 5_000_000,
	}))
	got, err = store.AccountCollection(ctx, acct)
	require.NoError(t, err)
	require.Equal(t, cycle.BillingModePrepaid, got.Mode)
	require.True(t, got.HasSpendCeiling)
	require.EqualValues(t, 5_000_000, got.SpendCeilingMicros)

	// Missing account → ErrAccountNotFound.
	err = store.UpdateAccountCollection(ctx, uuid.New(), cycle.AccountCollection{Mode: cycle.BillingModeArrears})
	require.ErrorIs(t, err, cycle.ErrAccountNotFound)
}

// TestTightenAndMarkRun_Integration verifies the atomic tighten: in ONE tx the
// account mode flips to prepaid AND the billing run is marked skipped_prepaid,
// and the new skipped_ceiling status satisfies migration 016's CHECK. PR #9.
func TestTightenAndMarkRun_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	start, end := mustTime(t, pStart), mustTime(t, pEnd)

	runID, should, err := store.InsertBillingRun(ctx, acct, start, end)
	require.NoError(t, err)
	require.True(t, should)

	require.NoError(t, store.TightenAndMarkRun(ctx, acct,
		cycle.AccountCollection{Mode: cycle.BillingModePrepaid, CreditLimitMicros: 25_000_000},
		runID, cycle.RunStatusSkippedPrepaid))

	// Mode persisted.
	got, err := store.AccountCollection(ctx, acct)
	require.NoError(t, err)
	require.Equal(t, cycle.BillingModePrepaid, got.Mode)

	// Run row marked skipped_prepaid (non-terminal → still reclaimable).
	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM ms_billing.billing_runs WHERE id = $1`, runID.String()).Scan(&status))
	require.Equal(t, "skipped_prepaid", status)

	// skipped_ceiling is accepted by the CHECK (born-clean migration 016).
	require.NoError(t, store.MarkBillingRun(ctx, runID, cycle.RunStatusSkippedCeiling, "", 0))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM ms_billing.billing_runs WHERE id = $1`, runID.String()).Scan(&status))
	require.Equal(t, "skipped_ceiling", status)
}
