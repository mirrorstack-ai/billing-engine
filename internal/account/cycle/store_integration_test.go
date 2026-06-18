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
	svc := cycle.NewService(store)
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
	svc := cycle.NewService(cycle.NewStore(pool))
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
	svc := cycle.NewService(store)
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
	svc := cycle.NewService(cycle.NewStore(pool))
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
