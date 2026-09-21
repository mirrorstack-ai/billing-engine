//go:build integration

package cycle_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 087 (billing-engine#218) against a real Postgres 17. The claims
// are DATABASE claims a fake cannot make: the header is bound to the run's
// frozen cents, the copied lines are exactly PeriodChargedTotal's row set, a
// Σ mismatch rolls the header back with the lines, the drift join finds a
// line the re-rollup overwrote in place, and both tables refuse an edit.
//
// Helpers (seedAccount, seedMetricDef, seedEvent, mustTime, pStart/pEnd) live
// in store_integration_test.go; seedEventDevServed in migration073.

func freezeRunForTest(t *testing.T, pool *pgxpool.Pool, runID, acct uuid.UUID, cents int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE ms_billing.billing_runs
		    SET frozen_charge_cents = $2,
		        charge_funding_account_id = $3,
		        charge_funding_generation = gen_random_uuid()
		  WHERE id = $1`,
		runID.String(), cents, acct.String())
	require.NoError(t, err)
}

func quantity(t *testing.T, s string) float64 {
	t.Helper()
	f, err := strconv.ParseFloat(s, 64)
	require.NoError(t, err)
	return f
}

func TestMigration087_FirstDerivationSurvivesTheReRollup(t *testing.T) {
	pool := testutil.NewTestDB(t) // 087.up already applied
	store := cycle.NewStore(pool)
	svc := cycle.NewService(store, nil)
	ctx := context.Background()
	start, end := mustTime(t, pStart), mustTime(t, pEnd)

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindSum, 50_000)
	seedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 20, "2026-06-01T00:00:00Z")
	// A tunnel line: priced, never collected, so never part of the snapshot.
	seedEventDevServed(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 7, "2026-06-02T00:00:00Z", true)
	_, err := svc.RollupPeriod(ctx, acct, start, end)
	require.NoError(t, err)
	total, err := store.PeriodChargedTotal(ctx, acct, start, end)
	require.NoError(t, err)
	require.EqualValues(t, 1_000_000, total) // 20 × 50_000

	runID, _, _, _, err := store.InsertBillingRun(ctx, acct, start, end)
	require.NoError(t, err)
	d := cycle.FreezeDerivation{FrozenCents: 100, UsageChargedMicros: total, ArrearsMicros: total}

	written, err := store.SnapshotFreezeDerivation(ctx, runID, d)
	require.NoError(t, err)
	require.False(t, written, "an unfrozen run has no figure to describe")

	freezeRunForTest(t, pool, runID, acct, 100)

	wrongCents := d
	wrongCents.FrozenCents = 99
	written, err = store.SnapshotFreezeDerivation(ctx, runID, wrongCents)
	require.NoError(t, err)
	require.False(t, written, "the header is bound to the figure the run holds")

	moved := d
	moved.UsageChargedMicros = total + 1
	_, err = store.SnapshotFreezeDerivation(ctx, runID, moved)
	require.ErrorContains(t, err, "the rollup moved in between")
	_, _, found, err := store.FreezeDerivationDrift(ctx, runID)
	require.NoError(t, err)
	require.False(t, found, "a refused snapshot rolls its header back with its lines")

	written, err = store.SnapshotFreezeDerivation(ctx, runID, d)
	require.NoError(t, err)
	require.True(t, written)
	written, err = store.SnapshotFreezeDerivation(ctx, runID, d)
	require.NoError(t, err)
	require.False(t, written, "first write wins: the snapshot is the FIRST derivation")

	var lines int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.billing_run_freeze_snapshot_lines WHERE run_id = $1`,
		runID.String()).Scan(&lines))
	require.Equal(t, 1, lines, "PeriodChargedTotal's row set: the dev_served line is not copied")

	snap, drift, found, err := store.FreezeDerivationDrift(ctx, runID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, d, snap)
	require.Empty(t, drift, "nothing has moved yet")

	// The re-rollup the issue is about: more usage lands in the closed period
	// and a second line appears. UpsertUsageAggregate overwrites the first
	// line in place; the snapshot still has it.
	seedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 4, "2026-06-03T00:00:00Z")
	seedMetricDef(t, pool, mod, "orders.refunded", usage.KindSum, 10_000)
	seedEvent(t, pool, acct, app, mod, "orders.refunded", usage.KindSum, 3, "2026-06-03T00:00:00Z")
	_, err = svc.RollupPeriod(ctx, acct, start, end)
	require.NoError(t, err)

	_, drift, found, err = store.FreezeDerivationDrift(ctx, runID)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, drift, 2, "the overwritten line and the new one; the dev_served line is not compared")
	byMetric := map[string]cycle.FrozenLineDrift{}
	for _, l := range drift {
		byMetric[l.Metric] = l
	}

	placed := byMetric["orders.placed"]
	require.True(t, placed.InFrozen)
	require.True(t, placed.InLive)
	require.Equal(t, app, placed.AppID)
	require.Equal(t, mod, placed.ModuleID)
	require.InDelta(t, 20, quantity(t, placed.FrozenQuantity), 1e-9)
	require.InDelta(t, 24, quantity(t, placed.LiveQuantity), 1e-9)
	require.EqualValues(t, 50_000, placed.FrozenUnitPriceMicros)
	require.EqualValues(t, 1_000_000, placed.FrozenChargedMicros, "the first derivation survives the overwrite")
	require.EqualValues(t, 1_200_000, placed.LiveChargedMicros)

	refunded := byMetric["orders.refunded"]
	require.False(t, refunded.InFrozen)
	require.True(t, refunded.InLive)
	require.Zero(t, refunded.FrozenChargedMicros)
	require.EqualValues(t, 30_000, refunded.LiveChargedMicros)

	// SEALED: neither table can be edited or emptied.
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.billing_run_freeze_snapshot_lines SET charged_micros = 0 WHERE run_id = $1`, runID.String())
	require.ErrorContains(t, err, "is sealed")
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.billing_run_freeze_snapshots SET frozen_cents = 1 WHERE run_id = $1`, runID.String())
	require.ErrorContains(t, err, "is sealed")
	_, err = pool.Exec(ctx,
		`DELETE FROM ms_billing.billing_run_freeze_snapshot_lines WHERE run_id = $1`, runID.String())
	require.ErrorContains(t, err, "is sealed")
}
