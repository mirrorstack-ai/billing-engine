package cycle_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

// billing-engine#218. The rollup re-runs on every cycle attempt and overwrites
// each aggregate line in place, so a reclaim that re-derived a different figure
// than the one frozen could not say which line moved. A fresh freeze now
// snapshots its derivation (migration 087), and a reclaim whose frozen figure
// differs from the live one logs the line-level diff. Both are diagnostic: the
// pairs below prove neither changes what the run charges.

func onlyRunID(t *testing.T, store *fakeStore) uuid.UUID {
	t.Helper()
	require.Len(t, store.insertedRuns, 1)
	var runID uuid.UUID
	for _, id := range store.insertedRuns {
		runID = id
	}
	return runID
}

// captureSlog routes the default logger into a buffer for one test. The cycle
// tests that swap it are sequential, so no parallel test logs into it.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func TestRunBillingCycle_FreshFreezeSnapshotsItsDerivationOnce(t *testing.T) {
	store := newFakeStore()
	store.hasPM = true
	store.stripeCustomer = "cus_freeze_snapshot"
	sc := newFakeStripe()
	store.chargedTotal = 1_000_000

	svc, _ := chargeSvcProposing(store, sc)
	resp, err := svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, resp.Status)

	runID := onlyRunID(t, store)
	snap, ok := store.freezeSnapshots[runID]
	require.True(t, ok, "a fresh freeze records the derivation it was taken from")
	require.EqualValues(t, frozenClaim(t, store).Cents, snap.FrozenCents, "the snapshot describes the figure the run froze")
	require.EqualValues(t, 100, snap.FrozenCents)
	require.EqualValues(t, 1_000_000, snap.UsageChargedMicros, "Σ of the lines = PeriodChargedTotal as this attempt read it")
	require.EqualValues(t, 1_000_000, snap.ArrearsMicros)
	require.Zero(t, store.driftCalls, "frozen == live: there is no drift to attribute")

	// A reclaim at the same live figure reuses the freeze: no second snapshot
	// (it is the FIRST derivation), nothing to diff.
	_, err = svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, 1, store.snapshotCalls, "only a fresh freeze snapshots")
	require.Zero(t, store.driftCalls)
}

func TestRunBillingCycle_SnapshotFailureNeverChangesTheCharge(t *testing.T) {
	run := func(t *testing.T, errSnapshot error) (*fakeStore, *cycle.ChargeSummary, int64) {
		t.Helper()
		store := newFakeStore()
		store.hasPM = true
		store.stripeCustomer = "cus_snapshot_failure"
		sc := newFakeStripe()
		store.chargedTotal = 1_234_567
		store.errSnapshot = errSnapshot
		svc, p := chargeSvcProposing(store, sc)
		resp, err := svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
		require.NoError(t, err, "a snapshot failure is logged, never returned")
		return store, resp, proposedMicros(t, p)
	}

	okStore, okResp, okProposed := run(t, nil)
	badStore, badResp, badProposed := run(t, errors.New("snapshot table unreachable"))

	require.Len(t, okStore.freezeSnapshots, 1)
	require.Equal(t, 1, badStore.snapshotCalls, "the snapshot was attempted")
	require.Empty(t, badStore.freezeSnapshots)

	require.Equal(t, okResp.Status, badResp.Status)
	require.Equal(t, okResp.ChargedCents, badResp.ChargedCents)
	require.Equal(t, frozenClaim(t, okStore).Cents, frozenClaim(t, badStore).Cents)
	require.Equal(t, okProposed, badProposed, "the sealed amount is identical with and without the snapshot")
}

func TestRunBillingCycle_ReclaimWithDriftLogsTheLineDiff(t *testing.T) {
	logs := captureSlog(t)
	store := newFakeStore()
	store.hasPM = true
	store.stripeCustomer = "cus_freeze_drift"
	sc := newFakeStripe()
	store.chargedTotal = 1_000_000
	runID := seedFrozenRun(t, store, sc, 100)
	app, mod := uuid.New(), uuid.New()
	store.freezeSnapshots = map[uuid.UUID]cycle.FreezeDerivation{
		runID: {FrozenCents: 100, UsageChargedMicros: 1_000_000, ArrearsMicros: 1_000_000},
	}
	store.freezeDrift = []cycle.FrozenLineDrift{{
		AppID: app, ModuleID: mod, Metric: "orders.placed", InFrozen: true, InLive: true,
		FrozenQuantity: "20", LiveQuantity: "16",
		FrozenUnitPriceMicros: 50_000, LiveUnitPriceMicros: 50_000,
		FrozenMarkupNum: 10, FrozenMarkupDen: 10, LiveMarkupNum: 10, LiveMarkupDen: 10,
		FrozenChargedMicros: 1_000_000, LiveChargedMicros: 800_000,
	}}
	store.chargedTotal = 800_000 // the closed period re-derives lower on the reclaim

	svc, p := chargeSvcProposing(store, sc)
	resp, err := svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, 1, store.driftCalls, "a reclaim that finds a drifted frozen figure reads the diff once")

	out := logs.String()
	require.Contains(t, out, `msg="freeze drift: frozen derivation vs live"`)
	require.Contains(t, out, "usage_charged_micros_frozen=1000000 usage_charged_micros_live=800000")
	require.Contains(t, out, "lines_moved=1")
	require.Contains(t, out, `msg="freeze drift: line"`)
	require.Contains(t, out, "metric=orders.placed")
	require.Contains(t, out, "quantity_frozen=20 quantity_live=16")
	require.Contains(t, out, "charged_micros_delta=-200000")

	// The money is exactly billing-engine#217's outcome: the diagnostic moved nothing.
	require.Equal(t, cycle.RunStatusProposed, resp.Status)
	require.EqualValues(t, 1, store.refreezeCalls)
	require.EqualValues(t, 80, frozenClaim(t, store).Cents)
	require.EqualValues(t, 800_000, proposedMicros(t, p))
	require.Zero(t, store.snapshotCalls, "a reclaim never snapshots; the snapshot stays the first derivation")
	require.EqualValues(t, 100, store.freezeSnapshots[runID].FrozenCents)
}

func TestRunBillingCycle_DriftOnARunFrozenBefore087SaysThereIsNoSnapshot(t *testing.T) {
	logs := captureSlog(t)
	store := newFakeStore()
	store.hasPM = true
	store.stripeCustomer = "cus_pre_087"
	sc := newFakeStripe()
	store.chargedTotal = 113_900_000
	seedFrozenRun(t, store, sc, 11390) // the 2026-09-11 freeze: no snapshot exists
	store.chargedTotal = 112_020_000

	svc, _ := chargeSvcProposing(store, sc)
	_, err := svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, 1, store.driftCalls)
	out := logs.String()
	require.Contains(t, out, "no snapshot exists to attribute it")
	require.Contains(t, out, "frozen_cents=11390 live_cents=11202")
	require.NotContains(t, out, `msg="freeze drift: line"`)
}

func TestRunBillingCycle_NoDriftNoDiffRead(t *testing.T) {
	store := newFakeStore()
	store.hasPM = true
	store.stripeCustomer = "cus_no_drift"
	sc := newFakeStripe()
	store.chargedTotal = 1_000_000
	seedFrozenRun(t, store, sc, 100) // frozen == live on the reclaim

	svc, _ := chargeSvcProposing(store, sc)
	_, err := svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Zero(t, store.driftCalls, "a reclaim whose frozen figure matches live has nothing to attribute")
	require.Zero(t, store.snapshotCalls)
}

// A concurrent daemon froze a DIFFERENT figure first (H6): this attempt adopts
// the winner's amount and must not describe the winner's freeze with its own
// lines — so it writes no snapshot, and reads the drift instead.
func TestRunBillingCycle_LostFreezeRaceWritesNoSnapshot(t *testing.T) {
	store := newFakeStore()
	store.hasPM = true
	store.stripeCustomer = "cus_lost_freeze_race"
	sc := newFakeStripe()
	store.chargedTotal = 1_000_000
	store.onFreezeCharge = func(runID uuid.UUID) {
		store.frozenCharges[runID] = cycle.FrozenBoundaryCharge{
			Cents:                   90,
			ChargeFundingAccountID:  chargeAccount,
			ChargeFundingGeneration: uuid.New(),
		}
	}

	svc, _ := chargeSvcProposing(store, sc)
	_, _ = svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.EqualValues(t, 90, frozenClaim(t, store).Cents, "fixture: the concurrent freeze won")
	require.Zero(t, store.snapshotCalls, "our lines do not describe the winner's figure")
	require.Equal(t, 1, store.driftCalls, "the adopted figure differs from our live derivation: the drift is read")
}
