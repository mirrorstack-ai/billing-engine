package usage_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// Reuses the bill_test.go helpers (same package): customLine / appInfraLine /
// moduleInfraLine build fake ledger rows, mirrorPeriod seeds the frozen
// [May 1, Jun 1) window, baseSnapKey keys the migration-028 snapshot fake.

// seqUUID returns a uuid whose LAST byte is n — bytewise-ordered fixtures so
// the deterministic-ordering assertions have a known expected order.
func seqUUID(n byte) uuid.UUID {
	var u uuid.UUID
	u[15] = n
	u[0] = 0x10 // keep it non-Nil even for n=0 and clear of the sentinel
	return u
}

// --- aggregation across apps ------------------------------------------------

func TestGetAccountBill_AggregatesUsageMirrorAndBothApps(t *testing.T) {
	// Three apps, one per enumeration class:
	//   appA — usage-only, UNMIRRORED (pre-backfill): enumerated via the usage
	//          half; base = legacy flat + usage-proxy overage (1 module → flat).
	//   appB — mirror-only, ZERO usage (just created, nothing metered): must
	//          still appear with its full base (created long before the period).
	//   appC — both halves: synced count 7 → FLAT $20 base (overage is pooled,
	//          migration 030); usage + module-attributed infra.
	// The account-wide pool is 0 (appB) + 7 (appC) = 7 → 2 over → $6 pooled
	// overage on the ACCOUNT (not on any app). Every per-app number is exactly
	// what GetAppBill would return for that app.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store) // frozen [May 1, Jun 1)

	appA, appB, appC := seqUUID(2), seqUUID(1), seqUUID(3)
	store.usageAppIDs = []uuid.UUID{appC, appA} // deliberately unsorted
	store.appMirrors[appB] = usage.AppMirrorInfo{ModuleCount: 0, CreatedAt: time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)}
	store.appMirrors[appC] = usage.AppMirrorInfo{ModuleCount: 7, CreatedAt: time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)}

	store.appBillRowsByApp[appA] = []usage.AppMetricUsageRaw{customLine(uuid.New(), "orders.placed", "", 1000)}
	store.appInfraBillRowsByApp[appA] = []usage.AppInfraUsage{appInfraLine("infra.egress.api.bytes", "network", 100, 2, 200)}
	store.appBillRowsByApp[appC] = []usage.AppMetricUsageRaw{customLine(uuid.New(), "views.count", "", 500)}
	store.appModuleInfraBillRowsByApp[appC] = []usage.AppModuleInfraUsage{
		moduleInfraLine(uuid.New(), "infra.compute.walltime.ms", "", 20, nil, 100, 24),
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)

	// Dedup + deterministic order: appC is in BOTH halves but appears once;
	// rows sort by app_id bytes (B=…01 < A=…02 < C=…03).
	require.Len(t, resp.Apps, 3)
	require.Equal(t, appB, resp.Apps[0].AppID)
	require.Equal(t, appA, resp.Apps[1].AppID)
	require.Equal(t, appC, resp.Apps[2].AppID)

	// appB: mirror-only zero-usage → full base, nothing else.
	require.Equal(t, usage.BaseFeeMicros, resp.Apps[0].BaseFeeMicros)
	require.Zero(t, resp.Apps[0].ModuleUsageMicros)
	require.Zero(t, resp.Apps[0].InfraMicros)
	require.Equal(t, usage.BaseFeeMicros, resp.Apps[0].TotalMicros)

	// appA: unmirrored → flat base (1 distinct module, within the bundle).
	require.Equal(t, usage.BaseFeeMicros, resp.Apps[1].BaseFeeMicros)
	require.EqualValues(t, 1000, resp.Apps[1].ModuleUsageMicros)
	require.EqualValues(t, 200, resp.Apps[1].InfraMicros)
	require.Equal(t, usage.BaseFeeMicros+1200, resp.Apps[1].TotalMicros)

	// appC: synced count 7 → FLAT base (overage is pooled, not per-app);
	// module-attributed infra folds in.
	require.Equal(t, usage.BaseFeeMicros, resp.Apps[2].BaseFeeMicros)
	require.EqualValues(t, 500, resp.Apps[2].ModuleUsageMicros)
	require.EqualValues(t, 24, resp.Apps[2].InfraMicros)
	require.Equal(t, usage.BaseFeeMicros+524, resp.Apps[2].TotalMicros)

	// Account totals are the column sums plus the account-wide POOLED overage.
	// All three apps are flat, so BaseFeeTotal = 3 × $20. Pool 7 → 2 over → $6.
	require.Equal(t, 3*usage.BaseFeeMicros, resp.BaseFeeTotalMicros)
	require.EqualValues(t, 1500, resp.ModuleUsageTotalMicros)
	require.EqualValues(t, 224, resp.InfraTotalMicros)
	require.Equal(t, usage.AccountOverageMicros(7), resp.AccountOverageMicros)
	require.EqualValues(t, 6_000_000, resp.AccountOverageMicros) // 2 over × $3
	require.Zero(t, resp.PaasCreditMicros)
	require.Equal(t,
		resp.BaseFeeTotalMicros+resp.ModuleUsageTotalMicros+resp.InfraTotalMicros+resp.AccountOverageMicros,
		resp.TotalMicros)
	// apps[].total_micros are PRE-credit AND exclude the account-level pooled
	// overage (it is never allocated per-app), so Σ apps == account total −
	// overage + credit.
	var perApp int64
	for _, a := range resp.Apps {
		perApp += a.TotalMicros
	}
	require.Equal(t, resp.TotalMicros-resp.AccountOverageMicros+resp.PaasCreditMicros, perApp,
		"Σ apps[].total (pre-credit) == account total − pooled overage + credit")
}

func TestGetAccountBill_DeterministicAppOrdering(t *testing.T) {
	// Enumeration order is store-/map-random; the response must sort by app_id
	// bytes regardless. Also proves stability across repeated calls.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)

	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var want []uuid.UUID
	for n := byte(1); n <= 6; n++ {
		id := seqUUID(n)
		want = append(want, id)
		if n%2 == 0 {
			store.usageAppIDs = append([]uuid.UUID{id}, store.usageAppIDs...) // reverse-ish insertion
		} else {
			store.appMirrors[id] = usage.AppMirrorInfo{CreatedAt: created} // map → random order
		}
	}

	for i := 0; i < 3; i++ {
		resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
			OwnerUserID: owner, PeriodID: pid.String(),
		})
		require.NoError(t, err)
		require.Len(t, resp.Apps, len(want))
		for j, a := range resp.Apps {
			require.Equal(t, want[j], a.AppID, "apps must sort ascending by app_id bytes")
			if j > 0 {
				require.Negative(t, bytes.Compare(resp.Apps[j-1].AppID[:], a.AppID[:]))
			}
		}
	}
}

// --- mirror window semantics ------------------------------------------------

func TestGetAccountBill_MirrorLifecycleAcrossTheWindow(t *testing.T) {
	// The mirror half enumerates [created_at, deleted_at) ∩ window:
	//   deleted BEFORE the period  → excluded entirely (base 0, no new usage),
	//   deleted DURING the period  → included, full spent base (D1e),
	//   created DURING the period  → included, PRORATED base,
	//   created AFTER the period   → excluded.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store) // [May 1, Jun 1), 31 days

	deletedBefore, deletedDuring, createdDuring, createdAfter := seqUUID(1), seqUUID(2), seqUUID(3), seqUUID(4)
	jan := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	store.appMirrors[deletedBefore] = usage.AppMirrorInfo{CreatedAt: jan, Deleted: true, DeletedAt: time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC)}
	store.appMirrors[deletedDuring] = usage.AppMirrorInfo{CreatedAt: jan, Deleted: true, DeletedAt: time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)}
	store.appMirrors[createdDuring] = usage.AppMirrorInfo{CreatedAt: time.Date(2026, 5, 22, 14, 30, 0, 0, time.UTC)}
	store.appMirrors[createdAfter] = usage.AppMirrorInfo{CreatedAt: time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 2, "only the apps alive at some point inside the window")
	require.Equal(t, deletedDuring, resp.Apps[0].AppID)
	require.Equal(t, usage.BaseFeeMicros, resp.Apps[0].BaseFeeMicros, "deleted mid-period keeps the spent base")
	require.Equal(t, createdDuring, resp.Apps[1].AppID)
	require.EqualValues(t, 6_451_613, resp.Apps[1].BaseFeeMicros, "created May 22 of 31 days → 10/31 proration")
	require.Equal(t, usage.BaseFeeMicros+6_451_613, resp.TotalMicros)
}

// --- snapshot-first base ----------------------------------------------------

func TestGetAccountBill_SnapshotBaseFlowsThroughChargedPeriod(t *testing.T) {
	// The account bill inherits GetAppBill's snapshot-first PER-APP base: the May
	// period was CHARGED at a flat $20 snapshot, then SyncAppModules moved the
	// mirror to 8. The per-app base is flat in both periods (overage is pooled).
	// The CURRENT (un-snapshotted) period's account-wide overage estimates from
	// the live pool 8 → 3 over → $9.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)
	app := seqUUID(1)
	store.appMirrors[app] = usage.AppMirrorInfo{ModuleCount: 8, CreatedAt: time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)}
	store.baseSnapshots[baseSnapKey(app, time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))] = usage.AppBaseSnapshotInfo{
		BaseMicros: usage.BaseFeeMicros,
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	require.Equal(t, usage.BaseFeeMicros, resp.Apps[0].BaseFeeMicros, "charged period shows the invoiced flat snapshot")

	resp, err = newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	require.Equal(t, usage.BaseFeeMicros, resp.Apps[0].BaseFeeMicros, "per-app base is flat regardless of count")
	require.EqualValues(t, 9_000_000, resp.AccountOverageMicros,
		"un-snapshotted current period estimates the pooled overage from the live pool 8 → 3 over → $9")
}

// --- period resolution --------------------------------------------------------

func TestGetAccountBill_EmptyPeriodIDResolvesCurrentWindow(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Empty(t, resp.PeriodID, `current live window echoes period_id ""`)
	require.False(t, resp.PeriodStart.IsZero())
	require.True(t, resp.PeriodEnd.After(resp.PeriodStart))
}

func TestGetAccountBill_FrozenPeriodIDResolvesItsWindow(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Equal(t, pid.String(), resp.PeriodID)
	require.True(t, resp.PeriodStart.Equal(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)))
	require.True(t, resp.PeriodEnd.Equal(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)))
}

func TestGetAccountBill_UnknownPeriodIsNotFound(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	_, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: uuid.NewString(),
	})
	requireCode(t, err, billing.CodeNotFound)
}

func TestGetAccountBill_PeriodWithoutAccountIsNotFound(t *testing.T) {
	// A period id is account-scoped; with no billing account the caller owns
	// no periods → NOT_FOUND, same as GetAppBill.
	_, err := newService(newFakeStore()).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: uuid.New(), PeriodID: uuid.NewString(),
	})
	requireCode(t, err, billing.CodeNotFound)
}

func TestGetAccountBill_MalformedPeriodIDIsInvalidInput(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	_, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: "not-a-period-id",
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

// --- lazy account + plan stub -------------------------------------------------

func TestGetAccountBill_LazyAccountZeroBill(t *testing.T) {
	// No billing account row → a ZERO bill on the synthetic current window,
	// empty (non-nil) apps, NOT an error — mirroring GetBillingPeriods.
	resp, err := newService(newFakeStore()).GetAccountBill(context.Background(), usage.GetAccountBillRequest{OwnerUserID: uuid.New()})
	require.NoError(t, err)
	require.Empty(t, resp.PeriodID)
	require.Equal(t, 1, resp.PeriodStart.Day(), "lazy account windows on the calendar month (anchor day 1)")
	require.True(t, resp.PeriodEnd.After(resp.PeriodStart))
	require.NotNil(t, resp.Apps)
	require.Empty(t, resp.Apps)
	require.Zero(t, resp.BaseFeeTotalMicros)
	require.Zero(t, resp.ModuleUsageTotalMicros)
	require.Zero(t, resp.InfraTotalMicros)
	require.Zero(t, resp.PaasCreditMicros)
	require.Zero(t, resp.TotalMicros)
	// The plan stub still renders (the card shows Hobby even pre-activation).
	require.Equal(t, usage.PlanStubName, resp.Plan.Name)
	require.True(t, resp.Plan.RenewsAt.Equal(resp.PeriodEnd))
}

func TestGetAccountBill_PlanStubFields(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Equal(t, "Hobby", resp.Plan.Name)
	require.Equal(t, "active", resp.Plan.Status)
	require.Zero(t, resp.Plan.PriceMicros)
	require.Equal(t, "usd", resp.Plan.Currency)
	require.True(t, resp.Plan.RenewsAt.Equal(resp.PeriodEnd), "renews_at is the resolved period's end")
}

// --- credit gating --------------------------------------------------------

func TestGetAccountBill_CreditGatedOffTotalsUncredited(t *testing.T) {
	// v1: the PaaS credit is subscription-gated OFF at the ACCOUNT level too —
	// infra present, credit 0, total = base + usage + infra (never 30% infra
	// by default). The cap invariant itself is pinned white-box in
	// accountbill_internal_test.go (the gate keeps it unreachable here).
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)
	app := seqUUID(1)
	store.usageAppIDs = []uuid.UUID{app}
	store.appBillRowsByApp[app] = []usage.AppMetricUsageRaw{customLine(uuid.New(), "orders.placed", "", 100)}
	store.appInfraBillRowsByApp[app] = []usage.AppInfraUsage{appInfraLine("infra.request.count", "requests", 1, 5000, 6000)}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.EqualValues(t, 6000, resp.InfraTotalMicros)
	require.Zero(t, resp.PaasCreditMicros, "credit gated off with no subscription system")
	require.Equal(t, resp.BaseFeeTotalMicros+100+6000, resp.TotalMicros)
}

// --- validation + error paths -----------------------------------------------

func TestGetAccountBill_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).GetAccountBill(context.Background(), usage.GetAccountBillRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetAccountBill_RejectsBothOwners(t *testing.T) {
	_, err := newService(newFakeStore()).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: uuid.New(), OwnerOrgID: uuid.New(),
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestGetAccountBill_InternalOnEnumerationErrors(t *testing.T) {
	for name, arm := range map[string]func(*fakeStore){
		"usage half":  func(f *fakeStore) { f.errAppIDsWithUsage = errors.New("boom") },
		"mirror half": func(f *fakeStore) { f.errMirroredApps = errors.New("boom") },
		"per-app bill": func(f *fakeStore) {
			f.usageAppIDs = []uuid.UUID{uuid.New()}
			f.errAppBill = errors.New("boom")
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := newFakeStore()
			owner := uuid.New()
			store.accounts[owner] = uuid.New()
			arm(store)
			_, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{OwnerUserID: owner})
			requireCode(t, err, billing.CodeInternal)
		})
	}
}
