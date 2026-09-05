package usage_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// Migration 073 — the READ half of dev_served. A tunnel-served line reaches the
// console with its real price attached and reaches the bill total not at all.
// Both halves are pinned in every test here: asserting only that the total
// excludes it would also pass for an implementation that hid the line, and
// asserting only that the line appears would pass for one that billed it.

// devLine builds one non-reserved module-usage line flagged dev_served.
func devLine(mod uuid.UUID, metric, version string, charged int64) usage.AppMetricUsageRaw {
	return usage.AppMetricUsageRaw{
		ModuleID: mod, Metric: metric, Kind: usage.KindCount, ModuleVersion: version,
		ChargedMicros: charged, DevServed: true,
	}
}

func TestGetAppBill_DevServedLineIsShownWithItsPriceButNotBilled(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()
	store.appBillRows = []usage.AppMetricUsageRaw{
		devLine(mod, "orders.placed", "", 7_500),
	}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)

	require.Len(t, resp.ModuleUsage, 1, "the developer must be able to SEE the line")
	require.True(t, resp.ModuleUsage[0].DevServed)
	require.EqualValues(t, 7_500, resp.ModuleUsage[0].ChargedMicros,
		"…with the real price on it — that is what it would have cost")

	require.Zero(t, resp.ModuleUsageTotalMicros, "and owe nothing for it")
	require.EqualValues(t, 7_500, resp.ModuleUsageDevServedMicros,
		"the figure is reported on its own field, not discarded")
	require.Equal(t, usage.BaseFeeMicros, resp.TotalMicros,
		"最終費用 is the base fee alone — the tunnel adds nothing")
}

func TestGetAppBill_MixedDevAndDeployedSplitTheTotals(t *testing.T) {
	// The shape migration 073 exists for: one module, one metric, one period,
	// both kinds of usage. The bill owes the deployed part exactly.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()
	store.appBillRows = []usage.AppMetricUsageRaw{
		customLine(mod, "orders.placed", "", 1_000), // deployed
		devLine(mod, "orders.placed", "", 4_000),    // tunnelled, same metric
	}

	resp, err := newService(store).GetAppBill(context.Background(), usage.GetAppBillRequest{OwnerUserID: owner, AppID: uuid.New()})
	require.NoError(t, err)

	require.Len(t, resp.ModuleUsage, 2, "two lines, not one collapsed into the other")
	var devCharged, deployedCharged int64
	for _, l := range resp.ModuleUsage {
		if l.DevServed {
			devCharged += l.ChargedMicros
			continue
		}
		deployedCharged += l.ChargedMicros
	}
	require.EqualValues(t, 1_000, deployedCharged)
	require.EqualValues(t, 4_000, devCharged)

	require.EqualValues(t, 1_000, resp.ModuleUsageTotalMicros, "the customer owes the deployed part")
	require.EqualValues(t, 4_000, resp.ModuleUsageDevServedMicros)
	require.Equal(t, usage.BaseFeeMicros+1_000, resp.TotalMicros,
		"the two totals never add together into 最終費用")
}

func TestGetAccountBill_DevServedStaysOutOfThePerModelRollup(t *testing.T) {
	// Agent.Models is a breakdown OF the charged bill and is clamped to it, so a
	// tunnel-served AI line must not accrue there: it would either overstate the
	// per-model spend or be silently trimmed by the clamp, and either way report
	// a number no invoice contains.
	//
	// The infra line is deliberately LARGE so the clamp's ceiling
	// (moduleUsageTotal + infraTotal) cannot bind. Without it, a regression that
	// let the tunnel accrue would be trimmed back to the same total this test
	// expects, and the assertion would pass against the bug.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)
	store.usageAppIDs = []uuid.UUID{uuid.Nil}

	const model = "anthropic.claude-haiku-4-5-20251001-v1:0"
	mod := seqUUID(7)
	dev := modelLine(mod, "agent.work.units", model, 9, 9_000)
	dev.DevServed = true
	store.appBillRowsByApp[uuid.Nil] = []usage.AppMetricUsageRaw{
		modelLine(mod, "agent.work.units", model, 1, 1_000), // deployed
		dev,
	}
	store.appInfraBillRowsByApp[uuid.Nil] = []usage.AppInfraUsage{
		appInfraLine("infra.egress.api.bytes", "network", 1, 50_000, 50_000),
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)

	require.EqualValues(t, 1_000, resp.Agent.ModuleUsageMicros, "the tunnel is not agent spend")
	require.EqualValues(t, 1_000, sumModelCharges(resp.Agent.Models),
		"the per-model rollup carries the deployed charge only, unclamped")
}

func TestGetAppUsageSummary_CarriesTheDevServedFlagPerLine(t *testing.T) {
	// This response has no total of its own, so the per-row flag IS the whole
	// contract: without it a consumer cannot tell the two sections apart, and
	// the only thing it can do with the rows is sum them.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	mod := uuid.New()
	store.appRows = []usage.AppMetricUsageRaw{
		customLine(mod, "orders.placed", "", 1_000),
		devLine(mod, "orders.placed", "", 4_000),
	}

	resp, err := newService(store).GetAppUsageSummary(context.Background(), usage.GetAppUsageSummaryRequest{
		OwnerUserID: owner, AppID: uuid.New(),
	})
	require.NoError(t, err)
	require.Len(t, resp.Metrics, 2)

	byDev := map[bool]usage.AppMetricUsage{}
	for _, m := range resp.Metrics {
		byDev[m.DevServed] = m
	}
	require.EqualValues(t, 1_000, byDev[false].ChargedMicros)
	require.EqualValues(t, 4_000, byDev[true].ChargedMicros,
		"the tunnel line keeps its real price on the wire")
}
