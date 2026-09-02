package usage_test

import (
	"bytes"
	"context"
	"encoding/json"
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

// modelLine builds one AppBill row carrying the raw model wire id. It mirrors
// customLine while exposing the two values aggregated into Agent.Models.
func modelLine(mod uuid.UUID, metric, model string, quantity float64, charged int64) usage.AppMetricUsageRaw {
	line := customLine(mod, metric, "", charged)
	line.Model = model
	line.BillableQuantity = quantity
	return line
}

// sumModelCharges totals ChargedMicros across the per-model rollup — the left
// side of the Σ models ≤ Agent.TotalMicros reconciliation invariant.
func sumModelCharges(models []usage.AgentModelUsage) int64 {
	var total int64
	for _, model := range models {
		total += model.ChargedMicros
	}
	return total
}

// --- aggregation across apps ------------------------------------------------

func TestGetAccountBill_AggregatesUsageMirrorAndBothApps(t *testing.T) {
	// Three apps, one per enumeration class:
	//   appA — usage-only, UNMIRRORED (pre-backfill): enumerated via the usage
	//          half; base = legacy flat + usage-proxy overage (1 module → flat).
	//   appB — mirror-only, ZERO usage (just created, nothing metered): must
	//          still appear with its full base (created long before the period).
	//   appC — both halves: synced count 7 → FLAT $20 base (overage is pooled,
	//          migration 032); usage + module-attributed infra.
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
	store.liveDomainCount = 3

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
	// All three apps are flat, so BaseFeeTotal = 3 × $20. Pool 7 → 2 over → 1 block → $5.
	require.Equal(t, 3*usage.BaseFeeMicros, resp.BaseFeeTotalMicros)
	require.EqualValues(t, 1500, resp.ModuleUsageTotalMicros)
	require.EqualValues(t, 224, resp.InfraTotalMicros)
	require.Equal(t, usage.AccountOverageMicros(7), resp.AccountOverageMicros)
	require.EqualValues(t, 5_000_000, resp.AccountOverageMicros) // 2 over → ceil(2/5) = 1 block × $5
	require.EqualValues(t, 3*usage.DomainFeeMicros, resp.CustomDomainsMicros)
	require.EqualValues(t, 6_000_000, resp.CustomDomainsMicros) // 3 domains × $2
	require.Zero(t, resp.PaasCreditMicros)
	require.Equal(t,
		resp.BaseFeeTotalMicros+resp.ModuleUsageTotalMicros+resp.InfraTotalMicros+
			resp.AccountOverageMicros+resp.CustomDomainsMicros,
		resp.TotalMicros)
	// apps[].total_micros are the app plane only: agent, pooled overage, and
	// account credit are never allocated back into these rows.
	var perApp int64
	for _, a := range resp.Apps {
		perApp += a.TotalMicros
	}
	require.Equal(t, resp.BaseFeeTotalMicros+resp.ModuleUsageTotalMicros+resp.InfraTotalMicros, perApp,
		"Σ apps[].total == base fee total + module usage total + infra total")
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

// --- account-level agent bucket --------------------------------------------

func TestGetAccountBill_AgentOnlyHasNoAppRowOrBaseFee(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)

	// The fake roster deliberately returns the sentinel verbatim, exercising
	// the service's defensive filter in addition to the real SQL exclusion.
	store.usageAppIDs = []uuid.UUID{uuid.Nil}
	store.appBillRowsByApp[uuid.Nil] = []usage.AppMetricUsageRaw{
		customLine(uuid.New(), "agent.work.units", "", 1100),
	}
	store.appInfraBillRowsByApp[uuid.Nil] = []usage.AppInfraUsage{
		appInfraLine("infra.ai.input.tokens", "ai", 100, 5, 600),
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)

	require.NotNil(t, resp.Apps)
	require.Empty(t, resp.Apps, "the zero UUID is account scope, never an app row")
	for _, app := range resp.Apps {
		require.NotEqual(t, uuid.Nil, app.AppID)
	}
	require.Zero(t, resp.BaseFeeTotalMicros)
	require.Zero(t, resp.ModuleUsageTotalMicros)
	require.Zero(t, resp.InfraTotalMicros)

	require.EqualValues(t, 1100, resp.Agent.ModuleUsageMicros)
	require.EqualValues(t, 600, resp.Agent.InfraMicros)
	require.Positive(t, resp.Agent.TotalMicros)
	require.Equal(t, resp.Agent.ModuleUsageMicros+resp.Agent.InfraMicros, resp.Agent.TotalMicros,
		"agent total is module usage + infra only; there is no base-fee term")
	require.Equal(t, resp.Agent.TotalMicros, resp.TotalMicros,
		"agent-only spend appears exactly once in the account total")

	var perApp int64
	for _, app := range resp.Apps {
		perApp += app.TotalMicros
	}
	require.Equal(t, resp.BaseFeeTotalMicros+resp.ModuleUsageTotalMicros+resp.InfraTotalMicros, perApp,
		"the app-plane identity excludes agent spend")
}

func TestGetAccountBill_AgentModelsDecomposeModelCarryingLines(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)

	modelA := "anthropic.claude-haiku-4-5-20251001-v1:0"
	modelB := "openai.gpt-5-mini"
	app := seqUUID(9)
	store.usageAppIDs = []uuid.UUID{uuid.Nil, app}

	// modelA is split across two metrics, while modelB ties its summed charge.
	// The response must aggregate modelA and resolve the tie by raw model id.
	// Model-less custom and reserved rows stay in the agent total as the
	// consumer-rendered "Other" residual.
	store.appBillRowsByApp[uuid.Nil] = []usage.AppMetricUsageRaw{
		modelLine(uuid.Nil, "infra.ai.input.tokens", modelA, 1.25, 400),
		modelLine(uuid.Nil, "infra.ai.output.tokens", modelA, 2.75, 600),
		modelLine(uuid.Nil, "infra.ai.input.tokens", modelB, 5, 1000),
		infraLine("infra.egress.api.bytes", 300),
		customLine(uuid.New(), "agent.work.units", "", 200),
	}
	// AppInfraBill remains authoritative for the existing infra scalar. These
	// charges mirror the reserved AppBill rows without changing its old math.
	store.appInfraBillRowsByApp[uuid.Nil] = []usage.AppInfraUsage{
		appInfraLine("infra.ai.input.tokens", "ai", 1, 6.25, 1400),
		appInfraLine("infra.ai.output.tokens", "ai", 1, 2.75, 600),
		appInfraLine("infra.egress.api.bytes", "network", 1, 300, 300),
	}

	// A real app in the same account proves this is only an additive projection
	// on the account-agent bucket; AccountAppBill has no models concept.
	store.appBillRowsByApp[app] = []usage.AppMetricUsageRaw{
		customLine(uuid.New(), "orders.placed", "", 700),
	}
	store.appInfraBillRowsByApp[app] = []usage.AppInfraUsage{
		appInfraLine("infra.request.count", "requests", 1, 80, 80),
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)

	require.Equal(t, []usage.AgentModelUsage{
		{Model: modelA, BillableQuantity: 4, ChargedMicros: 1000},
		{Model: modelB, BillableQuantity: 5, ChargedMicros: 1000},
	}, resp.Agent.Models, "equal charges sort by raw model id after per-model summing")
	require.LessOrEqual(t, sumModelCharges(resp.Agent.Models), resp.Agent.TotalMicros,
		"model lines decompose only the model-carrying subset of agent spend")

	// Existing scalars are unchanged: model-less module and infra charges remain
	// in the total, producing the residual the consumer labels "Other".
	require.EqualValues(t, 200, resp.Agent.ModuleUsageMicros)
	require.EqualValues(t, 2300, resp.Agent.InfraMicros)
	require.EqualValues(t, 2500, resp.Agent.TotalMicros)

	require.Equal(t, []usage.AccountAppBill{{
		AppID:         app,
		BaseFeeMicros: usage.BaseFeeMicros,
		// The live app owns the whole next-period recurring base here: it is
		// activated, and the account carries no overage or domain surcharges.
		ProjectedBaseFeeMicros: usage.BaseFeeMicros,
		ModuleUsageMicros:      700,
		InfraMicros:            80,
		TotalMicros:            usage.BaseFeeMicros + 780,
	}}, resp.Apps)
	appJSON, err := json.Marshal(resp.Apps[0])
	require.NoError(t, err)
	var appWire map[string]any
	require.NoError(t, json.Unmarshal(appJSON, &appWire))
	require.NotContains(t, appWire, "models", "the per-app wire contract is unchanged")
}

func TestGetAccountBill_AgentModelsClampedWhenLiveRoundingExceedsInfraTotal(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)

	modelA := "openai.gpt-5-mini"
	modelB := "anthropic.claude-haiku-4-5-20251001-v1:0"
	store.appBillRowsByApp[uuid.Nil] = []usage.AppMetricUsageRaw{
		modelLine(uuid.Nil, "infra.ai.input.tokens", modelA, 2, 5),
		modelLine(uuid.Nil, "infra.ai.input.tokens", modelB, 3, 3),
	}
	store.appInfraBillRowsByApp[uuid.Nil] = []usage.AppInfraUsage{
		appInfraLine("infra.ai.input.tokens", "ai", 1, 5, 7),
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)

	require.Equal(t, []usage.AgentModelUsage{
		{Model: modelA, BillableQuantity: 2, ChargedMicros: 5},
		{Model: modelB, BillableQuantity: 3, ChargedMicros: 2},
	}, resp.Agent.Models, "only the smaller tail model charge is trimmed")
	require.LessOrEqual(t, sumModelCharges(resp.Agent.Models), resp.Agent.TotalMicros)
	require.EqualValues(t, 7, resp.Agent.TotalMicros)
}

func TestGetAccountBill_AgentModelsDropReservedRealModuleAttributedLines(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)

	realModule := seqUUID(7)
	realModuleModel := "openai.gpt-5-mini"
	sentinelModel := "anthropic.claude-haiku-4-5-20251001-v1:0"
	store.appBillRowsByApp[uuid.Nil] = []usage.AppMetricUsageRaw{
		modelLine(realModule, "infra.ai.input.tokens", realModuleModel, 25, 0),
		modelLine(uuid.Nil, "infra.ai.output.tokens", sentinelModel, 2, 12),
	}
	store.appInfraBillRowsByApp[uuid.Nil] = []usage.AppInfraUsage{
		appInfraLine("infra.ai.output.tokens", "ai", 5, 2, 12),
	}
	store.appModuleInfraBillRowsByApp[uuid.Nil] = []usage.AppModuleInfraUsage{
		moduleInfraLine(realModule, "infra.ai.input.tokens", "", 1, nil, 25, 30),
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)

	require.Equal(t, []usage.AgentModelUsage{{
		Model: sentinelModel, BillableQuantity: 2, ChargedMicros: 12,
	}}, resp.Agent.Models)
	modelMicros := sumModelCharges(resp.Agent.Models)
	require.LessOrEqual(t, modelMicros, resp.Agent.TotalMicros)
	require.EqualValues(t, 42, resp.Agent.InfraMicros)
	require.EqualValues(t, 30, resp.Agent.TotalMicros-modelMicros,
		"the authoritative real-module charge stays in the Other residual")
}

func TestGetAccountBill_AgentModelsClampedWhenFrozenMetricDeactivated(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)

	modelA := "openai.gpt-5-mini"
	modelB := "anthropic.claude-haiku-4-5-20251001-v1:0"
	store.appBillRowsByApp[uuid.Nil] = []usage.AppMetricUsageRaw{
		modelLine(uuid.Nil, "infra.ai.input.tokens", modelA, 12, 1_200_000),
		modelLine(uuid.Nil, "infra.ai.input.tokens", modelB, 8, 800_000),
	}
	// The frozen ledger rows remain, but the deactivated metric is absent from
	// the catalog-anchored infra result. Only an unrelated active metric remains.
	store.appInfraBillRowsByApp[uuid.Nil] = []usage.AppInfraUsage{
		appInfraLine("infra.egress.api.bytes", "network", 1, 100_000, 100_000),
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)

	require.Equal(t, []usage.AgentModelUsage{{
		Model: modelA, BillableQuantity: 12, ChargedMicros: 100_000,
	}}, resp.Agent.Models, "a model fully trimmed to zero is dropped")
	require.LessOrEqual(t, sumModelCharges(resp.Agent.Models), resp.Agent.TotalMicros)
	require.EqualValues(t, 100_000, resp.Agent.TotalMicros)
}

func TestGetAccountBill_AgentModelsEmptyForModelLessUsage(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)
	store.appBillRowsByApp[uuid.Nil] = []usage.AppMetricUsageRaw{
		customLine(uuid.New(), "agent.work.units", "", 321),
		infraLine("infra.egress.api.bytes", 654),
	}
	store.appInfraBillRowsByApp[uuid.Nil] = []usage.AppInfraUsage{
		appInfraLine("infra.egress.api.bytes", "network", 1, 654, 654),
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Agent.Models)
	require.Empty(t, resp.Agent.Models)
	require.EqualValues(t, 321, resp.Agent.ModuleUsageMicros)
	require.EqualValues(t, 654, resp.Agent.InfraMicros)
	require.EqualValues(t, 975, resp.Agent.TotalMicros)
	require.Equal(t, resp.Agent.TotalMicros, resp.TotalMicros)
}

func TestGetAccountBill_MixedAppsAndAgentReconcileExactly(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)

	activeApp, deletedUnnamedApp := seqUUID(1), seqUUID(2)
	created := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	store.appMirrors[activeApp] = usage.AppMirrorInfo{
		Name: "Live app", ModuleCount: 6, CreatedAt: created,
	}
	store.appMirrors[deletedUnnamedApp] = usage.AppMirrorInfo{
		CreatedAt: created, Deleted: true,
		DeletedAt: time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC),
	}
	store.usageAppIDs = []uuid.UUID{deletedUnnamedApp, uuid.Nil, activeApp}

	store.appBillRowsByApp[activeApp] = []usage.AppMetricUsageRaw{
		customLine(uuid.New(), "orders.placed", "", 700),
	}
	store.appInfraBillRowsByApp[activeApp] = []usage.AppInfraUsage{
		appInfraLine("infra.request.count", "requests", 1, 80, 80),
	}
	store.appBillRowsByApp[deletedUnnamedApp] = []usage.AppMetricUsageRaw{
		customLine(uuid.New(), "views.count", "", 300),
	}
	store.appBillRowsByApp[uuid.Nil] = []usage.AppMetricUsageRaw{
		customLine(uuid.New(), "agent.work.units", "", 500),
	}
	store.appInfraBillRowsByApp[uuid.Nil] = []usage.AppInfraUsage{
		appInfraLine("infra.ai.output.tokens", "ai", 100, 1, 120),
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)

	require.Len(t, resp.Apps, 2)
	require.Equal(t, activeApp, resp.Apps[0].AppID)
	require.Equal(t, deletedUnnamedApp, resp.Apps[1].AppID)
	require.True(t, resp.Apps[1].IsDeleted)
	require.Empty(t, resp.Apps[1].Name, "a historical unnamed app remains a real app row")
	for _, app := range resp.Apps {
		require.NotEqual(t, uuid.Nil, app.AppID, "the agent sentinel never enters apps[]")
	}

	require.EqualValues(t, 500, resp.Agent.ModuleUsageMicros)
	require.EqualValues(t, 120, resp.Agent.InfraMicros)
	require.Equal(t, resp.Agent.ModuleUsageMicros+resp.Agent.InfraMicros, resp.Agent.TotalMicros,
		"agent total has no base-fee term")

	var perApp int64
	for _, app := range resp.Apps {
		perApp += app.TotalMicros
	}
	require.Equal(t, resp.BaseFeeTotalMicros+resp.ModuleUsageTotalMicros+resp.InfraTotalMicros, perApp,
		"Σ apps[].total == base fee total + module usage total + infra total")
	require.Equal(t,
		resp.BaseFeeTotalMicros+resp.ModuleUsageTotalMicros+resp.InfraTotalMicros+
			resp.AccountOverageMicros+resp.CustomDomainsMicros+
			resp.Agent.TotalMicros-resp.PaasCreditMicros,
		resp.TotalMicros,
		"agent spend is included exactly once alongside the app plane")
}

func TestGetAccountBill_AgentBucketDiscardsDefaultAppBaseFee(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)
	store.usageAppIDs = []uuid.UUID{uuid.Nil}
	store.appBillRowsByApp[uuid.Nil] = []usage.AppMetricUsageRaw{
		customLine(uuid.New(), "agent.work.units", "", 17),
	}
	store.appInfraBillRowsByApp[uuid.Nil] = []usage.AppInfraUsage{
		appInfraLine("infra.ai.input.tokens", "ai", 1, 23, 23),
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)

	// computeAppBill(uuid.Nil) has no mirror row and therefore resolves the
	// default $20 app base internally. The account-agent projection must drop it.
	require.Empty(t, resp.Apps)
	require.Zero(t, resp.BaseFeeTotalMicros)
	require.EqualValues(t, 40, resp.Agent.TotalMicros)
	require.Equal(t, resp.Agent.ModuleUsageMicros+resp.Agent.InfraMicros, resp.Agent.TotalMicros)
	require.EqualValues(t, 40, resp.TotalMicros)
	require.NotEqual(t, usage.BaseFeeMicros, resp.TotalMicros)
	require.NotEqual(t, usage.BaseFeeMicros+resp.Agent.TotalMicros, resp.TotalMicros,
		"the default app base is discarded, not folded into the agent bucket")
}

// --- mirror window semantics ------------------------------------------------

func TestGetAccountBill_MirrorLifecycleAcrossTheWindow(t *testing.T) {
	// The mirror half enumerates [created_at, deleted_at) ∩ window:
	//   deleted BEFORE the period       → excluded entirely (base 0, no new usage),
	//   deleted DURING, no arrears      → base estimate 0 (nothing will ever bill
	//                                     for it — the charge legs skip deleted
	//                                     apps) and the all-zero row is DROPPED,
	//   deleted DURING, with usage      → kept, usage-only total (base 0): the
	//                                     arrears still bill at the boundary,
	//   created DURING the period       → included, FULL advance base,
	//   created AFTER the period        → excluded.
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store) // [May 1, Jun 1), 31 days

	deletedBefore, deletedDuring, deletedWithUsage, createdDuring, createdAfter :=
		seqUUID(1), seqUUID(2), seqUUID(3), seqUUID(4), seqUUID(5)
	jan := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	may20 := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	store.appMirrors[deletedBefore] = usage.AppMirrorInfo{CreatedAt: jan, Deleted: true, DeletedAt: time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC)}
	store.appMirrors[deletedDuring] = usage.AppMirrorInfo{CreatedAt: jan, Deleted: true, DeletedAt: may20}
	store.appMirrors[deletedWithUsage] = usage.AppMirrorInfo{CreatedAt: jan, Deleted: true, DeletedAt: may20}
	store.appMirrors[createdDuring] = usage.AppMirrorInfo{CreatedAt: time.Date(2026, 5, 22, 14, 30, 0, 0, time.UTC)}
	store.appMirrors[createdAfter] = usage.AppMirrorInfo{CreatedAt: time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)}
	store.appBillRowsByApp[deletedWithUsage] = []usage.AppMetricUsageRaw{customLine(uuid.New(), "orders.placed", "", 700)}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 2, "deleted apps render only while they still owe arrears")
	require.Equal(t, deletedWithUsage, resp.Apps[0].AppID)
	require.Zero(t, resp.Apps[0].BaseFeeMicros, "a deleted app's uncharged base previews 0 (the charge legs skip deleted apps)")
	require.EqualValues(t, 700, resp.Apps[0].TotalMicros, "its accrued usage still renders (and bills) — usage-only row")
	require.True(t, resp.Apps[0].IsDeleted)
	require.Equal(t, createdDuring, resp.Apps[1].AppID)
	// Regression #63: mid-period creation must NOT prorate the recurring
	// estimate — the (created_at → period-end) proration is the one-time "New
	// creation" charge; this line previews the advance-base leg, always the
	// full fee. (Prod symptom: $20 plan showed 20×22/31 = $14.19 per app.)
	require.EqualValues(t, usage.BaseFeeMicros, resp.Apps[1].BaseFeeMicros, "created mid-period still previews the FULL advance base")
	require.Equal(t, usage.BaseFeeMicros+700, resp.TotalMicros)
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
	require.EqualValues(t, 5_000_000, resp.AccountOverageMicros,
		"un-snapshotted current period estimates the pooled overage from the live pool 8 → 3 over → 1 block → $5")
}

func TestGetAccountBill_ProjectsFullBaseForLiveApps(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store) // [May 1, Jun 1), 31 days

	const liveAppCount = 3
	periodStart := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	createdAt := time.Date(2026, 5, 22, 14, 30, 0, 0, time.UTC)
	proratedBase := usage.ProratedBaseMicros(usage.BaseFeeMicros, createdAt, periodStart, periodEnd)
	require.Less(t, proratedBase, usage.BaseFeeMicros, "fixture must accrue less than the full period base")

	for i := 1; i <= liveAppCount; i++ {
		app := seqUUID(byte(i))
		store.appMirrors[app] = usage.AppMirrorInfo{CreatedAt: createdAt}
		store.baseSnapshots[baseSnapKey(app, periodStart)] = usage.AppBaseSnapshotInfo{BaseMicros: proratedBase}
	}

	// A deleted app with arrears remains in Apps, but must not contribute a
	// full base to the projected period-end estimate.
	deletedApp := seqUUID(liveAppCount + 1)
	store.appMirrors[deletedApp] = usage.AppMirrorInfo{
		CreatedAt: time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC),
		Deleted:   true,
		DeletedAt: time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC),
	}
	store.appBillRowsByApp[deletedApp] = []usage.AppMetricUsageRaw{
		customLine(uuid.New(), "orders.placed", "", 700),
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, liveAppCount+1)
	require.True(t, resp.Apps[liveAppCount].IsDeleted, "deleted app with arrears stays visible")

	expectedAccruedBase := int64(liveAppCount) * proratedBase
	expectedProjectedBase := int64(liveAppCount) * usage.BaseFeeMicros
	require.Equal(t, expectedAccruedBase, resp.BaseFeeTotalMicros)
	require.Equal(t, expectedProjectedBase, resp.ProjectedBaseFeeTotalMicros)
	require.Equal(t, expectedAccruedBase+700, resp.TotalMicros)
	require.Equal(t, expectedProjectedBase+700, resp.ProjectedTotalMicros)
	require.Greater(t, resp.ProjectedTotalMicros, resp.TotalMicros)
	// Invariant: ONLY the base differs between the projected and accrued totals
	// — every usage/overage/domain/agent/credit component is carried identically.
	require.Equal(t,
		resp.ProjectedTotalMicros-resp.TotalMicros,
		resp.ProjectedBaseFeeTotalMicros-resp.BaseFeeTotalMicros,
		"projected and accrued totals must differ by exactly the base delta")
}

func TestGetAccountBill_ProjectedTotalIncludesUnresolvedCreationUntilDurableTerminal(t *testing.T) {
	store := newFakeStore()
	owner, accountID := uuid.New(), uuid.New()
	store.accounts[owner] = accountID
	store.anchorDays[accountID] = 11

	now := time.Date(2026, 7, 25, 11, 0, 0, 0, time.UTC)
	activatedAt := time.Date(2026, 6, 11, 9, 0, 0, 0, time.UTC)
	apps := []uuid.UUID{seqUUID(1), seqUUID(2), seqUUID(3), seqUUID(4), seqUUID(5)}
	for _, appID := range apps {
		store.appMirrors[appID] = usage.AppMirrorInfo{
			CreatedAt: time.Date(2026, 7, 15, 11, 0, 0, 0, time.UTC),
		}
	}

	// Four settled apps project $80 next-period base. The fifth is still in its
	// creation-charge lifecycle and remains one-time exposure until settlement.
	// One app carries $0.06 usage, and the account agent carries $0.01.
	store.appBillRowsByApp[apps[0]] = []usage.AppMetricUsageRaw{
		customLine(uuid.New(), "views.count", "", 60_000),
	}
	store.appBillRowsByApp[uuid.Nil] = []usage.AppMetricUsageRaw{
		customLine(uuid.New(), "agent.tokens", "", 10_000),
	}

	pendingApp := apps[4]
	createdAt := time.Date(2026, 7, 25, 11, 0, 0, 0, time.UTC)
	store.appMirrors[pendingApp] = usage.AppMirrorInfo{CreatedAt: createdAt}
	store.unresolvedOneTimeCharges = []usage.UnresolvedOneTimeChargeRaw{{
		Kind:                  usage.UnresolvedOneTimeChargeCreationBase,
		ChargeID:              pendingApp,
		AppID:                 pendingApp,
		ChargeAt:              createdAt,
		GraceExpiresAt:        usage.GraceExpiry(createdAt),
		ActivatedAt:           activatedAt,
		CountsTowardRecurring: false,
	}}
	store.activatedRecurring = &usage.RecurringFeeCounts{Apps: 4}

	svc := newService(store).WithNow(func() time.Time { return now })
	bill, err := svc.GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner,
	})
	require.NoError(t, err)

	const (
		recurringProjection = int64(80_070_000) // $80.07
		pendingCreation     = int64(10_967_742) // $10.967742 → UI $10.97
		wantProjection      = int64(91_037_742) // UI rounds to $91.04
	)
	require.Equal(t, int64(4)*usage.BaseFeeMicros, bill.ProjectedBaseFeeTotalMicros)
	require.Equal(t, recurringProjection, bill.ProjectedTotalMicros-pendingCreation)
	require.Equal(t, wantProjection, bill.ProjectedTotalMicros)

	// ETA is only a scheduling threshold. A delayed daily sweep must not make
	// the amount disappear while the durable guard is still unresolved.
	svc = newService(store).WithNow(func() time.Time {
		return usage.GraceExpiry(createdAt).Add(24 * time.Hour)
	})
	afterETA, err := svc.GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner,
	})
	require.NoError(t, err)
	require.Equal(t, wantProjection, afterETA.ProjectedTotalMicros)

	// Once the Stripe rail starts, migration 050 replaces dynamic recomputation
	// with the immutable first-write amount. The pending exposure must not blink
	// out during that ownership handoff or drift while Stripe recovery retries.
	store.unresolvedOneTimeCharges[0].Frozen = true
	store.unresolvedOneTimeCharges[0].FrozenAmountMicros = pendingCreation
	store.unresolvedOneTimeCharges[0].FrozenSnapshotPeriodStart =
		time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)
	store.unresolvedOneTimeCharges[0].FrozenSnapshotPeriodEnd =
		time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	store.unresolvedOneTimeCharges[0].FrozenSnapshotBaseMicros = pendingCreation
	frozenRetry, err := svc.GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner,
	})
	require.NoError(t, err)
	require.Equal(t, wantProjection, frozenRetry.ProjectedTotalMicros)

	// The store query drops the row atomically when either the settled guard or
	// permanent-skip guard arms. A successful settlement simultaneously admits
	// the app to the recurring forecast.
	store.unresolvedOneTimeCharges = nil
	store.activatedRecurring = &usage.RecurringFeeCounts{Apps: 5}
	terminal, err := svc.GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner,
	})
	require.NoError(t, err)
	require.Equal(t, int64(100_070_000), terminal.ProjectedTotalMicros)
}

func TestGetAccountBill_CurrentProjectedBaseJoinsOnlyAfterInitialChargesSettle(t *testing.T) {
	store := newFakeStore()
	owner, accountID := uuid.New(), uuid.New()
	store.accounts[owner] = accountID
	createdAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for i := byte(1); i <= 5; i++ {
		store.appMirrors[seqUUID(i)] = usage.AppMirrorInfo{CreatedAt: createdAt}
	}
	// One app owns nine modules, so the live account pool has four overage
	// units. Before that app and those four timers settle, neither belongs to
	// the next-period recurring base.
	app := store.appMirrors[seqUUID(5)]
	app.ModuleCount = 9
	store.appMirrors[seqUUID(5)] = app
	store.activatedRecurring = &usage.RecurringFeeCounts{Apps: 4}

	bill, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner,
	})
	require.NoError(t, err)
	require.EqualValues(t, 80_000_000, bill.ProjectedBaseFeeTotalMicros)

	// The creation settlement is the atomic handoff. It can occur after a
	// period boundary; once durable, the app's $20 and the block covering four
	// charged timer units join the forecast: $20×5 + ceil(4/5)×$5 = $105.
	store.activatedRecurring = &usage.RecurringFeeCounts{Apps: 5, ModuleOverages: 4}
	bill, err = newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner,
	})
	require.NoError(t, err)
	require.EqualValues(t, 105_000_000, bill.ProjectedBaseFeeTotalMicros)
	require.Equal(t, bill.ProjectedBaseFeeTotalMicros, bill.ProjectedTotalMicros)
}

// The bill page shows the next-period recurring base ON each app rather than as
// an account line, so the parts must equal the whole at the RESPONSE level, not
// just inside the allocator. If they ever diverge, the page silently shows the
// customer less than the estimated total it also prints.
func TestGetAccountBill_PerAppProjectedBaseSumsToTheAccountLine(t *testing.T) {
	store := newFakeStore()
	owner, accountID := uuid.New(), uuid.New()
	store.accounts[owner] = accountID
	createdAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for i := byte(1); i <= 5; i++ {
		store.appMirrors[seqUUID(i)] = usage.AppMirrorInfo{CreatedAt: createdAt}
	}
	// The owner's real shape: one app holding 13 modules and one custom domain,
	// four idle apps alongside it. 13 − 5 included = 8 over → 2 blocks.
	app := store.appMirrors[seqUUID(1)]
	app.ModuleCount = 13
	store.appMirrors[seqUUID(1)] = app
	store.liveDomainCount = 1
	store.activatedRecurring = &usage.RecurringFeeCounts{
		Apps: 5, ModuleOverages: 8, CustomDomains: 1,
	}

	bill, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner,
	})
	require.NoError(t, err)

	// $20×5 + 2 blocks + $2 domain = $112.00 — the account line as before.
	require.EqualValues(t, 112_000_000, bill.ProjectedBaseFeeTotalMicros)

	var perApp int64
	for _, row := range bill.Apps {
		perApp += row.ProjectedBaseFeeMicros
	}
	require.Equal(t, bill.ProjectedBaseFeeTotalMicros, perApp,
		"every recurring-base unit belongs to exactly one app row")

	// And it lands on the app that owns it: $20 + $10 of blocks + $2 = $32.
	byApp := make(map[uuid.UUID]int64, len(bill.Apps))
	for _, row := range bill.Apps {
		byApp[row.AppID] = row.ProjectedBaseFeeMicros
	}
	require.EqualValues(t, 32_000_000, byApp[seqUUID(1)],
		"the app holding the modules and the domain carries their fees")
	require.EqualValues(t, usage.BaseFeeMicros, byApp[seqUUID(2)],
		"an app owning no surcharge carries only the flat base")
}

// A frozen window has no surcharge forecast to allocate — its projection is the
// flat per-app base — so the same identity must hold there by a different route.
func TestGetAccountBill_FrozenWindowProjectsTheFlatBasePerApp(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store) // frozen [May 1, Jun 1)
	createdAt := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	for i := byte(1); i <= 3; i++ {
		store.appMirrors[seqUUID(i)] = usage.AppMirrorInfo{CreatedAt: createdAt}
	}
	// Surcharges the CURRENT window would forecast; a frozen one must not
	// allocate them to an app, so they stay in the account-level line.
	store.liveDomainCount = 2

	bill, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner,
		PeriodID:    pid.String(),
	})
	require.NoError(t, err)

	var perApp int64
	for _, row := range bill.Apps {
		perApp += row.ProjectedBaseFeeMicros
		require.EqualValues(t, usage.BaseFeeMicros, row.ProjectedBaseFeeMicros)
	}
	require.Equal(t, bill.ProjectedBaseFeeTotalMicros, perApp)
}

func TestGetAccountBill_UnresolvedOneTimeChargeShapesAndRecurringDedup(t *testing.T) {
	tests := []struct {
		name                  string
		now                   time.Time
		activatedAt           time.Time
		kind                  usage.UnresolvedOneTimeChargeKind
		chargeAt              time.Time
		graceExpiresAt        time.Time
		countsTowardRecurring bool
		moduleCount           int
		wantIncrementMicros   int64
	}{
		{
			name:                  "D1d fully pre-activation creation is deterministically free",
			now:                   time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC),
			activatedAt:           time.Date(2026, 7, 11, 9, 0, 0, 0, time.UTC),
			kind:                  usage.UnresolvedOneTimeChargeCreationBase,
			chargeAt:              time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
			graceExpiresAt:        time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC),
			countsTowardRecurring: true,
			wantIncrementMicros:   0,
		},
		{
			name:                  "D1d narrows a creation straddle to the post-activation period",
			now:                   time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
			activatedAt:           time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC),
			kind:                  usage.UnresolvedOneTimeChargeCreationBase,
			chargeAt:              time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC),
			graceExpiresAt:        time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC),
			countsTowardRecurring: true,
			wantIncrementMicros:   usage.BaseFeeMicros,
		},
		{
			name:                  "creation straddle overlapping forecast adds only raw first-period proration",
			now:                   time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC),
			activatedAt:           time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC),
			kind:                  usage.UnresolvedOneTimeChargeCreationBase,
			chargeAt:              time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
			graceExpiresAt:        time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC),
			countsTowardRecurring: true,
			wantIncrementMicros:   1_290_323, // $20 × 2 remaining days / 31
		},
		{
			name:                  "grace exactly at boundary is an inclusive straddle and deduplicates",
			now:                   time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC),
			activatedAt:           time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC),
			kind:                  usage.UnresolvedOneTimeChargeCreationBase,
			chargeAt:              time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			graceExpiresAt:        time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC),
			countsTowardRecurring: true,
			wantIncrementMicros:   1_935_484, // full next period is already in projected base
		},
		{
			name:                  "delayed older creation retains non-overlapping straddled period",
			now:                   time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC),
			activatedAt:           time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC),
			kind:                  usage.UnresolvedOneTimeChargeCreationBase,
			chargeAt:              time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
			graceExpiresAt:        time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC),
			countsTowardRecurring: true,
			wantIncrementMicros:   21_290_323,
		},
		{
			name:                  "normal module proration is additional to next-period recurring overage",
			now:                   time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC),
			activatedAt:           time.Date(2026, 6, 11, 9, 0, 0, 0, time.UTC),
			kind:                  usage.UnresolvedOneTimeChargeModuleTimer,
			chargeAt:              time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC),
			graceExpiresAt:        time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC),
			countsTowardRecurring: true,
			moduleCount:           usage.IncludedModules + 1,
			wantIncrementMicros:   709_677, // raw $1 × 22 remaining days / 31
		},
		{
			name:                  "module straddle uses raw wallet micros and removes overlapping recurring fee",
			now:                   time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC),
			activatedAt:           time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC),
			kind:                  usage.UnresolvedOneTimeChargeModuleTimer,
			chargeAt:              time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
			graceExpiresAt:        time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC),
			countsTowardRecurring: true,
			moduleCount:           usage.IncludedModules + 1,
			wantIncrementMicros:   64_516, // $1 × 2 remaining days / 31, not Stripe-cent rounded
		},
		{
			name:                  "attempted timer now included keeps full unresolved recovery shape",
			now:                   time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC),
			activatedAt:           time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC),
			kind:                  usage.UnresolvedOneTimeChargeModuleTimer,
			chargeAt:              time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
			graceExpiresAt:        time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC),
			countsTowardRecurring: false,
			moduleCount:           usage.IncludedModules,
			wantIncrementMicros:   1_064_516,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			owner, accountID, appID := uuid.New(), uuid.New(), uuid.New()
			store.accounts[owner] = accountID
			store.anchorDays[accountID] = tc.activatedAt.UTC().Day()
			store.appMirrors[appID] = usage.AppMirrorInfo{
				CreatedAt:   tc.chargeAt,
				ModuleCount: tc.moduleCount,
			}
			store.unresolvedOneTimeCharges = []usage.UnresolvedOneTimeChargeRaw{{
				Kind:                  tc.kind,
				ChargeID:              uuid.New(),
				AppID:                 appID,
				ChargeAt:              tc.chargeAt,
				GraceExpiresAt:        tc.graceExpiresAt,
				ActivatedAt:           tc.activatedAt,
				CountsTowardRecurring: tc.countsTowardRecurring,
			}}

			bill, err := newService(store).WithNow(func() time.Time {
				return tc.now
			}).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
				OwnerUserID: owner,
			})
			require.NoError(t, err)
			recurring := bill.ProjectedBaseFeeTotalMicros
			require.Equal(t, tc.wantIncrementMicros, bill.ProjectedTotalMicros-recurring)
		})
	}
}

func TestGetAccountBill_FrozenCombinedAttemptUsesExactMoneyAndExactRecurringMembership(t *testing.T) {
	activatedAt := time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	projectedStart := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	projectedEnd := time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)
	creationPeriodStart := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	const (
		frozenBase   = int64(21_290_323)
		frozenModule = int64(3_193_548)
	)

	tests := []struct {
		name                 string
		appLive              bool
		liveModuleCount      int
		baseRecurring        bool
		firstTimerRecurring  bool
		secondTimerRecurring bool
		snapshotStart        time.Time
		snapshotEnd          time.Time
		wantPending          int64
	}{
		{
			name:                 "live app and both exact children deduct their overlapping recurring units",
			appLive:              true,
			liveModuleCount:      usage.IncludedModules + 2,
			baseRecurring:        true,
			firstTimerRecurring:  true,
			secondTimerRecurring: true,
			snapshotStart:        projectedStart,
			snapshotEnd:          projectedEnd,
			// Frozen money is historical and unchanged; the recurring-overlap
			// deduction is one per-module stub ($1) per recurring child. The block-
			// priced recurring line and the per-module deduction do not cancel
			// exactly — see unresolvedOneTimeChargeIncrementMicros.
			wantPending: 5_677_419,
		},
		{
			name:                 "deleted app keeps full frozen base and timer recovery money",
			appLive:              false,
			baseRecurring:        false,
			firstTimerRecurring:  false,
			secondTimerRecurring: false,
			snapshotStart:        projectedStart,
			snapshotEnd:          projectedEnd,
			wantPending:          frozenBase + 2*frozenModule,
		},
		{
			name:                 "removed or now-included owned child is not deducted",
			appLive:              true,
			liveModuleCount:      usage.IncludedModules + 1,
			baseRecurring:        true,
			firstTimerRecurring:  true,
			secondTimerRecurring: false,
			snapshotStart:        projectedStart,
			snapshotEnd:          projectedEnd,
			wantPending:          frozenBase + 2*frozenModule - usage.BaseFeeMicros - usage.ModuleOverageFeeMicros,
		},
		{
			name:                 "delayed old full-period snapshot does not overlap the current forecast",
			appLive:              true,
			liveModuleCount:      usage.IncludedModules + 2,
			baseRecurring:        true,
			firstTimerRecurring:  true,
			secondTimerRecurring: true,
			snapshotStart:        projectedStart.AddDate(0, -1, 0),
			snapshotEnd:          projectedStart,
			wantPending:          frozenBase + 2*frozenModule,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			owner, accountID, appID := uuid.New(), uuid.New(), uuid.New()
			store.accounts[owner] = accountID
			store.anchorDays[accountID] = activatedAt.Day()
			mirror := usage.AppMirrorInfo{
				CreatedAt:   time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC),
				ModuleCount: tc.liveModuleCount,
			}
			if !tc.appLive {
				mirror.Deleted = true
				mirror.DeletedAt = time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC)
			}
			store.appMirrors[appID] = mirror

			frozen := func(
				kind usage.UnresolvedOneTimeChargeKind,
				id uuid.UUID,
				amount int64,
				recurring bool,
			) usage.UnresolvedOneTimeChargeRaw {
				return usage.UnresolvedOneTimeChargeRaw{
					Kind:                      kind,
					ChargeID:                  id,
					AppID:                     appID,
					ChargeAt:                  time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
					GraceExpiresAt:            time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC),
					ActivatedAt:               activatedAt,
					CountsTowardRecurring:     recurring,
					Frozen:                    true,
					FrozenAmountMicros:        amount,
					FrozenSnapshotPeriodStart: creationPeriodStart,
					FrozenSnapshotPeriodEnd:   projectedStart,
					FrozenSnapshotBaseMicros:  1_290_323,
					FrozenHasStraddle:         true,
					FrozenStraddlePeriodStart: tc.snapshotStart,
					FrozenStraddlePeriodEnd:   tc.snapshotEnd,
					FrozenStraddleBaseMicros:  usage.BaseFeeMicros,
				}
			}
			store.unresolvedOneTimeCharges = []usage.UnresolvedOneTimeChargeRaw{
				frozen(usage.UnresolvedOneTimeChargeCreationBase, appID, frozenBase, tc.baseRecurring),
				frozen(usage.UnresolvedOneTimeChargeModuleTimer, uuid.New(), frozenModule, tc.firstTimerRecurring),
				frozen(usage.UnresolvedOneTimeChargeModuleTimer, uuid.New(), frozenModule, tc.secondTimerRecurring),
			}

			bill, err := newService(store).WithNow(func() time.Time {
				return time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC)
			}).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
				OwnerUserID: owner,
			})
			require.NoError(t, err)
			recurring := bill.ProjectedBaseFeeTotalMicros
			require.Equal(t, tc.wantPending, bill.ProjectedTotalMicros-recurring)
		})
	}
}

func TestGetAccountBill_HistoricalPeriodNeverCarriesCurrentUnresolvedExposure(t *testing.T) {
	store := newFakeStore()
	owner, accountID, appID := uuid.New(), uuid.New(), uuid.New()
	store.accounts[owner] = accountID
	periodID := mirrorPeriod(store)
	store.appMirrors[appID] = usage.AppMirrorInfo{
		CreatedAt: time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC),
	}
	store.unresolvedOneTimeCharges = []usage.UnresolvedOneTimeChargeRaw{{
		Kind:                  usage.UnresolvedOneTimeChargeCreationBase,
		ChargeID:              appID,
		AppID:                 appID,
		ChargeAt:              time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC),
		GraceExpiresAt:        time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC),
		ActivatedAt:           time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		CountsTowardRecurring: true,
	}}

	bill, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner,
		PeriodID:    periodID.String(),
	})
	require.NoError(t, err)
	require.Equal(t, bill.ProjectedBaseFeeTotalMicros, bill.ProjectedTotalMicros)
}

func TestProjectedCreditChargeContainsOnlyUnpaidUsageExposure(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	app := seqUUID(1)
	store.appMirrors[app] = usage.AppMirrorInfo{
		ModuleCount: 7,
		CreatedAt:   time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
	}
	store.appBillRowsByApp[app] = []usage.AppMetricUsageRaw{
		customLine(uuid.New(), "orders.placed", "", 1_500),
	}
	store.appInfraBillRowsByApp[app] = []usage.AppInfraUsage{
		appInfraLine("infra.egress.api.bytes", "network", 100, 2, 200),
	}
	store.appBillRowsByApp[uuid.Nil] = []usage.AppMetricUsageRaw{
		customLine(uuid.New(), "agent.tokens", "", 300),
	}
	store.liveDomainCount = 2

	svc := newService(store)
	bill, err := svc.GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner,
	})
	require.NoError(t, err)
	projection, err := svc.ProjectedCreditCharge(context.Background(), owner, uuid.Nil)
	require.NoError(t, err)

	wantExposure := bill.ModuleUsageTotalMicros +
		bill.InfraTotalMicros +
		bill.Agent.TotalMicros -
		bill.PaasCreditMicros
	require.Equal(t, wantExposure, projection.AmountMicros)
	require.Equal(t,
		bill.ProjectedBaseFeeTotalMicros,
		bill.ProjectedTotalMicros-projection.AmountMicros,
		"recurring fees already settled by advance/proration legs are excluded from post-draw exposure")
	require.True(t, projection.PeriodStart.Equal(bill.PeriodStart))
	require.True(t, projection.PeriodEnd.Equal(bill.PeriodEnd))
}

// Migration 037: the account bill carries each app's FROZEN name + a deleted
// flag, and — the hoist guard — they show even on a CHARGED (snapshotted)
// period, where pre-037 the mirror was never read. A deleted app keeps its
// last-known name so its bill row renders the name instead of "unknown app".
func TestGetAccountBill_FrozenNameAndDeletedFlagOnChargedPeriod(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)
	app := seqUUID(1)
	// A DELETED app with a frozen name, and a CHARGED (snapshotted) base for the
	// period — the snapshot path, which pre-hoist skipped the mirror read.
	store.appMirrors[app] = usage.AppMirrorInfo{
		ModuleCount: 0, CreatedAt: time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC),
		Name: "影音教育平台", Deleted: true, DeletedAt: time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC),
	}
	store.baseSnapshots[baseSnapKey(app, time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))] = usage.AppBaseSnapshotInfo{
		BaseMicros: usage.BaseFeeMicros,
	}

	resp, err := newService(store).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	require.Equal(t, "影音教育平台", resp.Apps[0].Name, "the frozen name shows even on a charged/snapshotted period (hoist guard)")
	require.True(t, resp.Apps[0].IsDeleted, "the server-authoritative deleted flag")
	require.Equal(t, usage.BaseFeeMicros, resp.Apps[0].BaseFeeMicros, "the charged base is unaffected")
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
	require.Zero(t, resp.Agent.ModuleUsageMicros)
	require.Zero(t, resp.Agent.InfraMicros)
	require.Zero(t, resp.Agent.TotalMicros)
	require.NotNil(t, resp.Agent.Models)
	require.Empty(t, resp.Agent.Models)
	require.Zero(t, resp.PaasCreditMicros)
	require.Zero(t, resp.TotalMicros)
	// The plan stub still renders (the card shows Hobby even pre-activation).
	require.Equal(t, usage.PlanStubName, resp.Plan.Name)
	require.True(t, resp.Plan.RenewsAt.Equal(resp.PeriodEnd))
}

func TestGetAccountBill_AgentModelsJSONContract(t *testing.T) {
	modelID := "anthropic.claude-haiku-4-5-20251001-v1:0"
	encoded, err := json.Marshal(usage.GetAccountBillResponse{
		Agent: usage.AccountAgentBill{
			Models: []usage.AgentModelUsage{{
				Model: modelID, BillableQuantity: 1.5, ChargedMicros: 42,
			}},
		},
	})
	require.NoError(t, err)

	var wire map[string]any
	require.NoError(t, json.Unmarshal(encoded, &wire))
	require.NotContains(t, wire, "unresolved_one_time_micros",
		"the package-internal projection seam must not change the public JSON contract")
	agent, ok := wire["agent"].(map[string]any)
	require.True(t, ok)
	models, ok := agent["models"].([]any)
	require.True(t, ok, "agent.models must be present as an array")
	require.Len(t, models, 1)
	row, ok := models[0].(map[string]any)
	require.True(t, ok)
	rowKeys := make([]string, 0, len(row))
	for key := range row {
		rowKeys = append(rowKeys, key)
	}
	require.ElementsMatch(t, []string{"model", "billable_quantity", "charged_micros"}, rowKeys,
		"model-row JSON keys must match the deployed decoder exactly")
	require.Equal(t, modelID, row["model"])
	require.Equal(t, 1.5, row["billable_quantity"])
	require.Equal(t, float64(42), row["charged_micros"])

	// Exercise the actual lazy response too: its models key must encode as [],
	// never null, on every zero-bill response.
	lazy, err := newService(newFakeStore()).GetAccountBill(context.Background(), usage.GetAccountBillRequest{
		OwnerUserID: uuid.New(),
	})
	require.NoError(t, err)
	encoded, err = json.Marshal(lazy)
	require.NoError(t, err)
	wire = nil
	require.NoError(t, json.Unmarshal(encoded, &wire))
	agent, ok = wire["agent"].(map[string]any)
	require.True(t, ok)
	lazyModels, ok := agent["models"].([]any)
	require.True(t, ok, "lazy agent.models must encode as [] rather than null")
	require.Empty(t, lazyModels)
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
		"usage half":          func(f *fakeStore) { f.errAppIDsWithUsage = errors.New("boom") },
		"mirror half":         func(f *fakeStore) { f.errMirroredApps = errors.New("boom") },
		"live domain count":   func(f *fakeStore) { f.errLiveDomainCount = errors.New("boom") },
		"unresolved one-time": func(f *fakeStore) { f.errUnresolvedOneTime = errors.New("boom") },
		"unknown unresolved kind": func(f *fakeStore) {
			f.unresolvedOneTimeCharges = []usage.UnresolvedOneTimeChargeRaw{{
				Kind: "not-a-charge-kind",
			}}
		},
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
