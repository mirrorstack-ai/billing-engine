//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/budget"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// billing-engine#368 on a real Postgres 17: the source tags api-platform sends
// (platform_sidebar, member, module) are ORDINARY events. Each one lands as a
// charge line and an AI budget spend identical to its untagged twin, and the
// sandbox cap never sees it. Migration 096's CHECK holds the four names.

func TestUsageSource_OrdinaryTagsAreChargedExactlyLikeTheUntaggedTwin_Integration(t *testing.T) {
	e, ctx := newSbxEnv(t), context.Background()

	// Fund the org first so every event below attaches to its account.
	_, err := e.pool.Exec(ctx, `UPDATE ms_billing.accounts SET activated_at = '2026-01-01' WHERE id = $1`, e.acctA.String())
	require.NoError(t, err)
	_, err = e.pool.Exec(ctx, `INSERT INTO ms_billing.org_billing_designations (org_id, funding, updated_by) VALUES ($1, 'org', $2)`, e.orgA.String(), uuid.NewString())
	require.NoError(t, err)
	_, err = e.budget.SetBudget(ctx, budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: e.orgA, Category: budget.CategoryAI, LimitMicros: 1 << 40, Active: true})
	require.NoError(t, err)

	// One AI event per source, same value and model, each on its own app so each
	// has its own charge line: the untagged twin first, then the two console tags.
	twins := []struct{ source, event string }{{"", "twin-none"}, {usage.SourcePlatformSidebar, "twin-sidebar"}, {usage.SourceMember, "twin-member"}}
	apps := map[string]uuid.UUID{}
	var perEvent int64
	for i, tw := range twins {
		apps[tw.event] = uuid.New()
		resp, err := e.usage.RecordInfraUsage(ctx, usage.RecordInfraUsageRequest{
			EventID: tw.event, AppID: apps[tw.event], OwnerOrgID: e.orgA, Metric: "infra.ai.input.tokens", Model: sbxModel,
			Value: 1000, RecordedAt: sbxNow, Source: tw.source,
		})
		require.NoError(t, err, tw.event)
		require.True(t, resp.Recorded, "%s is recorded, not refused at ingest", tw.event)

		spend := e.spend(t, e.orgA, budget.CategoryAI).SpendMicros
		if i == 0 {
			perEvent = spend
			require.Positive(t, perEvent)
		}
		require.InDelta(t, float64(int64(i+1)*perEvent), float64(spend), 1, "%s moves the AI budget by what its untagged twin did", tw.event)
	}
	require.Zero(t, e.spend(t, e.orgA, budget.CategorySandbox).SpendMicros, "no ordinary tag feeds the sandbox cap")

	// What the database holds: the tag is stored, and nothing else about the row differs.
	var tagged, shaped int
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE source IS NOT NULL),
			count(*) FILTER (WHERE NOT dev_served AND run_id IS NULL AND stamped_price_micros IS NULL AND account_id = $1)
		FROM ms_billing.usage_events WHERE event_id LIKE 'twin-%'`, e.acctA.String()).Scan(&tagged, &shaped))
	require.Equal(t, 2, tagged)
	require.Equal(t, 3, shaped, "chargeable, no run id, no stamped price, on the org account")

	// The charge lines are identical: same quantity, price and charge, none dev_served.
	sum, err := cycle.NewService(cycle.NewStore(e.pool), nil).RollupPeriod(ctx, e.acctA, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	lines := map[uuid.UUID]cycle.MetricAggregate{}
	for _, a := range sum.Aggregates {
		lines[a.AppID] = a
	}
	require.Len(t, lines, 3)
	base := lines[apps["twin-none"]]
	require.Positive(t, base.ChargedMicros)
	for event, app := range apps {
		got := lines[app]
		require.False(t, got.DevServed, event)
		require.Equal(t, base.BillableQuantity, got.BillableQuantity, event)
		require.Equal(t, base.UnitPriceMicros, got.UnitPriceMicros, event)
		require.Equal(t, base.ChargedMicros, got.ChargedMicros, event)
	}
	require.Equal(t, 3*base.ChargedMicros, sum.TotalChargedMicros, "all three are collectable")

	// A module event is accepted and charged like any infra event of its meter.
	resp, err := e.usage.RecordInfraUsage(ctx, usage.RecordInfraUsageRequest{
		EventID: "twin-module", AppID: uuid.New(), OwnerOrgID: e.orgA, Metric: "infra.task.vcpu.hours", Value: 1,
		RecordedAt: sbxNow, Source: usage.SourceModule,
	})
	require.NoError(t, err)
	require.True(t, resp.Recorded)
}

// The invariants live in the schema, not only in the Go ingest: the CHECK takes
// the four contract names as ordinary rows and still refuses any other value.
func TestUsageSource_DatabaseHoldsTheFourNames_Integration(t *testing.T) {
	e := newSbxEnv(t)
	insert := func(source string) error {
		_, err := e.pool.Exec(context.Background(), `INSERT INTO ms_billing.usage_events
			(event_id, account_id, app_id, module_id, metric, kind, value, recorded_at, dev_served, source)
			VALUES ($1,$2,$3,$4,'infra.task.vcpu.hours','sum',1,$5,false,NULLIF($6::text,''))`,
			uuid.NewString(), e.acctA.String(), uuid.NewString(), usage.PlatformInfraModuleID().String(), sbxNow, source)
		return err
	}
	for _, source := range []string{"", "platform_sidebar", "member", "module"} {
		require.NoError(t, insert(source), "source %q is an ordinary row", source)
	}
	// sandbox_trial still has its own shape (dev_served, run id, price): an ordinary row is refused.
	for _, source := range []string{"sandbox_trial", "free_credit", "Member", "sandbox"} {
		var pg *pgconn.PgError
		require.ErrorAs(t, insert(source), &pg, source)
		require.Equal(t, "23514", pg.Code, source)
	}
}
