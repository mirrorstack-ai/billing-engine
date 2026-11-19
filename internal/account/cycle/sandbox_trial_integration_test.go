//go:build integration

package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/budget"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 095 / billing-engine#256 on a real Postgres 17: one run's events
// carry the tag and run id, the sandbox budget rises by the priced amount, an
// ordinary task event still has its charge line, and org B's row is unmoved.

const sbxModel = "anthropic.claude-sonnet-5-5"

var sbxNow = time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

type sbxEnv struct {
	pool         *pgxpool.Pool
	usage        *usage.Service
	budget       *budget.Service
	orgA, orgB   uuid.UUID
	acctA, acctB uuid.UUID
}

func newSbxEnv(t *testing.T) *sbxEnv {
	t.Helper()
	pool := testutil.NewTestDB(t)
	bs := budget.NewService(budget.NewStore(pool)).WithNow(func() time.Time { return sbxNow })
	e := &sbxEnv{pool: pool, budget: bs, usage: usage.NewService(usage.NewStore(pool)).WithBudgetEvaluator(bs)}
	for _, p := range []struct{ org, acct *uuid.UUID }{{&e.orgA, &e.acctA}, {&e.orgB, &e.acctB}} {
		*p.org, *p.acct = uuid.New(), uuid.New()
		_, err := pool.Exec(context.Background(),
			`INSERT INTO ms_billing.accounts (id, owner_kind, owner_org_id) VALUES ($1, 'org', $2)`, p.acct.String(), p.org.String())
		require.NoError(t, err)
		_, err = bs.SetBudget(context.Background(), budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: *p.org,
			Category: budget.CategorySandbox, LimitMicros: 1_000_000, Active: true, HardCap: true})
		require.NoError(t, err)
	}
	return e
}

func (e *sbxEnv) spend(t *testing.T, org uuid.UUID, cat budget.Category) *budget.GetBudgetStatusResponse {
	t.Helper()
	st, err := e.budget.GetBudgetStatus(context.Background(), budget.GetBudgetStatusRequest{Scope: budget.ScopeOrg, ScopeID: org, Category: cat})
	require.NoError(t, err)
	return st
}

func (e *sbxEnv) record(t *testing.T, id, metric, model, source string, value float64) {
	t.Helper()
	req := usage.RecordInfraUsageRequest{EventID: id, AppID: uuid.New(), OwnerOrgID: e.orgA, Metric: metric, Model: model, Value: value, RecordedAt: sbxNow}
	if source != "" {
		req.Source, req.RunID = source, "run-42"
	}
	resp, err := e.usage.RecordInfraUsage(context.Background(), req)
	require.NoError(t, err, id)
	require.True(t, resp.Recorded, id)
}

// price reads a live raw unit price the way an independent reader would.
func (e *sbxEnv) price(t *testing.T, metric, model string) float64 {
	t.Helper()
	var p int64
	q, args := `SELECT unit_price_micros FROM ms_billing.metric_definitions WHERE module_id=$1 AND metric=$2`, []any{usage.PlatformInfraModuleID().String(), metric}
	if model != "" {
		q, args = `SELECT unit_price_micros FROM ms_billing.metric_model_prices WHERE metric=$1 AND model=$2`, []any{metric, model}
	}
	require.NoError(t, e.pool.QueryRow(context.Background(), q, args...).Scan(&p))
	return float64(p)
}

func TestSandboxTrial_Acceptance_Integration(t *testing.T) {
	e, ctx := newSbxEnv(t), context.Background()
	require.EqualValues(t, 40450, e.price(t, "infra.sandbox.vcpu.hours", ""))

	e.record(t, "sbx-vcpu", "infra.sandbox.vcpu.hours", "", usage.SourceSandboxTrial, 0.5)
	e.record(t, "sbx-mem", "infra.sandbox.memory.gib_hours", "", usage.SourceSandboxTrial, 2)
	e.record(t, "sbx-in", "infra.ai.input.tokens", sbxModel, usage.SourceSandboxTrial, 10)
	e.record(t, "sbx-out", "infra.ai.output.tokens", sbxModel, usage.SourceSandboxTrial, 5)

	var tagged int
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.usage_events
		WHERE event_id LIKE 'sbx-%' AND source='sandbox_trial' AND run_id='run-42' AND dev_served AND stamped_price_micros IS NOT NULL`).Scan(&tagged))
	require.Equal(t, 4, tagged, "every run event carries the source, the run id and a stamped price, and is never charged")

	want := 0.5*e.price(t, "infra.sandbox.vcpu.hours", "") + 2*e.price(t, "infra.sandbox.memory.gib_hours", "") +
		10*e.price(t, "infra.ai.input.tokens", sbxModel) + 5*e.price(t, "infra.ai.output.tokens", sbxModel)
	got := e.spend(t, e.orgA, budget.CategorySandbox)
	require.InEpsilon(t, want, float64(got.SpendMicros), 0.01)
	require.False(t, got.Exhausted, "below the cap (control)")
	require.Zero(t, e.spend(t, e.orgB, budget.CategorySandbox).SpendMicros, "org B's row is unmoved")

	// CONTROL: an ordinary infra.task.vcpu.hours event keeps its charge line; the trial run has none.
	e.record(t, "ctl-task", "infra.task.vcpu.hours", "", "", 1)
	sum, err := cycle.NewService(cycle.NewStore(e.pool), nil).RollupPeriod(ctx, e.acctA, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	var taskLine bool
	for _, a := range sum.Aggregates {
		if a.Metric == "infra.task.vcpu.hours" {
			taskLine = true
			require.False(t, a.DevServed)
			require.Positive(t, a.ChargedMicros)
		} else {
			require.True(t, a.DevServed, "%s: a trial line is never a charge line", a.Metric)
		}
	}
	require.True(t, taskLine)
	require.EqualValues(t, 40450*12/10, sum.TotalChargedMicros, "the collectable total is the control event alone")

	// Trial tokens never reach the ai budget; an untagged AI event counts there and not in the sandbox cap.
	_, err = e.budget.SetBudget(ctx, budget.SetBudgetRequest{Scope: budget.ScopeOrg, ScopeID: e.orgA, Category: budget.CategoryAI, LimitMicros: 1 << 40, Active: true})
	require.NoError(t, err)
	require.Zero(t, e.spend(t, e.orgA, budget.CategoryAI).SpendMicros)
	e.record(t, "ctl-ai", "infra.ai.input.tokens", sbxModel, "", 1000)
	require.Equal(t, got.SpendMicros, e.spend(t, e.orgA, budget.CategorySandbox).SpendMicros)
	require.Positive(t, e.spend(t, e.orgA, budget.CategoryAI).SpendMicros)

	// The price is stamped: a later edit leaves old events alone; a new event takes the new price.
	_, err = e.pool.Exec(ctx, `UPDATE ms_billing.metric_definitions SET unit_price_micros = unit_price_micros * 10 WHERE metric LIKE 'infra.sandbox.%'`)
	require.NoError(t, err)
	require.Equal(t, got.SpendMicros, e.spend(t, e.orgA, budget.CategorySandbox).SpendMicros)
	e.record(t, "sbx-vcpu-2", "infra.sandbox.vcpu.hours", "", usage.SourceSandboxTrial, 1)
	require.InEpsilon(t, float64(got.SpendMicros)+404_500, float64(e.spend(t, e.orgA, budget.CategorySandbox).SpendMicros), 0.01)

	// The hard cap exhausts org A only, and the crossing is recorded as an alert.
	e.record(t, "sbx-vcpu-3", "infra.sandbox.vcpu.hours", "", usage.SourceSandboxTrial, 5)
	require.True(t, e.spend(t, e.orgA, budget.CategorySandbox).Exhausted)
	require.False(t, e.spend(t, e.orgB, budget.CategorySandbox).Exhausted)
	var alerts int
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.budget_alerts a JOIN ms_billing.budgets b ON b.id = a.budget_id
		WHERE b.category='sandbox' AND b.scope_id=$1 AND a.percent=100`, e.orgA.String()).Scan(&alerts))
	require.Equal(t, 1, alerts)
}

// The invariants live in the schema, not only in the Go ingest.
func TestSandboxTrial_DatabaseRefusesAChargeableTrialRow_Integration(t *testing.T) {
	e := newSbxEnv(t)
	insert := func(dev bool, source, run string, stamped, acct any) error {
		_, err := e.pool.Exec(context.Background(), `INSERT INTO ms_billing.usage_events
			(event_id, account_id, app_id, module_id, metric, kind, value, recorded_at, dev_served, source, run_id, stamped_price_micros)
			VALUES ($1,$2,$3,$4,'infra.sandbox.vcpu.hours','sum',1,$5,$6,NULLIF($7::text,''),NULLIF($8::text,''),$9)`,
			uuid.NewString(), acct, uuid.NewString(), usage.PlatformInfraModuleID().String(), sbxNow, dev, source, run, stamped)
		return err
	}
	a := e.acctA.String()
	require.NoError(t, insert(true, "sandbox_trial", "run-1", int64(5), a))
	for name, err := range map[string]error{
		"chargeable": insert(false, "sandbox_trial", "run-1", int64(5), a), "no run id": insert(true, "sandbox_trial", "", int64(5), a),
		"no price": insert(true, "sandbox_trial", "run-1", nil, a), "no account": insert(true, "sandbox_trial", "run-1", int64(5), nil),
		"unknown source": insert(true, "free_credit", "run-1", int64(5), a), "run id alone": insert(true, "", "run-1", nil, a),
	} {
		var pg *pgconn.PgError
		require.ErrorAs(t, err, &pg, name)
		require.Equal(t, "23514", pg.Code, name)
	}
}

func TestMigration095_UpDownUp_RoundTrips(t *testing.T) {
	e, ctx := newSbxEnv(t), context.Background()
	e.record(t, "sbx-vcpu", "infra.sandbox.vcpu.hours", "", usage.SourceSandboxTrial, 1)

	_, err := e.pool.Exec(ctx, migrationSQL(t, "095_sandbox_trial_meters.down.sql"))
	require.NoError(t, err)
	require.False(t, columnExists(t, e.pool, "usage_events", "source"))
	var n int
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.usage_events WHERE event_id='sbx-vcpu' AND dev_served`).Scan(&n))
	require.Equal(t, 1, n, "the fact stays in the ledger, still never charged")

	_, err = e.pool.Exec(ctx, migrationSQL(t, "095_sandbox_trial_meters.up.sql"))
	require.NoError(t, err)
	require.True(t, columnExists(t, e.pool, "usage_events", "stamped_price_micros"))
	require.EqualValues(t, 40450, e.price(t, "infra.sandbox.vcpu.hours", ""))
}
