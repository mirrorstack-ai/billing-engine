//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 092 seeds the per-model token prices for the 5.x Claude roster
// (OD-15). The claims are about the DATABASE: each of the 16 (metric, model)
// rows after the full chain at the raw list price in µ$/1k tokens, a re-run is a
// no-op (a finance edit survives), and the down removes only these rows.

const (
	mIn  = "infra.ai.input.tokens"
	mOut = "infra.ai.output.tokens"
	mCW  = "infra.ai.cache_write.tokens"
	mCR  = "infra.ai.cache_read.tokens"
)

type m092Key struct{ metric, model string }

// m092Want reads the shared fixture (internal/account/cycle/testdata/ai_model_prices.json,
// loaded by loadAIModelPriceFixture in ai_model_prices_integration_test.go) so
// this repo keeps exactly one copy of the 16 seeded numbers (µ$ per 1k tokens =
// USD per 1M tokens x 1000) instead of a second hand-copied table that could
// drift from it.
func m092Want(t *testing.T) map[m092Key]int64 {
	t.Helper()
	fixture := loadAIModelPriceFixture(t)
	want := make(map[m092Key]int64, 16)
	for model, prices := range fixture.Models {
		for metric, price := range prices {
			want[m092Key{metric, model}] = price
		}
	}
	return want
}

func m092Read(t *testing.T, pool *pgxpool.Pool, k m092Key) (price int64, active, ok bool) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT unit_price_micros, active FROM ms_billing.metric_model_prices WHERE metric = $1 AND model = $2`,
		k.metric, k.model).Scan(&price, &active)
	return price, active, err == nil
}

func m092Assert(t *testing.T, pool *pgxpool.Pool, why string) {
	t.Helper()
	prices := m092Want(t)
	require.Len(t, prices, 16)
	for k, want := range prices {
		got, active, ok := m092Read(t, pool, k)
		require.True(t, ok, "%s: %v row", why, k)
		require.True(t, active, "%s: %v active", why, k)
		require.Equal(t, want, got, "%s: %v", why, k)
	}
}

func m092Exec(t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), sql)
	require.NoError(t, err)
}

func TestMigration092_RowsAfterTheChainRerunIsANoOpAndDownRemovesOnlyThem(t *testing.T) {
	pool := testutil.NewTestDB(t) // the FULL chain, 092 included
	up := migrationSQL(t, "092_ai_model_prices_claude5_roster.up.sql")
	down := migrationSQL(t, "092_ai_model_prices_claude5_roster.down.sql")
	prices := m092Want(t)

	m092Assert(t, pool, "after the chain")

	// A finance edit on a seeded row survives a re-run; every other row is unchanged.
	edited := m092Key{mOut, "anthropic.claude-fable-5-1"}
	_, err := pool.Exec(context.Background(),
		`UPDATE ms_billing.metric_model_prices SET unit_price_micros = 61000 WHERE metric = $1 AND model = $2`,
		edited.metric, edited.model)
	require.NoError(t, err)
	m092Exec(t, pool, up)
	got, _, _ := m092Read(t, pool, edited)
	require.EqualValues(t, 61000, got, "a finance UPDATE is never overwritten")
	m092Exec(t, pool, up)
	for k, want := range prices {
		if k == edited {
			continue
		}
		got, _, ok := m092Read(t, pool, k)
		require.True(t, ok, "%v", k)
		require.Equal(t, want, got, "re-run leaves %v", k)
	}

	// The pre-092 rows (018 Haiku 4.5, 042 Sonnet 5) must survive the down.
	var before int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.metric_model_prices WHERE model NOT IN
		   ('anthropic.claude-haiku-5-5','anthropic.claude-sonnet-5-5','anthropic.claude-opus-5-5','anthropic.claude-fable-5-1')`).Scan(&before))
	require.Positive(t, before)

	m092Exec(t, pool, down)
	for k := range prices {
		_, _, ok := m092Read(t, pool, k)
		require.False(t, ok, "down removes %v", k)
	}
	var after int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.metric_model_prices`).Scan(&after))
	require.Equal(t, before, after, "down removes only the 092 rows")

	m092Exec(t, pool, up)
	m092Edited, _, _ := m092Read(t, pool, edited)
	require.EqualValues(t, 50000, m092Edited, "up after down reseeds the list price")
	m092Assert(t, pool, "up again")
}
