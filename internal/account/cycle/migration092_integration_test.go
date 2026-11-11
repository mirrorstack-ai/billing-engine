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

// µ$ per 1k tokens = USD per 1M tokens x 1000.
var m092Want = map[m092Key]int64{
	{mIn, "anthropic.claude-haiku-5-5"}: 100, {mOut, "anthropic.claude-haiku-5-5"}: 500,
	{mCW, "anthropic.claude-haiku-5-5"}: 125, {mCR, "anthropic.claude-haiku-5-5"}: 10,
	{mIn, "anthropic.claude-sonnet-5-5"}: 2000, {mOut, "anthropic.claude-sonnet-5-5"}: 10000,
	{mCW, "anthropic.claude-sonnet-5-5"}: 2500, {mCR, "anthropic.claude-sonnet-5-5"}: 100,
	{mIn, "anthropic.claude-opus-5-5"}: 4000, {mOut, "anthropic.claude-opus-5-5"}: 20000,
	{mCW, "anthropic.claude-opus-5-5"}: 5000, {mCR, "anthropic.claude-opus-5-5"}: 200,
	{mIn, "anthropic.claude-fable-5-1"}: 10000, {mOut, "anthropic.claude-fable-5-1"}: 50000,
	{mCW, "anthropic.claude-fable-5-1"}: 12500, {mCR, "anthropic.claude-fable-5-1"}: 250,
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
	require.Len(t, m092Want, 16)
	for k, want := range m092Want {
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
	for k, want := range m092Want {
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
	for k := range m092Want {
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
