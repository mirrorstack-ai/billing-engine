//go:build integration

package infrasync_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/infrasync"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 098 + PgStore: no row reads as zero, the upsert never moves the
// watermark backwards, a non-hour value is refused by the CHECK, and
// billing_svc holds no DELETE (deleting a row would silently reset a puller).
func TestPgStore_WatermarkIsDurableAndMonotonic(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	store := infrasync.NewPgStore(pool)

	got, err := store.Watermark(ctx, "cdn-egress")
	require.NoError(t, err)
	require.True(t, got.IsZero(), "no row yet")

	t1 := time.Date(2026, 9, 13, 8, 0, 0, 0, time.UTC)
	require.NoError(t, store.Advance(ctx, "cdn-egress", t1))
	require.NoError(t, store.Advance(ctx, "cdn-egress", t1.Add(-3*time.Hour)))
	got, err = store.Watermark(ctx, "cdn-egress")
	require.NoError(t, err)
	require.True(t, t1.Equal(got), "an older value must not move the watermark back, got %v", got)

	require.NoError(t, store.Advance(ctx, "cdn-egress", t1.Add(2*time.Hour)))
	got, err = store.Watermark(ctx, "cdn-egress")
	require.NoError(t, err)
	require.True(t, t1.Add(2*time.Hour).Equal(got))

	other, err := store.Watermark(ctx, "ssr-compute")
	require.NoError(t, err)
	require.True(t, other.IsZero(), "sources are independent")

	require.Error(t, store.Advance(ctx, "ssr-compute", t1.Add(30*time.Minute)),
		"confirmed_through must be a top of hour")

	var canDelete bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT has_table_privilege('billing_svc', 'ms_billing.infra_sync_watermarks', 'DELETE')`).Scan(&canDelete))
	require.False(t, canDelete)
}
