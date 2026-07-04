//go:build integration

package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 027 (ms_billing.apps) — verifies the SQL-level guard semantics the
// unit fakes only model: the ON CONFLICT registration no-op, the one-shot
// proration guard's WHERE … IS NULL (first-charge-wins), the deleted-row
// count freeze, and the live-roster scan the boundary advance leg sums.

func TestAppsMirror_Integration_GuardSemantics(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	appID := uuid.New()
	created := mustTime(t, "2026-06-10T08:00:00Z")

	// Register + a conflicting retry: the FIRST registration's created_at /
	// module_count survive (ON CONFLICT DO NOTHING).
	require.NoError(t, store.InsertAppMirror(ctx, appID, acct, 2, created))
	require.NoError(t, store.InsertAppMirror(ctx, appID, acct, 9, created.AddDate(0, 0, 5)))
	app, found, err := store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 2, app.ModuleCount)
	require.True(t, app.CreatedAt.Equal(created))
	require.Empty(t, app.ProrationInvoiceID)
	require.False(t, app.Deleted)

	// One-shot proration guard: first write wins, the second is a silent no-op.
	require.NoError(t, store.SetAppProrationInvoice(ctx, appID, "in_first"))
	require.NoError(t, store.SetAppProrationInvoice(ctx, appID, "in_second"))
	app, _, err = store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.Equal(t, "in_first", app.ProrationInvoiceID, "the guard is first-charge-wins")

	// Count sync works while live…
	require.NoError(t, store.SetAppModuleCount(ctx, appID, 7))
	app, _, err = store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.Equal(t, 7, app.ModuleCount)

	// …and freezes after deletion (WHERE deleted_at IS NULL), which is itself
	// idempotent (the first deletion instant is kept).
	require.NoError(t, store.MarkAppDeleted(ctx, appID))
	app, _, err = store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.True(t, app.Deleted)
	firstDeletedAt := app.DeletedAt
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, store.MarkAppDeleted(ctx, appID))
	require.NoError(t, store.SetAppModuleCount(ctx, appID, 99))
	app, _, err = store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.Equal(t, 7, app.ModuleCount, "a deleted app's count is frozen")
	require.True(t, app.DeletedAt.Equal(firstDeletedAt), "re-deletion never moves deleted_at")

	// Unknown app resolves found=false, never an error.
	_, found, err = store.AppMirror(ctx, uuid.New())
	require.NoError(t, err)
	require.False(t, found)
}

func TestAppsMirror_Integration_LiveRosterScan(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct, other := seedAccount(t, pool), seedAccount(t, pool)
	created := mustTime(t, "2026-06-01T00:00:00Z")

	live1, live2, dead := uuid.New(), uuid.New(), uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, live1, acct, 0, created))
	require.NoError(t, store.InsertAppMirror(ctx, live2, acct, 6, created))
	require.NoError(t, store.InsertAppMirror(ctx, dead, acct, 9, created))
	require.NoError(t, store.MarkAppDeleted(ctx, dead))
	require.NoError(t, store.InsertAppMirror(ctx, uuid.New(), other, 3, created)) // another account's app

	counts, err := store.LiveAppModuleCounts(ctx, acct)
	require.NoError(t, err)
	require.ElementsMatch(t, []int{0, 6}, counts,
		"deleted apps and other accounts' apps never enter the advance-base sum")

	// EnsureAccountForUser: get-or-create resolves the SAME account twice.
	userID := uuid.New()
	first, err := store.EnsureAccountForUser(ctx, userID)
	require.NoError(t, err)
	second, err := store.EnsureAccountForUser(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, first, second)
}
