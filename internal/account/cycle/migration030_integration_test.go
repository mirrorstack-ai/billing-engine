//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 030 (apps.created_module_count) — verifies at the SQL layer (the
// unit fakes only model this in Go) that the frozen creation-time module count
// survives a live module_count write untouched: InsertAppMirror stamps BOTH
// columns from the same value, and SetAppModuleCount (SyncAppModules' writer)
// touches ONLY module_count.

func TestCreatedModuleCount_Integration_FrozenAcrossSetAppModuleCount(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	appID := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, appID, acct, uuid.Nil, 2, mustTime(t, "2026-07-01T08:00:00Z"), ""))

	app, found, err := store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 2, app.ModuleCount)
	require.Equal(t, 2, app.CreatedModuleCount, "created_module_count starts equal to the registered count")

	// SyncAppModules' writer (module install/uninstall) — must move ONLY the
	// live column.
	require.NoError(t, store.SetAppModuleCount(ctx, appID, 9))

	app, found, err = store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 9, app.ModuleCount, "the live count moved")
	require.Equal(t, 2, app.CreatedModuleCount, "the frozen count must NOT move — no query writes it after insert")

	// A second live-count write (another install/uninstall cycle) still never
	// touches the frozen column.
	require.NoError(t, store.SetAppModuleCount(ctx, appID, 0))
	app, _, err = store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.Equal(t, 0, app.ModuleCount)
	require.Equal(t, 2, app.CreatedModuleCount)
}

func TestCreatedModuleCount_Integration_RetryKeepsFirstRegistrationsFrozenCount(t *testing.T) {
	// InsertAppMirror's ON CONFLICT (app_id) DO NOTHING means a RegisterApp
	// retry with a DIFFERENT module_count must not move either column — the
	// FIRST registration's frozen count is the stable proration anchor.
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	appID := uuid.New()
	created := mustTime(t, "2026-07-01T08:00:00Z")
	require.NoError(t, store.InsertAppMirror(ctx, appID, acct, uuid.Nil, 3, created, ""))
	require.NoError(t, store.InsertAppMirror(ctx, appID, acct, uuid.Nil, 12, created, "")) // retry, different count

	app, _, err := store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.Equal(t, 3, app.ModuleCount)
	require.Equal(t, 3, app.CreatedModuleCount)
}
