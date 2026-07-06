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

// Migration 031 (apps.proration_skipped_at) — verifies at the SQL layer that
// the permanent-skip marker (D1d, no retroactive catch-up) is a one-shot,
// first-write-wins guard, and that AppsPendingProration's work-list filter
// excludes a permanently-skipped app forever (the same partial-index
// predicate the migration re-defines).

func TestProrationSkipped_Integration_OneShotAndExcludedFromPending(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	cutoff := mustTime(t, "2026-04-05T00:00:00Z")

	skipped := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, skipped, acct, 0, mustTime(t, "2026-01-01T08:00:00Z"), ""))

	// Past grace, unarmed, not yet skipped → pending.
	ids, err := store.AppsPendingProration(ctx, cutoff)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{skipped}, ids)

	// Mark permanently skipped (what ChargeCreationProration does once it
	// determines the account activated after this app's period had closed).
	require.NoError(t, store.SetAppProrationSkipped(ctx, skipped))

	// Excluded from the pending work list from now on — a later sweep must
	// never resurface it (it would otherwise sit pending forever, since
	// proration_invoice_id stays NULL for a skipped charge).
	ids, err = store.AppsPendingProration(ctx, cutoff)
	require.NoError(t, err)
	require.Empty(t, ids, "a permanently-skipped app is excluded from every future sweep")

	// One-shot: a second SetAppProrationSkipped call is a harmless no-op — it
	// does not need to be idempotent-with-error, just idempotent-with-effect.
	require.NoError(t, store.SetAppProrationSkipped(ctx, skipped))

	app, _, err := store.AppMirror(ctx, skipped)
	require.NoError(t, err)
	require.True(t, app.ProrationSkipped)
	require.Empty(t, app.ProrationInvoiceID, "skipped, never charged")
}

func TestProrationSkipped_Integration_RefusesToSkipAnAlreadyChargedApp(t *testing.T) {
	// Defensive belt-and-suspenders: SetAppProrationSkipped's WHERE clause also
	// requires proration_invoice_id IS NULL, so it can never clobber a genuine
	// charge that raced in first.
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	appID := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, appID, acct, 0, mustTime(t, "2026-01-01T08:00:00Z"), ""))
	require.NoError(t, store.SetAppProrationInvoice(ctx, appID, "in_already_charged"))

	require.NoError(t, store.SetAppProrationSkipped(ctx, appID)) // no-op, not an error

	app, _, err := store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.False(t, app.ProrationSkipped, "an already-charged app must never be marked skipped")
	require.Equal(t, "in_already_charged", app.ProrationInvoiceID)
}
