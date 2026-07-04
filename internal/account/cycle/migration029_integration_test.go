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

// Migration 029 (apps_pending_proration_idx) — verifies the SQL-level semantics
// the unit fakes only model for the creation-grace sweep: the AppsPendingProration
// work-list filter (past grace AND live AND unarmed) and the ChargeProrationLocked
// FOR UPDATE section (deleted / already-armed short-circuits, and the atomic
// charge → mirror → snapshot → arm on a live unarmed app).

func TestAppsPendingProration_Integration_Filter(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	cutoff := mustTime(t, "2026-06-27T00:00:00Z") // now − GraceDays for a sweep on Jun 30

	pending := uuid.New() // created before cutoff, live, unarmed → returned
	young := uuid.New()   // created after cutoff (within grace) → excluded
	deleted := uuid.New() // before cutoff but soft-deleted → excluded
	charged := uuid.New() // before cutoff but guard armed → excluded
	require.NoError(t, store.InsertAppMirror(ctx, pending, acct, 0, mustTime(t, "2026-06-20T08:00:00Z")))
	require.NoError(t, store.InsertAppMirror(ctx, young, acct, 0, mustTime(t, "2026-06-29T08:00:00Z")))
	require.NoError(t, store.InsertAppMirror(ctx, deleted, acct, 0, mustTime(t, "2026-06-20T08:00:00Z")))
	require.NoError(t, store.InsertAppMirror(ctx, charged, acct, 0, mustTime(t, "2026-06-20T08:00:00Z")))
	require.NoError(t, store.MarkAppDeleted(ctx, deleted))
	require.NoError(t, store.SetAppProrationInvoice(ctx, charged, "in_already"))

	ids, err := store.AppsPendingProration(ctx, cutoff)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{pending}, ids, "only the past-grace, live, unarmed app is pending")
}

func TestChargeProrationLocked_Integration_Semantics(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	periodStart := mustTime(t, "2026-06-04T00:00:00Z")
	periodEnd := mustTime(t, "2026-07-04T00:00:00Z")

	// mkCharge builds the payload the service's charge callback would return.
	mkCharge := func(appID uuid.UUID, invID string) *cycle.ProrationCharge {
		return &cycle.ProrationCharge{
			InvoiceID: invID,
			Cents:     200,
			Invoice: cycle.InvoiceMirror{
				AccountID: acct, StripeInvoiceID: invID, Status: "open",
				AmountDueCents: 200, Currency: "usd",
				PeriodStart: mustTime(t, "2026-07-01T00:00:00Z"), PeriodEnd: periodEnd,
			},
			Snapshot: cycle.AppBaseSnapshot{
				AppID: appID, PeriodStart: periodStart, PeriodEnd: periodEnd,
				ModuleCount: 0, BaseMicros: 2_000_000,
			},
		}
	}

	// Live, unarmed app → the callback fires, and the invoice + snapshot + guard
	// commit atomically.
	live := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, live, acct, 0, mustTime(t, "2026-07-01T08:00:00Z")))
	called := false
	outcome, invID, err := store.ChargeProrationLocked(ctx, live, func(l cycle.AppMirror) (*cycle.ProrationCharge, error) {
		called = true
		require.Equal(t, live, l.AppID)
		return mkCharge(live, "in_live"), nil
	})
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, cycle.ProrationLockedCharged, outcome)
	require.Equal(t, "in_live", invID)
	app, _, err := store.AppMirror(ctx, live)
	require.NoError(t, err)
	require.Equal(t, "in_live", app.ProrationInvoiceID, "the guard is armed under the lock")

	// Re-run on the now-armed app → AlreadyCharged, callback NOT invoked, no
	// second invoice.
	called = false
	outcome, invID, err = store.ChargeProrationLocked(ctx, live, func(cycle.AppMirror) (*cycle.ProrationCharge, error) {
		called = true
		return mkCharge(live, "in_second"), nil
	})
	require.NoError(t, err)
	require.False(t, called, "an armed guard short-circuits before the Stripe charge")
	require.Equal(t, cycle.ProrationLockedAlreadyCharged, outcome)
	require.Equal(t, "in_live", invID)

	// Deleted app → Deleted, callback NOT invoked (a within-grace delete is never
	// charged, and a delete that won the race no-ops the charge).
	deleted := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, deleted, acct, 0, mustTime(t, "2026-07-01T08:00:00Z")))
	require.NoError(t, store.MarkAppDeleted(ctx, deleted))
	called = false
	outcome, _, err = store.ChargeProrationLocked(ctx, deleted, func(cycle.AppMirror) (*cycle.ProrationCharge, error) {
		called = true
		return mkCharge(deleted, "in_deleted"), nil
	})
	require.NoError(t, err)
	require.False(t, called, "a deleted app is never charged")
	require.Equal(t, cycle.ProrationLockedDeleted, outcome)

	// Callback declines (0 cents) → NoCharge, guard stays unarmed, nothing persisted.
	zero := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, zero, acct, 0, mustTime(t, "2026-07-01T08:00:00Z")))
	outcome, _, err = store.ChargeProrationLocked(ctx, zero, func(cycle.AppMirror) (*cycle.ProrationCharge, error) {
		return nil, nil
	})
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationLockedNoCharge, outcome)
	app, _, err = store.AppMirror(ctx, zero)
	require.NoError(t, err)
	require.Empty(t, app.ProrationInvoiceID, "a declined charge leaves the guard unarmed")
}
