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

// ChargeProrationLocked (finding 3, store.go) — verifies against a REAL
// Postgres that the row lock is released BEFORE the (simulated) Stripe network
// call, not held across it: a concurrent SyncAppModules/MarkAppDeleted write
// to the SAME row must complete promptly while the charge callback is still
// in flight, never blocking behind it.

func mkProrationCharge(acct, appID uuid.UUID, invID string, periodEnd time.Time) *cycle.ProrationCharge {
	return &cycle.ProrationCharge{
		InvoiceID: invID,
		Cents:     200,
		Invoice: cycle.InvoiceMirror{
			AccountID: acct, StripeInvoiceID: invID, Status: "open",
			AmountDueCents: 200, Currency: "usd",
			PeriodStart: periodEnd.AddDate(0, 0, -3), PeriodEnd: periodEnd,
		},
		Snapshot: cycle.AppBaseSnapshot{
			AppID: appID, PeriodStart: periodEnd.AddDate(0, -1, 0), PeriodEnd: periodEnd,
			ModuleCount: 0, BaseMicros: 2_000_000,
		},
	}
}

func TestChargeProrationLocked_Integration_LockNotHeldAcrossStripeCall(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	appID := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, appID, acct, 0, mustTime(t, "2026-07-01T08:00:00Z"), ""))

	insideCallback := make(chan struct{})
	release := make(chan struct{})
	chargeDone := make(chan error, 1)

	go func() {
		_, _, err := store.ChargeProrationLocked(ctx, appID, func(locked cycle.AppMirror) (*cycle.ProrationCharge, error) {
			close(insideCallback) // signal: the phase-1 lock has already been read + released
			<-release             // simulate a slow Stripe HTTP call
			return mkProrationCharge(acct, appID, "in_slow", mustTime(t, "2026-07-04T00:00:00Z")), nil
		})
		chargeDone <- err
	}()

	select {
	case <-insideCallback:
	case <-time.After(5 * time.Second):
		t.Fatal("charge callback never started")
	}

	// A concurrent write to the SAME row must complete promptly — it must NOT
	// be blocked behind the "Stripe call" the callback is simulating. Under the
	// pre-fix code (the lock held across the callback via one long-lived FOR
	// UPDATE transaction) this write would hang until `release` is closed.
	writeDone := make(chan error, 1)
	go func() { writeDone <- store.SetAppModuleCount(ctx, appID, 42) }()

	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("SetAppModuleCount blocked behind the in-flight charge callback — the row lock is still held across it")
	}

	close(release)
	require.NoError(t, <-chargeDone)

	app, _, err := store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.Equal(t, 42, app.ModuleCount, "the concurrent write landed")
	require.Equal(t, "in_slow", app.ProrationInvoiceID, "the charge still committed after release")
}

func TestChargeProrationLocked_Integration_ConcurrentDeleteDoesNotBlockOnLock(t *testing.T) {
	// Same shape as above, using MarkAppDeleted (the specific writer finding 3
	// calls out — SyncAppModules' soft-delete path) as the concurrent write.
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	appID := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, appID, acct, 0, mustTime(t, "2026-07-01T08:00:00Z"), ""))

	insideCallback := make(chan struct{})
	release := make(chan struct{})
	chargeDone := make(chan error, 1)

	go func() {
		_, _, err := store.ChargeProrationLocked(ctx, appID, func(locked cycle.AppMirror) (*cycle.ProrationCharge, error) {
			close(insideCallback)
			<-release
			return mkProrationCharge(acct, appID, "in_slow_del", mustTime(t, "2026-07-04T00:00:00Z")), nil
		})
		chargeDone <- err
	}()

	select {
	case <-insideCallback:
	case <-time.After(5 * time.Second):
		t.Fatal("charge callback never started")
	}

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- store.MarkAppDeleted(ctx, appID) }()

	select {
	case err := <-deleteDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("MarkAppDeleted blocked behind the in-flight charge callback — the row lock is still held across it")
	}

	close(release)
	require.NoError(t, <-chargeDone)

	// Judgment call (documented in store.go / proration.go and the PR
	// description): once the Stripe charge has already succeeded, a delete that
	// raced in during the (now-released) window does NOT unwind it — D1e
	// already forbids refunds, and the money has already moved. The invoice /
	// snapshot / guard are persisted regardless of the app's deleted_at.
	app, _, err := store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.True(t, app.Deleted, "the delete committed")
	require.Equal(t, "in_slow_del", app.ProrationInvoiceID, "the already-succeeded Stripe charge is still recorded, not silently dropped")
}

// Regression (review 2026-07-06): persistProrationCharge dropped the
// IsLargeAutoCollect field from its UpsertInvoiceParams, so the combined
// creation invoice (scenario 3/5a) was ALWAYS persisted with
// is_large_auto_collect = false in production even when the charge callback
// computed true — the unit suite never caught it because the fake store
// carries the InvoiceMirror struct through verbatim. Assert against the REAL
// pgx persist path.
func TestChargeProrationLocked_Integration_PersistsLargeAutoCollectFlag(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	appID := uuid.New()
	require.NoError(t, store.InsertAppMirror(ctx, appID, acct, 0, mustTime(t, "2026-07-01T08:00:00Z"), ""))

	pc := mkProrationCharge(acct, appID, "in_large_flag", mustTime(t, "2026-07-04T00:00:00Z"))
	pc.Invoice.IsLargeAutoCollect = true

	outcome, invID, err := store.ChargeProrationLocked(ctx, appID, func(cycle.AppMirror) (*cycle.ProrationCharge, error) {
		return pc, nil
	})
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationLockedCharged, outcome)
	require.Equal(t, "in_large_flag", invID)

	var flagged bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT is_large_auto_collect FROM ms_billing.invoices WHERE stripe_invoice_id = $1`,
		"in_large_flag").Scan(&flagged))
	require.True(t, flagged, "the charge callback's IsLargeAutoCollect must survive the pgx persist path")
}
