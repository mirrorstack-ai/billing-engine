//go:build integration

package cycle_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 050 freezes the exact timer identities and Stripe request shape
// owned by one app-creation attempt before its first network call. These tests
// intentionally mutate every live fact the old retry path recomputed
// (removal, FIFO rank, standalone markers) and assert that the durable winner
// remains authoritative until the single terminal transaction commits.

func combinedAttemptShape(appID, accountID uuid.UUID) cycle.CombinedProrationChargeShape {
	periodStart := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)
	return cycle.CombinedProrationChargeShape{
		AccountID:          accountID,
		Currency:           "usd",
		BaseChargeMicros:   10_000_000,
		BaseChargeCents:    1_000,
		ModuleChargeMicros: 1_500_000,
		ModuleChargeCents:  150,
		CoverageStart:      time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC),
		CoverageEnd:        periodEnd,
		BaseDescription:    "MirrorStack app base fee (prorated) — integration",
		ModuleDescription:  "MirrorStack module overage (prorated) — integration",
		Snapshot: cycle.AppBaseSnapshot{
			AppID:       appID,
			PeriodStart: periodStart,
			PeriodEnd:   periodEnd,
			ModuleCount: 7,
			BaseMicros:  10_000_000,
		},
	}
}

func seedCombinedAttemptApp(
	t *testing.T,
	pool *pgxpool.Pool,
	store cycle.Store,
	moduleCount int,
) (uuid.UUID, uuid.UUID, time.Time) {
	t.Helper()
	ctx := context.Background()
	accountID := seedAccount(t, pool)
	appID := uuid.New()
	createdAt := time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC)
	require.NoError(t, store.InsertAppMirror(
		ctx, appID, accountID, uuid.Nil, moduleCount, createdAt, "combined-attempt integration",
	))
	if moduleCount > 0 {
		require.NoError(t, store.InsertModuleOverageTimers(
			ctx,
			accountID,
			appID,
			createdAt,
			createdAt.AddDate(0, 0, usage.GraceDays),
			moduleCount,
		))
	}
	return accountID, appID, createdAt
}

func timerState(
	t *testing.T,
	pool *pgxpool.Pool,
	timerID uuid.UUID,
) (removed bool, resolved bool, attempted bool, invoiceID, itemID *string) {
	t.Helper()
	var removedAt, attemptedAt *time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT removed_at, grace_resolved, charge_attempted_at,
		       grace_invoice_id, grace_invoice_item_id
		FROM ms_billing.app_module_overage_timers
		WHERE id = $1`,
		timerID.String(),
	).Scan(&removedAt, &resolved, &attemptedAt, &invoiceID, &itemID))
	return removedAt != nil, resolved, attemptedAt != nil, invoiceID, itemID
}

func TestCombinedProrationAttempt_Integration_MigrationPreflightRejectsAttemptedSkipWithoutInvoice(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, migrationSQL(t, "050_combined_proration_attempts.down.sql"))
	require.NoError(t, err)

	accountID := seedAccount(t, pool)
	appID := uuid.New()
	createdAt := time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC)
	attemptedAt := createdAt.AddDate(0, 0, usage.GraceDays)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.apps (
		    app_id, account_id, module_count, created_module_count, created_at,
		    name, proration_attempted_at, proration_skipped_at
		) VALUES ($1, $2, 0, 0, $3, 'legacy attempted+skip', $4, $4)`,
		appID.String(), accountID.String(), createdAt, attemptedAt,
	)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, migrationSQL(t, "050_combined_proration_attempts.up.sql"))
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"requires all legacy attempted creation prorations to be terminal")
	var tableName *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT to_regclass('ms_billing.app_combined_proration_attempts')::text`,
	).Scan(&tableName))
	require.Nil(t, tableName, "the failed migration preflight rolls its new schema back")

	// A skip marker alone cannot prove that the pre-050 Stripe attempt moved no
	// money. Once ops reconciles a genuine invoice terminal, cutover may proceed.
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.apps
		SET proration_invoice_id = 'in_legacy_reconciled'
		WHERE app_id = $1`,
		appID.String(),
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, migrationSQL(t, "050_combined_proration_attempts.up.sql"))
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT to_regclass('ms_billing.app_combined_proration_attempts')::text`,
	).Scan(&tableName))
	require.NotNil(t, tableName)
}

func TestCombinedProrationAttempt_Integration_FrozenSetSurvivesRankAndRemoval(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	accountID, appID, createdAt := seedCombinedAttemptApp(t, pool, store, 7)
	shape := combinedAttemptShape(appID, accountID)

	attemptedAt := createdAt.AddDate(0, 0, usage.GraceDays)
	attempt, freezeOutcome, err := store.FreezeCombinedProrationAttempt(ctx, appID, attemptedAt, shape, false)
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailClaimed, freezeOutcome)
	require.Len(t, attempt.TimerIDs, 2, "seven co-created timers freeze exactly the two FIFO-over identities")
	require.True(t, attempt.AttemptedAt.Equal(attemptedAt))

	// A retry is first-write-wins: neither a later instant nor newly-computed
	// request fields can rewrite the durable Stripe request.
	changed := shape
	changed.BaseChargeMicros = 99_000_000
	changed.BaseChargeCents = 9_900
	winner, freezeOutcome, err := store.FreezeCombinedProrationAttempt(ctx, appID, attemptedAt.Add(time.Hour), changed, false)
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailClaimed, freezeOutcome)
	require.Equal(t, attempt, winner)

	var appAttemptedAt *time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT proration_attempted_at FROM ms_billing.apps WHERE app_id = $1`,
		appID.String(),
	).Scan(&appAttemptedAt))
	require.NotNil(t, appAttemptedAt, "the header, children, and legacy recovery marker commit together")

	unresolved, err := store.UnresolvedCombinedProrationAttempts(ctx, accountID)
	require.NoError(t, err)
	require.Equal(t, []cycle.UnresolvedCombinedProrationAmount{{
		AppID:              appID,
		BaseChargeMicros:   10_000_000,
		ModuleChargeMicros: 1_500_000,
		TimerCount:         2,
		TotalMicros:        13_000_000,
	}}, unresolved)

	owned := make(map[uuid.UUID]struct{}, len(attempt.TimerIDs))
	for _, timerID := range attempt.TimerIDs {
		owned[timerID] = struct{}{}
	}
	allTimers, err := store.CoCreatedOverModuleTimers(ctx, accountID, appID, createdAt, 0)
	require.NoError(t, err)
	require.Len(t, allTimers, 7)
	var early []uuid.UUID
	for _, timerID := range allTimers {
		if _, frozen := owned[timerID]; !frozen {
			early = append(early, timerID)
		}
	}
	require.Len(t, early, 5)

	// Remove two earlier FIFO timers directly. Both frozen timers improve from
	// ranks 5/6 (over) to ranks 3/4 (included), but ownership does not change.
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.app_module_overage_timers
		SET removed_at = $2
		WHERE id = ANY($1::uuid[])`,
		[]string{early[0].String(), early[1].String()},
		attemptedAt.Add(time.Minute),
	)
	require.NoError(t, err)
	for _, timerID := range attempt.TimerIDs {
		rank, err := store.LiveModuleTimerRankBefore(ctx, accountID, timerID, createdAt)
		require.NoError(t, err)
		require.Less(t, rank, usage.IncludedModules, "the fixture must actually exercise over→included rank drift")
		owner, err := store.TimerHasUnresolvedCombinedProrationOwner(ctx, timerID)
		require.NoError(t, err)
		require.True(t, owner)
		pending, err := store.ModuleTimerStillPending(ctx, timerID)
		require.NoError(t, err)
		require.False(t, pending, "standalone Leg 1 must defer before consulting the improved rank")
		require.NoError(t, store.MarkModuleTimerIncluded(ctx, timerID))
		_, resolved, _, _, _ := timerState(t, pool, timerID)
		require.False(t, resolved, "an unresolved frozen child cannot be terminally included")
	}

	// A standalone Stripe marker and wallet draw also lose to the durable owner.
	selected := attempt.TimerIDs[0]
	stamped, err := store.MarkModuleTimerChargeAttempted(ctx, selected, attemptedAt.Add(2*time.Minute))
	require.NoError(t, err)
	require.Zero(t, stamped)
	outcome, _, err := store.DrawModuleOverageFromWallet(ctx, selected, cycle.ModuleOverageWalletCharge{
		Ref:          "wallet:must-not-win",
		AmountMicros: 1_500_000,
		ChargedAt:    attemptedAt.Add(3 * time.Minute),
	})
	require.NoError(t, err)
	require.Equal(t, cycle.ModuleOverageWalletLockedStale, outcome)

	// Soft removal of a frozen child is likewise only a live-state change. The
	// child row and exact raw projection remain until atomic terminal resolve.
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.app_module_overage_timers
		SET removed_at = $2
		WHERE id = $1`,
		selected.String(),
		attemptedAt.Add(4*time.Minute),
	)
	require.NoError(t, err)
	removed, resolved, attempted, _, _ := timerState(t, pool, selected)
	require.True(t, removed)
	require.False(t, resolved)
	require.False(t, attempted)
	owner, err := store.TimerHasUnresolvedCombinedProrationOwner(ctx, selected)
	require.NoError(t, err)
	require.True(t, owner)
	unresolved, err = store.UnresolvedCombinedProrationAttempts(ctx, accountID)
	require.NoError(t, err)
	require.Len(t, unresolved, 1)
	require.EqualValues(t, 13_000_000, unresolved[0].TotalMicros)

	// The ownership row deliberately prevents hard deletion of a timer whose
	// invoice may already contain money-moving Stripe resources.
	_, err = pool.Exec(ctx,
		`DELETE FROM ms_billing.app_module_overage_timers WHERE id = $1`,
		selected.String(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "app_combined_proration_attempt_timers_timer_id_fkey")
}

func TestCombinedProrationAttempt_Integration_KnownEmptyAndLegacyUnknown(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	accountID, emptyApp, createdAt := seedCombinedAttemptApp(t, pool, store, 0)
	shape := combinedAttemptShape(emptyApp, accountID)
	shape.Snapshot.ModuleCount = 0
	attempt, outcome, err := store.FreezeCombinedProrationAttempt(
		ctx, emptyApp, createdAt.AddDate(0, 0, usage.GraceDays), shape, false,
	)
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailClaimed, outcome)
	require.Empty(t, attempt.TimerIDs, "header presence plus zero children is an intentional empty set")
	unresolved, err := store.UnresolvedCombinedProrationAttempts(ctx, accountID)
	require.NoError(t, err)
	require.EqualValues(t, 10_000_000, unresolved[0].TotalMicros,
		"the per-timer frozen amount is not counted when the exact set is empty")

	legacyApp := uuid.New()
	// Simulate a marker that predates migration 050 (or violates the deployment
	// drain after its preflight). Expand-only migration guards deliberately leave
	// no-header old-worker behavior untouched, while new code treats this as an
	// explicit fail-closed ops state.
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.apps (
		    app_id, account_id, module_count, created_module_count, created_at,
		    name, proration_attempted_at
		) VALUES ($1, $2, 0, 0, $3, 'legacy marker', $4)`,
		legacyApp.String(),
		accountID.String(),
		createdAt,
		createdAt.AddDate(0, 0, usage.GraceDays),
	)
	require.NoError(t, err)
	_, found, err := store.CombinedProrationAttempt(ctx, legacyApp)
	require.NoError(t, err)
	require.False(t, found)
	legacyShape := combinedAttemptShape(legacyApp, accountID)
	legacyShape.Snapshot.ModuleCount = 0
	_, _, err = store.FreezeCombinedProrationAttempt(
		ctx, legacyApp, createdAt.AddDate(0, 0, usage.GraceDays), legacyShape, false,
	)
	require.ErrorIs(t, err, cycle.ErrCombinedProrationAttemptUnknown,
		"a legacy marker without a header must never be reconstructed from current FIFO state")
}

func TestCombinedProrationAttempt_Integration_AccountLockedRailClaim(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	accountID, appID, createdAt := seedCombinedAttemptApp(t, pool, store, 7)
	shape := combinedAttemptShape(appID, accountID)
	attemptedAt := createdAt.AddDate(0, 0, usage.GraceDays)

	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`,
		accountID.String(),
	)
	require.NoError(t, err)
	attempt, outcome, err := store.FreezeCombinedProrationAttempt(
		ctx, appID, attemptedAt, shape, true,
	)
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailWalletRequired, outcome)
	require.Equal(t, cycle.CombinedProrationAttempt{}, attempt)
	_, found, err := store.CombinedProrationAttempt(ctx, appID)
	require.NoError(t, err)
	require.False(t, found)
	var appAttemptedAt *time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT proration_attempted_at FROM ms_billing.apps WHERE app_id = $1`,
		appID.String(),
	).Scan(&appAttemptedAt))
	require.Nil(t, appAttemptedAt, "wallet-required returns before every header/app/timer marker write")

	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.accounts SET billing_mode = 'standard' WHERE id = $1`,
		accountID.String(),
	)
	require.NoError(t, err)
	frozen, outcome, err := store.FreezeCombinedProrationAttempt(
		ctx, appID, attemptedAt.Add(time.Minute), shape, true,
	)
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailClaimed, outcome)
	require.Len(t, frozen.TimerIDs, 2)

	// Once the standard rail has a durable header, recovery remains
	// authoritative after a later credits transition: it may be reconciling
	// money that already moved and must never switch rails.
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`,
		accountID.String(),
	)
	require.NoError(t, err)
	recovered, outcome, err := store.FreezeCombinedProrationAttempt(
		ctx, appID, attemptedAt.Add(2*time.Minute), shape, true,
	)
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailClaimed, outcome)
	require.Equal(t, frozen, recovered)
}

func TestCombinedProrationAttempt_Integration_ModeTransitionWinsBeforeFreshClaim(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	accountID, appID, createdAt := seedCombinedAttemptApp(t, pool, store, 7)
	shape := combinedAttemptShape(appID, accountID)

	// Hold the same account row lock the PaaS/standard→credits transition uses.
	// Freeze may resolve the app id, but cannot claim Stripe until this durable
	// mode decision commits.
	transition, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = transition.Rollback(ctx) }()
	_, err = transition.Exec(ctx,
		`UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`,
		accountID.String(),
	)
	require.NoError(t, err)

	type result struct {
		attempt cycle.CombinedProrationAttempt
		outcome cycle.StripeRailClaimOutcome
		err     error
	}
	done := make(chan result, 1)
	go func() {
		attempt, outcome, freezeErr := store.FreezeCombinedProrationAttempt(
			ctx,
			appID,
			createdAt.AddDate(0, 0, usage.GraceDays),
			shape,
			true,
		)
		done <- result{attempt: attempt, outcome: outcome, err: freezeErr}
	}()
	select {
	case got := <-done:
		t.Fatalf("fresh Stripe claim escaped the account transition lock: %+v", got)
	case <-time.After(200 * time.Millisecond):
		// Expected: LockWalletAccount is waiting on transition.
	}
	require.NoError(t, transition.Commit(ctx))

	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.Equal(t, cycle.StripeRailWalletRequired, got.outcome)
		require.Equal(t, cycle.CombinedProrationAttempt{}, got.attempt)
	case <-time.After(5 * time.Second):
		t.Fatal("combined claim did not resume after the mode transition committed")
	}
	_, found, err := store.CombinedProrationAttempt(ctx, appID)
	require.NoError(t, err)
	require.False(t, found)
	var attemptedAt *time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT proration_attempted_at FROM ms_billing.apps WHERE app_id = $1`,
		appID.String(),
	).Scan(&attemptedAt))
	require.Nil(t, attemptedAt)
}

func TestCombinedProrationAttempt_Integration_MixedVersionTerminalWritesAreRejected(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	accountID, appID, createdAt := seedCombinedAttemptApp(t, pool, store, 7)
	shape := combinedAttemptShape(appID, accountID)
	attempt, outcome, err := store.FreezeCombinedProrationAttempt(
		ctx, appID, createdAt.AddDate(0, 0, usage.GraceDays), shape, false,
	)
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailClaimed, outcome)
	require.Len(t, attempt.TimerIDs, 2)

	// Expand-only compatibility: before the atomic billing-cycle alias cutover,
	// an old worker operating on an app with no migration-050 header retains its
	// old marker behavior. The deployment preflight/drain guarantees no such
	// unresolved marker exists when new code starts.
	freshApp := uuid.New()
	require.NoError(t, store.InsertAppMirror(
		ctx, freshApp, accountID, uuid.Nil, 0, createdAt, "old-worker marker probe",
	))
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.apps
		SET proration_attempted_at = $2
		WHERE app_id = $1`,
		freshApp.String(),
		createdAt.AddDate(0, 0, usage.GraceDays),
	)
	require.NoError(t, err)
	_, found, err := store.CombinedProrationAttempt(ctx, freshApp)
	require.NoError(t, err)
	require.False(t, found)
	var freshMarker *time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT proration_attempted_at FROM ms_billing.apps WHERE app_id = $1`,
		freshApp.String(),
	).Scan(&freshMarker))
	require.NotNil(t, freshMarker)

	// These are the writes an already-running pre-050 worker would issue after
	// seeing only proration_attempted_at. Both fail while the exact header is
	// unresolved, so old code cannot bury it with a recomputed invoice/set.
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.apps
		SET proration_invoice_id = 'in_legacy_worker'
		WHERE app_id = $1`,
		appID.String(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unresolved combined proration attempt")

	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.app_module_overage_timers
		SET grace_resolved = true,
		    grace_charged_at = $2,
		    grace_invoice_id = 'in_legacy_worker',
		    grace_invoice_item_id = 'ii_legacy_worker'
		WHERE id = $1`,
		attempt.TimerIDs[0].String(),
		createdAt.AddDate(0, 0, usage.GraceDays),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unresolved combined proration owner")

	stillFrozen, found, err := store.CombinedProrationAttempt(ctx, appID)
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, stillFrozen.ResolvedInvoiceID)
	var appInvoice *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT proration_invoice_id FROM ms_billing.apps WHERE app_id = $1`,
		appID.String(),
	).Scan(&appInvoice))
	require.Nil(t, appInvoice)
	for _, timerID := range attempt.TimerIDs {
		_, resolved, attempted, invoiceID, itemID := timerState(t, pool, timerID)
		require.False(t, resolved)
		require.False(t, attempted)
		require.Nil(t, invoiceID)
		require.Nil(t, itemID)
	}
}

func TestCombinedProrationAttempt_Integration_SelectionRecheckLosesToStandaloneMarker(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	accountID, appID, createdAt := seedCombinedAttemptApp(t, pool, store, 7)
	shape := combinedAttemptShape(appID, accountID)

	over, err := store.CoCreatedOverModuleTimers(
		ctx, accountID, appID, createdAt, usage.IncludedModules,
	)
	require.NoError(t, err)
	require.Len(t, over, 2)

	// Hold an uncommitted standalone marker on one selected row. Freeze's first
	// READ COMMITTED selection cannot see it, then blocks on that row's FOR
	// UPDATE recheck. Once this transaction wins, freeze must roll everything
	// back rather than storing a stale two-timer set.
	markerTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = markerTx.Rollback(ctx) }()
	_, err = markerTx.Exec(ctx, `
		UPDATE ms_billing.app_module_overage_timers
		SET charge_attempted_at = $2
		WHERE id = $1`,
		over[0].String(),
		createdAt.AddDate(0, 0, usage.GraceDays),
	)
	require.NoError(t, err)

	type freezeResult struct {
		attempt cycle.CombinedProrationAttempt
		err     error
	}
	started := make(chan struct{})
	done := make(chan freezeResult, 1)
	go func() {
		close(started)
		attempt, _, freezeErr := store.FreezeCombinedProrationAttempt(
			ctx,
			appID,
			createdAt.AddDate(0, 0, usage.GraceDays),
			shape,
			false,
		)
		done <- freezeResult{attempt: attempt, err: freezeErr}
	}()
	<-started
	select {
	case got := <-done:
		t.Fatalf("freeze returned before the selected-row lock was released: %+v", got)
	case <-time.After(200 * time.Millisecond):
		// Expected: row-level recheck is waiting on markerTx.
	}
	require.NoError(t, markerTx.Commit(ctx))

	select {
	case got := <-done:
		require.ErrorIs(t, got.err, cycle.ErrCombinedProrationSelectionChanged)
	case <-time.After(5 * time.Second):
		t.Fatal("freeze did not resume after the standalone marker committed")
	}
	_, found, err := store.CombinedProrationAttempt(ctx, appID)
	require.NoError(t, err)
	require.False(t, found, "a lost selection race leaves no partial header")
	var attemptedAt *time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT proration_attempted_at FROM ms_billing.apps WHERE app_id = $1`,
		appID.String(),
	).Scan(&attemptedAt))
	require.Nil(t, attemptedAt, "the app marker rolls back with the rejected freeze")

	// A clean retry excludes the standalone-owned timer and freezes the one
	// remaining coherent identity.
	attempt, outcome, err := store.FreezeCombinedProrationAttempt(
		ctx,
		appID,
		createdAt.AddDate(0, 0, usage.GraceDays).Add(time.Minute),
		shape,
		false,
	)
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailClaimed, outcome)
	require.Len(t, attempt.TimerIDs, 1)
	require.NotEqual(t, over[0], attempt.TimerIDs[0])
}

func TestCombinedProrationAttempt_Integration_TerminalPersistenceIsAtomic(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	accountID, appID, createdAt := seedCombinedAttemptApp(t, pool, store, 7)
	shape := combinedAttemptShape(appID, accountID)
	attempt, freezeOutcome, err := store.FreezeCombinedProrationAttempt(
		ctx, appID, createdAt.AddDate(0, 0, usage.GraceDays), shape, false,
	)
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailClaimed, freezeOutcome)
	require.Len(t, attempt.TimerIDs, 2)

	resolvedAt := createdAt.AddDate(0, 0, usage.GraceDays).Add(time.Minute)
	pc := &cycle.ProrationCharge{
		InvoiceID:  "in_combined_terminal",
		Cents:      shape.BaseChargeCents,
		ResolvedAt: resolvedAt,
		Invoice: cycle.InvoiceMirror{
			AccountID:       accountID,
			StripeInvoiceID: "in_combined_terminal",
			Status:          "open",
			AmountDueCents:  shape.BaseChargeCents + shape.ModuleChargeCents*int64(len(attempt.TimerIDs)),
			Currency:        shape.Currency,
			PeriodStart:     shape.CoverageStart,
			PeriodEnd:       shape.CoverageEnd,
		},
		Snapshot: shape.Snapshot,
	}
	for i, timerID := range attempt.TimerIDs {
		pc.TimerCharges = append(pc.TimerCharges, cycle.ModuleTimerCharge{
			TimerID:       timerID,
			ChargedAt:     resolvedAt,
			InvoiceID:     pc.InvoiceID,
			InvoiceItemID: fmt.Sprintf("ii_combined_%d", i),
		})
	}

	// An incomplete terminal payload must roll back every mirror/guard write.
	incomplete := *pc
	incomplete.TimerCharges = append([]cycle.ModuleTimerCharge(nil), pc.TimerCharges[:1]...)
	outcome, _, err := store.ChargeProrationLocked(ctx, appID, func(cycle.AppMirror) (*cycle.ProrationCharge, error) {
		return &incomplete, nil
	})
	require.Error(t, err)
	require.Zero(t, outcome)
	attemptAfterError, found, readErr := store.CombinedProrationAttempt(ctx, appID)
	require.NoError(t, readErr)
	require.True(t, found)
	require.Empty(t, attemptAfterError.ResolvedInvoiceID)
	var appInvoice *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT proration_invoice_id FROM ms_billing.apps WHERE app_id = $1`,
		appID.String(),
	).Scan(&appInvoice))
	require.Nil(t, appInvoice)
	for _, timerID := range attempt.TimerIDs {
		_, resolved, _, invoiceID, itemID := timerState(t, pool, timerID)
		require.False(t, resolved)
		require.Nil(t, invoiceID)
		require.Nil(t, itemID)
	}

	outcome, invoiceID, err := store.ChargeProrationLocked(ctx, appID, func(cycle.AppMirror) (*cycle.ProrationCharge, error) {
		return pc, nil
	})
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationLockedCharged, outcome)
	require.Equal(t, pc.InvoiceID, invoiceID)

	resolvedAttempt, found, err := store.CombinedProrationAttempt(ctx, appID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, pc.InvoiceID, resolvedAttempt.ResolvedInvoiceID)
	require.True(t, resolvedAttempt.ResolvedAt.Equal(resolvedAt))
	unresolved, err := store.UnresolvedCombinedProrationAttempts(ctx, accountID)
	require.NoError(t, err)
	require.Empty(t, unresolved)
	for i, timerID := range attempt.TimerIDs {
		owner, err := store.TimerHasUnresolvedCombinedProrationOwner(ctx, timerID)
		require.NoError(t, err)
		require.False(t, owner)
		_, resolved, attempted, timerInvoice, itemID := timerState(t, pool, timerID)
		require.True(t, resolved)
		require.False(t, attempted, "combined ownership never needs a standalone attempt marker")
		require.NotNil(t, timerInvoice)
		require.Equal(t, pc.InvoiceID, *timerInvoice)
		require.NotNil(t, itemID)
		require.Equal(t, fmt.Sprintf("ii_combined_%d", i), *itemID)
	}

	callbackCalled := false
	outcome, invoiceID, err = store.ChargeProrationLocked(ctx, appID, func(cycle.AppMirror) (*cycle.ProrationCharge, error) {
		callbackCalled = true
		return pc, nil
	})
	require.NoError(t, err)
	require.False(t, callbackCalled)
	require.Equal(t, cycle.ProrationLockedAlreadyCharged, outcome)
	require.Equal(t, pc.InvoiceID, invoiceID)
}

func TestCombinedProrationAttempt_Integration_CorruptChildCountFailsClosed(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	accountID, appID, createdAt := seedCombinedAttemptApp(t, pool, store, 7)
	shape := combinedAttemptShape(appID, accountID)
	attempt, outcome, err := store.FreezeCombinedProrationAttempt(
		ctx, appID, createdAt.AddDate(0, 0, usage.GraceDays), shape, false,
	)
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailClaimed, outcome)
	require.Len(t, attempt.TimerIDs, 2)

	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.app_combined_proration_attempts
		SET timer_count = timer_count + 1
		WHERE app_id = $1`,
		appID.String(),
	)
	require.NoError(t, err)
	_, _, err = store.CombinedProrationAttempt(ctx, appID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "declares 3 timers but has 2 child rows")
	_, err = store.UnresolvedCombinedProrationAttempts(ctx, accountID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "declares 3 timers but projection found 2")

	// Keep errors.Is useful when callers wrap the explicit legacy sentinel.
	require.False(t, errors.Is(err, cycle.ErrCombinedProrationAttemptUnknown))
}
