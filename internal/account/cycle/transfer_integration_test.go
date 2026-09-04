//go:build integration

package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// TransferApp against a real Postgres: the re-key, the money refusal, the
// idempotency record and the split guard. Gated by the `integration` tag.
//
// Every instant is derived from ONE pinned `now`. Nothing here reads the
// calendar — a fixture that does passes on luck, and this repo has already
// lost a day to that exact failure.

type transferFixture struct {
	pool    *pgxpool.Pool
	oldAcct uuid.UUID
	newAcct uuid.UUID
	// The owner_user_id each seeded account was created with. A test that
	// means "transfer to the SEEDED account" passes newOwner as OwnerUserID.
	//
	// 🔴 uuid.New() as OwnerUserID is NOT a shorthand for that. The service
	// resolves the target by owner, and an owner nobody seeded gets a FRESH
	// account from EnsureUserAccount — activated_at NULL, anchor day 1 — so
	// newAcct is never the target and every assertion about the target's
	// window or anchor is made against an account the test did not describe.
	// That is how the max() test below passed on nothing at f5c74ad7.
	oldOwner uuid.UUID
	newOwner uuid.UUID
	appID    uuid.UUID
	moduleID uuid.UUID
	now      time.Time
}

// seedTransferFixture builds a SETTLED app: creation proration resolved, no
// live domain awaiting its activation charge, no module timer inside grace. It
// is deliberately the state in which a transfer is ALLOWED, so each refusal
// test can introduce exactly one unsettled thing and nothing else differs.
func seedTransferFixture(t *testing.T) *transferFixture {
	t.Helper()
	// Equal anchors: the ordinary case. seedTransferFixtureAnchored varies
	// them, which is the ONLY way the max() term in the move window becomes
	// reachable — with equal anchors both branches return the same instant, so
	// a mutant that drops the max() passes every other test in this file.
	anchor := mustTime(t, "2026-05-04T00:00:00Z")
	return seedTransferFixtureAnchored(t, anchor, anchor)
}

func seedTransferFixtureAnchored(t *testing.T, oldActivated, newActivated time.Time) *transferFixture {
	t.Helper()
	ctx := context.Background()
	pool := testutil.NewTestDB(t)

	f := &transferFixture{
		pool:     pool,
		oldAcct:  uuid.New(),
		newAcct:  uuid.New(),
		oldOwner: uuid.New(),
		newOwner: uuid.New(),
		appID:    uuid.New(),
		moduleID: uuid.New(),
		now:      mustTime(t, "2026-08-15T12:00:00Z"),
	}
	for _, a := range []struct {
		id        uuid.UUID
		owner     uuid.UUID
		activated time.Time
	}{{f.oldAcct, f.oldOwner, oldActivated}, {f.newAcct, f.newOwner, newActivated}} {
		_, err := pool.Exec(ctx, `
			INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, activated_at)
			VALUES ($1, 'user', $2, $3)`, a.id.String(), a.owner.String(), a.activated)
		require.NoError(t, err)
	}
	// proration_invoice_id NOT NULL ⇒ the creation charge is settled, so the
	// one-time-charge guard permits the transfer.
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.apps (
		    app_id, account_id, module_count, created_module_count, created_at,
		    proration_invoice_id
		) VALUES ($1, $2, 0, 0, $3, 'in_settled')`,
		f.appID.String(), f.oldAcct.String(), mustTime(t, "2026-06-01T00:00:00Z"))
	require.NoError(t, err)
	return f
}

func transferSvc(t *testing.T, f *transferFixture) *cycle.Service {
	t.Helper()
	return cycle.NewService(cycle.NewStore(f.pool), nil).
		WithNow(func() time.Time { return f.now })
}

// requireSameInstant compares instants, not time.Time values: a timestamptz
// read back through pgx carries a different Location than one built with
// time.Date, and require.Equal would call two equal instants unequal.
func requireSameInstant(t *testing.T, want, got time.Time, what string) {
	t.Helper()
	require.True(t, got.Equal(want), "%s = %s, want %s", what, got.UTC(), want.UTC())
}

func (f *transferFixture) rosterAccount(t *testing.T) string {
	t.Helper()
	var got string
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT account_id::text FROM ms_billing.apps WHERE app_id = $1`, f.appID.String()).Scan(&got))
	return got
}

// keep and move differ ONLY in what happens to usage. Running both against the
// same fixture is the vacuity control: if move also moved zero, "keep moves
// nothing" would prove nothing at all.
func TestTransferAppKeepMovesNoUsageAndMoveMovesIt(t *testing.T) {
	for _, tc := range []struct {
		mode      string
		wantMoved int64
	}{
		{cycle.TransferModeKeep, 0},
		{cycle.TransferModeMove, 2},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			ctx := context.Background()
			f := seedTransferFixture(t)
			seedMetricDef(t, f.pool, f.moduleID, "orders.placed", "count", 1_000)
			// Both inside the open period and before the transfer instant.
			for _, at := range []string{"2026-08-05T10:00:00Z", "2026-08-10T10:00:00Z"} {
				_, err := f.pool.Exec(ctx, `
					INSERT INTO ms_billing.usage_events
					    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at, occurred_at)
					VALUES ($1, $2, $3, $4, 'orders.placed', 'count', 1, $5, $5)`,
					uuid.NewString(), f.oldAcct.String(), f.appID.String(), f.moduleID.String(), mustTime(t, at))
				require.NoError(t, err)
			}

			resp, err := transferSvc(t, f).TransferApp(ctx, cycle.TransferAppRequest{
				AppID:       f.appID,
				OwnerUserID: f.newOwner,
				Mode:        tc.mode,
				RequestID:   uuid.New(),
			})
			require.NoError(t, err)
			require.Equal(t, tc.wantMoved, resp.MovedEventCount)
			require.Equal(t, f.newAcct, resp.AccountID, "the transfer did not land on the seeded target account")

			// The re-key is UNCONDITIONAL — it happens in both modes. Being
			// late is the expensive direction: an app still on the old roster
			// at the next boundary makes that account prepay another whole
			// period for an app it no longer owns.
			require.Equal(t, resp.AccountID.String(), f.rosterAccount(t))

			var onOld int
			require.NoError(t, f.pool.QueryRow(ctx,
				`SELECT count(*) FROM ms_billing.usage_events WHERE app_id = $1 AND account_id = $2`,
				f.appID.String(), f.oldAcct.String()).Scan(&onOld))
			require.Equal(t, 2-int(tc.wantMoved), onOld)
		})
	}
}

// 🔴 THE MONEY REFUSAL. Each leg charges whoever the row points at when the
// sweep runs, so re-keying with one unresolved bills the new account for a
// window it did not own. Mutation: drop that leg from
// AppHasUnresolvedOneTimeCharge and its case here starts succeeding.
func TestTransferAppRefusesWhileAOneTimeChargeIsPending(t *testing.T) {
	cases := []struct {
		name string
		seed func(*testing.T, *transferFixture)
	}{
		{"creation proration owed", func(t *testing.T, f *transferFixture) {
			_, err := f.pool.Exec(context.Background(),
				`UPDATE ms_billing.apps SET proration_invoice_id = NULL WHERE app_id = $1`, f.appID.String())
			require.NoError(t, err)
		}},
		{"domain activation unresolved", func(t *testing.T, f *transferFixture) {
			_, err := f.pool.Exec(context.Background(), `
				INSERT INTO ms_billing.app_custom_domains
				    (id, account_id, app_id, hostname, activated_at, charge_resolved)
				VALUES ($1, $2, $3, 'example.test', $4, false)`,
				uuid.NewString(), f.oldAcct.String(), f.appID.String(), mustTime(t, "2026-08-01T00:00:00Z"))
			require.NoError(t, err)
		}},
		{"module grace unresolved", func(t *testing.T, f *transferFixture) {
			// Columns per migration 033: there is no module_id (one row per
			// install EVENT, not per module identity) and grace_expires_at is
			// NOT NULL. Same shape as org_deletion_integration_test.
			_, err := f.pool.Exec(context.Background(), `
				INSERT INTO ms_billing.app_module_overage_timers
				    (id, account_id, app_id, installed_at, grace_expires_at, grace_resolved)
				VALUES ($1, $2, $3, $4::timestamptz,
				        $4::timestamptz + interval '3 days', false)`,
				uuid.NewString(), f.oldAcct.String(), f.appID.String(),
				mustTime(t, "2026-08-01T00:00:00Z"))
			require.NoError(t, err)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := seedTransferFixture(t)
			tc.seed(t, f)
			before := f.rosterAccount(t)

			_, err := transferSvc(t, f).TransferApp(context.Background(), cycle.TransferAppRequest{
				AppID:       f.appID,
				OwnerUserID: f.newOwner,
				Mode:        cycle.TransferModeMove,
				RequestID:   uuid.New(),
			})

			require.Error(t, err)
			require.Contains(t, err.Error(), "app_transfer_charges_pending")
			// Nothing may have moved: a refusal that half-applied would be
			// worse than the bug it prevents.
			require.Equal(t, before, f.rosterAccount(t), "roster changed despite the refusal")
		})
	}
}

// The control for the three refusals above: with everything settled the SAME
// call succeeds. Without it, a guard that refused unconditionally would pass
// all three refusal tests.
func TestTransferAppAllowsASettledApp(t *testing.T) {
	f := seedTransferFixture(t)

	resp, err := transferSvc(t, f).TransferApp(context.Background(), cycle.TransferAppRequest{
		AppID:       f.appID,
		OwnerUserID: f.newOwner,
		Mode:        cycle.TransferModeKeep,
		RequestID:   uuid.New(),
	})

	require.NoError(t, err)
	require.Equal(t, f.newAcct, resp.AccountID)
	require.Equal(t, resp.AccountID.String(), f.rosterAccount(t))
	// The window is the TARGET's own anchored period, not the default calendar
	// month: the seeded account activated on the 4th, so with now = 08-15 its
	// open period is [08-04, 09-04) and the first boundary that bills this
	// app's recurring to the new account is 09-04. An unanchored (fresh)
	// target would have answered 09-01 here.
	requireSameInstant(t, mustTime(t, "2026-08-04T00:00:00Z"), resp.OpenPeriod.Start, "open_period.start")
	requireSameInstant(t, mustTime(t, "2026-09-04T00:00:00Z"), resp.OpenPeriod.End, "open_period.end")
	requireSameInstant(t, mustTime(t, "2026-09-04T00:00:00Z"), resp.RecurringFrom,
		"recurring_from must be the target account's next anchored boundary")
}

// api-platform fires this post-commit with retry, so a replay must return the
// FIRST result rather than transfer again.
func TestTransferAppIsIdempotentOnRequestID(t *testing.T) {
	ctx := context.Background()
	f := seedTransferFixture(t)
	svc := transferSvc(t, f)
	req := cycle.TransferAppRequest{
		AppID:       f.appID,
		OwnerUserID: f.newOwner,
		Mode:        cycle.TransferModeKeep,
		RequestID:   uuid.New(),
	}

	first, err := svc.TransferApp(ctx, req)
	require.NoError(t, err)
	second, err := svc.TransferApp(ctx, req)
	require.NoError(t, err)

	require.Equal(t, first.AccountID, second.AccountID)
	require.Equal(t, first.MovedEventCount, second.MovedEventCount)

	var events int
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.app_transfer_events WHERE request_id = $1`,
		req.RequestID.String()).Scan(&events))
	require.Equal(t, 1, events, "a replay wrote a second ledger row")

	// Same key, different target ⇒ conflict, never a second transfer. A fresh
	// owner is deliberate here: it resolves to a DIFFERENT account, which is
	// the whole condition being tested.
	req.OwnerUserID = uuid.New()
	_, err = svc.TransferApp(ctx, req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "app_transfer_conflict")
}

// 🔴 THE SPLIT GUARD. Before migration 071 nothing tied apps.account_id to the
// denormalised copies on timers and domains — no FK, no CHECK, no trigger, no
// asserting query — and a split bill renders without complaint. TransferApp is
// the first writer of those columns, so it ships the first guard.
//
// Mutation: drop the constraint triggers from 071 and this insert succeeds.
func TestSplitAppAttributionIsRefused(t *testing.T) {
	ctx := context.Background()
	f := seedTransferFixture(t)

	_, err := f.pool.Exec(ctx, `
		INSERT INTO ms_billing.app_custom_domains
		    (id, account_id, app_id, hostname, activated_at, charge_resolved)
		VALUES ($1, $2, $3, 'split.test', $4, true)`,
		uuid.NewString(), f.newAcct.String(), f.appID.String(), mustTime(t, "2026-08-01T00:00:00Z"))

	require.Error(t, err, "a live domain on a different account than its app's roster was accepted")
	require.Contains(t, err.Error(), "split app billing attribution")
}

// 🔴 THE ROLLUP BUCKETS ON COALESCE(billable_at, recorded_at), NOT occurred_at.
// occurred_at is NULL for every infra.* and platform.* event and for every
// legacy/v1 observation, and `NULL >= $1` is NULL — so a move filtered on
// occurred_at silently moves NONE of them, and the OLD account stays invoiced
// for the app's egress, AI and GPU across the whole open period.
//
// Mutation: put occurred_at back in MoveAppOpenUsage and this fails at 0.
func TestTransferAppMovesUsageWithNoOccurredAt(t *testing.T) {
	ctx := context.Background()
	f := seedTransferFixture(t)
	seedMetricDef(t, f.pool, f.moduleID, "orders.placed", "count", 1_000)

	// occurred_at omitted (NULL), recorded_at inside the window — the shape
	// every infra.*/platform.* event has.
	_, err := f.pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, $2, $3, $4, 'orders.placed', 'count', 5, $5)`,
		uuid.NewString(), f.oldAcct.String(), f.appID.String(), f.moduleID.String(),
		mustTime(t, "2026-08-05T10:00:00Z"))
	require.NoError(t, err)

	resp, err := transferSvc(t, f).TransferApp(ctx, cycle.TransferAppRequest{
		AppID:       f.appID,
		OwnerUserID: f.newOwner,
		Mode:        cycle.TransferModeMove,
		RequestID:   uuid.New(),
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), resp.MovedEventCount,
		"an event with NULL occurred_at did not move; the filter is not the rollup's expression")
}

// 🔴 THE max() TERM, actually exercised. With the TARGET's open period starting
// LATER than the old account's, an event inside the old period but before the
// target's period start must STAY: moving it would backdate usage into a window
// the target has already closed and billed (INV-011). An event after both
// starts is the control and must move.
//
// The arithmetic, all from the one pinned now = 2026-08-15:
//
//	old activated 05-04 → anchor day 4  → open period [08-04, 09-04)
//	new activated 05-10 → anchor day 10 → open period [08-10, 09-10)
//	move window = [max(08-04, 08-10), now) = [08-10, 08-15T12:00)
//	08-06 ∈ old period, < 08-10 → stays with the old account
//	08-12 ≥ 08-10             → moves
//
// The direction matters: had the new anchor been EARLIER (day 20 puts the
// target's start at 07-20), max() would return the OLD start and both events
// would move — which is what the first version of this test asserted against,
// and why it could not pass.
//
// Mutation: replace max() with fromWindow.Start and the window opens at 08-04,
// so BOTH events move and the count reads 2. Every other test in this file
// gives both accounts one anchor, where the two branches return the same
// instant and the mutant is invisible.
func TestTransferAppMoveLeavesUsageOlderThanTheTargetPeriod(t *testing.T) {
	ctx := context.Background()
	f := seedTransferFixtureAnchored(t,
		mustTime(t, "2026-05-04T00:00:00Z"),
		mustTime(t, "2026-05-10T00:00:00Z"))
	seedMetricDef(t, f.pool, f.moduleID, "orders.placed", "count", 1_000)

	stays := uuid.New()
	moves := uuid.New()
	for _, ev := range []struct {
		id uuid.UUID
		at string
	}{{stays, "2026-08-06T10:00:00Z"}, {moves, "2026-08-12T10:00:00Z"}} {
		_, err := f.pool.Exec(ctx, `
			INSERT INTO ms_billing.usage_events
			    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
			VALUES ($1, $2, $3, $4, 'orders.placed', 'count', 2, $5)`,
			ev.id.String(), f.oldAcct.String(), f.appID.String(), f.moduleID.String(), mustTime(t, ev.at))
		require.NoError(t, err)
	}

	// The SEEDED target, whose anchor this whole test is about — see the
	// fixture's newOwner comment for why a fresh owner would not do.
	resp, err := transferSvc(t, f).TransferApp(ctx, cycle.TransferAppRequest{
		AppID:       f.appID,
		OwnerUserID: f.newOwner,
		Mode:        cycle.TransferModeMove,
		RequestID:   uuid.New(),
	})
	require.NoError(t, err)
	require.Equal(t, f.newAcct, resp.AccountID)
	require.Equal(t, int64(1), resp.MovedEventCount,
		"exactly the 08-12 event should move: 08-06 is older than the target's open period (starts 08-10) and must stay")

	accountOf := func(id uuid.UUID) string {
		var got string
		require.NoError(t, f.pool.QueryRow(ctx,
			`SELECT account_id::text FROM ms_billing.usage_events WHERE event_id = $1`, id.String()).Scan(&got))
		return got
	}
	require.Equal(t, f.oldAcct.String(), accountOf(stays),
		"the 08-06 event was backdated into a period the target has already billed")
	require.Equal(t, f.newAcct.String(), accountOf(moves),
		"the 08-12 event is inside both open periods and should have moved")
}

// 🔴 A REPLAY IS VERBATIM, ACROSS A BOUNDARY. api-platform fires TransferApp
// post-commit with retry, so the retry can arrive after the target's next
// anchor boundary. The window and recurring_from are functions of the transfer
// instant and the target's anchor; recomputed from the RETRY's clock they would
// name the next period and a later recurring_from — a date the customer was
// never shown. The ledger stores what the first call answered and the replay
// returns the row.
//
// Distinct anchors, so the first answer also proves it is the TARGET's window
// (old anchor day 4 would say 09-04; the target's anchor day 10 says 09-10).
//
// Mutation: compute the replay's window from p.At again and the second call
// answers [09-10, 10-10) / 10-10 — the assertion below reads the difference.
func TestTransferAppReplayReturnsTheStoredWindowAcrossABoundary(t *testing.T) {
	ctx := context.Background()
	f := seedTransferFixtureAnchored(t,
		mustTime(t, "2026-05-04T00:00:00Z"),
		mustTime(t, "2026-05-10T00:00:00Z"))
	seedMetricDef(t, f.pool, f.moduleID, "orders.placed", "count", 1_000)
	// Inside both open periods at the first call, so mode=move moves it and
	// the replay has a non-zero count to repeat.
	_, err := f.pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, $2, $3, $4, 'orders.placed', 'count', 1, $5)`,
		uuid.NewString(), f.oldAcct.String(), f.appID.String(), f.moduleID.String(),
		mustTime(t, "2026-08-12T10:00:00Z"))
	require.NoError(t, err)

	svc := transferSvc(t, f)
	req := cycle.TransferAppRequest{
		AppID:       f.appID,
		OwnerUserID: f.newOwner,
		Mode:        cycle.TransferModeMove,
		RequestID:   uuid.New(),
	}

	first, err := svc.TransferApp(ctx, req)
	require.NoError(t, err)
	require.Equal(t, int64(1), first.MovedEventCount)
	requireSameInstant(t, mustTime(t, "2026-08-10T00:00:00Z"), first.OpenPeriod.Start, "first open_period.start")
	requireSameInstant(t, mustTime(t, "2026-09-10T00:00:00Z"), first.OpenPeriod.End, "first open_period.end")
	requireSameInstant(t, mustTime(t, "2026-09-10T00:00:00Z"), first.RecurringFrom, "first recurring_from")

	// Advance the service clock past the target's boundary. The control that
	// makes the equality below mean something: a recomputation at this instant
	// CANNOT return the first window, because the first window is closed.
	f.now = mustTime(t, "2026-09-15T12:00:00Z")
	require.True(t, first.OpenPeriod.End.Before(f.now),
		"fixture error: the replay clock must be past the first window's end")

	second, err := svc.TransferApp(ctx, req)
	require.NoError(t, err)

	require.Equal(t, first.AccountID, second.AccountID)
	require.Equal(t, first.MovedEventCount, second.MovedEventCount, "the replay recounted instead of returning the stored count")
	requireSameInstant(t, first.OpenPeriod.Start, second.OpenPeriod.Start, "replayed open_period.start")
	requireSameInstant(t, first.OpenPeriod.End, second.OpenPeriod.End, "replayed open_period.end")
	requireSameInstant(t, first.RecurringFrom, second.RecurringFrom, "replayed recurring_from")

	// And the ledger row is what both calls answered from.
	var storedStart, storedEnd, storedFrom time.Time
	require.NoError(t, f.pool.QueryRow(ctx, `
		SELECT open_period_start, open_period_end, recurring_from
		FROM ms_billing.app_transfer_events WHERE request_id = $1`, req.RequestID.String()).
		Scan(&storedStart, &storedEnd, &storedFrom))
	requireSameInstant(t, first.OpenPeriod.Start, storedStart, "ledger open_period_start")
	requireSameInstant(t, first.OpenPeriod.End, storedEnd, "ledger open_period_end")
	requireSameInstant(t, first.RecurringFrom, storedFrom, "ledger recurring_from")
}
