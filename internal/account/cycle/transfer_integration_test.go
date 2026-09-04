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
	pool     *pgxpool.Pool
	oldAcct  uuid.UUID
	newAcct  uuid.UUID
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
	ctx := context.Background()
	pool := testutil.NewTestDB(t)

	f := &transferFixture{
		pool:     pool,
		oldAcct:  uuid.New(),
		newAcct:  uuid.New(),
		appID:    uuid.New(),
		moduleID: uuid.New(),
		now:      mustTime(t, "2026-08-15T12:00:00Z"),
	}
	activatedAt := mustTime(t, "2026-05-04T00:00:00Z")

	for _, id := range []uuid.UUID{f.oldAcct, f.newAcct} {
		_, err := pool.Exec(ctx, `
			INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, activated_at)
			VALUES ($1, 'user', $2, $3)`, id.String(), uuid.NewString(), activatedAt)
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
				OwnerUserID: uuid.New(),
				Mode:        tc.mode,
				RequestID:   uuid.New(),
			})
			require.NoError(t, err)
			require.Equal(t, tc.wantMoved, resp.MovedEventCount)

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
			_, err := f.pool.Exec(context.Background(), `
				INSERT INTO ms_billing.app_module_overage_timers
				    (id, account_id, app_id, module_id, installed_at, grace_resolved)
				VALUES ($1, $2, $3, $4, $5, false)`,
				uuid.NewString(), f.oldAcct.String(), f.appID.String(), uuid.NewString(),
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
				OwnerUserID: uuid.New(),
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
		OwnerUserID: uuid.New(),
		Mode:        cycle.TransferModeKeep,
		RequestID:   uuid.New(),
	})

	require.NoError(t, err)
	require.Equal(t, resp.AccountID.String(), f.rosterAccount(t))
	require.False(t, resp.RecurringFrom.IsZero(), "recurring_from must carry the new account's next boundary")
}

// api-platform fires this post-commit with retry, so a replay must return the
// FIRST result rather than transfer again.
func TestTransferAppIsIdempotentOnRequestID(t *testing.T) {
	ctx := context.Background()
	f := seedTransferFixture(t)
	svc := transferSvc(t, f)
	req := cycle.TransferAppRequest{
		AppID:       f.appID,
		OwnerUserID: uuid.New(),
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

	// Same key, different target ⇒ conflict, never a second transfer.
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
