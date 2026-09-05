//go:build integration

package cycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
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
	return seedTransferFixtureWith(t, transferSeed{oldActivated: oldActivated, newActivated: newActivated})
}

// transferSeed is what varies between fixtures. A zero activation instant
// seeds an UNACTIVATED account (activated_at NULL) — the state the bounded
// refusal exists for.
type transferSeed struct {
	oldActivated time.Time
	newActivated time.Time
}

func seedTransferFixtureWith(t *testing.T, seed transferSeed) *transferFixture {
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
	}{{f.oldAcct, f.oldOwner, seed.oldActivated}, {f.newAcct, f.newOwner, seed.newActivated}} {
		var activated *time.Time
		if !a.activated.IsZero() {
			at := a.activated
			activated = &at
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, activated_at)
			VALUES ($1, 'user', $2, $3)`, a.id.String(), a.owner.String(), activated)
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

// giveOldAccountACard makes the OLD account one that can settle soon: a usable
// (not deleted, not expired) card on it, and the Stripe customer a card
// implies. With activated_at set and the default arrears mode this is exactly
// the state in which every charge leg would collect on its next sweep — and
// therefore the state in which the transfer must REFUSE rather than forfeit.
func (f *transferFixture) giveOldAccountACard(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	_, err := f.pool.Exec(ctx,
		`UPDATE ms_billing.accounts SET stripe_customer_id = $2 WHERE id = $1`,
		f.oldAcct.String(), "cus_transfer_"+f.oldAcct.String())
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `
		INSERT INTO ms_billing.payment_methods_mirror
		    (id, account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default)
		VALUES ($1, $2, $3, 'visa', '4242', 12, 2099, true)`,
		uuid.NewString(), f.oldAcct.String(), "pm_transfer_"+f.oldAcct.String())
	require.NoError(t, err)
}

// The three unsettled things a transfer has to decide about, each seeded on
// the OLD account in the shape its own leg would find it. Every one is past
// its grace / eligible at f.now, so a sweep at f.now lists it.
func (f *transferFixture) owePendingProration(t *testing.T) {
	t.Helper()
	_, err := f.pool.Exec(context.Background(),
		`UPDATE ms_billing.apps SET proration_invoice_id = NULL WHERE app_id = $1`, f.appID.String())
	require.NoError(t, err)
}

// armedAt is the arm marker in the shape the arm statements write it
// (ArmDomainStripeCharge / ArmModuleTimerStripeCharge): the instant AND the
// funding pin — the old account's current funding authorization, which is
// what the arm resolves and what 052's *_attempt_funding_check requires
// alongside charge_attempted_at. An "armed" row with no pin is not a state
// the writers can produce, and the CHECK refuses it at insert.
type armedAt struct {
	at             *time.Time
	fundingAccount *string
	generation     *string
}

func (f *transferFixture) armed(t *testing.T, attempted bool, at string) armedAt {
	t.Helper()
	if !attempted {
		return armedAt{}
	}
	instant := mustTime(t, at)
	var fundingAccount, generation string
	require.NoError(t, f.pool.QueryRow(context.Background(), `
		SELECT funding_account_id::text, generation::text
		FROM ms_billing.account_funding_authorizations WHERE account_id = $1`, f.oldAcct.String()).
		Scan(&fundingAccount, &generation), "fixture error: the old account has no funding authorization (052 creates one on insert)")
	return armedAt{at: &instant, fundingAccount: &fundingAccount, generation: &generation}
}

func (f *transferFixture) oweDomainActivation(t *testing.T, attempted bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	arm := f.armed(t, attempted, "2026-08-02T00:00:00Z")
	_, err := f.pool.Exec(context.Background(), `
		INSERT INTO ms_billing.app_custom_domains
		    (id, account_id, app_id, hostname, activated_at, charge_resolved,
		     charge_attempted_at, charge_funding_account_id, charge_funding_generation)
		VALUES ($1, $2, $3, 'example.test', $4, false, $5, $6, $7)`,
		id.String(), f.oldAcct.String(), f.appID.String(), mustTime(t, "2026-08-01T00:00:00Z"),
		arm.at, arm.fundingAccount, arm.generation)
	require.NoError(t, err)
	return id
}

func (f *transferFixture) oweModuleGrace(t *testing.T, attempted bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	arm := f.armed(t, attempted, "2026-08-05T00:00:00Z")
	// Columns per migration 033: there is no module_id (one row per install
	// EVENT, not per module identity) and grace_expires_at is NOT NULL. Same
	// shape as org_deletion_integration_test.
	_, err := f.pool.Exec(context.Background(), `
		INSERT INTO ms_billing.app_module_overage_timers
		    (id, account_id, app_id, installed_at, grace_expires_at, grace_resolved,
		     charge_attempted_at, charge_funding_account_id, charge_funding_generation)
		VALUES ($1, $2, $3, $4::timestamptz, $4::timestamptz + interval '3 days', false, $5, $6, $7)`,
		id.String(), f.oldAcct.String(), f.appID.String(), mustTime(t, "2026-08-01T00:00:00Z"),
		arm.at, arm.fundingAccount, arm.generation)
	require.NoError(t, err)
	return id
}

// ledgerRow reads the one ledger row a request wrote. found=false when the
// transfer was refused, which is the assertion most refusal tests make.
type transferLedgerRow struct {
	fromAccount        *string
	forfeitedProration bool
	forfeitedDomains   int64
	forfeitedTimers    int64
	forfeitReason      *string
}

func (f *transferFixture) ledgerRow(t *testing.T, requestID uuid.UUID) (transferLedgerRow, bool) {
	t.Helper()
	var row transferLedgerRow
	err := f.pool.QueryRow(context.Background(), `
		SELECT from_account::text, forfeited_proration, forfeited_domain_count,
		       forfeited_timer_count, forfeit_reason
		FROM ms_billing.app_transfer_events WHERE request_id = $1`, requestID.String()).
		Scan(&row.fromAccount, &row.forfeitedProration, &row.forfeitedDomains,
			&row.forfeitedTimers, &row.forfeitReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return transferLedgerRow{}, false
	}
	require.NoError(t, err)
	return row, true
}

// sweepPending runs the three one-time-charge sweeps at f.now and returns
// their work-list sizes (creation proration, domain activation, module
// grace). Pending is read straight off each work list, before any leg acts,
// so a nil Stripe client — which every leg refuses with an error that the
// sweep COUNTS rather than returns — cannot make the number lie.
func (f *transferFixture) sweepPending(t *testing.T) [3]int {
	t.Helper()
	ctx := context.Background()
	svc := transferSvc(t, f)
	prorations, err := svc.SweepCreationProrations(ctx, f.now)
	require.NoError(t, err)
	domains, err := svc.SweepDomainCharges(ctx, f.now)
	require.NoError(t, err)
	timers, err := svc.SweepModuleOverage(ctx, f.now)
	require.NoError(t, err)
	return [3]int{prorations.Pending, domains.Pending, timers.Pending}
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

// 🔴 THE MONEY REFUSAL, BOUNDED. Each leg charges whoever the row points at
// when the sweep runs, so re-keying with one unresolved bills the new account
// for a window it did not own. The transfer refuses when the OLD account is
// about to settle it — activated, arrears, a usable card, which is what the
// fixture's card gives it — or when an attempt is already in flight (armed at
// the provider), whatever the card situation. Every other pending state is
// FORFEITED instead; that is the next test, and the two are each other's
// vacuity control: drop the card from these fixtures and every "settleable"
// case here turns into a forfeit and a 200.
//
// Mutation: drop a leg from AppUnresolvedOneTimeCharges and its case here
// starts succeeding.
func TestTransferAppRefusesWhileAOneTimeChargeIsPending(t *testing.T) {
	cases := []struct {
		name string
		// settleable gives the old account a card; in-flight cases leave it
		// without one, so the refusal there is provably the arm marker's.
		settleable bool
		seed       func(*testing.T, *transferFixture)
	}{
		{"creation proration owed, old account can settle", true, func(t *testing.T, f *transferFixture) {
			f.owePendingProration(t)
		}},
		{"domain activation unresolved, old account can settle", true, func(t *testing.T, f *transferFixture) {
			f.oweDomainActivation(t, false)
		}},
		{"module grace unresolved, old account can settle", true, func(t *testing.T, f *transferFixture) {
			f.oweModuleGrace(t, false)
		}},
		// In flight: the arm marker is set, so money may already have moved
		// at the provider. No card on the old account — a forfeit here would
		// leave a possibly-collected charge with no mirror, so the refusal
		// must not depend on the card at all.
		{"creation proration attempted, no card", false, func(t *testing.T, f *transferFixture) {
			f.owePendingProration(t)
			_, err := f.pool.Exec(context.Background(),
				`UPDATE ms_billing.apps SET proration_attempted_at = $2 WHERE app_id = $1`,
				f.appID.String(), mustTime(t, "2026-08-10T00:00:00Z"))
			require.NoError(t, err)
		}},
		{"domain activation armed, no card", false, func(t *testing.T, f *transferFixture) {
			f.oweDomainActivation(t, true)
		}},
		{"module grace armed, no card", false, func(t *testing.T, f *transferFixture) {
			f.oweModuleGrace(t, true)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := seedTransferFixture(t)
			if tc.settleable {
				f.giveOldAccountACard(t)
			}
			tc.seed(t, f)
			before := f.rosterAccount(t)
			req := cycle.TransferAppRequest{
				AppID:       f.appID,
				OwnerUserID: f.newOwner,
				Mode:        cycle.TransferModeMove,
				RequestID:   uuid.New(),
			}

			_, err := transferSvc(t, f).TransferApp(ctx, req)

			require.Error(t, err)
			var be *billing.Error
			require.True(t, errors.As(err, &be), "not a billing.Error: %v", err)
			require.Equal(t, billing.CodeConflict, be.Code)
			require.Contains(t, err.Error(), "app_transfer_charges_pending")
			// Nothing may have moved OR been forfeited: a refusal that
			// half-applied would be worse than the bug it prevents.
			require.Equal(t, before, f.rosterAccount(t), "roster changed despite the refusal")
			_, found := f.ledgerRow(t, req.RequestID)
			require.False(t, found, "a refused transfer wrote a ledger row")
			var forfeited int
			require.NoError(t, f.pool.QueryRow(ctx, `
				SELECT (SELECT count(*) FROM ms_billing.apps WHERE app_id = $1 AND proration_skipped_at IS NOT NULL)
				     + (SELECT count(*) FROM ms_billing.app_custom_domains WHERE app_id = $1 AND charge_forfeited_by IS NOT NULL)
				     + (SELECT count(*) FROM ms_billing.app_module_overage_timers WHERE app_id = $1 AND grace_forfeited_by IS NOT NULL)`,
				f.appID.String()).Scan(&forfeited))
			require.Equal(t, 0, forfeited, "a refused transfer forfeited something")
		})
	}
}

// 🔴 THE FORFEIT. When the old account CANNOT settle soon — never activated,
// prepaid, or no usable card — the unresolved one-time charges do not block
// the transfer forever (the sweeps would skip them, transiently, on every
// run, for as long as the account stays so) and they do not travel to the new
// owner either. They are resolved inside the transfer transaction without
// being collected, stamped with the transfer that did it, and counted on the
// ledger row. Afterwards NO sweep lists them for EITHER account: the app is
// on the new (activated) account, so only the resolution keeps the domain and
// timer off their work lists, and only the skip marker keeps the app off the
// proration list.
//
// The pre-transfer sweep is the vacuity control for the post-transfer one:
// the same rows read as pending BEFORE, so "0 after" is the forfeit and not a
// work list that never listed them. For an unactivated old account the domain
// and timer lists gate on activation and read 0 before too — the row-state
// assertions carry that case.
//
// Mutation: make transferChargeDisposition refuse unconditionally and every
// case here is a CONFLICT; drop a forfeit writer and its leg reads pending
// after, on the NEW account.
func TestTransferAppForfeitsWhatTheOldAccountCannotSettle(t *testing.T) {
	activated := mustTime(t, "2026-05-04T00:00:00Z")
	cases := []struct {
		name          string
		seed          transferSeed
		prepare       func(*testing.T, *transferFixture)
		wantReason    string
		pendingBefore [3]int
	}{
		{"unactivated old account", transferSeed{newActivated: activated}, nil, "unactivated", [3]int{1, 0, 0}},
		{"prepaid old account", transferSeed{oldActivated: activated, newActivated: activated},
			func(t *testing.T, f *transferFixture) {
				// A card too, so the reason is provably the mode and not the
				// card: the card gate is later in the order.
				f.giveOldAccountACard(t)
				_, err := f.pool.Exec(context.Background(),
					`UPDATE ms_billing.accounts SET usage_billing_mode = 'prepaid' WHERE id = $1`, f.oldAcct.String())
				require.NoError(t, err)
			}, "prepaid", [3]int{1, 1, 1}},
		{"no usable card on the old account", transferSeed{oldActivated: activated, newActivated: activated},
			nil, "no_payment_method", [3]int{1, 1, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := seedTransferFixtureWith(t, tc.seed)
			if tc.prepare != nil {
				tc.prepare(t, f)
			}
			f.owePendingProration(t)
			domainID := f.oweDomainActivation(t, false)
			timerID := f.oweModuleGrace(t, false)
			require.Equal(t, tc.pendingBefore, f.sweepPending(t), "fixture error: the sweeps did not list what this test forfeits")

			req := cycle.TransferAppRequest{
				AppID:       f.appID,
				OwnerUserID: f.newOwner,
				Mode:        cycle.TransferModeKeep,
				RequestID:   uuid.New(),
			}
			resp, err := transferSvc(t, f).TransferApp(ctx, req)
			require.NoError(t, err)
			require.Equal(t, f.newAcct, resp.AccountID)
			require.Equal(t, f.newAcct.String(), f.rosterAccount(t))

			// The rows: resolved, uncharged, and each saying which transfer.
			var skippedAt time.Time
			require.NoError(t, f.pool.QueryRow(ctx,
				`SELECT proration_skipped_at FROM ms_billing.apps WHERE app_id = $1`, f.appID.String()).Scan(&skippedAt))
			requireSameInstant(t, f.now, skippedAt, "proration_skipped_at")

			var domainResolved bool
			var domainAccount, domainBy string
			var domainChargedAt, domainInvoice *string
			require.NoError(t, f.pool.QueryRow(ctx, `
				SELECT charge_resolved, account_id::text, charge_forfeited_by::text, charged_at::text, charge_invoice_id
				FROM ms_billing.app_custom_domains WHERE id = $1`, domainID.String()).
				Scan(&domainResolved, &domainAccount, &domainBy, &domainChargedAt, &domainInvoice))
			require.True(t, domainResolved, "the domain activation charge is still pending")
			require.Equal(t, req.RequestID.String(), domainBy, "the domain does not name the transfer that forfeited it")
			require.Nil(t, domainChargedAt, "a forfeit claimed a charge")
			require.Nil(t, domainInvoice, "a forfeit claimed an invoice")
			require.Equal(t, f.newAcct.String(), domainAccount, "the domain row did not follow the roster")

			var timerResolved bool
			var timerAccount, timerBy string
			var timerChargedAt *string
			require.NoError(t, f.pool.QueryRow(ctx, `
				SELECT grace_resolved, account_id::text, grace_forfeited_by::text, grace_charged_at::text
				FROM ms_billing.app_module_overage_timers WHERE id = $1`, timerID.String()).
				Scan(&timerResolved, &timerAccount, &timerBy, &timerChargedAt))
			require.True(t, timerResolved, "the module grace overage is still pending")
			require.Equal(t, req.RequestID.String(), timerBy, "the timer does not name the transfer that forfeited it")
			require.Nil(t, timerChargedAt, "a forfeit claimed a charge")
			require.Equal(t, f.newAcct.String(), timerAccount, "the timer row did not follow the roster")

			// The ledger: what was forfeited, and why.
			row, found := f.ledgerRow(t, req.RequestID)
			require.True(t, found, "no ledger row")
			require.NotNil(t, row.fromAccount)
			require.Equal(t, f.oldAcct.String(), *row.fromAccount)
			require.True(t, row.forfeitedProration)
			require.Equal(t, int64(1), row.forfeitedDomains)
			require.Equal(t, int64(1), row.forfeitedTimers)
			require.NotNil(t, row.forfeitReason, "a forfeit with no reason")
			require.Equal(t, tc.wantReason, *row.forfeitReason)

			// And no sweep charges anyone for any of it, ever.
			require.Equal(t, [3]int{0, 0, 0}, f.sweepPending(t),
				"a forfeited charge is still on a work list — it would be billed to the new account")
		})
	}
}

// The forfeit is a money decision that must survive a replay: the ledger says
// what the FIRST call forfeited, and a retry neither forfeits again (nothing
// is pending any more) nor reports otherwise.
func TestTransferAppForfeitIsRecordedOnce(t *testing.T) {
	ctx := context.Background()
	f := seedTransferFixture(t)
	f.owePendingProration(t)
	req := cycle.TransferAppRequest{
		AppID:       f.appID,
		OwnerUserID: f.newOwner,
		Mode:        cycle.TransferModeKeep,
		RequestID:   uuid.New(),
	}
	svc := transferSvc(t, f)
	_, err := svc.TransferApp(ctx, req)
	require.NoError(t, err)
	_, err = svc.TransferApp(ctx, req)
	require.NoError(t, err)

	var rows int
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.app_transfer_events WHERE app_id = $1 AND forfeited_proration`,
		f.appID.String()).Scan(&rows))
	require.Equal(t, 1, rows)
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

// 🔴 THE NULL-SOURCE HOLE. An unbilled org roster row (account_id NULL,
// migration 041) has no payer, so its creation proration has never been
// charged and its guard is unarmed. Re-key it and the row ACQUIRES a payer:
// AppsPendingProration selects it on the next sweep, and the only thing that
// could stop the charge — the D1d check — compares activation against the
// creation period only, which a target activated BEFORE the app was created
// passes. The new owner would be billed for the old owner's creation window.
//
// So a NULL-source transfer forfeits that window ("transferred from no
// payer", 071) and, as the org attach sweep does, synthesizes the app's first
// timers fresh at the transfer instant — prospective billing, nothing before
// the transfer is ever billed to the target.
//
// The arithmetic behind the D1d control below: created 06-01, target anchor
// day 4 → creation period [05-04, 06-04); target activated 05-04 < 06-04, so
// D1d does NOT forgive it. With the marker cleared (the mutant's world) the
// sweep lists the app and reaches the charge leg — which fails on the nil
// Stripe client rather than skipping, and that failure is the proof: the
// marker is the only thing between this app and a charge to the new owner.
//
// Mutation: skip the forfeit for a NULL source and the post-transfer sweep
// reads Pending 1.
func TestTransferAppFromNoPayerForfeitsTheCreationWindow(t *testing.T) {
	ctx := context.Background()
	f := seedTransferFixture(t)
	orgID := uuid.New()
	// The org's unbilled roster row: no account, two modules never timed
	// (a NULL account synthesizes no timers), creation charge unarmed.
	_, err := f.pool.Exec(ctx, `
		UPDATE ms_billing.apps
		SET account_id = NULL, owner_org_id = $2, module_count = 2, proration_invoice_id = NULL
		WHERE app_id = $1`, f.appID.String(), orgID.String())
	require.NoError(t, err)
	// The sweep would list this app on every predicate but the payer — the
	// premise of the hole. created_at is well past grace at f.now.
	var listable int
	require.NoError(t, f.pool.QueryRow(ctx, `
		SELECT count(*) FROM ms_billing.apps
		WHERE app_id = $1 AND created_at <= $2 AND proration_invoice_id IS NULL
		  AND proration_skipped_at IS NULL AND deleted_at IS NULL`,
		f.appID.String(), f.now.AddDate(0, 0, -3)).Scan(&listable))
	require.Equal(t, 1, listable, "fixture error: the app would not be on the proration work list even with a payer")

	req := cycle.TransferAppRequest{
		AppID:       f.appID,
		OwnerUserID: f.newOwner,
		Mode:        cycle.TransferModeKeep,
		RequestID:   uuid.New(),
	}
	resp, err := transferSvc(t, f).TransferApp(ctx, req)
	require.NoError(t, err)
	require.Equal(t, f.newAcct, resp.AccountID)

	var account string
	var ownerOrg *string
	var skippedAt *time.Time
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT account_id::text, owner_org_id::text, proration_skipped_at FROM ms_billing.apps WHERE app_id = $1`,
		f.appID.String()).Scan(&account, &ownerOrg, &skippedAt))
	require.Equal(t, f.newAcct.String(), account)
	require.Nil(t, ownerOrg, "a transfer to a user left the old org on the roster row")
	require.NotNil(t, skippedAt, "the creation window was not forfeited; the next sweep bills it to the new owner")
	requireSameInstant(t, f.now, *skippedAt, "proration_skipped_at")

	row, found := f.ledgerRow(t, req.RequestID)
	require.True(t, found)
	require.Nil(t, row.fromAccount, "transferred from no payer must record a NULL from_account")
	require.True(t, row.forfeitedProration)
	require.Equal(t, int64(0), row.forfeitedDomains)
	require.Equal(t, int64(0), row.forfeitedTimers)
	require.NotNil(t, row.forfeitReason)
	require.Equal(t, "no_payer", *row.forfeitReason)

	// Timers synthesized fresh, on the new account, anchored at the transfer
	// instant — exactly what attachOrgBilling does at designation.
	var timers int
	require.NoError(t, f.pool.QueryRow(ctx, `
		SELECT count(*) FROM ms_billing.app_module_overage_timers
		WHERE app_id = $1 AND account_id = $2 AND removed_at IS NULL`,
		f.appID.String(), f.newAcct.String()).Scan(&timers))
	require.Equal(t, 2, timers, "the app's modules were not timed on the new account")
	var installedAt, graceExpiresAt time.Time
	require.NoError(t, f.pool.QueryRow(ctx, `
		SELECT min(installed_at), min(grace_expires_at)
		FROM ms_billing.app_module_overage_timers
		WHERE app_id = $1 AND account_id = $2 AND removed_at IS NULL`,
		f.appID.String(), f.newAcct.String()).Scan(&installedAt, &graceExpiresAt))
	requireSameInstant(t, f.now, installedAt, "timer installed_at")
	requireSameInstant(t, f.now.AddDate(0, 0, 3), graceExpiresAt, "timer grace_expires_at")

	// Nothing lands on the target.
	sweep, err := transferSvc(t, f).SweepCreationProrations(ctx, f.now)
	require.NoError(t, err)
	require.Equal(t, 0, sweep.Pending, "the transferred app is on the proration work list; the new owner is about to be billed the old creation window")
	require.Equal(t, 0, sweep.Charged)
	require.Equal(t, 0, sweep.Proposed)

	// The D1d control: clear the marker and the sweep both lists the app and
	// gets PAST the D1d gate to the charge leg (Failed on the nil Stripe
	// client, not Skipped). Without this, "Pending 0" could be D1d's doing.
	_, err = f.pool.Exec(ctx,
		`UPDATE ms_billing.apps SET proration_skipped_at = NULL WHERE app_id = $1`, f.appID.String())
	require.NoError(t, err)
	unguarded, err := transferSvc(t, f).SweepCreationProrations(ctx, f.now)
	require.NoError(t, err)
	require.Equal(t, 1, unguarded.Pending, "control: with the marker cleared the app must be listed")
	require.Equal(t, 1, unguarded.Failed, "control: D1d must NOT forgive this window (the target activated before the app existed); the charge leg must be reached")
	require.Equal(t, 0, unguarded.Skipped)
}

// 🔴 THE BACKLOG REFUSAL. Usage recorded with NO account is the lazy org
// backlog; the repoint sweep (RepointOrgNullAccountEvents) reaches it only
// through apps.owner_org_id, which the transfer rewrites. Moving the app
// would either bill org A's never-billed backlog to org B — all of it — or,
// for a user target, strand it where no sweep can ever find it. The transfer
// refuses until the backlog is attached; the control deletes the one event
// and the same transfer succeeds.
//
// Mutation: drop AppHasUnbilledUsageBacklog from the store and the first call
// succeeds.
func TestTransferAppRefusesWhileAnUnbilledBacklogExists(t *testing.T) {
	ctx := context.Background()
	f := seedTransferFixture(t)
	seedMetricDef(t, f.pool, f.moduleID, "orders.placed", "count", 1_000)
	backlog := uuid.New()
	_, err := f.pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, NULL, $2, $3, 'orders.placed', 'count', 1, $4)`,
		backlog.String(), f.appID.String(), f.moduleID.String(), mustTime(t, "2026-08-05T10:00:00Z"))
	require.NoError(t, err)
	before := f.rosterAccount(t)
	svc := transferSvc(t, f)
	req := cycle.TransferAppRequest{
		AppID:       f.appID,
		OwnerUserID: f.newOwner,
		Mode:        cycle.TransferModeMove,
		RequestID:   uuid.New(),
	}

	_, err = svc.TransferApp(ctx, req)

	require.Error(t, err)
	var be *billing.Error
	require.True(t, errors.As(err, &be), "not a billing.Error: %v", err)
	require.Equal(t, billing.CodeConflict, be.Code)
	require.Contains(t, err.Error(), "app_transfer_unbilled_backlog")
	require.Equal(t, before, f.rosterAccount(t), "roster changed despite the refusal")
	_, found := f.ledgerRow(t, req.RequestID)
	require.False(t, found, "a refused transfer wrote a ledger row")

	// The control: the backlog was the only thing in the way.
	_, err = f.pool.Exec(ctx, `DELETE FROM ms_billing.usage_events WHERE event_id = $1`, backlog.String())
	require.NoError(t, err)
	req.RequestID = uuid.New()
	resp, err := svc.TransferApp(ctx, req)
	require.NoError(t, err)
	require.Equal(t, f.newAcct, resp.AccountID)
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

// A soft-deleted app is NOT_FOUND, not transferable. It is already out of
// every future base fee (D1e), so there is nothing to move, and re-keying its
// row would hand the new account whatever the deletion left on it. The live
// control below the deletion is what makes the refusal a refusal and not a
// broken lookup.
//
// Mutation: drop `AND deleted_at IS NULL` from LockAppForTransfer and the
// deleted app transfers.
func TestTransferAppRefusesADeletedApp(t *testing.T) {
	ctx := context.Background()
	f := seedTransferFixture(t)
	_, err := f.pool.Exec(ctx,
		`UPDATE ms_billing.apps SET deleted_at = $2 WHERE app_id = $1`,
		f.appID.String(), mustTime(t, "2026-08-10T00:00:00Z"))
	require.NoError(t, err)
	before := f.rosterAccount(t)

	_, err = transferSvc(t, f).TransferApp(ctx, cycle.TransferAppRequest{
		AppID:       f.appID,
		OwnerUserID: f.newOwner,
		Mode:        cycle.TransferModeKeep,
		RequestID:   uuid.New(),
	})

	require.Error(t, err)
	var be *billing.Error
	require.True(t, errors.As(err, &be), "not a billing.Error: %v", err)
	require.Equal(t, billing.CodeNotFound, be.Code)
	require.Contains(t, err.Error(), "app_unknown")
	require.Equal(t, before, f.rosterAccount(t), "a deleted app's roster row was re-keyed")

	var events int
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.app_transfer_events WHERE app_id = $1`,
		f.appID.String()).Scan(&events))
	require.Equal(t, 0, events, "a refused transfer wrote a ledger row")
}

// 🔴 THE LEDGER IS APPEND-ONLY. A row is the answer a replay returns and the
// only record of where a transfer moved usage from; edit it and the replay
// lies, delete it and the next retry of that request_id transfers again. The
// test connection is the table owner, so the REVOKE from billing_svc cannot be
// observed here (071's read-back asserts it at migration time); the trigger
// fires for the owner too, and that is what this pins.
//
// Mutation: drop app_transfer_events_append_only from 071 and both statements
// succeed.
func TestTransferLedgerIsAppendOnly(t *testing.T) {
	ctx := context.Background()
	f := seedTransferFixture(t)
	req := cycle.TransferAppRequest{
		AppID:       f.appID,
		OwnerUserID: f.newOwner,
		Mode:        cycle.TransferModeKeep,
		RequestID:   uuid.New(),
	}
	_, err := transferSvc(t, f).TransferApp(ctx, req)
	require.NoError(t, err)

	_, err = f.pool.Exec(ctx,
		`UPDATE ms_billing.app_transfer_events SET moved_event_count = 99 WHERE request_id = $1`,
		req.RequestID.String())
	require.Error(t, err, "a ledger row was edited")
	require.Contains(t, err.Error(), "append-only")

	_, err = f.pool.Exec(ctx,
		`DELETE FROM ms_billing.app_transfer_events WHERE request_id = $1`,
		req.RequestID.String())
	require.Error(t, err, "a ledger row was deleted")
	require.Contains(t, err.Error(), "append-only")

	var events int
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.app_transfer_events WHERE request_id = $1 AND moved_event_count = 0`,
		req.RequestID.String()).Scan(&events))
	require.Equal(t, 1, events, "the row is not as the transfer wrote it")
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
