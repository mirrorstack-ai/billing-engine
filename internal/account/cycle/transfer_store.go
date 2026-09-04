package cycle

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	billingaccount "github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
	"github.com/mirrorstack-ai/billing-engine/internal/meteringlock"
)

// TransferAppParams is the store-level transfer request. The service has
// already validated the shape and resolved ToAccount.
type TransferAppParams struct {
	AppID       uuid.UUID
	RequestID   uuid.UUID
	ToAccount   uuid.UUID
	OwnerUserID uuid.UUID
	OwnerOrgID  uuid.UUID
	Mode        string
	At          time.Time
}

// TransferOutcome discriminates the store's non-error refusals, so the service
// maps them to wire codes and the store never builds a billing error. Same
// shape as OrgDeletionFinalizationOutcome (org_deletion_store.go).
type TransferOutcome int

const (
	// TransferApplied — the transfer happened in this call.
	TransferApplied TransferOutcome = iota
	// TransferAlreadyApplied — this request_id already transferred this app to
	// this account; the response carries the STORED result.
	TransferAlreadyApplied
	// TransferRequestConflict — this request_id was used for a DIFFERENT
	// target. Never a second transfer.
	TransferRequestConflict
	// TransferAppUnknown — no ms_billing.apps row.
	TransferAppUnknown
	// TransferChargesPending — a mid-period one-time charge is still owed for
	// this app; re-keying now would bill it to the new account.
	TransferChargesPending
	// TransferPeriodClosed — the window the move would write into has already
	// been closed or invoiced for one of the accounts. Refusing is the only
	// safe answer: writing there backdates across a billed period.
	TransferPeriodClosed
)

// TransferApp re-points one app's billing account, in ONE transaction.
//
// LOCK ORDER, and both locks are required:
//  1. the per-app module-timer advisory lock — the key every timer-set writer
//     serializes on (lockModuleTimers). Without it a concurrent RegisterApp or
//     SyncAppModules reconciles timers against the roster it read BEFORE this
//     transfer and re-splits the attribution the transfer just aligned.
//  2. the apps row FOR UPDATE — serializes two concurrent transfers of the same
//     app and pins the from-account the event records.
//
// Everything decided is read under those locks, including the pending-charge
// refusal, so a sweep cannot slip between the check and the re-key.
func (s *pgxStore) TransferApp(ctx context.Context, p TransferAppParams) (*TransferAppResponse, TransferOutcome, error) {
	var (
		resp    *TransferAppResponse
		outcome TransferOutcome
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		if err := lockModuleTimers(ctx, tx, p.AppID); err != nil {
			return err
		}

		// Replay / conflict, before anything is read or written. A stored row
		// is the record of what the first call did; a repeat must never
		// recount or re-move.
		//
		// 🔴 EVERY FIELD COMES FROM THE ROW, p.At INCLUDED IN WHAT IS IGNORED.
		// The window and recurring_from are functions of the transfer instant
		// and the target's anchor. api-platform retries this call post-commit,
		// and a retry that lands after the target's boundary would, if
		// recomputed from the replay's clock, answer with the NEXT period and
		// a later recurring_from — a second date the customer was never shown.
		// The first call stored what it answered; the replay repeats it.
		prior, err := qtx.AppTransferEventByRequest(ctx, p.RequestID.String())
		switch {
		case err == nil:
			priorTo, parseErr := uuid.Parse(prior.ToAccount)
			if parseErr != nil {
				return parseErr
			}
			priorApp, parseErr := uuid.Parse(prior.AppID)
			if parseErr != nil {
				return parseErr
			}
			if priorTo != p.ToAccount || priorApp != p.AppID {
				outcome = TransferRequestConflict
				return nil
			}
			outcome = TransferAlreadyApplied
			resp = &TransferAppResponse{
				AccountID:       p.ToAccount,
				MovedEventCount: prior.MovedEventCount,
				OpenPeriod:      TransferPeriod{Start: prior.OpenPeriodStart, End: prior.OpenPeriodEnd},
				RecurringFrom:   prior.RecurringFrom,
			}
			return nil
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}

		app, err := qtx.LockAppForTransfer(ctx, p.AppID.String())
		if errors.Is(err, pgx.ErrNoRows) {
			outcome = TransferAppUnknown
			return nil
		}
		if err != nil {
			return err
		}

		// 🔴 THE MONEY REFUSAL. Creation proration, custom-domain activation and
		// per-module grace overage each charge whoever the row points at WHEN
		// THE SWEEP RUNS. Re-keying with one outstanding bills the new account
		// for a window it did not own, so the transfer refuses and the caller
		// retries after the sweeps settle.
		pending, err := qtx.AppHasUnresolvedOneTimeCharge(ctx, p.AppID.String())
		if err != nil {
			return err
		}
		if pending.Valid && pending.Bool {
			outcome = TransferChargesPending
			return nil
		}

		var fromAccount uuid.UUID
		if app.AccountID.Valid {
			fromAccount = app.AccountID.Bytes
		}

		// 🔴 THE PERIOD BARRIER. Ingest (usage/store.go) and the org repoint
		// sweep (RepointOrgNullAccountEvents) both take it; a writer that moves
		// usage between accounts and does NOT is racing the cycle Lambda. A
		// transfer straddling an anchor boundary while the rollup closes it can
		// have the moved rows counted twice — by the old account's rollup
		// before the move and the new account's after.
		//
		// 🔴 LOCKED IN A DETERMINISTIC ORDER, sorted by account id. Two
		// concurrent transfers in opposite directions (A→B and B→A) would
		// otherwise take the same two locks in opposite orders and deadlock.
		// Postgres would abort one with 40P01, which is survivable but is a
		// self-inflicted retry storm on the money path.
		if err := s.barrierBothAccounts(ctx, tx, qtx, fromAccount, p.ToAccount, p.At); err != nil {
			return err
		}

		// The window the move writes into must still be open for BOTH sides.
		// Refuse rather than advance: advancing silently changes which period
		// the usage lands in, and this RPC is called by a request/approve flow
		// that can retry after the boundary settles.
		closed, err := s.anyPeriodClosed(ctx, qtx, fromAccount, p.ToAccount, p.At)
		if err != nil {
			return err
		}
		if closed {
			outcome = TransferPeriodClosed
			return nil
		}

		orgCol := pgtype.UUID{}
		if p.OwnerOrgID != uuid.Nil {
			orgCol = pgtype.UUID{Bytes: p.OwnerOrgID, Valid: true}
		}
		if _, err := qtx.RekeyAppRoster(ctx, db.RekeyAppRosterParams{
			AppID:      p.AppID.String(),
			AccountID:  pgtype.UUID{Bytes: p.ToAccount, Valid: true},
			OwnerOrgID: orgCol,
		}); err != nil {
			return err
		}
		if _, err := qtx.RekeyAppTimers(ctx, db.RekeyAppTimersParams{
			AppID:     p.AppID.String(),
			AccountID: p.ToAccount.String(),
		}); err != nil {
			return err
		}
		if _, err := qtx.RekeyAppDomains(ctx, db.RekeyAppDomainsParams{
			AppID:     p.AppID.String(),
			AccountID: p.ToAccount.String(),
		}); err != nil {
			return err
		}

		var moved int64
		if p.Mode == TransferModeMove && fromAccount != uuid.Nil {
			start, mErr := s.moveWindowStart(ctx, qtx, fromAccount, p.ToAccount, p.At)
			if mErr != nil {
				return mErr
			}
			n, uErr := qtx.MoveAppOpenUsage(ctx, db.MoveAppOpenUsageParams{
				AppID:       p.AppID.String(),
				AccountID:   pgtype.UUID{Bytes: fromAccount, Valid: true},
				AccountID_2: pgtype.UUID{Bytes: p.ToAccount, Valid: true},
				WindowStart: start,
				WindowEnd:   p.At,
			})
			if uErr != nil {
				return uErr
			}
			moved = n
		}

		// The answer is derived once, here, and written to the ledger with the
		// transfer itself — the replay path above returns these columns, not a
		// recomputation. Read under the barrier, so the target's anchor cannot
		// change between this and the insert.
		window, from, wErr := s.transferWindows(ctx, qtx, p.ToAccount, p.At)
		if wErr != nil {
			return wErr
		}

		fromCol := pgtype.UUID{}
		if fromAccount != uuid.Nil {
			fromCol = pgtype.UUID{Bytes: fromAccount, Valid: true}
		}
		if err := qtx.InsertAppTransferEvent(ctx, db.InsertAppTransferEventParams{
			RequestID:       p.RequestID.String(),
			AppID:           p.AppID.String(),
			FromAccount:     fromCol,
			ToAccount:       p.ToAccount.String(),
			Mode:            p.Mode,
			MovedEventCount: moved,
			At:              p.At,
			OpenPeriodStart: window.Start,
			OpenPeriodEnd:   window.End,
			RecurringFrom:   from,
		}); err != nil {
			return err
		}

		outcome = TransferApplied
		resp = &TransferAppResponse{
			AccountID:       p.ToAccount,
			MovedEventCount: moved,
			OpenPeriod:      window,
			RecurringFrom:   from,
		}
		return nil
	})
	if err != nil {
		return nil, TransferApplied, err
	}
	return resp, outcome, nil
}

// transferWindows returns the TARGET account's open period and the boundary at
// which its recurring fees for this app begin.
//
// RecurringFrom is the END of the target's current open window: that is the
// next boundary the target runs, and a boundary charges the recurring for the
// period it OPENS. An unactivated target has no anchor yet, so both fall back to
// the transfer instant's default-anchored window rather than guessing.
func (s *pgxStore) transferWindows(ctx context.Context, qtx *db.Queries, account uuid.UUID, at time.Time) (TransferPeriod, time.Time, error) {
	anchor := billingperiod.DefaultAnchorDay
	activated, err := qtx.AccountActivatedAt(ctx, account.String())
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return TransferPeriod{}, time.Time{}, err
	}
	if err == nil && activated.Valid {
		anchor = billingperiod.AnchorDay(activated.Time)
	}
	start, end := billingperiod.AnchoredPeriodWindow(at, anchor)
	return TransferPeriod{Start: start, End: end}, end, nil
}

// moveWindowStart is the LATER of the two accounts' open-period starts.
//
// 🔴 THE LATER ONE, DELIBERATELY. An event older than the TARGET's open period
// would be re-attributed into a window the target has already closed and
// billed — backdating across an issued invoice, which INV-011 forbids. Taking
// max() leaves those events with the old account, which is where they were
// recorded and where they are already owed.
func (s *pgxStore) moveWindowStart(ctx context.Context, qtx *db.Queries, from, to uuid.UUID, at time.Time) (time.Time, error) {
	fromWindow, _, err := s.transferWindows(ctx, qtx, from, at)
	if err != nil {
		return time.Time{}, err
	}
	toWindow, _, err := s.transferWindows(ctx, qtx, to, at)
	if err != nil {
		return time.Time{}, err
	}
	if toWindow.Start.After(fromWindow.Start) {
		return toWindow.Start, nil
	}
	return fromWindow.Start, nil
}

// EnsureUserAccount is the user twin of EnsureOrgAccount: the same
// advisory-locked get-or-create, serialized on the user namespace ('lbta'),
// because ms_billing.accounts has no owner UNIQUE constraint and the lock IS
// the uniqueness guard.
//
// It creates without any funding check by design — see
// Service.transferTargetAccount. Do not reuse it for app CREATION, which must
// keep refusing an unfunded owner.
func (s *pgxStore) EnsureUserAccount(ctx context.Context, userID uuid.UUID) (uuid.UUID, error) {
	var id string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		if err := qtx.AcquireBillingAccountUserLock(ctx, db.AcquireBillingAccountUserLockParams{
			Column1: billingaccount.AdvisoryLockNamespaceBillingAccountUser,
			Column2: userID.String(),
		}); err != nil {
			return err
		}
		existing, err := qtx.SelectAccountByUser(ctx, pgtype.UUID{Bytes: userID, Valid: true})
		if err == nil {
			id = existing.ID
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		inserted, err := qtx.InsertUserAccount(ctx, pgtype.UUID{Bytes: userID, Valid: true})
		if err != nil {
			return err
		}
		id = inserted.ID
		return nil
	})
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(id)
}

// barrierBothAccounts takes the activation lock and the shared period barrier
// for both accounts, in a deterministic (sorted) order so two opposite-direction
// transfers cannot deadlock. A zero from-account (an unbilled org roster row)
// has no window to protect and is skipped.
func (s *pgxStore) barrierBothAccounts(ctx context.Context, tx pgx.Tx, qtx *db.Queries, from, to uuid.UUID, at time.Time) error {
	accounts := make([]uuid.UUID, 0, 2)
	if from != uuid.Nil {
		accounts = append(accounts, from)
	}
	accounts = append(accounts, to)
	sort.Slice(accounts, func(i, j int) bool {
		return accounts[i].String() < accounts[j].String()
	})
	for _, id := range accounts {
		if _, err := qtx.LockUsageAccountActivation(ctx, id.String()); err != nil {
			return err
		}
		window, _, err := s.transferWindows(ctx, qtx, id, at)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, meteringlock.SharedAdvisorySQL, meteringlock.PeriodKey(id, window.Start)); err != nil {
			return err
		}
	}
	return nil
}

// anyPeriodClosed reports whether either account's open window has already been
// closed or invoiced — read AFTER the barrier, so the answer cannot change
// under us for the rest of the transaction.
func (s *pgxStore) anyPeriodClosed(ctx context.Context, qtx *db.Queries, from, to uuid.UUID, at time.Time) (bool, error) {
	for _, id := range []uuid.UUID{from, to} {
		if id == uuid.Nil {
			continue
		}
		window, _, err := s.transferWindows(ctx, qtx, id, at)
		if err != nil {
			return false, err
		}
		_, err = qtx.ClosedBillingPeriodEndAtStart(ctx, db.ClosedBillingPeriodEndAtStartParams{
			AccountID:   id.String(),
			PeriodStart: window.Start,
		})
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return false, err
		}
	}
	return false, nil
}
