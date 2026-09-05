package cycle

import (
	"context"
	"errors"
	"fmt"
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
	// TransferAppUnknown — no LIVE ms_billing.apps row: never mirrored, or
	// soft-deleted. Both are NOT_FOUND to the caller.
	TransferAppUnknown
	// TransferChargesPending — a mid-period one-time charge is still owed for
	// this app AND the old account is about to settle it (or already has an
	// attempt in flight); re-keying now would bill it to the new account.
	// This is the BOUNDED refusal: when the old account cannot settle soon the
	// charge is forfeited instead and the transfer proceeds — see
	// transferChargeDisposition.
	TransferChargesPending
	// TransferPeriodClosed — the window the move would write into has already
	// been closed or invoiced for one of the accounts. Refusing is the only
	// safe answer: writing there backdates across a billed period.
	TransferPeriodClosed
	// TransferUnbilledBacklog — the app still has usage recorded with NO
	// account (the lazy org backlog, migration 041). The repoint sweep finds
	// that backlog through apps.owner_org_id, which this transfer rewrites:
	// re-key and the backlog is either billed to an org that never saw it or
	// stranded where no sweep can reach it. Refused until the old org funds.
	TransferUnbilledBacklog
)

// TransferForfeitReason is WHY a transfer forfeited the old account's
// unresolved one-time charges rather than refusing. Closed set, mirrored by
// the CHECK on app_transfer_events.forfeit_reason (071). Each value is one
// state in which the charge legs would skip the account transiently on every
// sweep — for as long as the account stays so — which is what made the
// unbounded refusal a transfer that could never happen.
type TransferForfeitReason string

const (
	// TransferForfeitNoPayer — the app was an unbilled org roster row
	// (account_id NULL): no account existed to settle anything.
	TransferForfeitNoPayer TransferForfeitReason = "no_payer"
	// TransferForfeitUnactivated — the old account never bound a card (D1d:
	// an unactivated account is never charged).
	TransferForfeitUnactivated TransferForfeitReason = "unactivated"
	// TransferForfeitPrepaid — the old account is in prepaid collection mode
	// (H10: never auto-charged off-session by any leg).
	TransferForfeitPrepaid TransferForfeitReason = "prepaid"
	// TransferForfeitNoPaymentMethod — activated and in arrears, but the
	// funder has no usable card, so every leg reads skipped_no_pm.
	TransferForfeitNoPaymentMethod TransferForfeitReason = "no_payment_method"
)

// transferForfeit is what one transfer forfeited. Decided BEFORE any write,
// from the classification read under the app row lock, and written to the
// ledger with the transfer — the row counts the forfeit writers report must
// match these numbers exactly, or the transaction aborts.
type transferForfeit struct {
	proration bool
	domains   int64
	timers    int64
	reason    TransferForfeitReason
}

func (f transferForfeit) any() bool {
	return f.proration || f.domains > 0 || f.timers > 0
}

// TransferApp re-points one app's billing account, in ONE transaction.
//
// LOCK ORDER, and every lock is required:
//  1. the per-app module-timer advisory lock — the key every timer-set writer
//     serializes on (lockModuleTimers). Without it a concurrent RegisterApp or
//     SyncAppModules reconciles timers against the roster it read BEFORE this
//     transfer and re-splits the attribution the transfer just aligned. It is
//     also what lets a NULL-source transfer synthesize the app's first timers
//     in THIS transaction (reconcileModuleTimersToTargetTx).
//  2. the apps row FOR UPDATE — serializes two concurrent transfers of the same
//     app, pins the from-account the event records, and is the row the
//     creation-proration sweep locks too (ChargeProrationLocked), so that
//     sweep cannot slip between the pending-charge read and the forfeit or
//     the re-key.
//  3. both accounts' activation rows FOR SHARE, then the period barrier, in
//     sorted account order (barrierBothAccounts).
//  4. the domain and timer rows, by the forfeit UPDATEs first and the re-key
//     UPDATEs second — same transaction, same order every time. The domain and
//     timer sweeps lock ONLY their own rows (ArmDomainStripeCharge,
//     ArmModuleTimerStripeCharge), never the app row, so they can interleave
//     with steps 1–3; the forfeit writers close that by re-checking the arm
//     marker in their WHERE and the store aborting on a row-count shortfall.
//     The 052 lifecycle guards these UPDATEs fire take SHARED org-lifecycle
//     advisory locks; only FinalizeOrgDeletion takes them exclusively.
//
// EVERY REFUSAL IS DECIDED BEFORE THE FIRST WRITE. The transaction commits on
// a refusal too (pgx.BeginFunc commits a nil return), so a write that
// preceded a refusal would land without the transfer it belonged to.
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

		var fromAccount uuid.UUID
		if app.AccountID.Valid {
			fromAccount = app.AccountID.Bytes
		}

		// 🔴 THE BACKLOG REFUSAL. Usage this app recorded with NO account is
		// reachable only through apps.owner_org_id (RepointOrgNullAccountEvents),
		// which the re-key below rewrites. Moving the app would either hand
		// the backlog to an org that never incurred it or strand it where no
		// sweep can bill it. Neither is this RPC's money outcome to choose.
		backlog, err := qtx.AppHasUnbilledUsageBacklog(ctx, p.AppID.String())
		if err != nil {
			return err
		}
		if backlog {
			outcome = TransferUnbilledBacklog
			return nil
		}

		// 🔴 THE MONEY DECISION. Creation proration, custom-domain activation
		// and per-module grace overage each charge whoever the row points at
		// WHEN THE SWEEP RUNS, so none of them may travel with the re-key.
		// Whether that means REFUSE (the old account settles it first) or
		// FORFEIT (it never will) is decided here, before any write.
		charges, err := qtx.AppUnresolvedOneTimeCharges(ctx, p.AppID.String())
		if err != nil {
			return err
		}
		forfeit, refuse, err := s.transferChargeDisposition(ctx, qtx, fromAccount, charges)
		if err != nil {
			return err
		}
		if refuse {
			outcome = TransferChargesPending
			return nil
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

		// The last refusal is behind us: from here every statement writes,
		// and the forfeit goes first so the rows are resolved while they still
		// name the account that owed them.
		if err := s.forfeitTransferCharges(ctx, qtx, p, forfeit); err != nil {
			return err
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

		// handoff is the instant the OLD account's attribution of this app's
		// usage ENDS: the transfer instant — unless a move pulls the open
		// window forward, in which case it is that window's start, because
		// every sample from there on has just been handed to the new account.
		// It is where the level streams below are cut.
		var moved int64
		handoff := p.At
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
			handoff = start
		}

		// 🔴 THE LEVEL STREAMS ARE CUT, IN BOTH MODES. A time_weighted gauge
		// is integrated as a step function, and the rollup holds a stream's
		// LAST sample in a period until PERIOD END. Left alone, the old
		// account's rollup would keep billing the level it last saw across the
		// rest of its period — the same stretch the new account bills from its
		// own samples. One zero-level sample per stream at the hand-off makes
		// the old integral stop there. After the move, so the query sees what
		// the old account still holds; under the period barrier taken above,
		// so the rollup cannot be closing the period the sample lands in.
		if _, err := s.terminateLevelStreams(ctx, qtx, p, fromAccount, handoff); err != nil {
			return err
		}

		// An unbilled org roster row had no account to tier on, so it holds
		// no timers (ReconcileModuleTimersToTarget declines to synthesize
		// against a NULL account). Now that it has one, synthesize them the
		// way the org attach sweep does (attachOrgBilling): fresh, anchored at
		// the transfer instant, grace running from now — prospective billing,
		// never a window the new account did not own. In THIS transaction,
		// under the advisory lock taken at the top, so there is no committed
		// state in which the app has an account and no timers.
		if fromAccount == uuid.Nil {
			if err := reconcileModuleTimersToTargetTx(ctx, qtx, p.AppID, p.At, moduleGraceExpiry(p.At), p.At); err != nil {
				return err
			}
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
		reasonCol := pgtype.Text{}
		if forfeit.any() {
			reasonCol = pgtype.Text{String: string(forfeit.reason), Valid: true}
		}
		if err := qtx.InsertAppTransferEvent(ctx, db.InsertAppTransferEventParams{
			RequestID:            p.RequestID.String(),
			AppID:                p.AppID.String(),
			FromAccount:          fromCol,
			ToAccount:            p.ToAccount.String(),
			Mode:                 p.Mode,
			MovedEventCount:      moved,
			At:                   p.At,
			OpenPeriodStart:      window.Start,
			OpenPeriodEnd:        window.End,
			RecurringFrom:        from,
			ForfeitedProration:   forfeit.proration,
			ForfeitedDomainCount: forfeit.domains,
			ForfeitedTimerCount:  forfeit.timers,
			ForfeitReason:        reasonCol,
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

// transferChargeDisposition decides what happens to the one-time charges the
// old account still owes for this app: nothing (none pending), REFUSE, or
// FORFEIT — and if forfeit, why.
//
// 🔴 THE BOUNDED RULE (decided 2026-09-04 after the be#193 review). The first
// version refused whenever anything was pending, and "pending" includes states
// the old account can NEVER leave on its own: skipped_no_pm and skipped_prepaid
// re-attempt on every sweep (proration.go), and an unactivated account arms no
// marker at all. A personal, never-funded account handing its app to an org —
// the owner's own flow — would have been refused indefinitely. So:
//
//   - REFUSE only when the old account can settle soon: activated AND in
//     arrears AND a usable card on its funder (TransferSourceSettlement — the
//     three gates the legs themselves apply). The next sweep collects, and the
//     caller retries after it.
//   - REFUSE, unconditionally, when a charge is already IN FLIGHT: armed at the
//     provider (charge_attempted_at) or frozen into an unresolved combined
//     attempt. Money may have moved; forfeiting it would leave a collected
//     charge with no mirror, and the recovery legs converge it soon anyway.
//     This is not a widening of the refusal — an armed attempt IS the old
//     account settling — and it is the one state the D1d posture does not
//     reach, because D1d forgives charges that never started.
//   - FORFEIT otherwise. The charges are resolved inside this transaction
//     without being collected and never carried to the new owner: the
//     no-retroactive-catch-up posture of D1d, applied at the instant the app
//     leaves the account that could not pay for it.
//
// A sealed proposal (a resolved combined attempt) is not pending at all — the
// intent rail owns it — and the classification query already reads it so.
//
// The reason is the FIRST failing gate in the legs' own order (activation,
// then mode, then card), which is also the order in which an account acquires
// them.
func (s *pgxStore) transferChargeDisposition(ctx context.Context, qtx *db.Queries, from uuid.UUID, charges db.AppUnresolvedOneTimeChargesRow) (transferForfeit, bool, error) {
	pending := charges.ProrationPending || charges.DomainPending > 0 || charges.TimerPending > 0
	if !pending {
		return transferForfeit{}, false, nil
	}
	if charges.ProrationInFlight || charges.DomainInFlight > 0 || charges.TimerInFlight > 0 {
		return transferForfeit{}, true, nil
	}
	forfeit := transferForfeit{
		proration: charges.ProrationPending,
		domains:   charges.DomainPending,
		timers:    charges.TimerPending,
	}
	if from == uuid.Nil {
		// An unbilled org roster row: there is no account whose activation,
		// mode or card could be asked about. Nothing to settle with.
		forfeit.reason = TransferForfeitNoPayer
		return forfeit, false, nil
	}
	src, err := qtx.TransferSourceSettlement(ctx, from.String())
	if err != nil {
		// ErrNoRows included: the roster row's account_id is a hard FK, so a
		// missing accounts row under the app lock is a code bug, not a skip.
		return transferForfeit{}, false, fmt.Errorf("transfer source account %s: %w", from, err)
	}
	switch {
	case !src.Activated:
		forfeit.reason = TransferForfeitUnactivated
	case !src.Arrears:
		forfeit.reason = TransferForfeitPrepaid
	case !src.HasUsablePaymentMethod:
		forfeit.reason = TransferForfeitNoPaymentMethod
	default:
		return transferForfeit{}, true, nil
	}
	return forfeit, false, nil
}

// forfeitTransferCharges performs the forfeit transferChargeDisposition
// decided: the skip marker, then every pending domain and timer, each stamped
// with the transfer's request_id.
//
// 🔴 EVERY WRITER'S ROW COUNT MUST EQUAL WHAT WAS READ AS PENDING. The
// classification and these UPDATEs are one transaction under the app row
// lock, but the domain and timer sweeps lock only their own rows, so an arm
// can commit between the read and the write. Each writer excludes an armed
// row in its WHERE; a shortfall here is therefore exactly that race, and the
// only safe answer is to abort — the caller retries, and the retry reads the
// arm as in-flight and refuses. Silently forfeiting fewer rows than the
// ledger will say was forfeited is the kind of mismatch nothing downstream
// would ever notice.
func (s *pgxStore) forfeitTransferCharges(ctx context.Context, qtx *db.Queries, p TransferAppParams, f transferForfeit) error {
	if !f.any() {
		return nil
	}
	if f.proration {
		n, err := qtx.ForfeitAppProrationOnTransfer(ctx, db.ForfeitAppProrationOnTransferParams{
			At:    p.At,
			AppID: p.AppID.String(),
		})
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("transfer %s: app %s read as owing its creation proration but the skip marker landed on %d rows", p.RequestID, p.AppID, n)
		}
	}
	if f.domains > 0 {
		n, err := qtx.ForfeitAppDomainChargesOnTransfer(ctx, db.ForfeitAppDomainChargesOnTransferParams{
			RequestID: p.RequestID.String(),
			AppID:     p.AppID.String(),
		})
		if err != nil {
			return err
		}
		if n != f.domains {
			return fmt.Errorf("transfer %s: app %s read %d pending domain charges but %d were forfeited; a domain sweep armed one in between, retry", p.RequestID, p.AppID, f.domains, n)
		}
	}
	if f.timers > 0 {
		n, err := qtx.ForfeitAppModuleTimersOnTransfer(ctx, db.ForfeitAppModuleTimersOnTransferParams{
			RequestID: p.RequestID.String(),
			AppID:     p.AppID.String(),
		})
		if err != nil {
			return err
		}
		if n != f.timers {
			return fmt.Errorf("transfer %s: app %s read %d pending module timers but %d were forfeited; an overage sweep armed one in between, retry", p.RequestID, p.AppID, f.timers, n)
		}
	}
	return nil
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

// terminateLevelStreams writes the terminal zero-level sample for every
// time_weighted stream the OLD account still holds for this app inside its
// open period, at handoff — the instant its attribution of the stream ends.
// The arithmetic, the choice of instant and what is deliberately left alone
// (peak) are on TerminateAppLevelStreamsOnTransfer in app_transfer.sql.
//
// The range is [old open-period start, handoff): a stream with no sample left
// there contributes nothing to the old account's integral and needs no
// terminal. A zero from-account (an unbilled org roster row) holds no stream
// at all — its usage is the NULL-account backlog this transfer refused on.
//
// Returns the number of streams cut; the rows name the transfer themselves
// (event_id and metadata), so the ledger carries no separate count.
func (s *pgxStore) terminateLevelStreams(ctx context.Context, qtx *db.Queries, p TransferAppParams, from uuid.UUID, handoff time.Time) (int64, error) {
	if from == uuid.Nil {
		return 0, nil
	}
	fromWindow, _, err := s.transferWindows(ctx, qtx, from, p.At)
	if err != nil {
		return 0, err
	}
	return qtx.TerminateAppLevelStreamsOnTransfer(ctx, db.TerminateAppLevelStreamsOnTransferParams{
		RequestID:   p.RequestID.String(),
		AccountID:   from.String(),
		AppID:       p.AppID.String(),
		At:          handoff,
		PeriodStart: fromWindow.Start,
	})
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
