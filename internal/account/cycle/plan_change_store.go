package cycle

// The pgxStore half of the plan-change ledger (migration 076) and the member
// history (migration 077). Kept beside store.go so the money transactions of
// one feature read as one file: OpenPlanChange (the decision, the row, the
// plan flip and the wallet draw, all under the app lock), the boundary apply
// (with the cap re-check), and the member high-water read.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

func planChangeFromRow(row db.MsBillingAppPlanChange) (PlanChange, error) {
	id, err := uuid.Parse(row.ID)
	if err != nil {
		return PlanChange{}, err
	}
	appID, err := uuid.Parse(row.AppID)
	if err != nil {
		return PlanChange{}, err
	}
	accountID, err := uuid.Parse(row.AccountID)
	if err != nil {
		return PlanChange{}, err
	}
	return PlanChange{
		ID: id, AppID: appID, AccountID: accountID,
		FromPlan: usage.Plan(row.FromPlan), ToPlan: usage.Plan(row.ToPlan), Kind: PlanChangeKind(row.Kind),
		RequestedAt: row.RequestedAt, EffectiveAt: row.EffectiveAt,
		PeriodStart: row.PeriodStart, PeriodEnd: row.PeriodEnd,
		FoldedIntoCreation: row.FoldedIntoCreation,
		AmountMicros:       row.AmountMicros, WalletMicros: row.WalletMicros, CardMicros: row.CardMicros,
		WalletDecided:   row.WalletDecidedAt.Valid,
		WalletDecidedAt: row.WalletDecidedAt.Time,
		CardRef:         row.CardRef.String,
		CardWindowStart: row.CardWindowStart.Time,
		Status:          PlanChangeStatus(row.Status),
		SettledAt:       row.SettledAt.Time,
	}, nil
}

func planChangesFromRows(rows []db.MsBillingAppPlanChange) ([]PlanChange, error) {
	out := make([]PlanChange, 0, len(rows))
	for _, r := range rows {
		c, err := planChangeFromRow(r)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// planCommitments counts the owner's live apps on `plan` or scheduled to move
// to it, excluding exceptAppID, UNDER the owner's advisory lock — the Free cap
// input. The lock is transaction-scoped: the caller commits the write that
// adds a commitment before releasing it, so two concurrent callers cannot
// both read "2 of 3" and both commit.
func planCommitments(ctx context.Context, qtx *db.Queries, accountID, ownerOrgID uuid.UUID, plan usage.Plan, exceptAppID uuid.UUID) (int, error) {
	ownerID := accountID
	if ownerOrgID != uuid.Nil {
		ownerID = ownerOrgID
	}
	if err := qtx.LockPlanCapOwner(ctx, ownerID.String()); err != nil {
		return 0, err
	}
	if ownerOrgID != uuid.Nil {
		n, err := qtx.CountOrgPlanCommitments(ctx, db.CountOrgPlanCommitmentsParams{
			OwnerOrgID: ownerOrgID.String(), Plan: string(plan), ExceptAppID: exceptAppID.String(),
		})
		return int(n), err
	}
	n, err := qtx.CountUserPlanCommitments(ctx, db.CountUserPlanCommitmentsParams{
		AccountID: accountID.String(), Plan: string(plan), ExceptAppID: exceptAppID.String(),
	})
	return int(n), err
}

// capReached reports whether the owner may commit one more app to `plan`:
// false when the plan is uncapped for that owner kind, else the count under
// the lock against usage.PlanTerms.MaxAppsFor.
func capReached(ctx context.Context, qtx *db.Queries, accountID, ownerOrgID uuid.UUID, plan usage.Plan, exceptAppID uuid.UUID) (bool, error) {
	limit := usage.TermsFor(plan).MaxAppsFor(ownerOrgID != uuid.Nil)
	if limit == usage.Unlimited {
		return false, nil
	}
	n, err := planCommitments(ctx, qtx, accountID, ownerOrgID, plan, exceptAppID)
	if err != nil {
		return false, err
	}
	return n >= limit, nil
}

// OpenPlanChange is the ONE transaction that opens a change: it locks the app
// row, re-verifies it live and still on FromPlan, returns an open change if
// one exists, enforces the destination plan's per-owner cap under the owner
// lock, and for an upgrade DECIDES — from the locked creation markers, never
// the caller's unlocked read — whether the change folds into a still-unbilled
// creation charge or is charged now, inserts the row, flips apps.plan, and in
// credits mode takes the wallet decision (a lots-only draw, capped at the
// posted balance net of expired grants). A wallet that cannot cover the
// amount with no card to take the remainder rolls the WHOLE transaction back:
// nothing written, the plan unmoved — the refusal the owner allowed is
// traceless.
//
// A creation-proration sweep pricing this app concurrently locks the same row
// (FreezeCombinedProrationAttempt / DrawCreationProrationFromWallet) and
// compares the plan it priced against the locked row's, so a fold committed
// here between its derivation and its freeze makes that attempt stale rather
// than mispriced. And because the fold is decided under the same lock those
// legs stamp their markers under, a CHARGED upgrade can only exist once the
// creation window is armed, skipped or attempted — the invariant the creation
// segments rely on.
func (s *pgxStore) OpenPlanChange(ctx context.Context, p OpenPlanChangeParams) (PlanChange, OpenPlanChangeOutcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return PlanChange{}, 0, err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)

	row, err := qtx.SelectAppMirrorForUpdate(ctx, p.AppID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return PlanChange{}, PlanChangeAppStale, nil
	}
	if err != nil {
		return PlanChange{}, 0, err
	}
	// The open row is checked BEFORE the derivation is: a second caller that
	// lost the race to a concurrent upgrade sees the plan already flipped, and
	// what it needs is that upgrade's row to resume, not a "stale" refusal
	// that sends it round again.
	existing, err := qtx.OpenPlanChangeForApp(ctx, p.AppID.String())
	if err == nil {
		c, err := planChangeFromRow(existing)
		if err != nil {
			return PlanChange{}, 0, err
		}
		if err := tx.Commit(ctx); err != nil {
			return PlanChange{}, 0, err
		}
		return c, PlanChangeExisting, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return PlanChange{}, 0, err
	}
	if row.DeletedAt.Valid || usage.Plan(row.Plan) != p.FromPlan || uuidFromPg(row.AccountID) != p.AccountID {
		return PlanChange{}, PlanChangeAppStale, nil
	}

	// The destination plan's cap, under the owner lock, counting apps already
	// on it AND downgrades scheduled to it.
	if full, err := capReached(ctx, qtx, p.AccountID, uuidFromPg(row.OwnerOrgID), p.ToPlan, p.AppID); err != nil {
		return PlanChange{}, 0, err
	} else if full {
		return PlanChange{}, PlanChangeCapReached, nil
	}

	shape := p.Downgrade
	folded := false
	status := PlanChangeScheduled
	if p.Kind == PlanChangeUpgrade {
		// 🔴 THE FOLD DECISION, FROM THE LOCKED ROW. The creation window is
		// still unbilled while all three markers are NULL — not "inside the
		// grace": the sweep can run days after it, and until it has, the
		// creation charge is the one charge pricing these days.
		folded = !row.ProrationInvoiceID.Valid && !row.ProrationSkippedAt.Valid && !row.ProrationAttemptedAt.Valid
		if folded {
			shape = p.Folded
		} else {
			shape = p.Charged
		}
		if shape == nil {
			return PlanChange{}, 0, fmt.Errorf("plan change for app %s: no %s shape supplied", p.AppID, map[bool]string{true: "folded", false: "charged"}[folded])
		}
		status = PlanChangePending
		if shape.AmountMicros == 0 {
			status = PlanChangeSettled
		}
		if status == PlanChangePending && !p.ChargeAllowed {
			// The caller expected a fold and its gates did not pass for a
			// charge; the locked row says charge. Nothing is written.
			return PlanChange{}, PlanChangeChargeRefused, nil
		}
	}
	if shape == nil {
		return PlanChange{}, 0, fmt.Errorf("plan change for app %s: no downgrade shape supplied", p.AppID)
	}

	settledAt := pgtype.Timestamptz{}
	decidedAt := pgtype.Timestamptz{}
	if status == PlanChangeSettled {
		// Nothing to collect — a fold, or a $0 delta. The wallet decision is
		// "0", taken now, so the row reads like every other settled upgrade.
		settledAt = pgtype.Timestamptz{Time: p.RequestedAt.UTC(), Valid: true}
		decidedAt = settledAt
	}
	inserted, err := qtx.InsertPlanChange(ctx, db.InsertPlanChangeParams{
		AppID: p.AppID.String(), AccountID: p.AccountID.String(),
		FromPlan: string(p.FromPlan), ToPlan: string(p.ToPlan), Kind: string(p.Kind),
		RequestedAt: p.RequestedAt.UTC(), EffectiveAt: shape.EffectiveAt.UTC(),
		PeriodStart: shape.PeriodStart.UTC(), PeriodEnd: shape.PeriodEnd.UTC(),
		FoldedIntoCreation: folded, AmountMicros: shape.AmountMicros,
		WalletDecidedAt: decidedAt, Status: string(status), SettledAt: settledAt,
	})
	if err != nil {
		return PlanChange{}, 0, err
	}
	changeID, err := uuid.Parse(inserted.ID)
	if err != nil {
		return PlanChange{}, 0, err
	}

	if p.Kind == PlanChangeUpgrade {
		n, err := qtx.SetAppPlan(ctx, db.SetAppPlanParams{AppID: p.AppID.String(), Plan: string(p.ToPlan)})
		if err != nil {
			return PlanChange{}, 0, err
		}
		if n != 1 {
			return PlanChange{}, 0, fmt.Errorf("plan change %s: the plan flip moved %d rows under the app lock", inserted.ID, n)
		}
	}

	// The wallet decision, in this same transaction, for a pending upgrade.
	if status == PlanChangePending {
		drawn := int64(0)
		if p.Wallet != nil && p.Wallet.Credits {
			var short bool
			drawn, short, err = drawPlanChangeLots(ctx, qtx, p.AccountID, changeID, shape.AmountMicros, p.Wallet.AllowRemainder)
			if err != nil {
				return PlanChange{}, 0, err
			}
			if short {
				// Rolled back by the deferred rollback: no row, no flip, no
				// draw. The owner's "refused only with no usable card either".
				return PlanChange{}, PlanChangeOpenWalletShort, nil
			}
		}
		n, err := qtx.DecidePlanChangeWallet(ctx, db.DecidePlanChangeWalletParams{
			WalletMicros: drawn, DecidedAt: p.RequestedAt.UTC(), ID: inserted.ID,
		})
		if err != nil {
			return PlanChange{}, 0, err
		}
		if n != 1 {
			return PlanChange{}, 0, fmt.Errorf("plan change %s: the wallet decision moved %d rows in the opening transaction", inserted.ID, n)
		}
	}

	final, err := qtx.PlanChangeByID(ctx, inserted.ID)
	if err != nil {
		return PlanChange{}, 0, err
	}
	change, err := planChangeFromRow(final)
	if err != nil {
		return PlanChange{}, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return PlanChange{}, 0, err
	}
	return change, PlanChangeOpened, nil
}

// drawPlanChangeLots is the credits-mode wallet leg of an upgrade: under the
// account and ledger locks, draw from the spendable LOTS ONLY, up to
// amountMicros and capped at the posted balance net of expired grants (the
// same cap the standard-mode boundary draw applies, so an upgrade never
// drives the wallet negative — unlike a credits-mode boundary draw, which may
// write an unsecured remainder under the credit policy; the owner's rule for
// an upgrade is that the card takes what the wallet does not hold). The mode
// is read under the lock: an account that moved to standard decides 0.
//
// short=true (the wallet cannot cover the amount and no remainder is allowed)
// writes nothing and leaves the caller to roll back.
func drawPlanChangeLots(ctx context.Context, qtx *db.Queries, accountID, changeID uuid.UUID, amountMicros int64, allowRemainder bool) (drawn int64, short bool, err error) {
	rawMode, err := qtx.LockWalletAccount(ctx, accountID.String())
	if err != nil {
		return 0, false, err
	}
	mode, err := parseCreditBillingMode(rawMode)
	if err != nil {
		return 0, false, err
	}
	if mode != CreditBillingModeCredits {
		// A standard account's credit applies at the boundary spine, not to a
		// mid-period charge — the creation charge takes the same posture.
		if !allowRemainder {
			return 0, true, nil
		}
		return 0, false, nil
	}
	if _, err := qtx.LockWalletLedgerEntries(ctx, accountID.String()); err != nil {
		return 0, false, err
	}
	balanceAfter, err := qtx.WalletSettledBalance(ctx, accountID.String())
	if err != nil {
		return 0, false, err
	}
	expiredMicros, err := qtx.WalletExpiredCreditBalance(ctx, accountID.String())
	if err != nil {
		return 0, false, err
	}
	if expiredMicros < 0 {
		return 0, false, fmt.Errorf("wallet expired-credit balance is negative: %d", expiredMicros)
	}
	capMicros := balanceAfter - expiredMicros
	if capMicros < 0 {
		capMicros = 0
	}
	lots, err := qtx.WalletSpendableLots(ctx, accountID.String())
	if err != nil {
		return 0, false, err
	}
	var coverable int64
	for _, lot := range lots {
		if lot.RemainingMicros <= 0 {
			return 0, false, fmt.Errorf(
				"wallet query returned a non-positive lot remainder: source=%s remaining=%d",
				lot.ID, lot.RemainingMicros,
			)
		}
		if coverable > math.MaxInt64-lot.RemainingMicros {
			coverable = math.MaxInt64
			break
		}
		coverable += lot.RemainingMicros
	}
	if coverable > capMicros {
		coverable = capMicros
	}
	if coverable < amountMicros && !allowRemainder {
		return 0, true, nil
	}

	left := amountMicros
	if left > coverable {
		left = coverable
	}
	for _, lot := range lots {
		if left == 0 {
			break
		}
		consume := lot.RemainingMicros
		if consume > left {
			consume = left
		}
		if balanceAfter < math.MinInt64+consume {
			return 0, false, fmt.Errorf("wallet balance_after_micros underflow: balance=%d draw=%d", balanceAfter, consume)
		}
		balanceAfter -= consume
		if err := qtx.InsertPlanChangeWalletDraw(ctx, db.InsertPlanChangeWalletDrawParams{
			AccountID:          accountID.String(),
			AmountMicros:       consume,
			BalanceAfterMicros: balanceAfter,
			IdempotencyKey: fmt.Sprintf(
				"wallet-draw:plan-change:%s:subscription_draw:%s", changeID.String(), lot.ID,
			),
			SourceCreditID: lot.ID,
		}); err != nil {
			return 0, false, err
		}
		left -= consume
		drawn += consume
	}
	return drawn, false, nil
}

func (s *pgxStore) OpenPlanChangeForApp(ctx context.Context, appID uuid.UUID) (PlanChange, bool, error) {
	row, err := s.q.OpenPlanChangeForApp(ctx, appID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return PlanChange{}, false, nil
	}
	if err != nil {
		return PlanChange{}, false, err
	}
	c, err := planChangeFromRow(row)
	return c, err == nil, err
}

func (s *pgxStore) PlanChange(ctx context.Context, id uuid.UUID) (PlanChange, bool, error) {
	row, err := s.q.PlanChangeByID(ctx, id.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return PlanChange{}, false, nil
	}
	if err != nil {
		return PlanChange{}, false, err
	}
	c, err := planChangeFromRow(row)
	return c, err == nil, err
}

func (s *pgxStore) PendingPlanChanges(ctx context.Context, requestedBefore time.Time) ([]PlanChange, error) {
	rows, err := s.q.PendingPlanChanges(ctx, requestedBefore.UTC())
	if err != nil {
		return nil, err
	}
	return planChangesFromRows(rows)
}

func (s *pgxStore) EffectivePlanChanges(ctx context.Context, appID uuid.UUID) ([]PlanChange, error) {
	rows, err := s.q.EffectivePlanChangesForApp(ctx, appID.String())
	if err != nil {
		return nil, err
	}
	return planChangesFromRows(rows)
}

// DrawPlanChangeFromWallet takes the wallet decision for a pending upgrade
// whose opening transaction did not (a row written before the decision moved
// into OpenPlanChange): the same lots-only, balance-capped draw, once.
func (s *pgxStore) DrawPlanChangeFromWallet(
	ctx context.Context, change PlanChange, allowRemainder bool, at time.Time,
) (PlanChangeWalletOutcome, int64, error) {
	if change.AmountMicros <= 0 {
		return PlanChangeWalletAlreadyDecided, 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)

	locked, err := qtx.LockPlanChange(ctx, change.ID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return PlanChangeWalletAlreadyDecided, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	if PlanChangeStatus(locked.Status) != PlanChangePending || locked.WalletDecidedAt.Valid {
		return PlanChangeWalletAlreadyDecided, 0, nil
	}
	if locked.AccountID != change.AccountID.String() {
		return 0, 0, fmt.Errorf("plan change %s: account differs from the row's under the lock", change.ID)
	}
	drawn, short, err := drawPlanChangeLots(ctx, qtx, change.AccountID, change.ID, change.AmountMicros, allowRemainder)
	if err != nil {
		return 0, 0, err
	}
	if short {
		return PlanChangeWalletShort, 0, nil
	}
	n, err := qtx.DecidePlanChangeWallet(ctx, db.DecidePlanChangeWalletParams{
		WalletMicros: drawn, DecidedAt: at.UTC(), ID: change.ID.String(),
	})
	if err != nil {
		return 0, 0, err
	}
	if n != 1 {
		return 0, 0, fmt.Errorf("plan change %s: the wallet decision moved %d rows under the lock", change.ID, n)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return PlanChangeWalletDecided, drawn, nil
}

func (s *pgxStore) EnsurePlanChangeCardWindow(ctx context.Context, id uuid.UUID, windowStart, reanchorBefore time.Time) (time.Time, error) {
	got, err := s.q.SetPlanChangeCardWindow(ctx, db.SetPlanChangeCardWindowParams{
		ReanchorBefore: reanchorBefore.UTC(), WindowStart: windowStart.UTC(), ID: id.String(),
	})
	if err != nil {
		return time.Time{}, err
	}
	if !got.Valid {
		return time.Time{}, fmt.Errorf("plan change %s: no card window after the anchor write", id)
	}
	return got.Time, nil
}

func (s *pgxStore) SettlePlanChangeCard(ctx context.Context, id uuid.UUID, cardMicros int64, cardRef string, at time.Time) (bool, error) {
	n, err := s.q.SettlePlanChangeCard(ctx, db.SettlePlanChangeCardParams{
		CardMicros: cardMicros, CardRef: cardRef, SettledAt: at.UTC(), ID: id.String(),
	})
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *pgxStore) CancelScheduledPlanChange(ctx context.Context, appID uuid.UUID, at time.Time) (bool, error) {
	n, err := s.q.CancelScheduledPlanChange(ctx, db.CancelScheduledPlanChangeParams{
		CancelledAt: at.UTC(), AppID: appID.String(),
	})
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// applyDueRows moves each due downgrade onto its plan — unless the
// destination plan's per-owner cap is full at the boundary, in which case the
// row is cancelled with a logged reason (a scheduled downgrade is a
// commitment counted against the cap, so this is only reachable when a
// concurrent commitment slipped past, or the cap itself moved). Each row's
// flip and close are one statement pair inside the caller's transaction.
func applyDueRows(ctx context.Context, qtx *db.Queries, rows []db.MsBillingAppPlanChange, dueAt time.Time) (applied, cancelled int, err error) {
	for _, row := range rows {
		app, err := qtx.SelectAppMirrorForUpdate(ctx, row.AppID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return applied, cancelled, err
		}
		appID, perr := uuid.Parse(row.AppID)
		if perr != nil {
			return applied, cancelled, perr
		}
		accountID, perr := uuid.Parse(row.AccountID)
		if perr != nil {
			return applied, cancelled, perr
		}
		live := err == nil && !app.DeletedAt.Valid
		if live {
			full, cerr := capReached(ctx, qtx, accountID, uuidFromPg(app.OwnerOrgID), usage.Plan(row.ToPlan), appID)
			if cerr != nil {
				return applied, cancelled, cerr
			}
			if full {
				n, cerr := qtx.CancelPlanChangeByID(ctx, db.CancelPlanChangeByIDParams{CancelledAt: dueAt.UTC(), ID: row.ID})
				if cerr != nil {
					return applied, cancelled, cerr
				}
				if n == 1 {
					slog.WarnContext(ctx, "scheduled plan change cancelled at the boundary: the destination plan's cap is full",
						"plan_change_id", row.ID, "app_id", row.AppID, "to_plan", row.ToPlan)
					cancelled++
				}
				continue
			}
			if _, err := qtx.SetAppPlan(ctx, db.SetAppPlanParams{AppID: row.AppID, Plan: row.ToPlan}); err != nil {
				return applied, cancelled, err
			}
		}
		n, err := qtx.MarkPlanChangeApplied(ctx, db.MarkPlanChangeAppliedParams{AppliedAt: dueAt.UTC(), ID: row.ID})
		if err != nil {
			return applied, cancelled, err
		}
		if n != 1 {
			return applied, cancelled, fmt.Errorf("plan change %s: applying moved %d rows under the lock", row.ID, n)
		}
		applied++
	}
	return applied, cancelled, nil
}

// ApplyDuePlanChanges moves every due scheduled downgrade of ONE account onto
// its plan (the belt inside RunBillingCycle). Idempotent: an applied or
// cancelled row drops out of the due list.
func (s *pgxStore) ApplyDuePlanChanges(ctx context.Context, accountID uuid.UUID, dueAt time.Time) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)
	due, err := qtx.DuePlanChangesForAccount(ctx, db.DuePlanChangesForAccountParams{
		AccountID: accountID.String(), DueAt: dueAt.UTC(),
	})
	if err != nil {
		return 0, err
	}
	applied, _, err := applyDueRows(ctx, qtx, due, dueAt)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return applied, nil
}

// ApplyAllDuePlanChanges is the driver's global apply: every account's due
// downgrades, including accounts the charge phase never reaches (an account
// whose only apps are still in their creation grace has no boundary run, and
// its downgrade must not land a period late).
func (s *pgxStore) ApplyAllDuePlanChanges(ctx context.Context, dueAt time.Time) (applied, cancelled int, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)
	due, err := qtx.DuePlanChangesAll(ctx, dueAt.UTC())
	if err != nil {
		return 0, 0, err
	}
	applied, cancelled, err = applyDueRows(ctx, qtx, due, dueAt)
	if err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return applied, cancelled, nil
}

// InsertFreeAppMirror registers a NEW app on the Free plan under the owner's
// cap lock: the commitment count and the insert are one transaction, so two
// concurrent creates cannot both take the last slot. capReached=true inserts
// nothing. A retry of an already-mirrored app is the ordinary idempotent
// no-op (never re-gated), like InsertAppMirror.
func (s *pgxStore) InsertFreeAppMirror(ctx context.Context, appID, accountID, ownerOrgID uuid.UUID, moduleCount, memberCount int, createdAt time.Time, name string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)
	if _, err := qtx.SelectAppMirror(ctx, appID.String()); err == nil {
		return false, nil // already mirrored: the first registration stands
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	full, err := capReached(ctx, qtx, accountID, ownerOrgID, usage.PlanFree, appID)
	if err != nil {
		return false, err
	}
	if full {
		return true, nil
	}
	if err := insertAppMirror(ctx, qtx, appID, accountID, ownerOrgID, moduleCount, memberCount, createdAt, name, usage.PlanFree); err != nil {
		return false, err
	}
	return false, tx.Commit(ctx)
}

// insertAppMirror is the roster insert plus, for a FRESH row, the first
// member-count history row at created_at (migration 077).
func insertAppMirror(ctx context.Context, qtx *db.Queries, appID, accountID, ownerOrgID uuid.UUID, moduleCount, memberCount int, createdAt time.Time, name string, plan usage.Plan) error {
	if plan == "" {
		plan = usage.DefaultPlan
	}
	n, err := qtx.InsertAppMirror(ctx, db.InsertAppMirrorParams{
		AppID:       appID.String(),
		AccountID:   pgUUIDOrNull(accountID),
		OwnerOrgID:  pgUUIDOrNull(ownerOrgID),
		ModuleCount: int32(moduleCount), //nolint:gosec // RegisterApp validates 0 ≤ count ≤ maxModuleCount (100000), far below int32 max
		MemberCount: int32(memberCount), //nolint:gosec // RegisterApp validates 0 ≤ count ≤ maxModuleCount (100000), far below int32 max
		CreatedAt:   createdAt,
		Name:        pgtype.Text{String: name, Valid: name != ""}, // NULL when the caller omits a name (frontend falls back)
		Plan:        string(plan),
	})
	if err != nil {
		return err
	}
	if n == 1 {
		return qtx.InsertAppMemberCount(ctx, db.InsertAppMemberCountParams{
			AppID: appID.String(), Count: int32(memberCount), RecordedAt: createdAt.UTC(), //nolint:gosec // validated above
		})
	}
	return nil
}

// SetAppMemberCount writes the new live count and, when it took, its history
// row, in one transaction. 0 rows on the update = the app is deleted (count
// frozen, no history); existence was already checked by the service.
func (s *pgxStore) SetAppMemberCount(ctx context.Context, appID uuid.UUID, memberCount int, at time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)
	n, err := qtx.SetAppMemberCount(ctx, db.SetAppMemberCountParams{
		AppID:       appID.String(),
		MemberCount: int32(memberCount), //nolint:gosec // SyncAppModules validates 0 ≤ count ≤ maxModuleCount (100000), far below int32 max
	})
	if err != nil {
		return err
	}
	if n == 1 {
		if err := qtx.InsertAppMemberCount(ctx, db.InsertAppMemberCountParams{
			AppID: appID.String(), Count: int32(memberCount), RecordedAt: at.UTC(), //nolint:gosec // validated above
		}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// MemberHighWater returns, per live app on the account, the period's
// high-water member count (owner 2026-09-13): the greater of the count in
// force when the period opened and the highest count recorded inside it.
func (s *pgxStore) MemberHighWater(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (map[uuid.UUID]int, error) {
	rows, err := s.q.MemberHighWaterForAccount(ctx, db.MemberHighWaterForAccountParams{
		PeriodStart: periodStart.UTC(), PeriodEnd: periodEnd.UTC(), AccountID: accountID.String(),
	})
	if err != nil {
		return nil, err
	}
	out := make(map[uuid.UUID]int, len(rows))
	for _, r := range rows {
		id, err := uuid.Parse(r.AppID)
		if err != nil {
			return nil, err
		}
		out[id] = int(r.MemberHwm)
	}
	return out, nil
}
