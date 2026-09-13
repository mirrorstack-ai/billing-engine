package cycle

// The pgxStore half of the plan-change ledger (migration 076). Kept beside
// store.go rather than inside it so the money transactions of one feature
// read as one file: OpenPlanChange (the row + the plan flip, under the app
// lock), DrawPlanChangeFromWallet (the wallet decision, once) and the boundary
// apply.

import (
	"context"
	"errors"
	"fmt"
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

// OpenPlanChange inserts the change and, for an upgrade, flips apps.plan in
// the SAME transaction, under the app row lock. The lock is what makes the
// derivation the service did on an unlocked read safe to commit: the row is
// re-verified live and still on FromPlan, and an open change found under the
// lock is returned instead of a second one.
//
// A creation-proration sweep pricing this app concurrently locks the same row
// (FreezeCombinedProrationAttempt / DrawCreationProrationFromWallet) and
// compares the plan it priced against the locked row's, so a change committed
// here between its derivation and its freeze makes that attempt stale rather
// than mispriced.
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
	if row.DeletedAt.Valid || usage.Plan(row.Plan) != p.FromPlan || uuidFromPg(row.AccountID) != p.AccountID {
		return PlanChange{}, PlanChangeAppStale, nil
	}
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

	settledAt := pgtype.Timestamptz{}
	decidedAt := pgtype.Timestamptz{}
	if p.Status == PlanChangeSettled {
		// Nothing to collect — a fold, or a $0 delta. The wallet decision is
		// "0", taken now, so the row reads like every other settled upgrade.
		settledAt = pgtype.Timestamptz{Time: p.RequestedAt, Valid: true}
		decidedAt = settledAt
	}
	inserted, err := qtx.InsertPlanChange(ctx, db.InsertPlanChangeParams{
		AppID: p.AppID.String(), AccountID: p.AccountID.String(),
		FromPlan: string(p.FromPlan), ToPlan: string(p.ToPlan), Kind: string(p.Kind),
		RequestedAt: p.RequestedAt.UTC(), EffectiveAt: p.EffectiveAt.UTC(),
		PeriodStart: p.PeriodStart.UTC(), PeriodEnd: p.PeriodEnd.UTC(),
		FoldedIntoCreation: p.Folded, AmountMicros: p.AmountMicros,
		WalletDecidedAt: decidedAt, Status: string(p.Status), SettledAt: settledAt,
	})
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
	change, err := planChangeFromRow(inserted)
	if err != nil {
		return PlanChange{}, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return PlanChange{}, 0, err
	}
	return change, PlanChangeOpened, nil
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

func (s *pgxStore) FoldedPlanChanges(ctx context.Context, appID uuid.UUID) ([]PlanChange, error) {
	rows, err := s.q.FoldedPlanChangesForApp(ctx, appID.String())
	if err != nil {
		return nil, err
	}
	return planChangesFromRows(rows)
}

// DrawPlanChangeFromWallet takes the wallet decision for one pending upgrade,
// once, in one transaction: lock the row, lock the account (the mode is read
// under the lock — a standard account decides 0 without touching the ledger),
// draw from the spendable LOTS ONLY up to the amount, record the draw on the
// row. Never the unsecured NULL-source remainder a credits-mode boundary draw
// may write: the owner's rule for an upgrade is that the card takes what the
// wallet does not hold, so a draw here is capped by the lots.
//
// allowRemainder=false (the caller found no usable card) makes a short wallet
// a refusal with NOTHING written, so the row stays undecided for a later
// retry rather than half-drawn against a remainder nobody can collect.
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
	accountID := change.AccountID
	if locked.AccountID != accountID.String() {
		return 0, 0, fmt.Errorf("plan change %s: account differs from the row's under the lock", change.ID)
	}

	decide := func(drawn int64) (PlanChangeWalletOutcome, int64, error) {
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

	rawMode, err := qtx.LockWalletAccount(ctx, accountID.String())
	if err != nil {
		return 0, 0, err
	}
	mode, err := parseCreditBillingMode(rawMode)
	if err != nil {
		return 0, 0, err
	}
	if mode != CreditBillingModeCredits {
		// A standard account's credit applies at the boundary spine, not to a
		// mid-period charge — the creation charge takes the same posture.
		return decide(0)
	}
	if _, err := qtx.LockWalletLedgerEntries(ctx, accountID.String()); err != nil {
		return 0, 0, err
	}
	balanceAfter, err := qtx.WalletSettledBalance(ctx, accountID.String())
	if err != nil {
		return 0, 0, err
	}
	lots, err := qtx.WalletSpendableLots(ctx, accountID.String())
	if err != nil {
		return 0, 0, err
	}
	var coverable int64
	for _, lot := range lots {
		if lot.RemainingMicros <= 0 {
			return 0, 0, fmt.Errorf(
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
	if coverable < change.AmountMicros && !allowRemainder {
		return PlanChangeWalletShort, 0, nil
	}

	left := change.AmountMicros
	var drawn int64
	for _, lot := range lots {
		if left == 0 {
			break
		}
		consume := lot.RemainingMicros
		if consume > left {
			consume = left
		}
		if balanceAfter < math.MinInt64+consume {
			return 0, 0, fmt.Errorf("wallet balance_after_micros underflow: balance=%d draw=%d", balanceAfter, consume)
		}
		balanceAfter -= consume
		if err := qtx.InsertPlanChangeWalletDraw(ctx, db.InsertPlanChangeWalletDrawParams{
			AccountID:          accountID.String(),
			AmountMicros:       consume,
			BalanceAfterMicros: balanceAfter,
			IdempotencyKey: fmt.Sprintf(
				"wallet-draw:plan-change:%s:subscription_draw:%s", change.ID.String(), lot.ID,
			),
			SourceCreditID: lot.ID,
		}); err != nil {
			return 0, 0, err
		}
		left -= consume
		drawn += consume
	}
	return decide(drawn)
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

// ApplyDuePlanChanges moves every due scheduled downgrade of the account onto
// its plan, each row and its plan flip in one transaction. Idempotent: an
// applied row drops out of the due list. A deleted app's row is closed
// without a flip (its plan is frozen with the rest of the row).
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
	applied := 0
	for _, row := range due {
		if _, err := qtx.SetAppPlan(ctx, db.SetAppPlanParams{AppID: row.AppID, Plan: row.ToPlan}); err != nil {
			return 0, err
		}
		n, err := qtx.MarkPlanChangeApplied(ctx, db.MarkPlanChangeAppliedParams{AppliedAt: dueAt.UTC(), ID: row.ID})
		if err != nil {
			return 0, err
		}
		if n != 1 {
			return 0, fmt.Errorf("plan change %s: applying moved %d rows under the lock", row.ID, n)
		}
		applied++
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return applied, nil
}

// CountLiveAppsOnPlan counts the owner's live apps on a plan, excluding
// exceptAppID: the org's when ownerOrgID is set (the Free cap is per org, and
// a sponsored org's apps sit on the sponsor's account), else the personal
// account's user-owned rows.
func (s *pgxStore) CountLiveAppsOnPlan(ctx context.Context, accountID, ownerOrgID uuid.UUID, plan usage.Plan, exceptAppID uuid.UUID) (int, error) {
	if ownerOrgID != uuid.Nil {
		n, err := s.q.CountLiveOrgAppsOnPlan(ctx, db.CountLiveOrgAppsOnPlanParams{
			OwnerOrgID: ownerOrgID.String(), Plan: string(plan), ExceptAppID: exceptAppID.String(),
		})
		return int(n), err
	}
	n, err := s.q.CountLiveUserAppsOnPlan(ctx, db.CountLiveUserAppsOnPlanParams{
		AccountID: accountID.String(), Plan: string(plan), ExceptAppID: exceptAppID.String(),
	})
	return int(n), err
}

func (s *pgxStore) SetAppMemberCount(ctx context.Context, appID uuid.UUID, memberCount int) error {
	// 0 rows = the app is deleted (count frozen, like SetAppModuleCount);
	// existence was already checked by the service via AppMirror.
	_, err := s.q.SetAppMemberCount(ctx, db.SetAppMemberCountParams{
		AppID:       appID.String(),
		MemberCount: int32(memberCount), //nolint:gosec // SyncAppModules validates 0 ≤ count ≤ maxModuleCount (100000), far below int32 max
	})
	return err
}
