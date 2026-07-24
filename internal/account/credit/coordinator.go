package credit

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

type Snapshot struct {
	AccountID              uuid.UUID
	OwnerUserID            uuid.UUID
	OwnerOrgID             uuid.UUID
	BillingMode            string
	SettledBalanceMicros   int64
	SpendableBalanceMicros int64
	CreditLimitMicros      int64
	PendingAutoTopUp       bool
	ActivatedAt            time.Time
}

type SnapshotProvider interface {
	CreditGateSnapshot(context.Context, uuid.UUID) (Snapshot, error)
}

type OwnerNotifier interface {
	// NotifyOwner returns the blocked verdict that was actually delivered.
	// The coordinator uses it to keep disposable notification claims aligned
	// with the notifier's fresh status read rather than its earlier snapshot.
	NotifyOwner(context.Context, uuid.UUID, uuid.UUID) (blocked bool, err error)
}

type Gate interface {
	OutOfCredits(context.Context, uuid.UUID) (bool, error)
}

type SettlementObserver interface {
	ObserveAccount(context.Context, uuid.UUID) error
}

type BoundaryReconciler interface {
	ReconcileBoundary(context.Context, uuid.UUID, time.Time, int64) error
}

type Coordinator struct {
	counter    Counter
	snapshots  SnapshotProvider
	projection ProjectionProvider
	notifier   OwnerNotifier
	nowFn      func() time.Time
}

func NewCoordinator(counter Counter, snapshots SnapshotProvider, projection ProjectionProvider, notifier OwnerNotifier) *Coordinator {
	if snapshots == nil || projection == nil {
		panic("credit.NewCoordinator: snapshots and projection must not be nil")
	}
	return &Coordinator{
		counter: counter, snapshots: snapshots, projection: projection,
		notifier: notifier, nowFn: time.Now,
	}
}

func (c *Coordinator) WithNotifier(notifier OwnerNotifier) *Coordinator {
	c.notifier = notifier
	return c
}

func (c *Coordinator) WithNow(nowFn func() time.Time) *Coordinator {
	if nowFn == nil {
		panic("credit.Coordinator.WithNow: nowFn must not be nil")
	}
	c.nowFn = nowFn
	return c
}

func (c *Coordinator) EvaluateCreditUsage(ctx context.Context, event UsageEvent) error {
	snapshot, err := c.snapshots.CreditGateSnapshot(ctx, event.AccountID)
	if err != nil {
		return fmt.Errorf("credit snapshot: %w", err)
	}
	if snapshot.BillingMode != "credits" {
		return nil
	}
	periodStart := c.currentPeriodStart(snapshot)
	if event.PeriodStart.IsZero() || !event.PeriodStart.Equal(periodStart) {
		// The real-time gate tracks only the account's current anchored period.
		// A late historical event remains durable and is priced by its boundary
		// rollup, but must not alter the current-period cache.
		return nil
	}
	if event.ForceLiveProjection {
		return c.evaluateForcedProjection(ctx, event.AccountID, snapshot, periodStart)
	}

	cacheErr := ErrUnavailable
	if c.counter != nil {
		_, estimate, found, err := c.counter.AddIfPresent(
			ctx, event.AccountID, periodStart, event.ApproximateChargeMicros,
		)
		if err == nil && found {
			if !blocks(snapshot, estimate) {
				return nil
			}
			// The ingest delta is deliberately approximate (peak and
			// time-weighted meters can overstate). A high cache value is only a
			// candidate: confirm it with the authoritative live bill before a
			// serving transition.
			projection, err := c.projection.ProjectedCreditCharge(ctx, snapshot.OwnerUserID, snapshot.OwnerOrgID)
			if err != nil {
				return fmt.Errorf("credit projection: %w", err)
			}
			targetSnapshot, targetPeriod, err := c.projectionTarget(
				ctx, event.AccountID, snapshot, periodStart, projection,
			)
			if err != nil {
				return err
			}
			if targetPeriod.Equal(periodStart) {
				if _, err := c.counter.SetIfEqual(ctx, event.AccountID, periodStart, estimate, projection.AmountMicros); err != nil {
					slog.ErrorContext(ctx, "credit estimate confirmation write failed; using live projection",
						"account_id", event.AccountID, "error", err)
				}
			} else if _, err := c.counter.SetMax(ctx, event.AccountID, targetPeriod, projection.AmountMicros); err != nil {
				slog.ErrorContext(ctx, "credit rollover estimate initialization failed; using live projection",
					"account_id", event.AccountID, "error", err)
			}
			if blocks(targetSnapshot, projection.AmountMicros) {
				return c.notifyBlock(ctx, targetSnapshot, targetPeriod)
			}
			return nil
		}
		cacheErr = err
	}

	projection, projectionErr := c.projection.ProjectedCreditCharge(ctx, snapshot.OwnerUserID, snapshot.OwnerOrgID)
	if projectionErr != nil {
		return fmt.Errorf("credit cache unavailable (%v) and live projection failed: %w", cacheErr, projectionErr)
	}
	targetSnapshot, targetPeriod, err := c.projectionTarget(
		ctx, event.AccountID, snapshot, periodStart, projection,
	)
	if err != nil {
		return err
	}
	estimate := projection.AmountMicros
	if c.counter == nil {
		// Live projection is sufficient for this request.
	} else if _, err := c.counter.SetMax(ctx, event.AccountID, targetPeriod, estimate); err != nil {
		slog.ErrorContext(ctx, "credit estimate cold initialization failed; using live projection", "account_id", event.AccountID, "error", err)
	}
	if blocks(targetSnapshot, estimate) {
		return c.notifyBlock(ctx, targetSnapshot, targetPeriod)
	}
	return nil
}

// evaluateForcedProjection handles fresh usage whose exact ingest-time price is
// unavailable. It reads the warm value only as a CAS baseline, always computes
// the authoritative live bill, and then either overwrites that unchanged
// baseline or falls back to SetMax when a concurrent writer won. This avoids
// both the warm-low false allow and erasing usage that arrived while the
// projection was running.
func (c *Coordinator) evaluateForcedProjection(
	ctx context.Context,
	accountID uuid.UUID,
	snapshot Snapshot,
	periodStart time.Time,
) error {
	var (
		before      int64
		foundBefore bool
		readOK      bool
	)
	if c.counter != nil {
		var err error
		before, foundBefore, err = c.counter.Get(ctx, accountID, periodStart)
		if err != nil {
			slog.ErrorContext(ctx, "forced credit projection baseline read failed; using live projection",
				"account_id", accountID, "error", err)
		} else {
			readOK = true
		}
	}

	projection, err := c.projection.ProjectedCreditCharge(
		ctx, snapshot.OwnerUserID, snapshot.OwnerOrgID,
	)
	if err != nil {
		return fmt.Errorf("credit projection: %w", err)
	}
	targetSnapshot, targetPeriod, err := c.projectionTarget(
		ctx, accountID, snapshot, periodStart, projection,
	)
	if err != nil {
		return err
	}

	if c.counter != nil {
		reconciled := false
		if targetPeriod.Equal(periodStart) && readOK && foundBefore {
			reconciled, err = c.counter.SetIfEqual(
				ctx, accountID, periodStart, before, projection.AmountMicros,
			)
			if err != nil {
				slog.ErrorContext(ctx, "forced credit projection CAS failed; using live projection",
					"account_id", accountID, "error", err)
			}
		}
		if !reconciled {
			// CAS false means another event changed the key while projection
			// ran. SetMax installs at least the authoritative projection while
			// preserving a higher concurrent estimate. A CAS transport failure
			// also gets this independent best-effort write attempt.
			if _, setErr := c.counter.SetMax(
				ctx, accountID, targetPeriod, projection.AmountMicros,
			); setErr != nil {
				slog.ErrorContext(ctx, "forced credit projection reconciliation failed; using live projection",
					"account_id", accountID, "error", setErr)
			}
		}
	}

	if blocks(targetSnapshot, projection.AmountMicros) {
		return c.notifyBlock(ctx, targetSnapshot, targetPeriod)
	}
	return nil
}

func (c *Coordinator) OutOfCredits(ctx context.Context, accountID uuid.UUID) (bool, error) {
	snapshot, err := c.snapshots.CreditGateSnapshot(ctx, accountID)
	if err != nil {
		return false, fmt.Errorf("credit snapshot: %w", err)
	}
	if snapshot.BillingMode != "credits" {
		return false, nil
	}

	periodStart := c.currentPeriodStart(snapshot)
	cacheErr := ErrUnavailable
	if c.counter != nil {
		estimate, found, err := c.counter.Get(ctx, accountID, periodStart)
		if err == nil && found {
			if !blocks(snapshot, estimate) {
				return false, nil
			}
			projection, projectionErr := c.projection.ProjectedCreditCharge(ctx, snapshot.OwnerUserID, snapshot.OwnerOrgID)
			if projectionErr != nil {
				return false, fmt.Errorf("credit projection: %w", projectionErr)
			}
			targetSnapshot, targetPeriod, err := c.projectionTarget(
				ctx, accountID, snapshot, periodStart, projection,
			)
			if err != nil {
				return false, err
			}
			if targetPeriod.Equal(periodStart) {
				if _, err := c.counter.SetIfEqual(ctx, accountID, periodStart, estimate, projection.AmountMicros); err != nil {
					slog.ErrorContext(ctx, "credit estimate confirmation write failed; using live projection",
						"account_id", accountID, "error", err)
				}
			} else if _, err := c.counter.SetMax(ctx, accountID, targetPeriod, projection.AmountMicros); err != nil {
				slog.ErrorContext(ctx, "credit rollover estimate initialization failed; using live projection",
					"account_id", accountID, "error", err)
			}
			return blocks(targetSnapshot, projection.AmountMicros), nil
		}
		cacheErr = err
	}

	projection, projectionErr := c.projection.ProjectedCreditCharge(ctx, snapshot.OwnerUserID, snapshot.OwnerOrgID)
	if projectionErr != nil {
		return false, fmt.Errorf("credit cache unavailable (%v) and live projection failed: %w", cacheErr, projectionErr)
	}
	targetSnapshot, targetPeriod, err := c.projectionTarget(
		ctx, accountID, snapshot, periodStart, projection,
	)
	if err != nil {
		return false, err
	}
	estimate := projection.AmountMicros
	if c.counter != nil {
		if _, err := c.counter.SetMax(ctx, accountID, targetPeriod, estimate); err != nil {
			slog.ErrorContext(ctx, "credit estimate reconciliation failed; using live projection", "account_id", accountID, "error", err)
		}
	}
	return blocks(targetSnapshot, estimate), nil
}

func (c *Coordinator) ObserveAccount(ctx context.Context, accountID uuid.UUID) error {
	snapshot, err := c.snapshots.CreditGateSnapshot(ctx, accountID)
	if err != nil {
		return fmt.Errorf("credit snapshot: %w", err)
	}
	if snapshot.BillingMode != "credits" {
		if c.counter != nil {
			_ = c.counter.ClearBlockNotification(ctx, accountID, c.currentPeriodStart(snapshot))
		}
		_, err := c.notify(ctx, snapshot)
		return err
	}
	periodStart := c.currentPeriodStart(snapshot)

	var (
		before      int64
		foundBefore bool
		readOK      bool
	)
	if c.counter != nil {
		before, foundBefore, err = c.counter.Get(ctx, accountID, periodStart)
		if err != nil {
			slog.ErrorContext(ctx, "credit estimate observer read failed; preserving cache monotonically",
				"account_id", accountID, "error", err)
		} else {
			readOK = true
		}
	}

	projection, err := c.projection.ProjectedCreditCharge(ctx, snapshot.OwnerUserID, snapshot.OwnerOrgID)
	if err != nil {
		return fmt.Errorf("credit projection: %w", err)
	}
	targetSnapshot, targetPeriod, err := c.projectionTarget(
		ctx, accountID, snapshot, periodStart, projection,
	)
	if err != nil {
		return err
	}
	if c.counter != nil {
		switch {
		case targetPeriod.Equal(periodStart) && readOK && foundBefore:
			_, setErr := c.counter.SetIfEqual(
				ctx, accountID, periodStart, before, projection.AmountMicros,
			)
			if setErr != nil {
				slog.ErrorContext(ctx, "credit estimate observer reconciliation failed",
					"account_id", accountID, "error", setErr)
			}
		default:
			_, setErr := c.counter.SetMax(ctx, accountID, targetPeriod, projection.AmountMicros)
			if setErr != nil {
				slog.ErrorContext(ctx, "credit estimate observer reconciliation failed",
					"account_id", accountID, "error", setErr)
			}
		}
	}
	deliveredBlocked, err := c.notify(ctx, targetSnapshot)
	if err != nil {
		return err
	}
	if c.counter != nil {
		if deliveredBlocked {
			if _, claimErr := c.counter.ClaimBlockNotification(ctx, accountID, targetPeriod); claimErr != nil {
				slog.ErrorContext(ctx, "credit observer delivered-block claim failed",
					"account_id", accountID, "error", claimErr)
			}
		} else if clearErr := c.counter.ClearBlockNotification(ctx, accountID, targetPeriod); clearErr != nil {
			slog.ErrorContext(ctx, "credit observer eligible claim release failed",
				"account_id", accountID, "error", clearErr)
		}
	}
	return nil
}

// ReconcileBoundary starts the period that just opened with zero unpaid
// exposure after the durable wallet draw. Recurring base, module-overage, and
// domain fees were paid in advance by that draw; seeding them again against
// the post-draw balance would double-reserve credit. SetMax makes the zero seed
// unable to erase usage that arrives between the draw and this callback.
//
// The post-draw snapshot still blocks immediately when the durable posted
// balance is negative (a credits-mode unsecured residual). Separate disposable
// notification claims make crash/retry replays winner-only without preventing
// a later usage crossing in the same period from notifying.
func (c *Coordinator) ReconcileBoundary(ctx context.Context, accountID uuid.UUID, periodStart time.Time, estimateMicros int64) error {
	if estimateMicros != 0 {
		return fmt.Errorf("boundary unpaid exposure must be zero after the durable draw: %d", estimateMicros)
	}
	if c.counter != nil {
		if _, err := c.counter.SetMax(ctx, accountID, periodStart, 0); err != nil {
			slog.ErrorContext(ctx, "boundary credit estimate seed failed; continuing with authoritative amount",
				"account_id", accountID, "error", err)
		}
	}

	snapshot, err := c.snapshots.CreditGateSnapshot(ctx, accountID)
	if err != nil {
		return fmt.Errorf("credit snapshot: %w", err)
	}
	if c.notifier == nil {
		// The zero seed is still useful, but there is no notification to claim.
		return nil
	}
	return c.notifyBoundary(ctx, snapshot, periodStart, blocks(snapshot, estimateMicros))
}

func (c *Coordinator) currentPeriodStart(snapshot Snapshot) time.Time {
	anchorDay := billingperiod.DefaultAnchorDay
	if !snapshot.ActivatedAt.IsZero() {
		anchorDay = billingperiod.AnchorDay(snapshot.ActivatedAt)
	}
	start, _ := billingperiod.AnchoredPeriodWindow(c.nowFn().UTC(), anchorDay)
	return start
}

// projectionTarget keeps a projection and every cache/notification operation
// on the same anchored-period key. A projection can cross a billing boundary
// after the caller has already read the prior key; in that case the returned
// period is authoritative and the snapshot is refreshed before deciding
// eligibility. The prior key is intentionally left untouched.
func (c *Coordinator) projectionTarget(
	ctx context.Context,
	accountID uuid.UUID,
	snapshot Snapshot,
	observedPeriod time.Time,
	projection Projection,
) (Snapshot, time.Time, error) {
	targetPeriod := projection.PeriodStart
	if targetPeriod.IsZero() {
		// Production projections always carry their anchored period. Retaining
		// the observed period here keeps custom providers fail-open without
		// ever moving a value to a guessed key.
		targetPeriod = observedPeriod
	}
	if targetPeriod.Equal(observedPeriod) {
		return snapshot, targetPeriod, nil
	}

	freshSnapshot, err := c.snapshots.CreditGateSnapshot(ctx, accountID)
	if err != nil {
		return Snapshot{}, time.Time{}, fmt.Errorf("credit rollover snapshot: %w", err)
	}
	return freshSnapshot, targetPeriod, nil
}

func blocks(snapshot Snapshot, estimateMicros int64) bool {
	return snapshot.BillingMode == "credits" &&
		snapshot.CreditLimitMicros == 0 &&
		!snapshot.PendingAutoTopUp &&
		(snapshot.SettledBalanceMicros < 0 ||
			estimateMicros > snapshot.SpendableBalanceMicros)
}

func (c *Coordinator) notify(ctx context.Context, snapshot Snapshot) (bool, error) {
	if c.notifier == nil {
		return false, nil
	}
	return c.notifier.NotifyOwner(ctx, snapshot.OwnerUserID, snapshot.OwnerOrgID)
}

func (c *Coordinator) notifyBlock(ctx context.Context, snapshot Snapshot, periodStart time.Time) error {
	if c.notifier == nil {
		// Do not consume a 45-day SETNX claim when no delivery path is wired.
		return nil
	}
	claimed := false
	if c.counter != nil {
		var err error
		claimed, err = c.counter.ClaimBlockNotification(ctx, snapshot.AccountID, periodStart)
		if err != nil {
			slog.ErrorContext(ctx, "credit block notification claim failed; notifying best-effort",
				"account_id", snapshot.AccountID, "error", err)
		} else if !claimed {
			return nil
		}
	}
	deliveredBlocked, err := c.notify(ctx, snapshot)
	if err != nil {
		if c.counter != nil && claimed {
			if clearErr := c.counter.ClearBlockNotification(ctx, snapshot.AccountID, periodStart); clearErr != nil {
				slog.ErrorContext(ctx, "credit block notification claim release failed",
					"account_id", snapshot.AccountID, "error", clearErr)
			}
		}
		return err
	}
	if claimed && !deliveredBlocked {
		// The notifier re-read fresher state (for example a purchase committed
		// after our snapshot) and actually pushed eligible. Do not retain a
		// "blocked notified" marker that would suppress the next real crossing.
		if clearErr := c.counter.ClearBlockNotification(ctx, snapshot.AccountID, periodStart); clearErr != nil {
			slog.ErrorContext(ctx, "stale block notification claim release failed",
				"account_id", snapshot.AccountID, "error", clearErr)
		}
	}
	return nil
}

func (c *Coordinator) notifyBoundary(ctx context.Context, snapshot Snapshot, periodStart time.Time, predictedBlocked bool) error {
	claimed := false
	if c.counter != nil {
		var err error
		if predictedBlocked {
			claimed, err = c.counter.ClaimBlockNotification(ctx, snapshot.AccountID, periodStart)
		} else {
			claimed, err = c.counter.ClaimBoundaryNotification(ctx, snapshot.AccountID, periodStart)
		}
		if err != nil {
			slog.ErrorContext(ctx, "boundary notification claim failed; notifying best-effort",
				"account_id", snapshot.AccountID, "blocked", predictedBlocked, "error", err)
		} else if !claimed {
			return nil
		}
	}

	deliveredBlocked, err := c.notify(ctx, snapshot)
	if err != nil {
		if c.counter != nil && claimed {
			c.clearNotificationClaim(ctx, snapshot.AccountID, periodStart, predictedBlocked)
		}
		return err
	}
	if c.counter != nil && claimed && deliveredBlocked != predictedBlocked {
		// The notifier's status read is fresher than the snapshot used to pick
		// the claim. Convert the marker so future opposite transitions are not
		// suppressed by a stale claim type.
		c.clearNotificationClaim(ctx, snapshot.AccountID, periodStart, predictedBlocked)
		var claimErr error
		if deliveredBlocked {
			_, claimErr = c.counter.ClaimBlockNotification(ctx, snapshot.AccountID, periodStart)
		} else {
			_, claimErr = c.counter.ClaimBoundaryNotification(ctx, snapshot.AccountID, periodStart)
		}
		if claimErr != nil {
			slog.ErrorContext(ctx, "boundary delivered-verdict claim failed",
				"account_id", snapshot.AccountID, "blocked", deliveredBlocked, "error", claimErr)
		}
	}
	return nil
}

func (c *Coordinator) clearNotificationClaim(ctx context.Context, accountID uuid.UUID, periodStart time.Time, blocked bool) {
	var err error
	if blocked {
		err = c.counter.ClearBlockNotification(ctx, accountID, periodStart)
	} else {
		err = c.counter.ClearBoundaryNotification(ctx, accountID, periodStart)
	}
	if err != nil {
		slog.ErrorContext(ctx, "credit notification claim release failed",
			"account_id", accountID, "blocked", blocked, "error", err)
	}
}
