package credit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	coordinatorNow   = time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	coordinatorStart = time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
)

type fakeCounter struct {
	mu sync.Mutex

	value  int64
	found  bool
	getErr error
	addErr error
	setErr error

	gets           int
	adds           int
	setMaxes       int
	conditional    int
	sets           int
	blockClaimed   bool
	boundaryClaim  bool
	blockClaims    int
	boundaryCalls  int
	clears         int
	boundaryClears int
	lastPeriod     time.Time
}

func (f *fakeCounter) Get(_ context.Context, _ uuid.UUID, periodStart time.Time) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	f.lastPeriod = periodStart
	return f.value, f.found, f.getErr
}

func (f *fakeCounter) AddIfPresent(_ context.Context, _ uuid.UUID, periodStart time.Time, delta int64) (int64, int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adds++
	f.lastPeriod = periodStart
	if f.addErr != nil {
		return 0, 0, false, f.addErr
	}
	if !f.found {
		return 0, 0, false, nil
	}
	previous := f.value
	f.value += delta
	return previous, f.value, true, nil
}

func (f *fakeCounter) SetMax(_ context.Context, _ uuid.UUID, periodStart time.Time, value int64) (MaxResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setMaxes++
	f.lastPeriod = periodStart
	if f.setErr != nil {
		return MaxResult{}, f.setErr
	}
	result := MaxResult{FoundBefore: f.found, PreviousMicros: f.value}
	if !f.found || value > f.value {
		f.value = value
		f.found = true
		result.Advanced = true
	}
	result.StoredMicros = f.value
	return result, nil
}

func (f *fakeCounter) SetIfEqual(_ context.Context, _ uuid.UUID, periodStart time.Time, expected, value int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conditional++
	f.lastPeriod = periodStart
	if f.setErr != nil {
		return false, f.setErr
	}
	if !f.found || f.value != expected {
		return false, nil
	}
	f.value = value
	return true, nil
}

func (f *fakeCounter) Set(_ context.Context, _ uuid.UUID, periodStart time.Time, value int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sets++
	f.lastPeriod = periodStart
	if f.setErr != nil {
		return f.setErr
	}
	f.value, f.found = value, true
	return nil
}

func (f *fakeCounter) ClaimBlockNotification(context.Context, uuid.UUID, time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blockClaims++
	if f.blockClaimed {
		return false, nil
	}
	f.blockClaimed = true
	return true, nil
}

func (f *fakeCounter) ClearBlockNotification(context.Context, uuid.UUID, time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clears++
	f.blockClaimed = false
	return nil
}

func (f *fakeCounter) ClaimBoundaryNotification(context.Context, uuid.UUID, time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.boundaryCalls++
	if f.boundaryClaim {
		return false, nil
	}
	f.boundaryClaim = true
	return true, nil
}

func (f *fakeCounter) ClearBoundaryNotification(context.Context, uuid.UUID, time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.boundaryClears++
	f.boundaryClaim = false
	return nil
}

type fakeSnapshots struct {
	mu       sync.Mutex
	snapshot Snapshot
	err      error
	calls    int
}

func (f *fakeSnapshots) CreditGateSnapshot(context.Context, uuid.UUID) (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.snapshot, f.err
}

func (f *fakeSnapshots) setBalance(balance int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshot.SettledBalanceMicros = balance
	f.snapshot.SpendableBalanceMicros = balance
}

type fakeProjection struct {
	mu         sync.Mutex
	projection Projection
	err        error
	calls      int
}

func (f *fakeProjection) ProjectedCreditCharge(context.Context, uuid.UUID, uuid.UUID) (Projection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.projection, f.err
}

type blockingProjection struct {
	started     chan struct{}
	release     chan struct{}
	value       int64
	periodStart time.Time
}

func (p blockingProjection) ProjectedCreditCharge(context.Context, uuid.UUID, uuid.UUID) (Projection, error) {
	close(p.started)
	<-p.release
	periodStart := p.periodStart
	if periodStart.IsZero() {
		periodStart = coordinatorStart
	}
	return Projection{AmountMicros: p.value, PeriodStart: periodStart}, nil
}

type fakeNotifier struct {
	mu       sync.Mutex
	calls    int
	failures int
	verdicts []bool
}

func (f *fakeNotifier) NotifyOwner(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	deliveredBlocked := true
	if i := f.calls - 1; i < len(f.verdicts) {
		deliveredBlocked = f.verdicts[i]
	}
	if f.calls <= f.failures {
		return false, errors.New("transient notify failure")
	}
	return deliveredBlocked, nil
}

func (f *fakeNotifier) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func testSnapshot(accountID uuid.UUID, spendable int64) *fakeSnapshots {
	return &fakeSnapshots{snapshot: Snapshot{
		AccountID: accountID, OwnerUserID: uuid.New(), BillingMode: "credits",
		SettledBalanceMicros:   spendable,
		SpendableBalanceMicros: spendable,
		ActivatedAt:            coordinatorStart,
	}}
}

func testCoordinator(counter Counter, snapshots SnapshotProvider, projection ProjectionProvider, notifier OwnerNotifier) *Coordinator {
	return NewCoordinator(counter, snapshots, projection, notifier).
		WithNow(func() time.Time { return coordinatorNow })
}

func usageEvent(accountID uuid.UUID, delta int64) UsageEvent {
	return UsageEvent{
		AccountID: accountID, EventID: uuid.NewString(),
		PeriodStart: coordinatorStart, ApproximateChargeMicros: delta,
	}
}

func TestCoordinatorColdKeyRebuildsAndBlocksStrictlyAboveSpendable(t *testing.T) {
	accountID := uuid.New()
	for _, tc := range []struct {
		name   string
		charge int64
		notify int
	}{
		{name: "below", charge: 99},
		{name: "equal", charge: 100},
		{name: "above", charge: 101, notify: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counter := &fakeCounter{}
			projection := &fakeProjection{projection: Projection{
				AmountMicros: tc.charge, PeriodStart: coordinatorStart,
			}}
			notifier := &fakeNotifier{}
			err := testCoordinator(counter, testSnapshot(accountID, 100), projection, notifier).
				EvaluateCreditUsage(context.Background(), usageEvent(accountID, 1))
			require.NoError(t, err)
			require.Equal(t, 1, projection.calls)
			require.Equal(t, 1, counter.setMaxes)
			require.Equal(t, tc.notify, notifier.count())
		})
	}
}

func TestCoordinatorWarmBelowFastAllowsWithoutProjection(t *testing.T) {
	accountID := uuid.New()
	counter := &fakeCounter{value: 98, found: true}
	projection := &fakeProjection{err: errors.New("must not run")}

	err := testCoordinator(counter, testSnapshot(accountID, 100), projection, nil).
		EvaluateCreditUsage(context.Background(), usageEvent(accountID, 2))

	require.NoError(t, err)
	require.Zero(t, projection.calls)
	require.EqualValues(t, 100, counter.value)
}

func TestCoordinatorForceLiveProjectionBypassesWarmLowAndBlocks(t *testing.T) {
	accountID := uuid.New()
	counter := &fakeCounter{value: 10, found: true}
	projection := &fakeProjection{projection: Projection{
		AmountMicros: 101, PeriodStart: coordinatorStart,
	}}
	notifier := &fakeNotifier{}
	event := usageEvent(accountID, 0)
	event.ForceLiveProjection = true

	err := testCoordinator(counter, testSnapshot(accountID, 100), projection, notifier).
		EvaluateCreditUsage(context.Background(), event)

	require.NoError(t, err)
	require.Equal(t, 1, projection.calls,
		"unpriced infra must bypass the warm-low fast allow")
	require.Zero(t, counter.adds, "force-live events never apply a guessed delta")
	require.Equal(t, 1, counter.gets)
	require.Equal(t, 1, counter.conditional)
	require.EqualValues(t, 101, counter.value)
	require.Equal(t, 1, notifier.count())
}

func TestCoordinatorForceLiveProjectionCASLossPreservesConcurrentEstimate(t *testing.T) {
	accountID := uuid.New()
	counter := &fakeCounter{value: 10, found: true}
	started := make(chan struct{})
	release := make(chan struct{})
	event := usageEvent(accountID, 0)
	event.ForceLiveProjection = true
	coordinator := testCoordinator(
		counter,
		testSnapshot(accountID, 100),
		blockingProjection{started: started, release: release, value: 100},
		nil,
	)

	done := make(chan error, 1)
	go func() {
		done <- coordinator.EvaluateCreditUsage(context.Background(), event)
	}()
	<-started
	_, estimate, found, err := counter.AddIfPresent(
		context.Background(), accountID, coordinatorStart, 200,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 210, estimate)
	close(release)
	require.NoError(t, <-done)

	require.EqualValues(t, 210, counter.value,
		"CAS loss falls back to SetMax and never erases concurrent usage")
	require.Equal(t, 1, counter.conditional)
	require.Equal(t, 1, counter.setMaxes)
}

func TestCoordinatorWarmOverestimateLiveConfirmsAndOverwrites(t *testing.T) {
	accountID := uuid.New()
	counter := &fakeCounter{value: 150, found: true}
	projection := &fakeProjection{projection: Projection{
		AmountMicros: 90, PeriodStart: coordinatorStart,
	}}
	notifier := &fakeNotifier{verdicts: []bool{false, true}}

	blocked, err := testCoordinator(counter, testSnapshot(accountID, 100), projection, notifier).
		OutOfCredits(context.Background(), accountID)

	require.NoError(t, err)
	require.False(t, blocked)
	require.Equal(t, 1, projection.calls, "a warm high estimate is only a candidate")
	require.EqualValues(t, 90, counter.value, "authoritative confirmation overwrites an overestimate")
	require.Zero(t, notifier.count())
}

func TestCoordinatorOutOfCreditsWarmBelowDoesNotProject(t *testing.T) {
	accountID := uuid.New()
	projection := &fakeProjection{err: errors.New("must not run")}
	blocked, err := testCoordinator(
		&fakeCounter{value: 100, found: true},
		testSnapshot(accountID, 100),
		projection,
		nil,
	).OutOfCredits(context.Background(), accountID)
	require.NoError(t, err)
	require.False(t, blocked)
	require.Zero(t, projection.calls)
}

func TestCoordinatorOutOfCreditsRolloverUsesProjectionPeriodWithoutCorruptingOldKey(t *testing.T) {
	accountID := uuid.New()
	counter := testRedisCounter(t)
	ctx := context.Background()
	nextPeriod := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, counter.Set(ctx, accountID, coordinatorStart, 1))

	snapshots := testSnapshot(accountID, 0)
	started := make(chan struct{})
	release := make(chan struct{})
	projection := blockingProjection{
		started: started, release: release, value: 50, periodStart: nextPeriod,
	}
	var now atomic.Value
	now.Store(time.Date(2026, time.July, 31, 23, 59, 0, 0, time.UTC))
	coordinator := NewCoordinator(counter, snapshots, projection, nil).
		WithNow(func() time.Time { return now.Load().(time.Time) })

	result := make(chan struct {
		blocked bool
		err     error
	}, 1)
	go func() {
		blocked, err := coordinator.OutOfCredits(ctx, accountID)
		result <- struct {
			blocked bool
			err     error
		}{blocked: blocked, err: err}
	}()
	<-started

	// The boundary commits while the authoritative projection is in flight.
	// Its period and a refreshed post-purchase balance must drive the result.
	snapshots.setBalance(100)
	now.Store(time.Date(2026, time.August, 1, 0, 1, 0, 0, time.UTC))
	close(release)

	got := <-result
	require.NoError(t, got.err)
	require.False(t, got.blocked,
		"the refreshed next-period balance covers the authoritative projection")

	oldEstimate, found, err := counter.Get(ctx, accountID, coordinatorStart)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 1, oldEstimate,
		"a next-period projection must never CAS or overwrite the prior-period key")

	nextEstimate, found, err := counter.Get(ctx, accountID, nextPeriod)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 50, nextEstimate)
	require.Equal(t, 2, snapshots.calls,
		"a period rollover refreshes the snapshot before deciding eligibility")
}

func TestCoordinatorPendingAutoTopUpStillMaintainsColdEstimate(t *testing.T) {
	accountID := uuid.New()
	snapshots := testSnapshot(accountID, 0)
	snapshots.snapshot.PendingAutoTopUp = true
	counter := &fakeCounter{}
	projection := &fakeProjection{projection: Projection{
		AmountMicros: 500, PeriodStart: coordinatorStart,
	}}
	notifier := &fakeNotifier{}

	err := testCoordinator(counter, snapshots, projection, notifier).
		EvaluateCreditUsage(context.Background(), usageEvent(accountID, 1))

	require.NoError(t, err)
	require.Equal(t, 1, projection.calls)
	require.Equal(t, 1, counter.setMaxes)
	require.EqualValues(t, 500, counter.value)
	require.Zero(t, notifier.count(), "pending auto top-up suppresses only the block transition")
}

func TestCoordinatorCacheAndProjectionFailureSurfacesBestEffortError(t *testing.T) {
	accountID := uuid.New()
	coordinator := testCoordinator(
		&fakeCounter{addErr: errors.New("redis down")},
		testSnapshot(accountID, 0),
		&fakeProjection{err: errors.New("projection down")},
		nil,
	)
	require.Error(t, coordinator.EvaluateCreditUsage(context.Background(), usageEvent(accountID, 1)))
}

func TestCoordinatorColdConcurrentProjectionNeverRegressesAndNotifiesOnce(t *testing.T) {
	accountID := uuid.New()
	counter := &fakeCounter{}
	snapshots := testSnapshot(accountID, 100)
	notifier := &fakeNotifier{}
	olderStarted := make(chan struct{})
	releaseOlder := make(chan struct{})
	older := testCoordinator(
		counter, snapshots,
		blockingProjection{started: olderStarted, release: releaseOlder, value: 150},
		notifier,
	)
	olderDone := make(chan error, 1)
	go func() {
		olderDone <- older.EvaluateCreditUsage(context.Background(), usageEvent(accountID, 1))
	}()
	<-olderStarted

	newer := testCoordinator(counter, snapshots, &fakeProjection{projection: Projection{
		AmountMicros: 200, PeriodStart: coordinatorStart,
	}}, notifier)
	require.NoError(t, newer.EvaluateCreditUsage(context.Background(), usageEvent(accountID, 1)))
	close(releaseOlder)
	require.NoError(t, <-olderDone)

	counter.mu.Lock()
	require.True(t, counter.found)
	require.EqualValues(t, 200, counter.value)
	counter.mu.Unlock()
	require.Equal(t, 1, notifier.count(), "the disposable block claim chooses one notifier")
}

func TestCoordinatorConcurrentWarmHighConfirmationNotifiesOnce(t *testing.T) {
	accountID := uuid.New()
	counter := &fakeCounter{value: 99, found: true}
	projection := &fakeProjection{projection: Projection{
		AmountMicros: 103, PeriodStart: coordinatorStart,
	}}
	notifier := &fakeNotifier{}
	coordinator := testCoordinator(counter, testSnapshot(accountID, 100), projection, notifier)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- coordinator.EvaluateCreditUsage(context.Background(), usageEvent(accountID, 2))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, 1, notifier.count())
}

func TestCoordinatorFreshDeliveredVerdictReleasesStaleBlockClaim(t *testing.T) {
	accountID := uuid.New()
	counter := &fakeCounter{}
	notifier := &fakeNotifier{verdicts: []bool{false, true}}
	coordinator := testCoordinator(
		counter,
		testSnapshot(accountID, 0),
		&fakeProjection{projection: Projection{AmountMicros: 1, PeriodStart: coordinatorStart}},
		notifier,
	)

	// The coordinator's snapshot says shortfall, but the notifier's fresher
	// status read observes a concurrent purchase and actually pushes eligible.
	require.NoError(t, coordinator.EvaluateCreditUsage(context.Background(), usageEvent(accountID, 1)))
	require.Equal(t, 1, notifier.count())
	require.False(t, counter.blockClaimed,
		"an eligible delivery must not remain recorded as a blocked delivery")

	// A later true shortfall must still win a fresh block claim and notify.
	require.NoError(t, coordinator.EvaluateCreditUsage(context.Background(), usageEvent(accountID, 0)))
	require.Equal(t, 2, notifier.count())
	require.True(t, counter.blockClaimed)
}

func TestCoordinatorWithoutNotifierNeverConsumesNotificationClaims(t *testing.T) {
	accountID := uuid.New()
	counter := &fakeCounter{}
	coordinator := testCoordinator(
		counter,
		testSnapshot(accountID, 0),
		&fakeProjection{projection: Projection{AmountMicros: 1, PeriodStart: coordinatorStart}},
		nil,
	)

	require.NoError(t, coordinator.EvaluateCreditUsage(context.Background(), usageEvent(accountID, 1)))
	require.NoError(t, coordinator.ReconcileBoundary(context.Background(), accountID, coordinatorStart, 0))
	require.Zero(t, counter.blockClaims)
	require.Zero(t, counter.boundaryCalls)
}

func TestCoordinatorBoundarySeedsZeroUnpaidExposureAndPushesWinnerOnlyVerdict(t *testing.T) {
	accountID := uuid.New()
	for _, tc := range []struct {
		name           string
		settledBalance int64
		spendable      int64
		wantBlock      bool
	}{
		{name: "unsecured residual blocks immediately", settledBalance: -1, wantBlock: true},
		{name: "exactly covered draw ending at zero is eligible"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counter := &fakeCounter{}
			notifier := &fakeNotifier{verdicts: []bool{tc.wantBlock}}
			snapshots := testSnapshot(accountID, tc.spendable)
			snapshots.snapshot.SettledBalanceMicros = tc.settledBalance
			coordinator := testCoordinator(
				counter,
				snapshots,
				&fakeProjection{projection: Projection{PeriodStart: coordinatorStart}},
				notifier,
			)

			require.NoError(t, coordinator.ReconcileBoundary(context.Background(), accountID, coordinatorStart, 0))
			require.NoError(t, coordinator.ReconcileBoundary(context.Background(), accountID, coordinatorStart, 0))
			require.Zero(t, counter.value)
			require.True(t, counter.lastPeriod.Equal(coordinatorStart))
			require.Equal(t, 1, notifier.count(), "boundary retry must not push twice")
			require.Equal(t, tc.wantBlock, counter.blockClaimed)
			require.Equal(t, !tc.wantBlock, counter.boundaryClaim)
		})
	}
}

func TestCoordinatorBoundaryPostDrawBalanceDoesNotDoubleReserveAdvance(t *testing.T) {
	accountID := uuid.New()
	const (
		openingBalance = int64(100_000_000)
		advanceDraw    = int64(20_000_000)
		postDraw       = openingBalance - advanceDraw
	)
	counter := &fakeCounter{}
	snapshots := testSnapshot(accountID, postDraw)
	projection := &fakeProjection{projection: Projection{
		AmountMicros: postDraw + 1,
		PeriodStart:  coordinatorStart,
	}}
	notifier := &fakeNotifier{verdicts: []bool{false, true}}
	coordinator := testCoordinator(counter, snapshots, projection, notifier)

	require.NoError(t, coordinator.ReconcileBoundary(context.Background(), accountID, coordinatorStart, 0))
	require.Zero(t, counter.value, "the already-paid advance is not reserved a second time")
	require.Equal(t, 1, notifier.count(), "the boundary pushes its post-draw eligible verdict")

	require.NoError(t, coordinator.EvaluateCreditUsage(
		context.Background(), usageEvent(accountID, 60_000_001),
	))
	require.Equal(t, 1, notifier.count(),
		"post-draw credit remains available past the old double-reserve threshold")
	require.NoError(t, coordinator.EvaluateCreditUsage(
		context.Background(), usageEvent(accountID, 19_999_999),
	))
	require.Equal(t, 1, notifier.count(), "usage exactly equal to spendable credit is eligible")
	require.NoError(t, coordinator.EvaluateCreditUsage(
		context.Background(), usageEvent(accountID, 1),
	))
	require.Equal(t, 2, notifier.count(), "the first micro above post-draw spendable credit blocks")
}

func TestCoordinatorBoundarySeedCannotEraseUsageThatArrivedAfterDraw(t *testing.T) {
	accountID := uuid.New()
	counter := &fakeCounter{value: 7, found: true}
	coordinator := testCoordinator(
		counter,
		testSnapshot(accountID, 100),
		&fakeProjection{projection: Projection{PeriodStart: coordinatorStart}},
		nil,
	)

	require.NoError(t, coordinator.ReconcileBoundary(context.Background(), accountID, coordinatorStart, 0))
	require.EqualValues(t, 7, counter.value,
		"SetMax(0) preserves a usage increment that won the draw-to-callback race")
}

func TestCoordinatorObserverCASRaceUsesDeliveredVerdictAndLaterReblocks(t *testing.T) {
	accountID := uuid.New()
	counter := &fakeCounter{value: 150, found: true, blockClaimed: true}
	started := make(chan struct{})
	release := make(chan struct{})
	notifier := &fakeNotifier{verdicts: []bool{false, true}}
	snapshots := testSnapshot(accountID, 200)
	coordinator := testCoordinator(
		counter,
		snapshots,
		blockingProjection{started: started, release: release, value: 100},
		notifier,
	)

	done := make(chan error, 1)
	go func() {
		done <- coordinator.ObserveAccount(context.Background(), accountID)
	}()
	<-started // ObserveAccount has read 150 and is now inside the projection.
	_, estimate, found, err := counter.AddIfPresent(
		context.Background(), accountID, coordinatorStart, 25,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 175, estimate)
	close(release)
	require.NoError(t, <-done)

	require.EqualValues(t, 175, counter.value,
		"the observer CAS must preserve the increment instead of writing its stale projection")
	require.False(t, counter.blockClaimed,
		"the actually delivered eligible verdict clears the old claim even when cache CAS loses")

	reblock := testCoordinator(
		counter,
		snapshots,
		&fakeProjection{projection: Projection{AmountMicros: 201, PeriodStart: coordinatorStart}},
		notifier,
	)
	require.NoError(t, reblock.EvaluateCreditUsage(
		context.Background(), usageEvent(accountID, 26),
	))
	require.Equal(t, 2, notifier.count(),
		"the first true shortfall after the concurrent increment must notify again")
	require.True(t, counter.blockClaimed)
}

func TestCoordinatorNotificationFailureReleasesClaimsForRetry(t *testing.T) {
	accountID := uuid.New()

	t.Run("block", func(t *testing.T) {
		counter := &fakeCounter{}
		notifier := &fakeNotifier{failures: 1}
		coordinator := testCoordinator(
			counter,
			testSnapshot(accountID, 0),
			&fakeProjection{projection: Projection{AmountMicros: 1, PeriodStart: coordinatorStart}},
			notifier,
		)

		require.Error(t, coordinator.EvaluateCreditUsage(context.Background(), usageEvent(accountID, 1)))
		require.False(t, counter.blockClaimed, "failed delivery releases the SETNX claim")
		require.NoError(t, coordinator.EvaluateCreditUsage(context.Background(), usageEvent(accountID, 0)))
		require.NoError(t, coordinator.EvaluateCreditUsage(context.Background(), usageEvent(accountID, 0)))
		require.Equal(t, 2, notifier.count(), "one retry succeeds and later duplicates stay winner-only")
		require.True(t, counter.blockClaimed)
		require.Equal(t, 1, counter.clears)
	})

	t.Run("eligible boundary", func(t *testing.T) {
		counter := &fakeCounter{}
		notifier := &fakeNotifier{failures: 1, verdicts: []bool{false, false}}
		coordinator := testCoordinator(
			counter,
			testSnapshot(accountID, 0),
			&fakeProjection{projection: Projection{PeriodStart: coordinatorStart}},
			notifier,
		)

		require.Error(t, coordinator.ReconcileBoundary(context.Background(), accountID, coordinatorStart, 0))
		require.False(t, counter.boundaryClaim, "failed delivery releases the boundary claim")
		require.NoError(t, coordinator.ReconcileBoundary(context.Background(), accountID, coordinatorStart, 0))
		require.NoError(t, coordinator.ReconcileBoundary(context.Background(), accountID, coordinatorStart, 0))
		require.Equal(t, 2, notifier.count())
		require.True(t, counter.boundaryClaim)
		require.Equal(t, 1, counter.boundaryClears)
	})
}

func TestCoordinatorPurchaseToProjectionEqualityClearsBlockAndUnblocks(t *testing.T) {
	accountID := uuid.New()
	counter := &fakeCounter{value: 150, found: true, blockClaimed: true}
	snapshots := testSnapshot(accountID, 100)
	projection := &fakeProjection{projection: Projection{
		AmountMicros: 150, PeriodStart: coordinatorStart,
	}}
	notifier := &fakeNotifier{verdicts: []bool{false}}
	coordinator := testCoordinator(counter, snapshots, projection, notifier)

	// A settled purchase supplies exactly the prior shortfall.
	snapshots.setBalance(150)
	require.NoError(t, coordinator.ObserveAccount(context.Background(), accountID))
	blocked, err := coordinator.OutOfCredits(context.Background(), accountID)

	require.NoError(t, err)
	require.False(t, blocked, "strict equality is eligible")
	require.Equal(t, 1, notifier.count(), "the first settlement pushes the unblock")
	require.False(t, counter.blockClaimed)
	require.Equal(t, 1, counter.clears)
}
