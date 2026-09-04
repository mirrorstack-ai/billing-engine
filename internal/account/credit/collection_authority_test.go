package credit

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// EvaluateCreditUsage drops a usage event whose PeriodStart is not the start
// of the anchored period containing the coordinator's clock, so the fixtures
// below share one pinned instant: the coordinator reads fixtureNow, and the
// event and projection carry the window that instant falls in for an account
// anchored on the 4th. Reading the wall clock instead put the suite on a
// timer — it went red at midnight UTC on the fixture's PeriodEnd.
var (
	fixtureNow         = time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	fixtureActivatedAt = time.Date(2026, time.May, 4, 0, 0, 0, 0, time.UTC)

	fixturePeriodStart, fixturePeriodEnd = billingperiod.AnchoredPeriodWindow(
		fixtureNow, billingperiod.AnchorDay(fixtureActivatedAt),
	)
)

// topUpCandidateSnapshot returns a snapshot that autoTopUpCandidate
// accepts: credits mode, no credit limit, auto-top-up armed, and a
// balance at or below the threshold.
func topUpCandidateSnapshot(accountID uuid.UUID) Snapshot {
	return Snapshot{
		AccountID:              accountID,
		OwnerUserID:            uuid.New(),
		BillingMode:            "credits",
		CreditLimitMicros:      0,
		SettledBalanceMicros:   500,
		SpendableBalanceMicros: 500,
		AutoTopUpEnabled:       true,
		AutoTopUpThreshold:     1_000,
		ActivatedAt:            fixtureActivatedAt,
	}
}

func usageEventFor(accountID uuid.UUID) UsageEvent {
	return UsageEvent{
		AccountID:               accountID,
		EventID:                 "evt-" + accountID.String(),
		ApproximateChargeMicros: 1,
		PeriodStart:             fixturePeriodStart,
		PeriodEnd:               fixturePeriodEnd,
	}
}

func candidateCoordinator(trigger *fakeAutoTopUpTrigger) (*Coordinator, uuid.UUID) {
	accountID := uuid.New()
	snapshots := &fakeSnapshots{snapshot: topUpCandidateSnapshot(accountID)}
	projection := &fakeProjection{projection: Projection{
		AmountMicros: 0,
		PeriodStart:  fixturePeriodStart,
		PeriodEnd:    fixturePeriodEnd,
	}}
	// counter nil: OutOfCredits takes the live-projection path, which is
	// the shorter of its two routes to maybeTriggerAutoTopUp.
	return NewCoordinator(nil, snapshots, projection, nil).
		WithAutoTopUpTrigger(trigger).
		WithNow(func() time.Time { return fixtureNow }), accountID
}

// maybeTriggerAutoTopUp is the one place in this package where a card
// charge can originate. It must refuse without an explicit grant.
//
// This is the property the whole change rests on. docs/SECURITY.md §2
// records four ordinary read and ingest paths reaching the executor,
// and the reason they could is that suppression was opt-out: safety
// depended on every caller remembering credit.SuppressAutoTopUp. A
// caller who forgets an opt-out charges a card. A caller who forgets an
// opt-in does not.
func TestCollectionIsDeniedWithoutAuthority(t *testing.T) {
	trigger := &fakeAutoTopUpTrigger{}
	coordinator, accountID := candidateCoordinator(trigger)
	snapshot := topUpCandidateSnapshot(accountID)

	// A plain context: nothing granted the capability.
	coordinator.maybeTriggerAutoTopUp(context.Background(), accountID, snapshot, 0, false)

	if got := trigger.count(); got != 0 {
		t.Fatalf("reached the executor %d time(s) with no collection authority; "+
			"the default must be deny, or a new caller acquires the capability by accident", got)
	}
}

// The grant does reach the executor, so the deny above is a real gate
// rather than a broken candidacy check.
func TestCollectionProceedsWithAuthority(t *testing.T) {
	trigger := &fakeAutoTopUpTrigger{}
	coordinator, accountID := candidateCoordinator(trigger)
	snapshot := topUpCandidateSnapshot(accountID)

	coordinator.maybeTriggerAutoTopUp(authorizeCollection(context.Background()), accountID, snapshot, 0, false)

	if got := trigger.count(); got != 1 {
		t.Fatalf("an authorized path reached the executor %d time(s), want 1", got)
	}
}

// Suppression is kept rather than replaced by the grant, and it must
// still win: the webhook transports pass it so that observing a
// settlement cannot originate a fresh charge. A deny that two
// independent mechanisms agree on is worth more than a tidier one.
func TestSuppressionOverridesAuthority(t *testing.T) {
	trigger := &fakeAutoTopUpTrigger{}
	coordinator, accountID := candidateCoordinator(trigger)
	snapshot := topUpCandidateSnapshot(accountID)

	ctx := SuppressAutoTopUp(authorizeCollection(context.Background()))
	coordinator.maybeTriggerAutoTopUp(ctx, accountID, snapshot, 0, false)

	if got := trigger.count(); got != 0 {
		t.Fatalf("explicit suppression lost to the grant: %d call(s)", got)
	}
}

// 🔴 OutOfCredits still collects, and this pins it so that the day it
// stops is a deliberate edit to a test that says why.
//
// It is the read behind the platform's service-block gate
// (internal/account/billing/service.go:392), and docs/SECURITY.md §2
// lists it as a gap. It keeps the capability only because removing it
// deadlocks: an account refused a refill here answers "blocked", a
// blocked account serves nothing, and serving nothing it records no
// usage — which is what drives the other refill path. It would wait for
// the period boundary to become usable.
//
// The remedy is the intent executor of docs/DESIGN.md §11, not a
// narrower grant here. When it lands, delete this test.
func TestOutOfCreditsStillCollects_knownGap(t *testing.T) {
	trigger := &fakeAutoTopUpTrigger{}
	coordinator, accountID := candidateCoordinator(trigger)

	if _, err := coordinator.OutOfCredits(context.Background(), accountID); err != nil {
		t.Fatalf("OutOfCredits: %v", err)
	}

	if trigger.count() == 0 {
		t.Fatal("OutOfCredits no longer reaches the executor. If that was intentional, " +
			"check a blocked account can still be refilled without waiting for the period " +
			"boundary, then delete this test and close the gap register row.")
	}
}

// The deny must not silence the paths that legitimately refill a
// balance. EvaluateCreditUsage is an ingest event, not a read: it is
// how spend arrives, and refusing it a top-up would block customers
// rather than protect them.
//
// It is still an explicit grant rather than an absence of denial, so
// that adding a new caller does not silently acquire the capability.
func TestUsageIngestStillTriggersTopUp(t *testing.T) {
	trigger := &fakeAutoTopUpTrigger{}
	coordinator, accountID := candidateCoordinator(trigger)

	err := coordinator.EvaluateCreditUsage(context.Background(), usageEventFor(accountID))
	if err != nil {
		t.Fatalf("EvaluateCreditUsage: %v", err)
	}

	if trigger.count() == 0 {
		t.Fatal("usage ingest no longer reaches auto-top-up; the deny was drawn too wide " +
			"and a credits customer would be blocked instead of refilled")
	}
}

// A transport that explicitly suppresses must still be obeyed on the
// ingest path too, since that is what the webhook binaries rely on to
// keep settlement observation from originating a fresh charge.
func TestUsageIngestObeysExplicitSuppression(t *testing.T) {
	trigger := &fakeAutoTopUpTrigger{}
	coordinator, accountID := candidateCoordinator(trigger)

	err := coordinator.EvaluateCreditUsage(SuppressAutoTopUp(context.Background()), usageEventFor(accountID))
	if err != nil {
		t.Fatalf("EvaluateCreditUsage: %v", err)
	}
	if got := trigger.count(); got != 0 {
		t.Fatalf("explicit suppression was ignored on the ingest path: %d call(s)", got)
	}
}
