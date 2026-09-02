package usage

import (
	"testing"

	"github.com/google/uuid"
)

// appIDAt returns a deterministic, ORDER-BEARING app id: id N sorts before id
// N+1 bytewise, so a test can pin which app a tie-break gives a micro to.
func appIDAt(n byte) uuid.UUID {
	return uuid.UUID{n, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
}

// accountProjectedBase is the account line GetAccountBill publishes, written
// out independently of the allocator so the test compares two derivations
// rather than an implementation against itself.
func accountProjectedBase(shares []AppRecurringFeeShare, baseFeeMicros int64) int64 {
	counts := RecurringFeeCountsOf(shares)
	return int64(counts.Apps)*baseFeeMicros +
		ModuleBlockMicros(int64(counts.ModuleOverages)) +
		int64(counts.CustomDomains)*DomainFeeMicros
}

func sumAllocated(byApp map[uuid.UUID]int64) int64 {
	var total int64
	for _, micros := range byApp {
		total += micros
	}
	return total
}

// The identity the whole per-app presentation rests on: allocate the account's
// recurring base to its apps and the parts still add up to the whole. A bill UI
// that shows the base ON each app has nowhere to put a shortfall, so a drift
// here would silently under-bill the customer's own view of what they owe.
func TestProjectedBaseFeeByAppSumsToAccountTotal(t *testing.T) {
	cases := []struct {
		name   string
		shares []AppRecurringFeeShare
	}{
		{
			name:   "no shares",
			shares: nil,
		},
		{
			name: "one app, no surcharges",
			shares: []AppRecurringFeeShare{
				{AppID: appIDAt(1), Activated: true},
			},
		},
		{
			// The owner's real shape: 13 modules and one domain on a single app.
			name: "one app owns every surcharge",
			shares: []AppRecurringFeeShare{
				{AppID: appIDAt(1), Activated: true, OverModuleCount: 8, CustomDomainCount: 1},
				{AppID: appIDAt(2), Activated: true},
				{AppID: appIDAt(3), Activated: true},
			},
		},
		{
			// Two apps two-over each: the ACCOUNT pays one block, so recomputing
			// ceil() per app would bill two. Distribution cannot.
			name: "one block split across two apps",
			shares: []AppRecurringFeeShare{
				{AppID: appIDAt(1), Activated: true, OverModuleCount: 2},
				{AppID: appIDAt(2), Activated: true, OverModuleCount: 2},
			},
		},
		{
			// An indivisible split: $5 over three apps is 1666666.67 micros each.
			name: "block indivisible by the over-count",
			shares: []AppRecurringFeeShare{
				{AppID: appIDAt(1), Activated: true, OverModuleCount: 1},
				{AppID: appIDAt(2), Activated: true, OverModuleCount: 1},
				{AppID: appIDAt(3), Activated: true, OverModuleCount: 1},
			},
		},
		{
			// Pending creation: contributes surcharges, but no base fee.
			name: "unactivated app still owns a domain",
			shares: []AppRecurringFeeShare{
				{AppID: appIDAt(1), Activated: false, CustomDomainCount: 2},
				{AppID: appIDAt(2), Activated: true, OverModuleCount: 7},
			},
		},
		{
			name: "many apps, lopsided overage",
			shares: []AppRecurringFeeShare{
				{AppID: appIDAt(1), Activated: true, OverModuleCount: 11, CustomDomainCount: 3},
				{AppID: appIDAt(2), Activated: true, OverModuleCount: 1},
				{AppID: appIDAt(3), Activated: false, OverModuleCount: 1},
				{AppID: appIDAt(4), Activated: true},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := accountProjectedBase(tc.shares, BaseFeeMicros)
			got := sumAllocated(projectedBaseFeeByApp(tc.shares, BaseFeeMicros))
			if got != want {
				t.Fatalf("allocated %d micros, account line is %d", got, want)
			}
		})
	}
}

// The owner's account, priced end to end: $20 base + 2 blocks + 1 domain = $32
// on the app that owns them, and nothing on the apps that do not.
func TestProjectedBaseFeeByAppPricesTheOwnerAccount(t *testing.T) {
	owner, idle := appIDAt(1), appIDAt(2)
	byApp := projectedBaseFeeByApp([]AppRecurringFeeShare{
		{AppID: owner, Activated: true, OverModuleCount: 8, CustomDomainCount: 1},
		{AppID: idle, Activated: true},
	}, BaseFeeMicros)

	// 13 installed − 5 included = 8 over → ceil(8/5) = 2 blocks = $10.
	const want = BaseFeeMicros + 2*ModuleBlockFeeMicros + DomainFeeMicros
	if want != 32_000_000 {
		t.Fatalf("fixture drifted from the owner's bill: want $32.00, constants give %d", want)
	}
	if got := byApp[owner]; got != want {
		t.Errorf("owning app: got %d micros, want %d", got, want)
	}
	if got := byApp[idle]; got != BaseFeeMicros {
		t.Errorf("idle app: got %d micros, want the flat base %d", got, BaseFeeMicros)
	}
}

// Distribution, not recomputation: an app's block money must follow its share
// of the account's over-count, never a per-app ceil() of its own.
func TestProjectedBaseFeeByAppDistributesRatherThanRecomputesBlocks(t *testing.T) {
	first, second := appIDAt(1), appIDAt(2)
	byApp := projectedBaseFeeByApp([]AppRecurringFeeShare{
		{AppID: first, OverModuleCount: 2},
		{AppID: second, OverModuleCount: 2},
	}, BaseFeeMicros)

	// 4 over → ONE block ($5) for the account. Per-app ceil() would bill $5 each.
	if got := byApp[first] + byApp[second]; got != ModuleBlockFeeMicros {
		t.Fatalf("split 4 over-modules into %d micros, one block is %d", got, ModuleBlockFeeMicros)
	}
	if byApp[first] != byApp[second] {
		t.Errorf("equal over-counts split unequally: %d vs %d", byApp[first], byApp[second])
	}
}

// A remainder micro must land somewhere repeatable. Map iteration is random in
// Go, so an allocator that leaned on it would move money between apps run to
// run and make a bill unreproducible.
func TestProjectedBaseFeeByAppIsDeterministic(t *testing.T) {
	shares := []AppRecurringFeeShare{
		{AppID: appIDAt(1), Activated: true, OverModuleCount: 1},
		{AppID: appIDAt(2), Activated: true, OverModuleCount: 1},
		{AppID: appIDAt(3), Activated: true, OverModuleCount: 1},
	}
	first := projectedBaseFeeByApp(shares, BaseFeeMicros)
	for i := 0; i < 50; i++ {
		again := projectedBaseFeeByApp(shares, BaseFeeMicros)
		for appID, micros := range first {
			if again[appID] != micros {
				t.Fatalf("run %d moved %s from %d to %d", i, appID, micros, again[appID])
			}
		}
	}
}
