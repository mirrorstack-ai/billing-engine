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

// accountProjectedBase is the account line GetAccountBill publishes for the
// current period (accountbill.go: activatedBaseMicros + moduleOverageBlocksMicros
// + domains), written out independently of the allocator so the test compares
// two derivations rather than an implementation against itself. Since
// billing-engine#202 PR-2c module overage is priced per app against that app's
// OWN plan allowance, never pooled — so the account's block total is Σ each
// share's OWN ModuleBlockMicros, not ModuleBlockMicros of the summed over-count
// (that pooled shape no longer exists anywhere in the codebase).
func accountProjectedBase(shares []AppRecurringFeeShare) int64 {
	counts := RecurringFeeCountsOf(shares)
	return activatedBaseMicros(shares) +
		moduleOverageBlocksMicros(shares) +
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
			// Two apps two-over each: each app prices its OWN block against its
			// OWN plan allowance (billing-engine#202 PR-2c) — two blocks total,
			// never pooled into one shared account block.
			name: "two apps each own their own block",
			shares: []AppRecurringFeeShare{
				{AppID: appIDAt(1), Activated: true, OverModuleCount: 2},
				{AppID: appIDAt(2), Activated: true, OverModuleCount: 2},
			},
		},
		{
			// Three apps one-over each: each still owns a WHOLE block on its own
			// (ceil(1/5) = 1 per app) — three blocks total, never a fractional
			// split of one shared block.
			name: "three small over-counts each still cost a whole block",
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
			want := accountProjectedBase(tc.shares)
			got := sumAllocated(projectedBaseFeeByApp(tc.shares))
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
	})

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

// Recomputation, not distribution (billing-engine#202 PR-2c): an app's block
// money is ITS OWN ceil() against its OWN plan allowance, never a share of
// some pooled account over-count. Two apps with equal over-counts bill EQUAL
// blocks each, and the pair's total is TWO blocks, not the one block the old
// pooled-account model would have split between them.
func TestProjectedBaseFeeByAppPricesEachAppsBlocksIndependently(t *testing.T) {
	first, second := appIDAt(1), appIDAt(2)
	byApp := projectedBaseFeeByApp([]AppRecurringFeeShare{
		{AppID: first, OverModuleCount: 2},
		{AppID: second, OverModuleCount: 2},
	})

	// Each app's own 2-over costs ceil(2/5) = 1 block ($5) — TWO blocks total,
	// never pooled into the ONE block the account-wide model would have billed.
	if got := byApp[first] + byApp[second]; got != 2*ModuleBlockFeeMicros {
		t.Fatalf("priced 2-over-each apps at %d micros total, want %d (two independent blocks)", got, 2*ModuleBlockFeeMicros)
	}
	if byApp[first] != byApp[second] {
		t.Errorf("equal over-counts priced unequally: %d vs %d", byApp[first], byApp[second])
	}

	// Blocks aren't linear in over-count: one app with a 1-over costs the SAME
	// whole block ($5) as a 5-over on that same app — the free headroom inside
	// a block (ModuleBlockMicros' doc) — so doubling the over-count inside a
	// block must never double the price.
	lowByApp := projectedBaseFeeByApp([]AppRecurringFeeShare{{AppID: first, OverModuleCount: 1}})
	highByApp := projectedBaseFeeByApp([]AppRecurringFeeShare{{AppID: first, OverModuleCount: 5}})
	if lowByApp[first] != ModuleBlockFeeMicros || highByApp[first] != ModuleBlockFeeMicros {
		t.Fatalf("1-over priced %d, 5-over priced %d, want both at the one-block price %d", lowByApp[first], highByApp[first], ModuleBlockFeeMicros)
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
	first := projectedBaseFeeByApp(shares)
	for i := 0; i < 50; i++ {
		again := projectedBaseFeeByApp(shares)
		for appID, micros := range first {
			if again[appID] != micros {
				t.Fatalf("run %d moved %s from %d to %d", i, appID, micros, again[appID])
			}
		}
	}
}
