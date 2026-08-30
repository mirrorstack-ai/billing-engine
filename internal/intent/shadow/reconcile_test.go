package shadow

import (
	"strings"
	"testing"
)

// diff builds an UNMARKED-UP comparison: base == charged, so these
// reconciliation tests keep asking exactly what they asked before the
// markup split. Markup behaviour is tested separately, in
// markup_test.go — mixing it in here would change what these tests mean.
func diff(account, period string, legacy, shadow int64) Difference {
	return Difference{
		AccountID: account, PeriodID: period,
		LegacyMicros: legacy, LegacyBaseMicros: legacy, ShadowMicros: shadow,
		IntentDigest: "digest-" + account + "-" + period,
	}
}

// Money is integer micro-dollars, so agreement is exact. No tolerance
// is offered, because a tolerance is a place for a real difference to
// hide.
func TestAgreementIsExact(t *testing.T) {
	if !diff("a", "p", 10_000, 10_000).Agrees() {
		t.Error("identical totals did not agree")
	}
	if diff("a", "p", 10_000, 10_001).Agrees() {
		t.Error("a one-micro difference was treated as agreement")
	}
}

func TestAgreeingRowsAreCountedNotListed(t *testing.T) {
	report := Reconcile([]Difference{
		diff("a", "p1", 100, 100),
		diff("b", "p1", 200, 200),
		diff("c", "p1", 300, 300),
	})

	if report.Compared != 3 || report.Agreed != 3 {
		t.Fatalf("compared=%d agreed=%d, want 3 and 3", report.Compared, report.Agreed)
	}
	if len(report.Findings) != 0 {
		t.Errorf("a report whose body is matching lines is one nobody reads to the end: %d findings", len(report.Findings))
	}
	if !report.Ready() {
		t.Error("a fully agreeing pass over real data was not Ready")
	}
}

// The default for a difference is unexplained, and unexplained blocks.
// docs/DESIGN.md §11: "Never tune the rater to hide an unexplained
// difference."
func TestAnUnexplainedDifferenceBlocks(t *testing.T) {
	report := Reconcile([]Difference{
		diff("a", "p1", 100, 100),
		diff("b", "p1", 10_000, 12_000),
	})

	if report.Unexplained() != 1 {
		t.Fatalf("unexplained=%d, want 1", report.Unexplained())
	}
	if report.Ready() {
		t.Fatal("a cutover was permitted with an unexplained difference")
	}
	if report.Findings[0].Kind != KindUnexplained {
		t.Errorf("finding kind = %v, want %v", report.Findings[0].Kind, KindUnexplained)
	}
}

// "We found no problems" is not the same claim as "we looked". A pass
// over nothing produces zero unexplained differences, and §11 puts
// reconciliation before cutover so that the looking happened.
func TestAPassOverNothingIsNotReady(t *testing.T) {
	report := Reconcile(nil)

	if report.Unexplained() != 0 {
		t.Fatalf("an empty pass reported %d unexplained", report.Unexplained())
	}
	if report.Ready() {
		t.Fatal("a reconciliation that compared nothing reported itself ready for cutover")
	}
	if !strings.Contains(report.String(), "NOT READY") {
		t.Error("the report does not say why it is not ready")
	}
}

// An explanation must be bounded, directed and scoped. Each of those is
// a way an entry could otherwise absorb a difference nobody reasoned
// about.
func TestExplanationScopeBinds(t *testing.T) {
	restore := explanations
	t.Cleanup(func() { explanations = restore })

	explanations = []Explanation{{
		Scope: Scope{
			AccountID:         "acct-1",
			MaxAbsDeltaMicros: 1_000,
			Direction:         ShadowHigher,
		},
		Why: "the legacy path rounds each line before summing; the rater sums then rounds once, per the published terms",
	}}

	cases := []struct {
		name          string
		difference    Difference
		wantExplained bool
	}{
		{"inside every bound", diff("acct-1", "p1", 10_000, 10_500), true},
		{"a different account", diff("acct-2", "p1", 10_000, 10_500), false},
		{"larger than the bound", diff("acct-1", "p1", 10_000, 12_000), false},
		{"exactly at the bound", diff("acct-1", "p1", 10_000, 11_000), true},
		{"the other direction", diff("acct-1", "p1", 10_500, 10_000), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			report := Reconcile([]Difference{tc.difference})
			explained := report.Findings[0].Kind == KindExplained
			if explained != tc.wantExplained {
				t.Fatalf("explained = %v, want %v (delta %d)",
					explained, tc.wantExplained, tc.difference.DeltaMicros())
			}
		})
	}
}

// A malformed entry must cover nothing rather than everything. An
// explanation with no bound, or a direction nobody recognises, fails
// closed.
func TestMalformedExplanationCoversNothing(t *testing.T) {
	restore := explanations
	t.Cleanup(func() { explanations = restore })

	for name, entry := range map[string]Explanation{
		"no magnitude bound": {
			Scope: Scope{MaxAbsDeltaMicros: 0, Direction: EitherDirection},
			Why:   "unbounded",
		},
		"negative bound": {
			Scope: Scope{MaxAbsDeltaMicros: -1, Direction: EitherDirection},
			Why:   "nonsense bound",
		},
		"unrecognised direction": {
			Scope: Scope{MaxAbsDeltaMicros: 1_000_000, Direction: "sideways"},
			Why:   "unknown direction",
		},
	} {
		t.Run(name, func(t *testing.T) {
			explanations = []Explanation{entry}
			report := Reconcile([]Difference{diff("a", "p", 10_000, 10_100)})
			if report.Findings[0].Kind != KindUnexplained {
				t.Fatalf("a %s explanation covered a difference", name)
			}
		})
	}
}

// The biggest unexplained difference is the one worth reading first.
func TestUnexplainedComeFirstThenBySize(t *testing.T) {
	restore := explanations
	t.Cleanup(func() { explanations = restore })
	explanations = []Explanation{{
		Scope: Scope{AccountID: "explained", MaxAbsDeltaMicros: 1_000_000, Direction: EitherDirection},
		Why:   "a reviewed reason",
	}}

	report := Reconcile([]Difference{
		diff("explained", "p", 10_000, 900_000),
		diff("small", "p", 10_000, 10_100),
		diff("large", "p", 10_000, 60_000),
	})

	if got := report.Findings[0].Difference.AccountID; got != "large" {
		t.Errorf("first finding is %q, want the largest unexplained (\"large\")", got)
	}
	if got := report.Findings[1].Difference.AccountID; got != "small" {
		t.Errorf("second finding is %q, want \"small\"", got)
	}
	if report.Findings[2].Kind != KindExplained {
		t.Error("the explained finding did not sort last")
	}
}

// The register starts empty, and that is correct: no shadow run has
// happened, so no difference has been reasoned about. A non-empty
// default would mean differences were pre-approved.
func TestTheRegisterStartsEmpty(t *testing.T) {
	if len(explanations) != 0 {
		t.Fatalf("the explanation register ships with %d entries; every one is a "+
			"pre-approved difference nobody reviewed against real data", len(explanations))
	}
}
