package shadow

import "testing"

// migrations/billing/009_usage_aggregates.up.sql:28 states what the rollup
// writes:
//
//	charged_micros = round_half_up( raw_cost_micros * num / den )
//
// The intent rater derives quantity x unit_price — the PRE-markup figure. So
// the comparison must use raw_cost_micros. Comparing against charged_micros
// makes every marked-up metric disagree systematically for a reason that has
// nothing to do with the rater.
//
// Platform-infra carries 12/10, so a $10.00 base is charged $12.00. Under the
// old comparison a perfectly correct rater reported a 2_000_000-micro
// discrepancy on every single infra line, and the real signal would have been
// buried under artefacts.
func TestAMarkedUpPeriodAgreesWhenTheRaterIsRight(t *testing.T) {
	d := Difference{
		AccountID:        "acct-1",
		PeriodID:         "period-1",
		LegacyBaseMicros: 10_000_000, // raw_cost_micros
		LegacyMicros:     12_000_000, // charged_micros, 12/10 applied
		ShadowMicros:     10_000_000, // what a CORRECT rater derives
		IntentDigest:     "d",
	}

	if !d.Agrees() {
		t.Fatalf("a correct rater was reported as disagreeing by %d micros — "+
			"the comparison is against the post-markup figure", d.DeltaMicros())
	}
	if got := d.DeltaMicros(); got != 0 {
		t.Fatalf("DeltaMicros = %d, want 0", got)
	}
}

// The markup must not vanish. It is reported rather than compared, because a
// cutover that silently dropped it would under-bill every platform-infra line
// by 1/6 — and a tool that compared only the base would call that agreement.
func TestTheMarkupIsReportedNotDiscarded(t *testing.T) {
	d := Difference{
		LegacyBaseMicros: 10_000_000,
		LegacyMicros:     12_000_000,
		ShadowMicros:     10_000_000,
	}
	if got := d.MarkupMicros(); got != 2_000_000 {
		t.Fatalf("MarkupMicros = %d, want 2_000_000", got)
	}

	// The report lists only DISAGREEING rows, so the line under test has
	// to come from one — an agreeing period is counted, not printed.
	d.ShadowMicros = 10_500_000

	line := Reconcile([]Difference{d}).String()
	for _, want := range []string{"legacy_base=", "legacy_charged=", "markup="} {
		if !contains(line, want) {
			t.Fatalf("the report does not name %q, so a reader cannot see the markup:\n%s", want, line)
		}
	}
}

// A genuinely wrong rater must still be caught. Without this, the test above
// passes just as well against a comparison that always agrees.
func TestARealDisagreementStillFailsUnderMarkup(t *testing.T) {
	d := Difference{
		LegacyBaseMicros: 10_000_000,
		LegacyMicros:     12_000_000,
		ShadowMicros:     10_500_000, // rater is 500_000 too high
	}
	if d.Agrees() {
		t.Fatal("a rater 500_000 micros too high was reported as agreeing")
	}
	if got := d.DeltaMicros(); got != 500_000 {
		t.Fatalf("DeltaMicros = %d, want 500_000 — the delta must be against the base", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
