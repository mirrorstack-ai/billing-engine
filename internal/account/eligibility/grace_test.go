package eligibility

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// legacyEvaluate is a frozen copy of Evaluate as it was before the unpaid grace
// (billing-engine#329). With GraceEnabled false the live Evaluate must equal it
// for every input — that is the "flag off = byte-identical" contract.
func legacyEvaluate(s Signals) Verdict {
	var reasons []Reason
	if s.UsableNonFraudCardCount < 1 {
		reasons = append(reasons, ReasonNoUsableCard)
	}
	if s.FirstCharge == FirstChargeFailed {
		reasons = append(reasons, ReasonFirstChargeFailed)
	}
	if s.FailedChargeStreak >= MaxFailedChargeStreak {
		reasons = append(reasons, ReasonTooManyFailures)
	}
	if s.UnpaidInvoiceCount >= MaxUnpaidInvoices {
		reasons = append(reasons, ReasonUnpaidInvoices)
	}
	if len(reasons) == 0 {
		return Verdict{Blocked: false, Reason: ReasonEligible}
	}
	return Verdict{Blocked: true, Reason: reasons[0], Reasons: reasons}
}

var graceT0 = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

// TestEvaluate_GraceOff_GoldenTable: every combination of the four gates (and the
// grace-only fields set to arbitrary values) gives exactly the legacy verdict.
func TestEvaluate_GraceOff_GoldenTable(t *testing.T) {
	notice := graceT0
	for _, cards := range []int{0, 1, 3} {
		for _, fc := range []FirstChargeState{"", FirstChargeNone, FirstChargeSucceeded, FirstChargePending, FirstChargeFailed} {
			for _, streak := range []int{0, 1, 2, 5} {
				for _, unpaid := range []int{0, 1, 2, 7} {
					for _, n := range []*time.Time{nil, &notice} {
						for _, now := range []time.Time{{}, graceT0, graceT0.Add(30 * 24 * time.Hour)} {
							in := Signals{
								UsableNonFraudCardCount: cards, FirstCharge: fc,
								FailedChargeStreak: streak, UnpaidInvoiceCount: unpaid,
								GraceEnabled: false, Now: now, UnpaidNoticeSentAt: n,
							}
							got, want := Evaluate(in), legacyEvaluate(in)
							if !reflect.DeepEqual(got, want) {
								t.Fatalf("flag off diverged for %+v:\n got  %+v\n want %+v", in, got, want)
							}
							if got.GraceEndsAt != nil {
								t.Fatalf("flag off set GraceEndsAt for %+v", in)
							}
						}
					}
				}
			}
		}
	}
}

func TestEvaluate_Grace(t *testing.T) {
	end := graceT0.Add(UnpaidNoticeDays * 24 * time.Hour)
	// One input per unpaid gate (2, 3, 4), each with a card on file.
	gates := []struct {
		name   string
		in     Signals
		reason Reason
	}{
		{"first charge failed", Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeFailed}, ReasonFirstChargeFailed},
		{"failed streak", Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeSucceeded, FailedChargeStreak: MaxFailedChargeStreak}, ReasonTooManyFailures},
		{"unpaid invoices", Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeSucceeded, UnpaidInvoiceCount: MaxUnpaidInvoices}, ReasonUnpaidInvoices},
	}
	for _, g := range gates {
		t.Run(g.name, func(t *testing.T) {
			base := g.in
			base.GraceEnabled = true

			// No notice sent: fail toward service.
			s := base
			s.Now = graceT0
			v := Evaluate(s)
			if v.Blocked || v.Reason != ReasonGrace || v.GraceEndsAt != nil {
				t.Fatalf("no notice: want unblocked GRACE without end, got %+v", v)
			}
			if !reflect.DeepEqual(v.Reasons, []Reason{g.reason}) {
				t.Fatalf("Reasons must still list the failing gate, got %v", v.Reasons)
			}

			// Notice at T, one minute before day 14: still graced, ends at T+14d.
			s = base
			s.UnpaidNoticeSentAt = &graceT0
			s.Now = end.Add(-time.Minute)
			v = Evaluate(s)
			if v.Blocked || v.Reason != ReasonGrace || v.GraceEndsAt == nil || !v.GraceEndsAt.Equal(end) {
				t.Fatalf("T+13d23h59m: want GRACE ending %v, got %+v", end, v)
			}

			// Exactly T+14d: blocked as today.
			s.Now = end
			v = Evaluate(s)
			if !v.Blocked || v.Reason != g.reason || v.GraceEndsAt != nil {
				t.Fatalf("T+14d: want blocked %s, got %+v", g.reason, v)
			}
			if want := legacyEvaluate(s); !reflect.DeepEqual(v, want) {
				t.Fatalf("after grace must equal today's verdict: got %+v want %+v", v, want)
			}

			// Long after: blocked.
			s.Now = end.Add(48 * time.Hour)
			if v = Evaluate(s); !v.Blocked || v.Reason != g.reason {
				t.Fatalf("T+16d: want blocked, got %+v", v)
			}

			// Gate 1 also failing: blocked at once, in grace and without notice.
			for _, n := range []*time.Time{nil, &graceT0} {
				s = base
				s.UsableNonFraudCardCount = 0
				s.UnpaidNoticeSentAt = n
				s.Now = graceT0.Add(time.Hour)
				v = Evaluate(s)
				if !v.Blocked || v.Reason != ReasonNoUsableCard || v.GraceEndsAt != nil {
					t.Fatalf("no card + %s: want immediate NO_USABLE_CARD, got %+v", g.name, v)
				}
				if want := legacyEvaluate(s); !reflect.DeepEqual(v, want) {
					t.Fatalf("gate 1 must equal today's verdict: got %+v want %+v", v, want)
				}
			}
		})
	}
}

func TestEvaluate_GraceOn_NotAnUnpaidBill(t *testing.T) {
	// Healed: eligible, no grace marker, even with an old open notice.
	v := Evaluate(Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeSucceeded, GraceEnabled: true, Now: graceT0, UnpaidNoticeSentAt: &graceT0})
	if v.Blocked || v.Reason != ReasonEligible || v.GraceEndsAt != nil || len(v.Reasons) != 0 {
		t.Fatalf("healed: want plain ELIGIBLE, got %+v", v)
	}
	// Card-only failure is not an unpaid bill: no grace.
	v = Evaluate(Signals{UsableNonFraudCardCount: 0, FirstCharge: FirstChargeNone, GraceEnabled: true, Now: graceT0})
	if !v.Blocked || v.Reason != ReasonNoUsableCard {
		t.Fatalf("no card alone: want blocked, got %+v", v)
	}
}

// TestEvaluate_GraceDoesNotMutateSignals / purity: same input, same output.
func TestEvaluate_GracePure(t *testing.T) {
	in := Signals{UsableNonFraudCardCount: 1, UnpaidInvoiceCount: 2, GraceEnabled: true, Now: graceT0, UnpaidNoticeSentAt: &graceT0}
	a, b := Evaluate(in), Evaluate(in)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("Evaluate is not deterministic: %+v vs %+v", a, b)
	}
}

func TestUnpaidNoticeDays(t *testing.T) {
	if UnpaidNoticeDays != 14 {
		t.Fatalf("UnpaidNoticeDays = %d, the Terms promise 14", UnpaidNoticeDays)
	}
}

// TestPackageHasNoClock: the gate stays pure — no file in this package may
// import "time" for a clock call or name time.Now / time.Since / time.Until.
func TestPackageHasNoClock(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob: %v %v", files, err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parser.ParseFile(token.NewFileSet(), f, src, parser.ImportsOnly); err != nil {
			t.Fatal(err)
		}
		for _, banned := range []string{"time.Now", "time.Since", "time.Until", "time.After", "time.Tick"} {
			if strings.Contains(stripComments(string(src)), banned) {
				t.Errorf("%s uses %s: the caller passes Signals.Now", f, strconv.Quote(banned))
			}
		}
	}
}

// stripComments drops // line comments so prose naming time.Now does not trip
// the check.
func stripComments(src string) string {
	var out []string
	for _, l := range strings.Split(src, "\n") {
		if i := strings.Index(l, "//"); i >= 0 {
			l = l[:i]
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
}
