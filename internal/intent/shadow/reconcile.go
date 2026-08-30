// Package shadow compares what the new rater would charge against what
// the legacy path actually charged.
//
// docs/DESIGN.md §11 steps 3 and 4: generate shadow intents from
// current usage that notify nobody and move no money, then "reconcile
// shadow totals against current invoices until every difference is
// explained. Never tune the rater to hide an unexplained difference."
//
// That last sentence is the whole design of this package. Tuning is the
// easy path and it is available at every difference: nudge a rounding
// mode, add a special case, and the columns agree. The result looks
// like a reconciled migration and is a second implementation of the
// legacy bug, now with a clean report.
//
// So a difference is not closed by becoming zero. It is closed by
// someone writing down why it exists, in a form another person can
// disagree with. Explanations live in explanations.go, and a difference
// with no matching explanation keeps the migration blocked.
package shadow

import (
	"sort"
	"strings"
)

// Difference is one account-period where the shadow total and the
// legacy charge disagree.
type Difference struct {
	// AccountID and PeriodID identify the comparison.
	AccountID string
	PeriodID  string

	// LegacyMicros is what the current code CHARGED, read from the
	// settled record rather than recomputed — post-markup.
	LegacyMicros int64
	// LegacyBaseMicros is the same usage PRE-markup, and it is the
	// figure the comparison actually uses. See Source.Period.
	LegacyBaseMicros int64
	// ShadowMicros is what the intent rater derived for the same
	// inputs.
	ShadowMicros int64

	// IntentDigest identifies the shadow document, so a reviewer can
	// pull the exact lines that produced ShadowMicros.
	IntentDigest string
}

// DeltaMicros is shadow minus legacy. Positive means the new rater
// would have charged more.
func (d Difference) DeltaMicros() int64 { return d.ShadowMicros - d.LegacyBaseMicros }

// MarkupMicros is what the legacy rollup added on top of the base. It is
// reported rather than compared, because a cutover that silently dropped
// it would under-bill every platform-infra line by 1/6 and this tool
// would call that agreement.
func (d Difference) MarkupMicros() int64 { return d.LegacyMicros - d.LegacyBaseMicros }

// Agrees reports whether the two totals match exactly. Money is integer
// micro-dollars, so there is no tolerance and none is offered: a
// tolerance is a place for a real difference to hide.
func (d Difference) Agrees() bool { return d.ShadowMicros == d.LegacyBaseMicros }

// Kind classifies a difference by what is known about its cause.
type Kind string

const (
	// KindExplained: someone has written down why this difference
	// exists and why the shadow figure is the right one.
	KindExplained Kind = "explained"
	// KindUnexplained: nobody has. The migration stays blocked.
	KindUnexplained Kind = "unexplained"
)

// Finding pairs a difference with its explanation, if it has one.
type Finding struct {
	Difference  Difference
	Kind        Kind
	Explanation string
}

// Report is the outcome of a reconciliation pass.
type Report struct {
	// Compared is how many account-periods were examined. A report
	// over nothing is not a clean report, and Ready says so.
	Compared int
	// Agreed is how many matched exactly.
	Agreed int
	// Findings are the differences, explained and not, ordered so the
	// unexplained come first.
	Findings []Finding
}

// Unexplained counts the differences nobody has accounted for.
func (r Report) Unexplained() int {
	n := 0
	for _, f := range r.Findings {
		if f.Kind == KindUnexplained {
			n++
		}
	}
	return n
}

// Ready reports whether the reconciliation permits a cutover.
//
// It requires both that nothing is unexplained and that something was
// actually compared. A pass over zero account-periods produces zero
// unexplained differences, and "we found no problems" is not the same
// claim as "we looked". docs/DESIGN.md §11 puts reconciliation before
// cutover precisely so that the looking happened.
func (r Report) Ready() bool {
	return r.Compared > 0 && r.Unexplained() == 0
}

// Reconcile classifies a batch of differences against the recorded
// explanations.
//
// Agreeing rows are counted, not listed: a report whose body is
// thousands of matching lines is a report nobody reads to the end.
func Reconcile(differences []Difference) Report {
	report := Report{Compared: len(differences)}

	for _, d := range differences {
		if d.Agrees() {
			report.Agreed++
			continue
		}
		if reason, ok := explanationFor(d); ok {
			report.Findings = append(report.Findings, Finding{
				Difference: d, Kind: KindExplained, Explanation: reason,
			})
			continue
		}
		report.Findings = append(report.Findings, Finding{
			Difference: d, Kind: KindUnexplained,
		})
	}

	// Unexplained first, then by size of the disagreement: the biggest
	// unexplained difference is the one worth reading about first.
	sort.SliceStable(report.Findings, func(i, j int) bool {
		a, b := report.Findings[i], report.Findings[j]
		if (a.Kind == KindUnexplained) != (b.Kind == KindUnexplained) {
			return a.Kind == KindUnexplained
		}
		return abs(a.Difference.DeltaMicros()) > abs(b.Difference.DeltaMicros())
	})
	return report
}

// String renders a report for a human reviewing a cutover.
func (r Report) String() string {
	var b strings.Builder
	b.WriteString("shadow reconciliation\n")
	b.WriteString("  compared:    " + itoa(r.Compared) + "\n")
	b.WriteString("  agreed:      " + itoa(r.Agreed) + "\n")
	b.WriteString("  unexplained: " + itoa(r.Unexplained()) + "\n")
	for _, f := range r.Findings {
		d := f.Difference
		b.WriteString("  [" + string(f.Kind) + "] " + d.AccountID + "/" + d.PeriodID +
			" legacy_base=" + itoa64(d.LegacyBaseMicros) +
			" legacy_charged=" + itoa64(d.LegacyMicros) +
			" markup=" + itoa64(d.MarkupMicros()) +
			" shadow=" + itoa64(d.ShadowMicros) +
			" delta=" + itoa64(d.DeltaMicros()))
		// The digest names the shadow document, so a reviewer can pull
		// the exact lines that produced the figure — and when rating
		// refused, it carries the reason instead. A period that
		// contributed zero because it could not be rated looks
		// identical to one that was simply cheaper, unless the report
		// says which.
		if d.IntentDigest != "" {
			b.WriteString("\n      " + d.IntentDigest)
		}
		if f.Explanation != "" {
			b.WriteString("\n      " + f.Explanation)
		}
		b.WriteString("\n")
	}
	if !r.Ready() && r.Compared == 0 {
		b.WriteString("  NOT READY: nothing was compared. A pass over zero periods finds zero problems.\n")
	}
	return b.String()
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func itoa(n int) string { return itoa64(int64(n)) }

func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		return "-" + string(b)
	}
	return string(b)
}
