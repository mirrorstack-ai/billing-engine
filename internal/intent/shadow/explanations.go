package shadow

// Explanation records why a shadow total is allowed to differ from what
// the legacy path charged.
//
// docs/DESIGN.md §11 step 4: "Reconcile shadow totals against current
// invoices until every difference is explained. Never tune the rater to
// hide an unexplained difference."
//
// An explanation is a claim about which figure is right and why, and it
// is written here rather than encoded as a tolerance or a special case
// in the rater. The distinction is the point. A special case makes the
// columns agree and destroys the evidence; an entry here leaves the
// difference visible and says what it means, so a reviewer can decide
// the reasoning is wrong.
type Explanation struct {
	// Scope selects which differences this covers.
	Scope Scope
	// Why must say which figure is correct and why the other one is
	// not. "Rounding" is not an explanation; "the legacy path rounds
	// each line before summing, the rater sums then rounds, and the
	// published terms say the total is rounded once" is.
	Why string
}

// Scope selects the differences an explanation covers. An explanation
// with no scope covers nothing: a blanket entry would close the whole
// register at once, which is the failure this file exists to prevent.
type Scope struct {
	// AccountID limits the explanation to one account. Empty matches
	// any account, which is only appropriate for a systematic
	// difference — a pricing rule change, not one customer's data.
	AccountID string
	// PeriodID limits it to one period. Empty matches any.
	PeriodID string
	// MaxAbsDeltaMicros bounds how large a difference this explanation
	// may cover. Required and non-zero: an unbounded explanation would
	// silently absorb a difference far larger than the one that was
	// reasoned about.
	MaxAbsDeltaMicros int64
	// Direction restricts the sign. A reason that explains the rater
	// charging LESS does not also explain it charging more.
	Direction Direction
}

// Direction is which way a difference may run.
type Direction string

const (
	// ShadowHigher: the new rater derives more than the legacy path
	// charged.
	ShadowHigher Direction = "shadow_higher"
	// ShadowLower: the new rater derives less.
	ShadowLower Direction = "shadow_lower"
	// EitherDirection is deliberately available but rarely right: a
	// cause that can push a total either way is usually two causes.
	EitherDirection Direction = "either"
)

// explanations is the reviewed register.
//
// It is empty, and that is the correct state before any shadow run has
// happened. Every entry added here is a decision that a difference is
// acceptable, so entries arrive one at a time with the reasoning in
// Why, and they are reviewed as carefully as a pricing change — because
// that is what they are.
var explanations []Explanation

// explanationFor returns the reason covering a difference, if any.
//
// A difference matches only if the scope covers its account, its
// period, its direction, and its magnitude. Magnitude is checked last
// and is the clause most likely to refuse: an explanation reasoned
// about a small systematic gap must not silently absorb a large one
// that happens to share a cause.
func explanationFor(d Difference) (string, bool) {
	delta := d.DeltaMicros()
	for _, e := range explanations {
		if e.Scope.AccountID != "" && e.Scope.AccountID != d.AccountID {
			continue
		}
		if e.Scope.PeriodID != "" && e.Scope.PeriodID != d.PeriodID {
			continue
		}
		if e.Scope.MaxAbsDeltaMicros <= 0 {
			// An unbounded explanation covers nothing. Requiring a
			// bound here rather than at construction means a malformed
			// entry fails closed instead of matching everything.
			continue
		}
		if abs(delta) > e.Scope.MaxAbsDeltaMicros {
			continue
		}
		switch e.Scope.Direction {
		case ShadowHigher:
			if delta <= 0 {
				continue
			}
		case ShadowLower:
			if delta >= 0 {
				continue
			}
		case EitherDirection:
		default:
			// An unrecognised direction covers nothing.
			continue
		}
		return e.Why, true
	}
	return "", false
}
