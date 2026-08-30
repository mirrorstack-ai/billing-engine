package intent

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Line is one priced row of a charge intent.
//
// It holds only factors. There is no amount field, because a struct
// carrying a total alongside the quantity and price that produce it is
// a struct where the two can disagree — and then something has to
// decide which is real, and whatever it decides, the number is no
// longer derived.
//
// An earlier version did store the amount, computed by NewLine. The
// factors are exported, so Line{Quantity: 5, UnitPriceMicros: 100} was
// a valid struct literal with an amount of zero: a silent undercharge,
// which is the failure nobody reports. Computing on access removes the
// possibility rather than normalising it away afterwards.
type Line struct {
	Meter           string
	Module          string
	ModuleVersion   string
	Quantity        int64
	UnitPriceMicros int64
}

// NewLine is a convenience for reading call sites; a struct literal is
// equally correct.
func NewLine(meter, module, moduleVersion string, quantity, unitPriceMicros int64) Line {
	return Line{
		Meter:           meter,
		Module:          module,
		ModuleVersion:   moduleVersion,
		Quantity:        quantity,
		UnitPriceMicros: unitPriceMicros,
	}
}

// AmountMicros is the line total, derived on every read.
func (l Line) AmountMicros() int64 { return l.Quantity * l.UnitPriceMicros }

// TaxDetermination is the frozen tax decision for an intent.
//
// docs/DESIGN.md §7 requires the engine to refuse to guess: a
// determination that has not been made is not the same as one that came
// out at zero, so Resolved distinguishes them. INV-004 forbids an
// unknown input from silently becoming zero, and tax is the input where
// that mistake is most tempting.
type TaxDetermination struct {
	// Resolved is false when no determination has been made. An intent
	// with an unresolved determination cannot be sealed.
	Resolved bool
	// Jurisdiction is the authority whose rule was applied.
	Jurisdiction string
	// RuleRevision identifies the frozen rule set used.
	RuleRevision string
	// AmountMicros may legitimately be zero — a justified zero-tax
	// result is a determination, and Resolved is what says so.
	AmountMicros int64
}

// Draft is a proposed charge before it is sealed.
//
// It is an ordinary mutable struct on purpose: this is the stage where
// a rater assembles a proposal. Sealing is what makes it an intent, and
// what makes it immutable.
type Draft struct {
	// Payer is who would be charged.
	Payer Subject
	// Currency is the ISO code the amounts are denominated in.
	Currency string
	// Lines are the priced rows.
	Lines []Line
	// PriceBookRevision identifies the one effective price revision
	// resolved for this intent. Reproducibility depends on it: a total
	// whose price source is unnamed cannot be recomputed later.
	PriceBookRevision string
	// Tax is the frozen determination.
	Tax TaxDetermination
	// AuthorizationID references the BillingAuthorization that permits
	// this debit (INV-006).
	AuthorizationID string
	// NoticePolicy names the delivery rule this intent is disclosed
	// under (INV-005).
	NoticePolicy string
	// ExecuteNotBefore and ExecuteNotAfter bound when the intent may be
	// executed. A window rather than an instant, because the caller
	// must not choose executeAt (INV-001) and an unbounded intent is
	// one that can be settled long after the customer stopped
	// expecting it.
	ExecuteNotBefore time.Time
	ExecuteNotAfter  time.Time
	// SourceFactKeys are the idempotency keys of the usage facts this
	// intent was derived from, so a reader can walk back from a total
	// to what was reported.
	SourceFactKeys []string
}

// ChargeIntent is a sealed, immutable proposal to move money.
//
// INV-003: "Once sealed, a ChargeIntent may not update source
// references, lines, policy versions, tax result, currency, rounding,
// payer, authorization, notice policy, execution window or total. A
// one-unit change creates a new intent, supersedes the old one, and
// repeats every notice and authorization check."
//
// Immutability is structural, not documented. Every field is
// unexported and there is exactly one constructor, so a sealed intent
// cannot be edited from outside this package — an invariant a reviewer
// can confirm from the type rather than by auditing every caller.
//
// "Otherwise the document says one thing when you read it and another
// when it settles. Nobody can put a number on how far a mutable object
// drifts, so it is not made mutable. Superseding is cheap; editing is
// unanswerable."
type ChargeIntent struct {
	payer             Subject
	currency          string
	lines             []Line
	priceBookRevision string
	tax               TaxDetermination
	authorizationID   string
	noticePolicy      string
	executeNotBefore  time.Time
	executeNotAfter   time.Time
	sourceFactKeys    []string

	subtotalMicros int64
	totalMicros    int64
	digest         string

	// supersedes is the digest of the intent this one replaces, empty
	// for an original. A correction is a new intent pointing at the old
	// one, which is what makes the history of a charge readable.
	supersedes string
}

// Errors from Seal.
var (
	ErrNoLines            = errors.New("intent: a charge intent with no lines proposes nothing")
	ErrCurrencyMissing    = errors.New("intent: no currency")
	ErrPriceBookMissing   = errors.New("intent: no price book revision")
	ErrTaxUnresolved      = errors.New("intent: tax is undetermined; a charge must not be sealed over a guess")
	ErrAuthorizationUnset = errors.New("intent: no billing authorization referenced")
	ErrNoticePolicyUnset  = errors.New("intent: no notice policy")
	ErrWindowUnset        = errors.New("intent: no execution window")
	ErrWindowInverted     = errors.New("intent: execution window ends before it begins")
	ErrPayerUnknown       = errors.New("intent: payer is not a subject kind the engine knows")
	ErrNoSourceFacts      = errors.New("intent: no source facts; a total with no reported usage behind it is an assertion")
	ErrNegativeLine       = errors.New("intent: a line has a negative quantity or price")
)

// Seal validates a draft and returns the immutable intent.
//
// Every refusal here is INV-004: an unknown input must quarantine the
// intent rather than dispatch an effect. Nothing is defaulted. An
// absent currency is not USD, an undetermined tax is not zero, and a
// missing authorization is not permission.
func Seal(draft Draft) (ChargeIntent, error) {
	if !draft.Payer.Valid() {
		return ChargeIntent{}, fmt.Errorf("%w: %q", ErrPayerUnknown, draft.Payer.Kind)
	}
	if strings.TrimSpace(draft.Currency) == "" {
		return ChargeIntent{}, ErrCurrencyMissing
	}
	if len(draft.Lines) == 0 {
		return ChargeIntent{}, ErrNoLines
	}
	for i, line := range draft.Lines {
		if line.Quantity < 0 || line.UnitPriceMicros < 0 {
			return ChargeIntent{}, fmt.Errorf("%w: line %d", ErrNegativeLine, i)
		}
	}
	if strings.TrimSpace(draft.PriceBookRevision) == "" {
		return ChargeIntent{}, ErrPriceBookMissing
	}
	if !draft.Tax.Resolved {
		return ChargeIntent{}, ErrTaxUnresolved
	}
	if strings.TrimSpace(draft.AuthorizationID) == "" {
		return ChargeIntent{}, ErrAuthorizationUnset
	}
	if strings.TrimSpace(draft.NoticePolicy) == "" {
		return ChargeIntent{}, ErrNoticePolicyUnset
	}
	if draft.ExecuteNotBefore.IsZero() || draft.ExecuteNotAfter.IsZero() {
		return ChargeIntent{}, ErrWindowUnset
	}
	if draft.ExecuteNotAfter.Before(draft.ExecuteNotBefore) {
		return ChargeIntent{}, ErrWindowInverted
	}
	if len(draft.SourceFactKeys) == 0 {
		return ChargeIntent{}, ErrNoSourceFacts
	}

	intent := ChargeIntent{
		payer:             draft.Payer,
		currency:          strings.ToUpper(strings.TrimSpace(draft.Currency)),
		lines:             append([]Line(nil), draft.Lines...),
		priceBookRevision: draft.PriceBookRevision,
		tax:               draft.Tax,
		authorizationID:   draft.AuthorizationID,
		noticePolicy:      draft.NoticePolicy,
		executeNotBefore:  draft.ExecuteNotBefore.UTC(),
		executeNotAfter:   draft.ExecuteNotAfter.UTC(),
		sourceFactKeys:    append([]string(nil), draft.SourceFactKeys...),
	}

	// The total is computed here and nowhere else. INV-002: one
	// derivation powers preview and settlement, so there is one place
	// this arithmetic happens and every consumer reads its result.
	for _, line := range intent.lines {
		intent.subtotalMicros += line.AmountMicros()
	}
	intent.totalMicros = intent.subtotalMicros + intent.tax.AmountMicros
	intent.digest = intent.computeDigest()
	return intent, nil
}

// Supersede seals a replacement for this intent.
//
// INV-003: a one-unit change creates a new intent, supersedes the old
// one, and repeats every notice and authorization check. Editing is not
// offered, so this is the only way a sealed proposal can change, and
// the result is a distinct document with its own digest that names what
// it replaced.
func (c ChargeIntent) Supersede(draft Draft) (ChargeIntent, error) {
	replacement, err := Seal(draft)
	if err != nil {
		return ChargeIntent{}, err
	}
	replacement.supersedes = c.digest
	// The link is part of what is being attested, so the digest is
	// taken again with it in place.
	replacement.digest = replacement.computeDigest()
	return replacement, nil
}

func (c ChargeIntent) computeDigest() string {
	e := newCanonicalEncoder()
	e.string(c.payer.Kind)
	e.string(c.payer.ID)
	e.string(c.currency)

	e.count(len(c.lines))
	for _, line := range c.lines {
		e.string(line.Meter)
		e.string(line.Module)
		e.string(line.ModuleVersion)
		e.int(line.Quantity)
		e.int(line.UnitPriceMicros)
		e.int(line.AmountMicros())
	}

	e.string(c.priceBookRevision)
	e.string(c.tax.Jurisdiction)
	e.string(c.tax.RuleRevision)
	e.int(c.tax.AmountMicros)
	e.string(c.authorizationID)
	e.string(c.noticePolicy)
	e.time(c.executeNotBefore)
	e.time(c.executeNotAfter)

	e.count(len(c.sourceFactKeys))
	for _, key := range c.sourceFactKeys {
		e.string(key)
	}

	e.int(c.subtotalMicros)
	e.int(c.totalMicros)
	e.string(c.supersedes)
	return e.digest()
}

// Digest is the identity of this exact document. It is what a
// disclosure is bound to and what an acceptance receipt references, so
// a fabricated acceptance is something that can later be pointed at.
func (c ChargeIntent) Digest() string { return c.digest }

// Payer is who would be charged.
func (c ChargeIntent) Payer() Subject { return c.payer }

// Currency is the ISO code the amounts are denominated in.
func (c ChargeIntent) Currency() string { return c.currency }

// Lines returns a copy, so a caller holding the slice cannot reach back
// into the sealed intent.
func (c ChargeIntent) Lines() []Line { return append([]Line(nil), c.lines...) }

// PriceBookRevision names the one effective price revision used.
func (c ChargeIntent) PriceBookRevision() string { return c.priceBookRevision }

// Tax is the frozen determination.
func (c ChargeIntent) Tax() TaxDetermination { return c.tax }

// AuthorizationID references the authorization permitting this debit.
func (c ChargeIntent) AuthorizationID() string { return c.authorizationID }

// NoticePolicy names the delivery rule this intent is disclosed under.
func (c ChargeIntent) NoticePolicy() string { return c.noticePolicy }

// ExecutionWindow bounds when this intent may be executed.
func (c ChargeIntent) ExecutionWindow() (notBefore, notAfter time.Time) {
	return c.executeNotBefore, c.executeNotAfter
}

// SourceFactKeys are the usage facts this intent was derived from.
func (c ChargeIntent) SourceFactKeys() []string {
	return append([]string(nil), c.sourceFactKeys...)
}

// SubtotalMicros is the sum of the lines, before tax.
func (c ChargeIntent) SubtotalMicros() int64 { return c.subtotalMicros }

// TotalMicros is what would be collected.
func (c ChargeIntent) TotalMicros() int64 { return c.totalMicros }

// Supersedes is the digest of the intent this one replaces, or empty.
func (c ChargeIntent) Supersedes() string { return c.supersedes }

// Sealed reports whether this is a real sealed intent rather than a
// zero value. A zero ChargeIntent has no digest, and nothing may be
// executed against it.
func (c ChargeIntent) Sealed() bool { return c.digest != "" }
