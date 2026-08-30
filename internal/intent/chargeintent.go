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
	// Kind is what this charge is for, from the closed catalog in
	// docs/DESIGN.md §6.
	//
	// It is sealed into the intent rather than passed to Permits,
	// because it selects which rule of a standing authorization
	// applies. A caller that chose it at authorization time could pick
	// the permission its charge fits — which is INV-001's shape, one
	// field further in: the engine must derive what is being charged
	// for, not be told.
	Kind ChargeKind
	// TermsRevision is the customer terms this charge is made under.
	// INV-003 seals "policy versions", and terms are one: a standing
	// authorization accepted under one revision does not cover a charge
	// made under another, and without this field on the intent there is
	// nothing for the authorization to compare against.
	TermsRevision string
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
	kind              ChargeKind
	priceBookRevision string
	termsRevision     string
	tax               TaxDetermination
	authorizationID   string
	noticePolicy      string
	executeNotBefore  time.Time
	executeNotAfter   time.Time
	sourceFactKeys    []string

	subtotalMicros int64
	totalMicros    int64
	digest         string

	// collects is the digest of an intent this one collects the REMAINDER
	// of, empty unless this is a receivable.
	//
	// It is deliberately NOT supersedes, and the distinction is the whole
	// point. Superseding REPLACES a document: the original is no longer
	// owed. A receivable LINKS to one that is still owed and collects
	// what is left of it — both stay live, with a stated arithmetic
	// relation between them. docs/DESIGN.md §6 names the kind
	// (collect_receivable) and its funding: "a linked intent for the
	// remaining amount only, under a new FundingPlan and a
	// source-capacity reservation".
	//
	// Building this on supersedes would mark the original replaced while
	// the customer still owes it.
	collects string

	// supersedes is the digest of the intent this one replaces, empty
	// for an original. A correction is a new intent pointing at the old
	// one, which is what makes the history of a charge readable.
	supersedes string
}

// Errors from Seal.
var (
	ErrNoLines                 = errors.New("intent: a charge intent with no lines proposes nothing")
	ErrCurrencyMissing         = errors.New("intent: no currency")
	ErrPriceBookMissing        = errors.New("intent: no price book revision")
	ErrTermsMissing            = errors.New("intent: no terms revision")
	ErrKindMissing             = errors.New("intent: no charge kind")
	ErrNotSealed               = errors.New("intent: the source intent is not sealed")
	ErrReceivableExceedsSource = errors.New("intent: a receivable cannot exceed the intent it collects")
	ErrReceivablePayerMoved    = errors.New("intent: a receivable must name the same payer as the intent it collects")
	ErrReceivableCurrencyMoved = errors.New("intent: a receivable must name the same currency as the intent it collects")
	ErrKindNotInCatalog        = errors.New("intent: charge kind is not in the closed catalog of docs/DESIGN.md §6")
	ErrTaxUnresolved           = errors.New("intent: tax is undetermined; a charge must not be sealed over a guess")
	ErrTaxNegative             = errors.New("intent: tax is negative")
	ErrAuthorizationUnset      = errors.New("intent: no billing authorization referenced")
	ErrNoticePolicyUnset       = errors.New("intent: no notice policy")
	ErrWindowUnset             = errors.New("intent: no execution window")
	ErrWindowInverted          = errors.New("intent: execution window ends before it begins")
	ErrPayerUnknown            = errors.New("intent: payer is not a subject kind the engine knows")
	ErrNoSourceFacts           = errors.New("intent: no source facts; a total with no reported usage behind it is an assertion")
	ErrNegativeLine            = errors.New("intent: a line has a negative quantity or price")
	ErrAmountOverflow          = errors.New("intent: the amount does not fit in int64")
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
		// Quantity x price is the one multiplication in this package,
		// and int64 wraps silently. A wrapped product does not fail
		// loudly — it turns an enormous charge into a small or negative
		// one, and every later check then agrees with it because the
		// number really is small. Refusing is the only outcome that
		// keeps the total meaning what it says.
		if _, ok := mulOK(line.Quantity, line.UnitPriceMicros); !ok {
			return ChargeIntent{}, fmt.Errorf("%w: line %d, %d x %d",
				ErrAmountOverflow, i, line.Quantity, line.UnitPriceMicros)
		}
	}
	if strings.TrimSpace(draft.PriceBookRevision) == "" {
		return ChargeIntent{}, ErrPriceBookMissing
	}
	if strings.TrimSpace(draft.TermsRevision) == "" {
		return ChargeIntent{}, ErrTermsMissing
	}
	if strings.TrimSpace(string(draft.Kind)) == "" {
		return ChargeIntent{}, ErrKindMissing
	}
	// docs/DESIGN.md §6: "A positive customer charge kind that is not
	// listed below must not be proposed, and must not be collected. No
	// private caller, module, adapter, webhook, tax vendor or operator may
	// introduce a kind from free text."
	//
	// Refused at Seal, not at the predicate, because unlike a policy
	// revision there is no phase in which an invented kind is legitimate.
	// A revision that is not yet published still names a real decision the
	// shadow phase is allowed to propose under; a kind outside the catalog
	// names nothing at all, and sealing it puts a word into the digest that
	// no published rule defines.
	if !KindInCatalog(draft.Kind) {
		return ChargeIntent{}, fmt.Errorf("%w: %q", ErrKindNotInCatalog, draft.Kind)
	}
	if !draft.Tax.Resolved {
		return ChargeIntent{}, ErrTaxUnresolved
	}
	// Tax adds to a charge; it does not fund one. A negative amount
	// would pull the total below the subtotal the customer was shown,
	// and the lines are already refused for being negative — leaving
	// this open would be the same hole with one more step.
	if draft.Tax.AmountMicros < 0 {
		return ChargeIntent{}, fmt.Errorf("%w: %d", ErrTaxNegative, draft.Tax.AmountMicros)
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
		kind:              draft.Kind,
		priceBookRevision: draft.PriceBookRevision,
		termsRevision:     draft.TermsRevision,
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
	//
	// The sums are checked for the same reason the products are. Each
	// line can fit and their total not.
	for _, line := range intent.lines {
		sum, ok := addOK(intent.subtotalMicros, line.AmountMicros())
		if !ok {
			return ChargeIntent{}, fmt.Errorf("%w: subtotal", ErrAmountOverflow)
		}
		intent.subtotalMicros = sum
	}
	total, ok := addOK(intent.subtotalMicros, intent.tax.AmountMicros)
	if !ok {
		return ChargeIntent{}, fmt.Errorf("%w: total with tax", ErrAmountOverflow)
	}
	intent.totalMicros = total
	intent.digest = intent.computeDigest()
	return intent, nil
}

// CollectRemainderOf seals a receivable for what is still owed on this
// intent.
//
// docs/DESIGN.md §6 lists `collect_receivable` as its own intent kind,
// funded by "a linked intent for the remaining amount only, under a new
// FundingPlan and a source-capacity reservation".
//
// The remainder is passed in rather than derived here, because only the
// caller knows what has been collected so far — but it is BOUNDED here,
// because a receivable for more than the original owed is not a
// remainder, it is a new charge wearing a link.
//
// The result is a distinct document naming what it collects. Both stay
// live: the original is still owed until its remainder reaches zero,
// which is what distinguishes this from Supersede.
func (c ChargeIntent) CollectRemainderOf(draft Draft) (ChargeIntent, error) {
	if !c.Sealed() {
		return ChargeIntent{}, ErrNotSealed
	}
	if draft.Kind != KindCollectReceivable {
		return ChargeIntent{}, fmt.Errorf("%w: a receivable must be sealed as %q, not %q",
			ErrKindNotInCatalog, KindCollectReceivable, draft.Kind)
	}
	receivable, err := Seal(draft)
	if err != nil {
		return ChargeIntent{}, err
	}
	if receivable.totalMicros > c.totalMicros {
		return ChargeIntent{}, fmt.Errorf("%w: receivable of %d exceeds the %d it collects",
			ErrReceivableExceedsSource, receivable.totalMicros, c.totalMicros)
	}
	if receivable.payer != c.payer {
		return ChargeIntent{}, ErrReceivablePayerMoved
	}
	if receivable.currency != c.currency {
		return ChargeIntent{}, ErrReceivableCurrencyMoved
	}
	receivable.collects = c.digest
	receivable.digest = receivable.computeDigest()
	return receivable, nil
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

	e.string(string(c.kind))
	e.string(c.priceBookRevision)
	e.string(c.termsRevision)
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
	e.string(c.collects)
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

// Kind is what this charge is for.
func (c ChargeIntent) Kind() ChargeKind { return c.kind }

// TermsRevision names the customer terms this charge is made under.
func (c ChargeIntent) TermsRevision() string { return c.termsRevision }

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

// Collects is the digest of the intent whose remainder this one collects,
// empty unless this is a receivable.
func (c ChargeIntent) Collects() string { return c.collects }

// Sealed reports whether this is a real sealed intent rather than a
// zero value. A zero ChargeIntent has no digest, and nothing may be
// executed against it.
func (c ChargeIntent) Sealed() bool { return c.digest != "" }

// mulOK multiplies and reports whether the result is exact.
//
// Written with a division check rather than by widening, because there
// is no wider integer type here and money must not pass through a
// float: a float64 loses exactness above 2^53, which is well inside the
// range a micro-dollar amount can reach.
func mulOK(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	product := a * b
	if product/b != a {
		return 0, false
	}
	return product, true
}

// addOK adds and reports whether the result is exact. Overflow shows up
// as a sum that moved the wrong way relative to the addend.
func addOK(a, b int64) (int64, bool) {
	sum := a + b
	if (b > 0 && sum < a) || (b < 0 && sum > a) {
		return 0, false
	}
	return sum, true
}

// Stored is a charge intent as it was read back from durable storage.
//
// It exists because a row is not a sealed intent. Seal validates and
// derives; a SELECT does neither, so a value reconstructed from columns
// has had none of the checks that made the original trustworthy — and
// docs/DESIGN.md's whole argument rests on the sealed document being
// the one the customer saw.
type Stored struct {
	Digest string

	Payer             Subject
	Currency          string
	Lines             []Line
	PriceBookRevision string
	TermsRevision     string
	Kind              ChargeKind
	Tax               TaxDetermination
	AuthorizationID   string
	NoticePolicy      string
	ExecuteNotBefore  time.Time
	ExecuteNotAfter   time.Time
	SourceFactKeys    []string
	Supersedes        string

	SubtotalMicros int64
	TotalMicros    int64
}

// ErrDigestMismatch is returned when a stored intent does not hash to
// the digest stored beside it.
var ErrDigestMismatch = errors.New("intent: stored intent does not match its digest")

// Rehydrate reconstructs a sealed intent from storage, and refuses one
// whose contents no longer produce its own digest.
//
// This is the control the digest was always for. Every field is fed
// back through the same canonical encoding and the result compared with
// the digest the row carries. A tampered amount, a swapped payer, an
// edited price-book revision — any of them changes the hash, and the
// intent does not load.
//
// So the check runs against the database rather than against a caller.
// The trigger on ms_billing.charge_intents refuses an UPDATE, but a
// trigger protects one table in one deployment; this protects the
// document itself, wherever it has been. A restored backup, a replicated
// row, a migration that rewrote a column in passing, and a deliberate
// edit by someone holding the database credential all fail the same way.
//
// It also recomputes the totals rather than trusting the stored ones,
// for the reason Seal does: a total nobody derived is an assertion.
func Rehydrate(stored Stored) (ChargeIntent, error) {
	if strings.TrimSpace(stored.Digest) == "" {
		return ChargeIntent{}, fmt.Errorf("%w: no digest stored", ErrDigestMismatch)
	}

	rebuilt, err := Seal(Draft{
		Payer:             stored.Payer,
		Currency:          stored.Currency,
		Lines:             stored.Lines,
		Kind:              stored.Kind,
		PriceBookRevision: stored.PriceBookRevision,
		TermsRevision:     stored.TermsRevision,
		Tax:               stored.Tax,
		AuthorizationID:   stored.AuthorizationID,
		NoticePolicy:      stored.NoticePolicy,
		ExecuteNotBefore:  stored.ExecuteNotBefore,
		ExecuteNotAfter:   stored.ExecuteNotAfter,
		SourceFactKeys:    stored.SourceFactKeys,
	})
	if err != nil {
		// A row that cannot be sealed is a row that should never have
		// been written. Surfacing the reason is more useful than a bare
		// mismatch.
		return ChargeIntent{}, fmt.Errorf("%w: %w", ErrDigestMismatch, err)
	}

	// The supersede link is part of what is attested, so it has to be in
	// place before the digest is recomputed.
	if stored.Supersedes != "" {
		rebuilt.supersedes = stored.Supersedes
		rebuilt.digest = rebuilt.computeDigest()
	}

	if rebuilt.digest != stored.Digest {
		return ChargeIntent{}, fmt.Errorf("%w: stored %s, recomputed %s",
			ErrDigestMismatch, stored.Digest, rebuilt.digest)
	}

	// The stored totals are compared, not adopted. A row whose total
	// disagrees with its own lines is one where something wrote a number
	// nobody derived — and because the totals are inside the digest, the
	// check above has usually caught it already. Usually is not a
	// guarantee worth resting a charge on.
	if stored.SubtotalMicros != rebuilt.subtotalMicros || stored.TotalMicros != rebuilt.totalMicros {
		return ChargeIntent{}, fmt.Errorf("%w: stored total %d/%d, derived %d/%d",
			ErrDigestMismatch, stored.SubtotalMicros, stored.TotalMicros,
			rebuilt.subtotalMicros, rebuilt.totalMicros)
	}

	return rebuilt, nil
}
