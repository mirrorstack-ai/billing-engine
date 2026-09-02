package intent

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ChargeKind is a category of thing a customer can be charged for.
// An authorization names the kinds it permits, so a standing permission
// to refill a wallet does not also permit a subscription increase.
type ChargeKind string

// AuthorizationScope distinguishes the two shapes INV-006 allows.
type AuthorizationScope string

const (
	// ScopeOneTime permits exactly one sealed intent, named by digest.
	ScopeOneTime AuthorizationScope = "one-time"
	// ScopeStanding permits repeated charges within declared bounds.
	ScopeStanding AuthorizationScope = "standing"
)

// BillingAuthorization is the customer's permission for a debit.
//
// INV-006: "Every debit must reference a valid BillingAuthorization,
// either one-time for one sealed intent or standing. A standing
// authorization declares charge kinds, currencies, cadence, price and
// terms revisions, ceilings, notice rules, effective time and expiry."
//
// 🔴 The same invariant records what this object cannot do. api-platform
// holds the customer session and this engine treats the subject id as
// opaque, so a hostile or buggy caller can assert an acceptance that
// never happened and nothing here can disprove it. That is a trust
// assumption, not a control.
//
// What this type therefore aims at is reproducibility rather than
// prevention: the disclosure digest it is bound to, and the bounds it
// declares, stay readable so that a fabricated acceptance is something
// a customer can later point at. Building it as though it prevented
// something would produce ceremony that protects nothing while leaving
// the real gap open.
//
// Fields are unexported for the same reason ChargeIntent's are: a
// permission that can be widened in place is not a permission, and
// INV-006 says a private service credential must not create, widen or
// revive one by assertion.
type BillingAuthorization struct {
	id       string
	scope    AuthorizationScope
	subject  Subject
	currency string

	// intentDigest is set for a one-time authorization: the exact
	// document it permits. A one-time permission that names no document
	// is a standing one with worse paperwork.
	intentDigest string

	// kinds are the charge kinds a standing authorization permits.
	kinds map[ChargeKind]bool

	// perChargeCeilingMicros and periodCeilingMicros bound what may be
	// collected. A ceiling can only ever reduce, so a wrong one refuses
	// a charge that should have gone through — a failure in the safe
	// direction.
	perChargeCeilingMicros int64
	periodCeilingMicros    int64
	// frequencyCeiling bounds the COUNT of attempts, which no amount
	// ceiling does.
	frequencyCeiling int
	// triggerBelowMicros and topUpAmountMicros are the accepted balance
	// trigger and the accepted top-up size.
	triggerBelowMicros int64
	topUpAmountMicros  int64
	provider           string
	mandateReference   string
	noticeLeadTime     time.Duration

	// termsRevision and priceBookRevision pin what the customer agreed
	// to. A price book moving under a standing authorization is a
	// change to the deal, so an intent priced under a different
	// revision is not covered by it.
	termsRevision     string
	priceBookRevision string

	noticePolicy string

	effectiveFrom time.Time
	expiresAt     time.Time

	// acceptanceDigest binds this authorization to the engine-signed
	// disclosure the customer was shown. It is the artifact that makes
	// a fabricated acceptance pointable-at afterwards.
	acceptanceDigest string

	revokedAt time.Time
}

// AuthorizationGrant is the input to Authorize.
type AuthorizationGrant struct {
	ID               string
	Scope            AuthorizationScope
	Subject          Subject
	Currency         string
	IntentDigest     string
	Kinds            []ChargeKind
	PerChargeCeiling int64
	PeriodCeiling    int64
	// FrequencyCeiling is the most attempts this authorization permits in
	// its period. It is a COUNT, not an amount, and it is not implied by
	// the two ceilings above: a trigger that fires a hundred times for one
	// cent each is inside every amount bound and is still a runaway.
	//
	// docs/DESIGN.md §6 requires it by name for auto_topup — "per-attempt,
	// frequency and period ceilings" — alongside the amount bounds.
	FrequencyCeiling int

	// TriggerBelowMicros and TopUpAmountMicros are §6's "balance trigger"
	// and "amount rule" for auto_topup: the arrangement is "when my
	// balance falls below X, charge me exactly Y", and BOTH halves are
	// part of what the customer accepted.
	//
	// Without them a standing authorization bounds how MUCH may be taken
	// and says nothing about WHEN or IN WHAT SIZE — so any balance read
	// could trigger any amount inside the ceilings, which is not the
	// arrangement anybody agreed to. Zero on both means "not a
	// balance-triggered authorization" and is legal for every other kind.
	TriggerBelowMicros int64
	TopUpAmountMicros  int64

	// Provider and MandateReference are §6's "provider and mandate"
	// binding: WHICH rail, and WHICH reusable mandate on it, the customer
	// accepted. An off-session standing authorization that names neither
	// authorises a charge against whatever instrument happens to be on
	// file later — which is not what anyone accepted, and survives the
	// customer replacing their card.
	//
	// Given together or not at all; an authorization that names a rail
	// without a mandate has not bound an instrument.
	Provider         string
	MandateReference string

	// NoticeLeadTime is §6's "lead time": how long after notice is
	// DELIVERED the customer must be left before money moves.
	//
	// Without it on the authorization, the wait is whatever the caller
	// put in the receipt — predicate.ClauseNoticeWaitElapsed compares
	// `now` against a caller-supplied EligibilityNotBefore and nothing
	// checks that timestamp against anything the customer agreed to. A
	// caller could set it to the delivery instant and the wait would
	// elapse immediately.
	NoticeLeadTime   time.Duration
	TermsRevision    string
	PriceBook        string
	NoticePolicy     string
	EffectiveFrom    time.Time
	ExpiresAt        time.Time
	AcceptanceDigest string
}

// Errors from Authorize.
var (
	ErrAuthIDMissing             = errors.New("intent: authorization has no id")
	ErrAuthScopeUnknown          = errors.New("intent: authorization scope is neither one-time nor standing")
	ErrAuthSubjectUnknown        = errors.New("intent: authorization subject is not a kind the engine knows")
	ErrAuthCurrencyMissing       = errors.New("intent: authorization names no currency")
	ErrAuthDigestMissing         = errors.New("intent: a one-time authorization must name the intent it permits")
	ErrAuthKindsMissing          = errors.New("intent: a standing authorization must declare the charge kinds it permits")
	ErrAuthCeilingMissing        = errors.New("intent: a standing authorization must declare a per-charge ceiling")
	ErrAuthFrequencyMissing      = errors.New("intent: a standing authorization needs a frequency ceiling")
	ErrAuthTriggerIncomplete     = errors.New("intent: a balance trigger and an amount rule must be given together")
	ErrAuthInstrumentIncomplete  = errors.New("intent: a provider and a mandate reference must be given together")
	ErrAuthNoticeLeadTimeMissing = errors.New("intent: a standing authorization needs a notice lead time")
	ErrAuthRuleExceedsCeiling    = errors.New("intent: the amount rule is above the per-charge ceiling, so no attempt could ever satisfy it")
	ErrAuthCeilingNegative       = errors.New("intent: a ceiling is negative")
	ErrAuthTermsMissing          = errors.New("intent: authorization pins no terms revision")
	ErrAuthPriceBookMissing      = errors.New("intent: authorization pins no price book revision")
	ErrAuthNoticeMissing         = errors.New("intent: authorization names no notice policy")
	ErrAuthWindowMissing         = errors.New("intent: authorization has no effective window")
	ErrAuthWindowInverted        = errors.New("intent: authorization expires before it takes effect")
	ErrAuthAcceptanceMissing     = errors.New("intent: authorization references no acceptance receipt")
	// ErrAuthAcceptanceMismatch is an acceptance that names a DIFFERENT
	// document than the terms being minted — a caller showing a customer one
	// set of ceilings and minting another.
	ErrAuthAcceptanceMismatch = errors.New("intent: the acceptance names a different document than these terms")
)

// Authorize validates a grant and returns the immutable authorization.
//
// A one-time grant must name the digest it covers; a standing grant
// must declare its kinds and a per-charge ceiling. Neither is
// defaulted: an authorization with no ceiling is not an unlimited one,
// it is a refused grant.
// AuthorizeAccepted is Authorize for a caller that IS the party which showed
// the customer their terms.
//
// It computes the disclosure digest from the grant instead of requiring one,
// which is correct only when the same code both rendered the document and is
// minting the authorization. In production nothing satisfies that: the terms
// are shown by api-platform and relayed back, so the digest must ARRIVE and
// be compared — which is what Authorize does and what makes the comparison a
// control rather than a tautology.
//
// 🔴 It is therefore for tests and local development only, and
// internal/architecture fails the build if a non-test file calls it. Without
// that pin this function is a hole straight through the check it exists
// beside: any caller could mint an authorization for any terms and have the
// engine agree with itself about them.
func AuthorizeAccepted(grant AuthorizationGrant) (BillingAuthorization, error) {
	grant.AcceptanceDigest = DisclosureDigestFor(grant)
	return Authorize(grant)
}

func Authorize(grant AuthorizationGrant) (BillingAuthorization, error) {
	if strings.TrimSpace(grant.ID) == "" {
		return BillingAuthorization{}, ErrAuthIDMissing
	}
	if grant.Scope != ScopeOneTime && grant.Scope != ScopeStanding {
		return BillingAuthorization{}, fmt.Errorf("%w: %q", ErrAuthScopeUnknown, grant.Scope)
	}
	if !grant.Subject.Valid() {
		return BillingAuthorization{}, fmt.Errorf("%w: %q", ErrAuthSubjectUnknown, grant.Subject.Kind)
	}
	if strings.TrimSpace(grant.Currency) == "" {
		return BillingAuthorization{}, ErrAuthCurrencyMissing
	}
	if grant.PerChargeCeiling < 0 || grant.PeriodCeiling < 0 {
		return BillingAuthorization{}, ErrAuthCeilingNegative
	}
	if grant.TriggerBelowMicros < 0 || grant.TopUpAmountMicros < 0 {
		return BillingAuthorization{}, ErrAuthCeilingNegative
	}
	// The two halves of a balance-triggered arrangement travel together.
	// A trigger with no amount rule permits any size once the balance
	// falls; an amount rule with no trigger permits that size at any
	// time. Either alone is a different arrangement from the one §6
	// describes, and neither is one a customer could have accepted.
	if (grant.TriggerBelowMicros > 0) != (grant.TopUpAmountMicros > 0) {
		return BillingAuthorization{}, ErrAuthTriggerIncomplete
	}
	// A rail without a mandate has not bound an instrument, and a mandate
	// without a rail does not say where it is valid.
	if (strings.TrimSpace(grant.Provider) == "") != (strings.TrimSpace(grant.MandateReference) == "") {
		return BillingAuthorization{}, ErrAuthInstrumentIncomplete
	}
	// An amount rule above the per-charge ceiling can never be satisfied:
	// every attempt would refuse for being over the ceiling, so the
	// authorization is dead on arrival rather than restrictive.
	if grant.TopUpAmountMicros > 0 && grant.PerChargeCeiling > 0 &&
		grant.TopUpAmountMicros > grant.PerChargeCeiling {
		return BillingAuthorization{}, ErrAuthRuleExceedsCeiling
	}
	if grant.Scope == ScopeOneTime && strings.TrimSpace(grant.IntentDigest) == "" {
		return BillingAuthorization{}, ErrAuthDigestMissing
	}
	if grant.Scope == ScopeStanding {
		if len(grant.Kinds) == 0 {
			return BillingAuthorization{}, ErrAuthKindsMissing
		}
		if grant.PerChargeCeiling == 0 {
			return BillingAuthorization{}, ErrAuthCeilingMissing
		}
		// A standing authorization with no attempt bound is a standing
		// authorization to retry forever. The amount ceilings do not
		// cover it: many small attempts stay inside both.
		if grant.FrequencyCeiling <= 0 {
			return BillingAuthorization{}, ErrAuthFrequencyMissing
		}
		// A standing authorization that can collect automatically needs
		// a lead time, or "notice was given" collapses to "a receipt
		// exists" and the customer is told at the moment they are
		// charged.
		if grant.NoticeLeadTime <= 0 {
			return BillingAuthorization{}, ErrAuthNoticeLeadTimeMissing
		}
	}
	if strings.TrimSpace(grant.TermsRevision) == "" {
		return BillingAuthorization{}, ErrAuthTermsMissing
	}
	if strings.TrimSpace(grant.PriceBook) == "" {
		return BillingAuthorization{}, ErrAuthPriceBookMissing
	}
	if strings.TrimSpace(grant.NoticePolicy) == "" {
		return BillingAuthorization{}, ErrAuthNoticeMissing
	}
	if grant.EffectiveFrom.IsZero() || grant.ExpiresAt.IsZero() {
		return BillingAuthorization{}, ErrAuthWindowMissing
	}
	if grant.ExpiresAt.Before(grant.EffectiveFrom) {
		return BillingAuthorization{}, ErrAuthWindowInverted
	}
	if strings.TrimSpace(grant.AcceptanceDigest) == "" {
		return BillingAuthorization{}, ErrAuthAcceptanceMissing
	}

	// 🔴 The acceptance must name THESE terms.
	//
	// Until now this field was any non-empty string, and docs/DESIGN.md §4's
	// standing gate rests entirely on it: predicate.authorityEvidenceBinds
	// returns true when the scope is standing and the digest is non-empty. So
	// a single character satisfied the only evidence a recurring, automatic
	// charge requires.
	//
	// The digest is now the identity of the document these terms constitute
	// (AuthorizationDisclosure), computed here from the grant itself. A caller
	// that shows a customer one set of ceilings and mints another gets a
	// mismatch and no authorization — which is the check §4 asks for ("a bare
	// accepted: true carrying no disclosure digest has no effect at all"),
	// applied to the standing document rather than only to a fresh receipt.
	//
	// It does NOT prove the customer accepted. Nothing here can: INV-006 says
	// "the engine cannot tell a relayed acceptance from an invented one", and
	// api-platform relays. What it removes is the ability to claim acceptance
	// of terms that were never the terms.
	if want := DisclosureDigestFor(grant); grant.AcceptanceDigest != want {
		return BillingAuthorization{}, fmt.Errorf(
			"%w: the acceptance names %s, these terms are %s",
			ErrAuthAcceptanceMismatch, grant.AcceptanceDigest, want)
	}

	kinds := make(map[ChargeKind]bool, len(grant.Kinds))
	for _, k := range grant.Kinds {
		kinds[k] = true
	}

	return BillingAuthorization{
		id:                     grant.ID,
		scope:                  grant.Scope,
		subject:                grant.Subject,
		currency:               strings.ToUpper(strings.TrimSpace(grant.Currency)),
		intentDigest:           grant.IntentDigest,
		kinds:                  kinds,
		perChargeCeilingMicros: grant.PerChargeCeiling,
		periodCeilingMicros:    grant.PeriodCeiling,
		frequencyCeiling:       grant.FrequencyCeiling,
		triggerBelowMicros:     grant.TriggerBelowMicros,
		topUpAmountMicros:      grant.TopUpAmountMicros,
		provider:               strings.TrimSpace(grant.Provider),
		mandateReference:       strings.TrimSpace(grant.MandateReference),
		noticeLeadTime:         grant.NoticeLeadTime,
		termsRevision:          grant.TermsRevision,
		priceBookRevision:      grant.PriceBook,
		noticePolicy:           grant.NoticePolicy,
		effectiveFrom:          grant.EffectiveFrom.UTC(),
		expiresAt:              grant.ExpiresAt.UTC(),
		acceptanceDigest:       grant.AcceptanceDigest,
	}, nil
}

// Refusal names why an authorization does not cover an intent.
type Refusal string

const (
	RefusalNotAuthorized Refusal = "not_authorized"
	// RefusalDifferentAuthorization: the intent names a different
	// authorization than the one being asked. Without this, any valid
	// standing authorization for the payer would permit an intent
	// sealed against another one, and the AuthorizationID a customer
	// can read on their charge bundle would not be the permission that
	// was actually consulted.
	RefusalDifferentAuthorization Refusal = "different_authorization"
	RefusalWrongIntent            Refusal = "wrong_intent"
	RefusalWrongSubject           Refusal = "wrong_subject"
	RefusalWrongCurrency          Refusal = "wrong_currency"
	RefusalKindNotPermitted       Refusal = "kind_not_permitted"
	RefusalOverPerCharge          Refusal = "over_per_charge_ceiling"
	RefusalOverPeriod             Refusal = "over_period_ceiling"
	RefusalOverFrequency          Refusal = "over_frequency_ceiling"
	// RefusalAmountNotTheAcceptedRule: the intent's total is not the
	// top-up size the customer accepted. A charge inside the ceilings is
	// not thereby the charge that was agreed.
	RefusalAmountNotTheAcceptedRule Refusal = "amount_is_not_the_accepted_rule"
	// RefusalAttemptUnresolved: a previous attempt under this
	// authorization has an unknown outcome. It may already have taken the
	// money.
	RefusalAttemptUnresolved Refusal = "prior_attempt_unresolved"
	RefusalTermsMoved        Refusal = "terms_revision_moved"
	RefusalPriceBookMoved    Refusal = "price_book_moved"
	RefusalNoticePolicyMoved Refusal = "notice_policy_moved"
	RefusalNotYetEffective   Refusal = "not_yet_effective"
	RefusalExpired           Refusal = "expired"
	RefusalRevoked           Refusal = "revoked"
)

// Decision is the result of asking whether an authorization covers an
// intent. Refusals are plural because a customer asking why a charge
// was refused deserves every reason, not the first one found.
type Decision struct {
	Permitted bool
	Refusals  []Refusal
}

// Permits reports whether this authorization covers the given sealed
// intent at the given instant, with priorSpendMicros already collected
// under it in the current period.
//
// It is a pure total function: no I/O, no clock read, no error return.
// The clock arrives as an argument because a policy decision that reads
// the time cannot be replayed, and docs/VERIFICATION.md §4 requires a
// customer to be able to recheck a charge offline. This is the shape of
// internal/account/eligibility, which is the discipline docs/DESIGN.md
// asks for.
// PriorUse is what this authorization has already been used for in the
// current period.
//
// A struct rather than a bare int64 so that adding a bound is a COMPILE
// ERROR at every call site rather than a silently-defaulted zero. The
// frequency ceiling was added on 2026-08-30 and this is how every caller
// was forced to say what it knew about attempts.
type PriorUse struct {
	// SpendMicros is what has already been charged in the period.
	SpendMicros int64
	// Attempts is how many times this authorization has already been
	// acted on in the period — successful or not. A failed attempt still
	// consumed one, which is the point: retrying forever is the runaway
	// the frequency ceiling exists to stop.
	Attempts int

	// Unresolved is how many of those attempts have an UNKNOWN outcome —
	// submitted to a provider and never confirmed either way.
	//
	// This is not a policy knob. An attempt whose outcome is unknown may
	// have taken the customer's money, so starting another is a coin
	// flip on double-charging them. docs/DESIGN.md §6 requires a standing
	// authorization to bind its "pending-or-failed treatment"; refusing
	// while anything is in flight is the treatment that cannot be wrong,
	// and the only one available before the outcome is known.
	//
	// A count, not a bool, so a caller that cannot answer says so by
	// leaving it zero only when it has actually looked.
	Unresolved int
}

func (a BillingAuthorization) Permits(
	intent ChargeIntent,
	at time.Time,
	prior PriorUse,
) Decision {
	var refusals []Refusal

	if a.id == "" || !intent.Sealed() {
		return Decision{Refusals: []Refusal{RefusalNotAuthorized}}
	}
	// The intent seals the authorization it was rated under. Asking a
	// different one whether it permits the charge answers a question
	// nobody asked.
	if intent.AuthorizationID() != a.id {
		refusals = append(refusals, RefusalDifferentAuthorization)
	}
	if !a.revokedAt.IsZero() && !at.Before(a.revokedAt) {
		refusals = append(refusals, RefusalRevoked)
	}
	if at.Before(a.effectiveFrom) {
		refusals = append(refusals, RefusalNotYetEffective)
	}
	if at.After(a.expiresAt) {
		refusals = append(refusals, RefusalExpired)
	}
	if a.subject != intent.Payer() {
		refusals = append(refusals, RefusalWrongSubject)
	}
	if a.currency != intent.Currency() {
		refusals = append(refusals, RefusalWrongCurrency)
	}
	// A price book moving under a standing authorization changes the
	// deal the customer agreed to, so an intent priced under a
	// different revision is outside it.
	if a.priceBookRevision != intent.PriceBookRevision() {
		refusals = append(refusals, RefusalPriceBookMoved)
	}
	// Terms are a policy version like any other. An authorization
	// accepted under one revision does not cover a charge made under
	// another — that is a change to the agreement, not to the amount.
	if a.termsRevision != intent.TermsRevision() {
		refusals = append(refusals, RefusalTermsMoved)
	}
	if a.noticePolicy != intent.NoticePolicy() {
		refusals = append(refusals, RefusalNoticePolicyMoved)
	}

	switch a.scope {
	case ScopeOneTime:
		// A one-time authorization covers exactly the document it
		// names. Anything else — including a superseding correction —
		// needs its own permission, which is INV-003's "repeats every
		// notice and authorization check".
		if a.intentDigest != intent.Digest() {
			refusals = append(refusals, RefusalWrongIntent)
		}
	case ScopeStanding:
		if !a.kinds[intent.Kind()] {
			refusals = append(refusals, RefusalKindNotPermitted)
		}
		if a.perChargeCeilingMicros > 0 && intent.TotalMicros() > a.perChargeCeilingMicros {
			refusals = append(refusals, RefusalOverPerCharge)
		}
		if a.periodCeilingMicros > 0 && prior.SpendMicros+intent.TotalMicros() > a.periodCeilingMicros {
			refusals = append(refusals, RefusalOverPeriod)
		}
		// The count bound, which neither amount bound implies. This
		// intent is the next attempt, so it is the prior count PLUS
		// one that has to fit.
		if a.frequencyCeiling > 0 && prior.Attempts+1 > a.frequencyCeiling {
			refusals = append(refusals, RefusalOverFrequency)
		}
		// The amount RULE, which the ceilings do not express. A ceiling
		// says "no more than"; the rule says "exactly this". A top-up
		// of $5 under an accepted rule of $20 is inside every bound and
		// is still not the arrangement the customer agreed to — and so
		// is a top-up of $19.
		if a.topUpAmountMicros > 0 && intent.TotalMicros() != a.topUpAmountMicros {
			refusals = append(refusals, RefusalAmountNotTheAcceptedRule)
		}
		// Never stack an attempt on an unknown outcome.
		if prior.Unresolved > 0 {
			refusals = append(refusals, RefusalAttemptUnresolved)
		}
	}

	return Decision{Permitted: len(refusals) == 0, Refusals: refusals}
}

// Revoke returns a copy that stops permitting at the given instant.
//
// It returns a new value rather than mutating, so an authorization
// already captured in a charge bundle stays as it was read.
//
// A zero instant revokes unconditionally rather than doing nothing.
// The internal "not revoked" state is a zero time, so passing one
// through would produce an authorization that looked revoked to the
// caller and permitted every charge — and of the two ways to be wrong
// about a revocation, one refuses charges that should have gone
// through and the other keeps charging a customer who asked you to
// stop. This picks the first.
func (a BillingAuthorization) Revoke(at time.Time) BillingAuthorization {
	if at.IsZero() {
		a.revokedAt = a.effectiveFrom
		if a.revokedAt.IsZero() {
			// Nothing to anchor to: use an instant before any plausible
			// evaluation so Permits refuses from the start.
			a.revokedAt = time.Unix(0, 0).UTC()
		}
		return a
	}
	a.revokedAt = at.UTC()
	return a
}

// ID identifies this authorization.
func (a BillingAuthorization) ID() string { return a.id }

// Scope is one-time or standing.
func (a BillingAuthorization) Scope() AuthorizationScope { return a.scope }

// AcceptanceDigest binds this authorization to the disclosure the
// customer was shown.
func (a BillingAuthorization) AcceptanceDigest() string { return a.acceptanceDigest }

// TermsRevision is what the customer agreed to.
func (a BillingAuthorization) TermsRevision() string { return a.termsRevision }

// Grant returns the authorization as the grant that would produce it.
//
// The persistable form of an authorization is the input that created
// it, which means storage round-trips through Authorize and gets every
// validation on the way back in. A separate projection type would be a
// second description of the same thing, and the two would drift — the
// looser one being whichever the database used.
//
// RevokedAt is deliberately not part of the grant: revocation happens
// after the fact, so it is stored beside the grant and reapplied by the
// caller.
// InstrumentBound reports whether this authorization names the rail and
// mandate it was accepted against.
//
// It is what predicate.ClauseInstrumentBinding needs in order to be more
// than a caller-supplied boolean: an executor cannot have VERIFIED a
// binding for an authorization that never named one.
func (a BillingAuthorization) InstrumentBound() bool {
	return a.provider != "" && a.mandateReference != ""
}

// NoticeLeadTime is how long after delivery the customer must be left
// before money moves.
func (a BillingAuthorization) NoticeLeadTime() time.Duration { return a.noticeLeadTime }

// Provider and MandateReference are the accepted rail and mandate.
func (a BillingAuthorization) Provider() string         { return a.provider }
func (a BillingAuthorization) MandateReference() string { return a.mandateReference }

func (a BillingAuthorization) Grant() AuthorizationGrant {
	kinds := make([]ChargeKind, 0, len(a.kinds))
	for kind := range a.kinds {
		kinds = append(kinds, kind)
	}
	// Sorted so that two authorizations with the same permissions
	// produce identical rows, and a diff of stored state is readable.
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })

	return AuthorizationGrant{
		ID:                 a.id,
		Scope:              a.scope,
		Subject:            a.subject,
		Currency:           a.currency,
		IntentDigest:       a.intentDigest,
		Kinds:              kinds,
		PerChargeCeiling:   a.perChargeCeilingMicros,
		FrequencyCeiling:   a.frequencyCeiling,
		TriggerBelowMicros: a.triggerBelowMicros,
		TopUpAmountMicros:  a.topUpAmountMicros,
		Provider:           a.provider,
		MandateReference:   a.mandateReference,
		NoticeLeadTime:     a.noticeLeadTime,
		PeriodCeiling:      a.periodCeilingMicros,
		TermsRevision:      a.termsRevision,
		PriceBook:          a.priceBookRevision,
		NoticePolicy:       a.noticePolicy,
		EffectiveFrom:      a.effectiveFrom,
		ExpiresAt:          a.expiresAt,
		AcceptanceDigest:   a.acceptanceDigest,
	}
}

// RevokedAt is the instant this authorization stopped permitting, or
// the zero time if it has not been revoked.
func (a BillingAuthorization) RevokedAt() time.Time { return a.revokedAt }
