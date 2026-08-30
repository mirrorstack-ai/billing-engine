package intent

import (
	"errors"
	"fmt"
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
	TermsRevision    string
	PriceBook        string
	NoticePolicy     string
	EffectiveFrom    time.Time
	ExpiresAt        time.Time
	AcceptanceDigest string
}

// Errors from Authorize.
var (
	ErrAuthIDMissing         = errors.New("intent: authorization has no id")
	ErrAuthScopeUnknown      = errors.New("intent: authorization scope is neither one-time nor standing")
	ErrAuthSubjectUnknown    = errors.New("intent: authorization subject is not a kind the engine knows")
	ErrAuthCurrencyMissing   = errors.New("intent: authorization names no currency")
	ErrAuthDigestMissing     = errors.New("intent: a one-time authorization must name the intent it permits")
	ErrAuthKindsMissing      = errors.New("intent: a standing authorization must declare the charge kinds it permits")
	ErrAuthCeilingMissing    = errors.New("intent: a standing authorization must declare a per-charge ceiling")
	ErrAuthCeilingNegative   = errors.New("intent: a ceiling is negative")
	ErrAuthTermsMissing      = errors.New("intent: authorization pins no terms revision")
	ErrAuthPriceBookMissing  = errors.New("intent: authorization pins no price book revision")
	ErrAuthNoticeMissing     = errors.New("intent: authorization names no notice policy")
	ErrAuthWindowMissing     = errors.New("intent: authorization has no effective window")
	ErrAuthWindowInverted    = errors.New("intent: authorization expires before it takes effect")
	ErrAuthAcceptanceMissing = errors.New("intent: authorization references no acceptance receipt")
)

// Authorize validates a grant and returns the immutable authorization.
//
// A one-time grant must name the digest it covers; a standing grant
// must declare its kinds and a per-charge ceiling. Neither is
// defaulted: an authorization with no ceiling is not an unlimited one,
// it is a refused grant.
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
	RefusalTermsMoved             Refusal = "terms_revision_moved"
	RefusalPriceBookMoved         Refusal = "price_book_moved"
	RefusalNoticePolicyMoved      Refusal = "notice_policy_moved"
	RefusalNotYetEffective        Refusal = "not_yet_effective"
	RefusalExpired                Refusal = "expired"
	RefusalRevoked                Refusal = "revoked"
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
func (a BillingAuthorization) Permits(
	intent ChargeIntent,
	kind ChargeKind,
	at time.Time,
	priorSpendMicros int64,
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
		if !a.kinds[kind] {
			refusals = append(refusals, RefusalKindNotPermitted)
		}
		if a.perChargeCeilingMicros > 0 && intent.TotalMicros() > a.perChargeCeilingMicros {
			refusals = append(refusals, RefusalOverPerCharge)
		}
		if a.periodCeilingMicros > 0 && priorSpendMicros+intent.TotalMicros() > a.periodCeilingMicros {
			refusals = append(refusals, RefusalOverPeriod)
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
