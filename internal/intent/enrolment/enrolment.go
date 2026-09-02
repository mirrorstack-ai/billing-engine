// Package enrolment mints the standing authorization a migrated account needs.
//
// The goal this exists for is "change all existing billing accounts to the new
// billing services". Measured 2026-08-31, `billing_authorizations` holds ZERO
// rows, so migrating an account is not a data copy — INV-006 requires an
// authorization for every charge, and an authorization is something a customer
// ACCEPTS.
//
// # It is deliberately two phases
//
// Offer computes the terms a customer would be shown and records that the
// engine issued them. Accept records the answer and mints. Nothing mints
// without an answer, because an authorization minted on a customer's behalf
// would make INV-006 a statement about our own records rather than about the
// customer's decision — the exact failure §12 item 16 describes for acceptance
// relayed by api-platform, one step worse.
//
// # What this package does NOT decide
//
// 🔴 Every commercial number is an INPUT. The ceilings, the cadence, the
// notice lead time, the validity window and the policy revisions are §12 item
// 1, which is the owner's open decision — so they arrive in Terms and this
// package never invents one. What it owns is the PROTOCOL: resolve the payer,
// compute the document, issue it, record the answer, mint under exactly the
// terms that were shown.
//
// That split is the point. When item 1 is answered, the answer is a value
// passed in, not a change here.
package enrolment

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// Store is the durable half, narrowed to the five reads and writes enrolment
// needs. It cannot seal an intent, take a claim or record an outcome.
type Store interface {
	PayerForAccount(ctx context.Context, accountID string) (intent.Subject, error)
	IssueAcceptance(ctx context.Context, a IssuedAcceptance) error
	AcceptIssuedAcceptance(ctx context.Context, authorizationID, disclosureDigest string, at time.Time) error
	SaveAuthorization(ctx context.Context, auth intent.BillingAuthorization) error
	LoadAuthorization(ctx context.Context, id string) (intent.BillingAuthorization, error)
}

// IssuedAcceptance mirrors the store's own type, declared here so this package
// depends on a shape rather than on the store package.
type IssuedAcceptance struct {
	AuthorizationID  string
	DisclosureDigest string
	Payer            intent.Subject
	Nonce            string
	Audience         string
	ReplayIdentity   string
	IssuedAt         time.Time
	ExpiresAt        time.Time
}

// Terms are the commercial decision, supplied by the caller.
//
// 🔴 Every field here is §12 item 1, which is open. None of them has a default
// in this package: a default would be this code choosing a ceiling, and a
// ceiling nobody decided is the alert-only budget docs/SECURITY.md §2 warns
// about wearing an authorization's name.
type Terms struct {
	// AccountID is the ms_billing.accounts row being migrated. The payer is
	// RESOLVED from it — a caller does not name a subject, for the reason
	// proposer.Charge does not either.
	AccountID string

	// Kinds is what the customer is authorising. From §6's closed catalog.
	Kinds []intent.ChargeKind

	PerChargeCeilingMicros int64
	PeriodCeilingMicros    int64
	FrequencyCeiling       int
	NoticeLeadTime         time.Duration

	Provider         string
	MandateReference string

	TermsRevision     string
	PriceBookRevision string
	NoticePolicy      string

	EffectiveFrom time.Time
	ExpiresAt     time.Time
}

// Offer is what a customer is shown, and what the engine recorded issuing.
type Offer struct {
	// AuthorizationID is the id the authorization will carry when minted, so
	// the answer can name it before it exists.
	AuthorizationID string
	// DisclosureDigest is the identity of the document. The customer's answer
	// must name exactly this, and Authorize refuses a grant whose acceptance
	// names anything else.
	DisclosureDigest string
	Payer            intent.Subject
	Nonce            string
	Audience         string
	ExpiresAt        time.Time
	// Terms is the document itself, so whoever renders it shows what was
	// digested rather than re-deriving it and hoping the two agree.
	Terms Terms
}

var (
	ErrIncompleteTerms = errors.New("enrolment: the terms omit something §12 item 1 must decide")
	ErrNotOffered      = errors.New("enrolment: no offer was issued for those terms")
)

// Nonces supplies the per-offer freshness values.
//
// Injected rather than generated here so an offer is reproducible in a test,
// and so this package holds no randomness of its own — the one place a nonce
// is chosen is visible to whoever wires it.
type Nonces interface {
	Next() (nonce, replayIdentity string, err error)
}

// Enroller mints standing authorizations.
type Enroller struct {
	store    Store
	nonces   Nonces
	audience string
	now      func() time.Time
	// offerValidity is how long a customer has to answer. It is a property
	// of the enrolment flow rather than of the terms, so it lives here.
	offerValidity time.Duration
}

// New builds an Enroller. Every dependency is required: a nil clock or nonce
// source would make an offer unreproducible, and an empty audience would make
// an acceptance replayable at a party it was never meant for.
func New(store Store, nonces Nonces, audience string, now func() time.Time, offerValidity time.Duration) (*Enroller, error) {
	if store == nil || nonces == nil || now == nil {
		return nil, errors.New("enrolment: store, nonce source and clock are all required")
	}
	if strings.TrimSpace(audience) == "" {
		return nil, errors.New("enrolment: an offer must name the audience it is for")
	}
	if offerValidity <= 0 {
		return nil, errors.New("enrolment: an offer must expire")
	}
	return &Enroller{store: store, nonces: nonces, audience: audience, now: now, offerValidity: offerValidity}, nil
}

// Offer computes the document for an account and records that it was issued.
//
// It mints nothing. The returned Offer is what api-platform renders; the
// authorization exists only after Accept.
func (e *Enroller) Offer(ctx context.Context, t Terms) (Offer, error) {
	if err := t.validate(); err != nil {
		return Offer{}, err
	}

	payer, err := e.store.PayerForAccount(ctx, t.AccountID)
	if err != nil {
		return Offer{}, fmt.Errorf("enrolment: resolve payer: %w", err)
	}

	grant := t.grant(payer)

	// The document the engine says these terms constitute. Authorize derives
	// the same value and refuses anything else, so this is the ONLY digest
	// that can later mint these terms.
	digest := intent.DisclosureDigestFor(grant)

	nonce, replay, err := e.nonces.Next()
	if err != nil {
		return Offer{}, fmt.Errorf("enrolment: mint nonce: %w", err)
	}

	at := e.now()
	expires := at.Add(e.offerValidity)
	if err := e.store.IssueAcceptance(ctx, IssuedAcceptance{
		AuthorizationID:  grant.ID,
		DisclosureDigest: digest,
		Payer:            payer,
		Nonce:            nonce,
		Audience:         e.audience,
		ReplayIdentity:   replay,
		IssuedAt:         at,
		ExpiresAt:        expires,
	}); err != nil {
		return Offer{}, fmt.Errorf("enrolment: issue acceptance: %w", err)
	}

	return Offer{
		AuthorizationID:  grant.ID,
		DisclosureDigest: digest,
		Payer:            payer,
		Nonce:            nonce,
		Audience:         e.audience,
		ExpiresAt:        expires,
		Terms:            t,
	}, nil
}

// Accept records the customer's answer and mints the authorization.
//
// The answer is carried into Authorize, which derives the document these terms
// constitute and refuses a grant whose acceptance names anything else. So a
// caller that offered one set of ceilings and accepted another mints nothing —
// and that rule lives in exactly one place.
func (e *Enroller) Accept(ctx context.Context, t Terms, acceptedDigest string, at time.Time) (intent.BillingAuthorization, error) {
	if err := t.validate(); err != nil {
		return intent.BillingAuthorization{}, err
	}

	payer, err := e.store.PayerForAccount(ctx, t.AccountID)
	if err != nil {
		return intent.BillingAuthorization{}, fmt.Errorf("enrolment: resolve payer: %w", err)
	}

	grant := t.grant(payer)

	// 🔴 The ANSWER is carried through, not a digest this function computes.
	//
	// Authorize derives the document these terms constitute and refuses a
	// grant whose acceptance names anything else, so a caller that offered
	// one ceiling and accepted another is refused there. Re-deriving and
	// comparing here first would be a SECOND copy of that rule — and a second
	// copy is what drifts. Mutation-tested: replacing the comparison that
	// used to be here changed nothing, because Authorize was already refusing.
	grant.AcceptanceDigest = acceptedDigest

	auth, err := intent.Authorize(grant)
	if err != nil {
		return intent.BillingAuthorization{}, fmt.Errorf("enrolment: %w", err)
	}

	// The answer is recorded BEFORE the authorization is saved. If the save
	// fails, a re-run re-records the same answer (the update is a no-op on an
	// already-accepted row) and mints again from the same terms. If the order
	// were reversed, an authorization could exist whose acceptance the
	// predicate would never find — and the predicate refuses on that, so the
	// account would be enrolled and uncollectable.
	if err := e.store.AcceptIssuedAcceptance(ctx, grant.ID, grant.AcceptanceDigest, at); err != nil {
		return intent.BillingAuthorization{}, fmt.Errorf("enrolment: record acceptance: %w", err)
	}
	if err := e.store.SaveAuthorization(ctx, auth); err != nil {
		return intent.BillingAuthorization{}, fmt.Errorf("enrolment: save authorization: %w", err)
	}
	return auth, nil
}

// AuthorizationIDFor is the deterministic id an account's standing
// authorization carries.
//
// Deterministic so enrolling the same account twice is the same authorization
// rather than a second one beside it — SaveAuthorization is a no-op on the
// grant's terms, so a re-run converges instead of accumulating.
func AuthorizationIDFor(accountID string) string { return "standing:" + accountID }

func (t Terms) grant(payer intent.Subject) intent.AuthorizationGrant {
	return intent.AuthorizationGrant{
		ID:               AuthorizationIDFor(t.AccountID),
		Scope:            intent.ScopeStanding,
		Subject:          payer,
		Currency:         "USD",
		Kinds:            t.Kinds,
		PerChargeCeiling: t.PerChargeCeilingMicros,
		PeriodCeiling:    t.PeriodCeilingMicros,
		FrequencyCeiling: t.FrequencyCeiling,
		NoticeLeadTime:   t.NoticeLeadTime,
		Provider:         t.Provider,
		MandateReference: t.MandateReference,
		TermsRevision:    t.TermsRevision,
		PriceBook:        t.PriceBookRevision,
		NoticePolicy:     t.NoticePolicy,
		EffectiveFrom:    t.EffectiveFrom,
		ExpiresAt:        t.ExpiresAt,
	}
}

// validate refuses terms that omit a decision.
//
// Every one of these is §12 item 1. None gets a default here: a defaulted
// ceiling is this code deciding what a customer authorised.
func (t Terms) validate() error {
	if strings.TrimSpace(t.AccountID) == "" {
		return fmt.Errorf("%w: account", ErrIncompleteTerms)
	}
	if len(t.Kinds) == 0 {
		return fmt.Errorf("%w: permitted charge kinds", ErrIncompleteTerms)
	}
	for _, n := range []struct {
		name  string
		value int64
	}{
		{"per-charge ceiling", t.PerChargeCeilingMicros},
		{"period ceiling", t.PeriodCeilingMicros},
	} {
		if n.value <= 0 {
			return fmt.Errorf("%w: %s", ErrIncompleteTerms, n.name)
		}
	}
	if t.FrequencyCeiling <= 0 {
		// A standing authorization with no attempt bound is a standing
		// authorization to retry forever; the amount ceilings do not cover
		// it, because many small attempts stay inside both.
		return fmt.Errorf("%w: frequency ceiling", ErrIncompleteTerms)
	}
	if t.NoticeLeadTime <= 0 {
		// Without a lead time "notice was given" collapses to "a receipt
		// exists", and the customer is told at the moment they are charged.
		return fmt.Errorf("%w: notice lead time", ErrIncompleteTerms)
	}
	for _, s := range []struct{ name, value string }{
		{"provider", t.Provider},
		{"mandate reference", t.MandateReference},
		{"terms revision", t.TermsRevision},
		{"price book revision", t.PriceBookRevision},
		{"notice policy", t.NoticePolicy},
	} {
		if strings.TrimSpace(s.value) == "" {
			return fmt.Errorf("%w: %s", ErrIncompleteTerms, s.name)
		}
	}
	if t.EffectiveFrom.IsZero() || t.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: validity window", ErrIncompleteTerms)
	}
	return nil
}
