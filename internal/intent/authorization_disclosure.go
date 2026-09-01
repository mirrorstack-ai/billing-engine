package intent

import (
	"sort"
	"strings"
)

// AuthorizationDisclosure is the document a customer accepts when they grant
// a standing authorization.
//
// 🔴 It exists because `AcceptanceDigest` used to be ANY non-empty string.
//
// docs/DESIGN.md §4's standing gate rests entirely on that field —
// predicate.authorityEvidenceBinds returns true when the scope is standing and
// `Authorization.AcceptanceDigest() != ""`. So a single non-empty character
// satisfied the only evidence a recurring, automatic charge requires. §12
// item 16 names that as the consent-authority problem, and option C's second
// piece is to bind the digest to bytes the ENGINE issued rather than to
// whatever a caller typed.
//
// This is the engine's half: the exact document these terms constitute. It is
// DERIVED from the authorization, never supplied, so a caller cannot describe
// the terms as anything other than what they are minting.
//
// # What it is NOT
//
// It is not proof the customer accepted. Nothing here can be — INV-006 says
// "the engine cannot tell a relayed acceptance from an invented one", and
// api-platform relays acceptance. What it makes possible is the check §4 asks
// for one level up: a receipt "must name this document", and now there is a
// document to name.
type AuthorizationDisclosure struct {
	Scope    AuthorizationScope
	Subject  Subject
	Currency string
	// Kinds is what the authorization permits. Order does not matter to a
	// customer reading it, so it is sorted before digesting: two grants
	// permitting the same set must produce one document, or the same terms
	// would be two different documents depending on map iteration order.
	Kinds []ChargeKind

	PerChargeCeilingMicros int64
	PeriodCeilingMicros    int64
	FrequencyCeiling       int
	TriggerBelowMicros     int64
	TopUpAmountMicros      int64

	Provider           string
	NoticeLeadTimeSecs int64

	TermsRevision     string
	PriceBookRevision string
	NoticePolicy      string

	EffectiveFromUnix int64
	ExpiresAtUnix     int64
}

// DisclosureSchema tags the encoding, so a verifier reproducing a digest
// knows which layout produced it. It moves with the layout, for the reason
// canonicalSchema does: two different rules must never produce one digest.
const DisclosureSchema = "mirrorstack.authorization-disclosure/v1"

// disclosureFor is the document a grant constitutes.
//
// Every term that changes what may be charged is in it. A field left out
// would be a term the customer's acceptance does not cover, which is the
// whole failure this type exists to close — so the omission would be silent
// and the digest would still verify.
//
// MandateReference is deliberately absent: it names the stored instrument,
// which the customer may change without re-accepting the terms. IntentDigest
// is absent for the same reason — a one-time authorization binds an intent,
// and this is the standing document.
func disclosureFor(g AuthorizationGrant) AuthorizationDisclosure {
	kinds := make([]ChargeKind, len(g.Kinds))
	copy(kinds, g.Kinds)
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })

	return AuthorizationDisclosure{
		Scope:                  g.Scope,
		Subject:                g.Subject,
		Currency:               strings.ToUpper(strings.TrimSpace(g.Currency)),
		Kinds:                  kinds,
		PerChargeCeilingMicros: g.PerChargeCeiling,
		PeriodCeilingMicros:    g.PeriodCeiling,
		FrequencyCeiling:       g.FrequencyCeiling,
		TriggerBelowMicros:     g.TriggerBelowMicros,
		TopUpAmountMicros:      g.TopUpAmountMicros,
		Provider:               strings.TrimSpace(g.Provider),
		NoticeLeadTimeSecs:     int64(g.NoticeLeadTime.Seconds()),
		TermsRevision:          g.TermsRevision,
		PriceBookRevision:      g.PriceBook,
		NoticePolicy:           g.NoticePolicy,
		EffectiveFromUnix:      g.EffectiveFrom.UTC().Unix(),
		ExpiresAtUnix:          g.ExpiresAt.UTC().Unix(),
	}
}

// Digest is the identity of the disclosure bytes.
//
// Same length-prefixed encoder as the intent digest, for the same reason: the
// encoding must be injective, or an acceptance of one document would attest
// to another. Several fields here are free text a caller controls.
func (d AuthorizationDisclosure) Digest() string {
	e := &canonicalEncoder{}
	e.string(DisclosureSchema)
	e.string(string(d.Scope))
	e.string(d.Subject.Kind)
	e.string(d.Subject.ID)
	e.string(d.Currency)

	e.count(len(d.Kinds))
	for _, k := range d.Kinds {
		e.string(string(k))
	}

	e.int(d.PerChargeCeilingMicros)
	e.int(d.PeriodCeilingMicros)
	e.int(int64(d.FrequencyCeiling))
	e.int(d.TriggerBelowMicros)
	e.int(d.TopUpAmountMicros)

	e.string(d.Provider)
	e.int(d.NoticeLeadTimeSecs)

	e.string(d.TermsRevision)
	e.string(d.PriceBookRevision)
	e.string(d.NoticePolicy)

	e.int(d.EffectiveFromUnix)
	e.int(d.ExpiresAtUnix)
	return e.digest()
}

// DisclosureDigestFor returns the digest of the document a grant constitutes.
//
// It is exported so whoever SHOWS a customer their terms can compute the same
// value the engine will, present it, and hand the accepted digest back. That
// is the whole protocol: the engine says what the document is, and refuses to
// mint an authorization whose acceptance names anything else.
func DisclosureDigestFor(g AuthorizationGrant) string {
	return disclosureFor(g).Digest()
}
