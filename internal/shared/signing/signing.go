// Package signing produces and checks the signed statements
// docs/VERIFICATION.md §2 requires, and refuses to produce one it cannot
// make honestly.
//
// 🔴 Until this package existed the repository had NO signing primitive of
// any kind: no ed25519, no HMAC, not even crypto/rand outside tests. Every
// "signed" object in docs/DESIGN.md — the evidence record of INV-014, the
// BillingDecisionProof of INV-012, the engine-issued disclosure §12 item 16
// asks for — was unbuilt for the same reason.
//
// # Why asymmetric
//
// docs/VERIFICATION.md:81-83: "Pin the root, not the response. A verifier
// that learns its trust root from the service it is checking has checked
// nothing. That root must ship in this repository."
//
// A MAC cannot satisfy that. Verifying a MAC requires the signing secret, so
// the only party who can check a MirrorStack statement is MirrorStack — which
// is the relayed-trust problem §12 item 16 is about, restated in
// cryptography. Ed25519 lets the public half ship in this repository, in
// releases, and in a separately operated channel, while the private half
// never leaves the signer.
//
// # Why a key names its domain
//
// docs/VERIFICATION.md:79-80: "A key valid for `billing-capabilities/v1`
// therefore cannot sign `customer-acceptance/v1`." A key here CARRIES its
// domain, and both Sign and Verify refuse a statement whose domain is not
// the key's. So domain separation is a property of the key material rather
// than a string the caller remembers to set, and a capabilities key that
// leaks cannot mint a customer acceptance.
//
// The domain is also mixed into the signed bytes, so a signature cannot be
// lifted from one domain and replayed in another even if a verifier were
// somehow given the wrong key.
//
// # Fail-closed
//
// A Signer with no key is not a Signer that signs nothing — every caller
// would then have to remember to check. NewSigner refuses to construct one,
// and the zero Signer refuses to sign. A deployment with no key material
// cannot produce evidence, and the surface that reports readiness says so.
package signing

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Algorithm is the only algorithm this package implements.
//
// It is stated in every statement rather than assumed, because a verifier
// reading a statement must not have to know which algorithm was in fashion
// when it was written. A statement naming an algorithm this build does not
// implement fails to verify rather than being checked with the wrong one.
const Algorithm = "ed25519"

// Domains are the signature domains this deployment knows.
//
// The list is closed. An unknown domain cannot be signed, because a domain
// nobody declared is one no verifier can have pinned a key for, and a
// statement no one can check is not evidence.
const (
	// DomainCustomerAcceptance covers what a customer accepted: the
	// engine-issued disclosure of §12 item 16 option C.
	DomainCustomerAcceptance = "customer-acceptance/v1"
	// DomainBillingEvidence covers INV-014's outbox records.
	DomainBillingEvidence = "billing-evidence/v1"
	// DomainBillingCapabilities covers the Capabilities report of
	// docs/VERIFICATION.md §2.
	DomainBillingCapabilities = "billing-capabilities/v1"
)

func knownDomain(d string) bool {
	switch d {
	case DomainCustomerAcceptance, DomainBillingEvidence, DomainBillingCapabilities:
		return true
	}
	return false
}

var (
	ErrNoKey            = errors.New("signing: this deployment holds no signing key, so it cannot produce evidence")
	ErrDomainMismatch   = errors.New("signing: the key is not valid for this signature domain")
	ErrUnknownDomain    = errors.New("signing: unknown signature domain")
	ErrIncomplete       = errors.New("signing: the statement omits something docs/VERIFICATION.md §2 requires it to name")
	ErrWindowInverted   = errors.New("signing: a validity interval must not end before it starts")
	ErrUnknownKey       = errors.New("signing: no pinned public key with this id")
	ErrBadSignature     = errors.New("signing: the signature does not verify")
	ErrAlgorithmUnknown = errors.New("signing: the statement names an algorithm this build does not implement")
)

// Statement is one signed statement.
//
// Every field of docs/VERIFICATION.md:78-79 is present and required:
// "algorithm, key id, issuer, audience, environment, schema, signature
// domain, payload digest, validity interval and checkpoint."
//
// They are required rather than optional because each one is a question a
// verifier has to answer and cannot answer from an absent field. An
// unaudienced statement can be replayed at a party it was never meant for;
// an unenvironmented one lets a staging signature stand in production; a
// statement with no validity interval never expires.
type Statement struct {
	// Algorithm, KeyID and Domain identify what signed this and under
	// which separation. Domain must be the key's own.
	Algorithm string `json:"algorithm"`
	KeyID     string `json:"key_id"`
	Domain    string `json:"domain"`

	// Issuer is who is making the statement, Audience is who may rely on
	// it, and Environment stops a signature crossing between deployments.
	Issuer      string `json:"issuer"`
	Audience    string `json:"audience"`
	Environment string `json:"environment"`

	// Schema is the shape of whatever PayloadDigest covers, so a verifier
	// knows how to interpret the bytes it fetches separately.
	Schema string `json:"schema"`

	// PayloadDigest is the identity of the signed content. The content
	// itself is NOT here: a statement is a claim ABOUT bytes, and keeping
	// them apart is what lets an evidence record be signed without the
	// signer holding the customer's data.
	PayloadDigest string `json:"payload_digest"`

	// Checkpoint is the position of this statement in the log that
	// ordered it — for an evidence record, the outbox sequence
	// (docs/VERIFICATION.md:131, "the outbox checkpoint"). Without it a
	// verifier can check that a statement is authentic but not that it is
	// the latest, so a withheld correction is undetectable.
	Checkpoint string `json:"checkpoint"`

	// NotBefore and NotAfter are the validity interval.
	NotBefore time.Time `json:"not_before"`
	NotAfter  time.Time `json:"not_after"`
}

// Signed is a statement and the signature over it.
type Signed struct {
	Statement Statement `json:"statement"`
	// Signature is base64 (standard, padded) so it survives JSON and a
	// database TEXT column without a second encoding decision.
	Signature string `json:"signature"`
}

// Key is a signing key bound to one domain.
//
// The binding is the point: a Key cannot sign outside its domain, so
// "which statements can this key make" is answered by the key rather than
// by every call site.
type Key struct {
	id      string
	domain  string
	private ed25519.PrivateKey
}

// NewKey builds a signing key from raw private key bytes.
//
// The key id is DERIVED from the public half rather than supplied. A
// supplied id is a second fact about the same key that can disagree with the
// first: a rotated key kept under its old id verifies against a pinned key
// it no longer matches, and the failure reads as a bad signature rather than
// a bad deployment. Deriving it means an id and its material cannot come
// apart, and rotating the key changes the id by construction — so a verifier
// that has not been given the new public half fails with "unknown key",
// which is the true reason.
//
// It refuses an unknown domain and a wrong-sized key rather than accepting
// them and failing later at a verifier: a deployment holding unusable key
// material should not start.
//
// The error never contains the key material. This package is one of the few
// places in the tree that handles a secret, and an error string is the most
// common way one escapes into a log.
func NewKey(domain string, private []byte) (Key, error) {
	if !knownDomain(domain) {
		return Key{}, fmt.Errorf("%w: %q", ErrUnknownDomain, domain)
	}
	if len(private) != ed25519.PrivateKeySize {
		return Key{}, fmt.Errorf("%w: an ed25519 private key is %d bytes, got %d",
			ErrIncomplete, ed25519.PrivateKeySize, len(private))
	}
	pk := ed25519.PrivateKey(private)
	return Key{
		id:      KeyID(pk.Public().(ed25519.PublicKey)),
		domain:  domain,
		private: pk,
	}, nil
}

// KeyID is the identity of a public key: the first 128 bits of its SHA-256,
// in hex.
//
// Truncation is safe here because the id is a LOOKUP key inside a pinned
// root, not a security boundary — the signature is what authenticates, and a
// collision would select a key whose signature then fails to verify.
func KeyID(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:16])
}

// ID and Domain report what this key is for.
func (k Key) ID() string     { return k.id }
func (k Key) Domain() string { return k.domain }

// Public returns the verification half, hex-encoded, for pinning.
func (k Key) Public() string {
	if k.private == nil {
		return ""
	}
	return hex.EncodeToString(k.private.Public().(ed25519.PublicKey))
}

// Sign produces a signature over the statement.
//
// The statement's Algorithm and Domain are SET here rather than read: a
// caller that could choose them could sign a customer acceptance with a
// capabilities key by writing the other domain into the struct. Everything
// else is the caller's, and everything else is required.
func (k Key) Sign(s Statement) (Signed, error) {
	if k.private == nil {
		return Signed{}, ErrNoKey
	}
	// A caller that set a domain must have set the key's own. Silently
	// overwriting a mismatch would hide a wiring mistake at exactly the
	// place the separation is supposed to hold.
	// A caller that set a domain must have set the key's own. Silently
	// overwriting a mismatch would hide a wiring mistake at exactly the
	// place the separation is supposed to hold.
	if s.Domain != "" && s.Domain != k.domain {
		return Signed{}, fmt.Errorf("%w: key %s signs %q, statement says %q",
			ErrDomainMismatch, k.id, k.domain, s.Domain)
	}
	s.Algorithm = Algorithm
	s.KeyID = k.id
	s.Domain = k.domain

	if err := s.validate(); err != nil {
		return Signed{}, err
	}
	return Signed{
		Statement: s,
		Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(k.private, s.signedBytes())),
	}, nil
}

func (s Statement) validate() error {
	for _, f := range []struct{ name, value string }{
		{"issuer", s.Issuer},
		{"audience", s.Audience},
		{"environment", s.Environment},
		{"schema", s.Schema},
		{"payload digest", s.PayloadDigest},
		{"checkpoint", s.Checkpoint},
	} {
		if strings.TrimSpace(f.value) == "" {
			return fmt.Errorf("%w: %s", ErrIncomplete, f.name)
		}
	}
	if s.NotBefore.IsZero() || s.NotAfter.IsZero() {
		return fmt.Errorf("%w: validity interval", ErrIncomplete)
	}
	if s.NotAfter.Before(s.NotBefore) {
		return fmt.Errorf("%w: %s..%s", ErrWindowInverted, s.NotBefore, s.NotAfter)
	}
	return nil
}

// signedBytes is the byte string a signature is taken over.
//
// It uses the same length-prefixed encoding as the intent digest, for the
// same reason: a separator-joined encoding collides on values containing the
// separator, and several of these fields are free text. Two different
// statements must never produce one signable string, or a signature over
// either attests to both.
//
// The domain is FIRST, so a signature can never be reinterpreted under
// another domain even by a verifier handed the wrong key.
func (s Statement) signedBytes() []byte {
	e := newEncoder()
	e.str(s.Domain)
	e.str(s.Algorithm)
	e.str(s.KeyID)
	e.str(s.Issuer)
	e.str(s.Audience)
	e.str(s.Environment)
	e.str(s.Schema)
	e.str(s.PayloadDigest)
	e.str(s.Checkpoint)
	e.str(s.NotBefore.UTC().Format(time.RFC3339Nano))
	e.str(s.NotAfter.UTC().Format(time.RFC3339Nano))
	return e.bytes()
}

// Verify checks a signed statement against a PINNED public key.
//
// The key comes from the caller's own trust root, never from the statement:
// the KeyID selects among keys the verifier already holds, and an id it does
// not hold fails. That is docs/VERIFICATION.md:81 — "no relayed response may
// introduce a new one" — expressed as a signature.
//
// `now` is supplied rather than read, so a verdict is a function of its
// inputs and a refusal is reproducible.
func Verify(root TrustRoot, signed Signed, domain string, now time.Time) error {
	s := signed.Statement
	if s.Algorithm != Algorithm {
		return fmt.Errorf("%w: %q", ErrAlgorithmUnknown, s.Algorithm)
	}
	if !knownDomain(domain) {
		return fmt.Errorf("%w: %q", ErrUnknownDomain, domain)
	}
	// The domain the CALLER expects, not the one the statement asserts. A
	// verifier that read the domain off the statement would accept whatever
	// domain the statement chose to be in.
	if s.Domain != domain {
		return fmt.Errorf("%w: expected %q, statement says %q", ErrDomainMismatch, domain, s.Domain)
	}
	if err := s.validate(); err != nil {
		return err
	}

	pub, ok := root.PublicKey(s.KeyID, domain)
	if !ok {
		return fmt.Errorf("%w: %q for %s", ErrUnknownKey, s.KeyID, domain)
	}
	sig, err := base64.StdEncoding.DecodeString(signed.Signature)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBadSignature, err)
	}
	if !ed25519.Verify(pub, s.signedBytes(), sig) {
		return ErrBadSignature
	}
	if now.Before(s.NotBefore) || now.After(s.NotAfter) {
		return fmt.Errorf("%w: %s is outside %s..%s",
			ErrBadSignature, now.UTC(), s.NotBefore.UTC(), s.NotAfter.UTC())
	}
	return nil
}
