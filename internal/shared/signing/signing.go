// Package signing produces and checks the signed statements
// docs/VERIFICATION.md §2 requires, and refuses to produce one it cannot
// make honestly.
//
// 🔴 Until this package existed the repository could VERIFY a signature and
// could not PRODUCE one.
//
// internal/shared/stripe/client.go:752 has verified inbound Stripe webhooks
// with HMAC-SHA256 since long before this, in production, in both webhook
// binaries. What did not exist was an outbound signer: nothing in the tree
// could make a statement another party could check. Every "signed" object in
// docs/DESIGN.md — the evidence record of INV-014, the BillingDecisionProof
// of INV-012, the engine-issued disclosure §12 item 16 asks for — was unbuilt
// for that reason.
//
// The distinction matters because the two need different cryptography, which
// is the next paragraph.
//
// # Why asymmetric
//
// docs/VERIFICATION.md:81: "Pin the root, not the response. A verifier that
// learns its trust root from the service it is checking has checked nothing.
// That root must ship in this repository."
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
// docs/VERIFICATION.md:86-87: "A key valid for `billing-capabilities/v1`
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
	ErrNoKey          = errors.New("signing: this deployment holds no signing key, so it cannot produce evidence")
	ErrDomainMismatch = errors.New("signing: the key is not valid for this signature domain")
	ErrUnknownDomain  = errors.New("signing: unknown signature domain")
	ErrIncomplete     = errors.New("signing: the statement omits something docs/VERIFICATION.md §2 requires it to name")
	ErrWindowInverted = errors.New("signing: a validity interval must not end before it starts")
	ErrUnknownKey     = errors.New("signing: no pinned public key with this id")
	ErrBadSignature   = errors.New("signing: the signature does not verify")
	// ErrExpired is separate from ErrBadSignature deliberately. A statement
	// whose signature verified perfectly and whose validity window has passed
	// is stale, not forged, and a caller classifying refusals — an alert, a
	// retry decision, an operator reading a log — needs to tell those apart.
	// Returning the forgery sentinel for an expired-but-authentic statement
	// is the same class of defect as a clause named for a check it does not
	// perform.
	ErrExpired         = errors.New("signing: the statement is outside its validity interval")
	ErrKeyInconsistent = errors.New("signing: the key material is not a consistent ed25519 key")
	ErrKeyReused       = errors.New("signing: one key is configured for more than one signature domain")
	// ErrNotAddressedToUs is an AUTHENTIC statement meant for someone else,
	// some other environment, or from another issuer. Distinct from
	// ErrBadSignature because the response is different: a forgery is an
	// attack, a misaddressed statement is usually a misconfiguration.
	ErrNotAddressedToUs = errors.New("signing: the statement is authentic but is not addressed to this verifier")
	ErrAlgorithmUnknown = errors.New("signing: the statement names an algorithm this build does not implement")
)

// Statement is one signed statement.
//
// Every field of docs/VERIFICATION.md:85-86 is present and required:
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
	// ("the outbox checkpoint", docs/VERIFICATION.md's receipt table). Without it a
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

// SeedSize is the length of the key material this package accepts.
const SeedSize = ed25519.SeedSize

// NewKey builds a signing key from a 32-byte ed25519 SEED.
//
// 🔴 It takes a seed, not a private key, and that is a correctness decision
// rather than an ergonomic one.
//
// An ed25519 "private key" in Go is 64 bytes: seed || public. The two halves
// have to agree, and nothing in the format makes them. The first version of
// this function accepted the 64-byte form and checked only its LENGTH, so
// 64 bytes of raw randomness — precisely what `openssl rand -hex 64` gives
// you, and the most natural thing an operator would paste into SSM — loaded
// without complaint. The Key then reported an id derived from the wrong
// trailing bytes, CanSign returned true, Sign returned no error, and every
// signature it produced verified against nothing. The relying party saw
// ErrBadSignature: a forgery signal, for a deployment that was simply
// misconfigured.
//
// A seed cannot be inconsistent with itself. Deriving the whole key from it
// removes the failure rather than detecting it, and the belt-and-braces
// self-check below removes the rest: the key signs a fixed vector and
// verifies it before this function returns, so "this key can produce a
// verifiable signature" is PROVEN at load rather than assumed.
//
// It also stops aliasing the caller's buffer. NewKeyFromSeed allocates, so a
// caller that wipes its own material — the correct thing to do with a secret
// — no longer silently zeroes the key this Key is holding.
//
// The error never contains the material. This package is one of the few
// places in the tree that handles a secret, and an error string is the most
// common way one escapes into a log.
func NewKey(domain string, seed []byte) (Key, error) {
	if !knownDomain(domain) {
		return Key{}, fmt.Errorf("%w: %q", ErrUnknownDomain, domain)
	}
	if len(seed) != SeedSize {
		// The 64-byte case gets its own sentence, because it is the mistake
		// this signature exists to prevent: `openssl rand -hex 64` and every
		// "ed25519 private key" export produce it, and the old API accepted
		// it and signed with material nothing could verify.
		if len(seed) == ed25519.PrivateKeySize {
			return Key{}, fmt.Errorf("%w: this is the %d-byte private-key form; pass the %d-byte SEED "+
				"(openssl rand -hex %d)", ErrKeyInconsistent, ed25519.PrivateKeySize, SeedSize, SeedSize)
		}
		return Key{}, fmt.Errorf("%w: an ed25519 seed is %d bytes, got %d",
			ErrKeyInconsistent, SeedSize, len(seed))
	}
	pk := ed25519.NewKeyFromSeed(seed)
	pub := pk.Public().(ed25519.PublicKey)

	// Prove it before returning it.
	const probe = "mirrorstack.signing/self-check"
	if !ed25519.Verify(pub, []byte(probe), ed25519.Sign(pk, []byte(probe))) {
		return Key{}, ErrKeyInconsistent
	}

	return Key{id: KeyID(pub), domain: domain, private: pk}, nil
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

// String and GoString redact the key material.
//
// fmt prints unexported struct fields, so without these `fmt.Sprintf("%v", k)`
// emits the complete private key as a decimal byte slice — into a log line, a
// panic message, or a struct dumped during debugging. A secret that is one
// %+v away from a log is a secret that will eventually be in one.
func (k Key) String() string {
	if k.private == nil {
		return "signing.Key{unset}"
	}
	return "signing.Key{id:" + k.id + " domain:" + k.domain + " material:REDACTED}"
}

// GoString covers %#v, which does not route through String.
func (k Key) GoString() string { return k.String() }

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

// Expect is what a verifier requires a statement to say.
//
// 🔴 It exists because signing a field and CHECKING it are different things,
// and the first version of Verify did only the first.
//
// Issuer, Audience and Environment were covered by the signature and required
// to be non-empty — and compared to nothing. So a statement signed by a
// pinned key with Environment "staging", Issuer "someone-else" and Audience
// "not-you" verified in production. Every one of those fields exists to
// answer a question ("who said this", "who may rely on it", "which
// deployment"), and a field a verifier never reads answers nothing.
//
// Every field is required. A verifier that does not know what environment it
// is in cannot state one, and cannot safely accept whatever arrives.
type Expect struct {
	Domain      string
	Issuer      string
	Audience    string
	Environment string
}

func (e Expect) validate() error {
	if !knownDomain(e.Domain) {
		return fmt.Errorf("%w: %q", ErrUnknownDomain, e.Domain)
	}
	for _, f := range []struct{ name, value string }{
		{"issuer", e.Issuer},
		{"audience", e.Audience},
		{"environment", e.Environment},
	} {
		if strings.TrimSpace(f.value) == "" {
			return fmt.Errorf("%w: expected %s", ErrIncomplete, f.name)
		}
	}
	return nil
}

// Verify checks a signed statement against a PINNED public key and against
// what the caller expects it to say.
//
// The key comes from the caller's own trust root, never from the statement:
// the KeyID selects among keys the verifier already holds, and an id it does
// not hold fails. That is docs/VERIFICATION.md:81 — "no relayed response may
// introduce a new one" — expressed as a signature.
//
// `now` is supplied rather than read, so a verdict is a function of its
// inputs and a refusal is reproducible.
func Verify(root TrustRoot, signed Signed, expect Expect, now time.Time) error {
	if err := expect.validate(); err != nil {
		return err
	}
	s := signed.Statement
	if s.Algorithm != Algorithm {
		return fmt.Errorf("%w: %q", ErrAlgorithmUnknown, s.Algorithm)
	}
	// The domain the CALLER expects, not the one the statement asserts. A
	// verifier that read the domain off the statement would accept whatever
	// domain the statement chose to be in.
	if s.Domain != expect.Domain {
		return fmt.Errorf("%w: expected %q, statement says %q", ErrDomainMismatch, expect.Domain, s.Domain)
	}
	if err := s.validate(); err != nil {
		return err
	}

	pub, ok := root.PublicKey(s.KeyID, expect.Domain)
	if !ok {
		return fmt.Errorf("%w: %q for %s", ErrUnknownKey, s.KeyID, expect.Domain)
	}
	sig, err := base64.StdEncoding.DecodeString(signed.Signature)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBadSignature, err)
	}
	if !ed25519.Verify(pub, s.signedBytes(), sig) {
		return ErrBadSignature
	}

	// Everything below here is about an AUTHENTIC statement, so each failure
	// gets its own sentinel: a caller must be able to tell "this is not ours"
	// from "this is forged".
	for _, m := range []struct {
		field, want, got string
	}{
		{"issuer", expect.Issuer, s.Issuer},
		{"audience", expect.Audience, s.Audience},
		{"environment", expect.Environment, s.Environment},
	} {
		if m.want != m.got {
			return fmt.Errorf("%w: expected %s %q, statement says %q",
				ErrNotAddressedToUs, m.field, m.want, m.got)
		}
	}

	if now.Before(s.NotBefore) || now.After(s.NotAfter) {
		return fmt.Errorf("%w: %s is outside %s..%s",
			ErrExpired, now.UTC(), s.NotBefore.UTC(), s.NotAfter.UTC())
	}
	return nil
}
