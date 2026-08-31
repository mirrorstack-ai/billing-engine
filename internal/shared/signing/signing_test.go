package signing

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
)

var (
	at       = time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	validity = 24 * time.Hour
)

// testKey builds a real key for a domain. Deterministic from the seed, so a
// failure is reproducible and nothing here depends on a clock or entropy.
func testKey(t *testing.T, domain string, seed byte) Key {
	t.Helper()
	s := make([]byte, ed25519.SeedSize)
	for i := range s {
		s[i] = seed
	}
	k, err := NewKey(domain, ed25519.NewKeyFromSeed(s))
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}
	return k
}

func rootFor(t *testing.T, keys ...Key) TrustRoot {
	t.Helper()
	entries := make([]PinnedKey, 0, len(keys))
	for _, k := range keys {
		entries = append(entries, PinnedKey{ID: k.ID(), Domain: k.Domain(), PublicKeyHex: k.Public()})
	}
	root, err := NewTrustRoot(entries)
	if err != nil {
		t.Fatalf("NewTrustRoot: %v", err)
	}
	return root
}

func validStatement() Statement {
	return Statement{
		Issuer:        "billing-engine",
		Audience:      "customer",
		Environment:   "prod",
		Schema:        "acceptance/v1",
		PayloadDigest: "abc123",
		Checkpoint:    "outbox:1",
		NotBefore:     at,
		NotAfter:      at.Add(validity),
	}
}

func TestASignedStatementVerifiesAgainstItsPinnedKey(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 1)
	signed, err := k.Sign(validStatement())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if err := Verify(rootFor(t, k), signed, DomainCustomerAcceptance, at.Add(time.Hour)); err != nil {
		t.Fatalf("a statement this deployment just signed did not verify: %v", err)
	}
}

// 🔴 docs/VERIFICATION.md:79-80: "A key valid for `billing-capabilities/v1`
// therefore cannot sign `customer-acceptance/v1`."
//
// Both halves are tested: the key refuses to sign outside its domain, AND a
// signature made under one domain does not verify under another even when
// the verifier holds the signing key. The second half is what makes the
// separation cryptographic rather than procedural — the domain is inside the
// signed bytes, so there is nothing to strip.
func TestAKeyCannotSignOutsideItsDomain(t *testing.T) {
	caps := testKey(t, DomainBillingCapabilities, 2)

	s := validStatement()
	s.Domain = DomainCustomerAcceptance
	if _, err := caps.Sign(s); !errors.Is(err, ErrDomainMismatch) {
		t.Fatalf("a capabilities key signed a customer acceptance: %v", err)
	}
}

func TestASignatureFromOneDomainDoesNotVerifyInAnother(t *testing.T) {
	caps := testKey(t, DomainBillingCapabilities, 2)
	signed, err := caps.Sign(validStatement())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	// The verifier holds the exact key that signed it, and is asked the
	// wrong question.
	root, err := NewTrustRoot([]PinnedKey{
		{ID: caps.ID(), Domain: DomainBillingCapabilities, PublicKeyHex: caps.Public()},
		{ID: caps.ID(), Domain: DomainCustomerAcceptance, PublicKeyHex: caps.Public()},
	})
	if err != nil {
		t.Fatalf("NewTrustRoot: %v", err)
	}
	if err := Verify(root, signed, DomainCustomerAcceptance, at.Add(time.Hour)); !errors.Is(err, ErrDomainMismatch) {
		t.Fatalf("a capabilities signature verified as a customer acceptance: %v", err)
	}
}

// A verifier must not learn its trust root from the statement it is checking
// (docs/VERIFICATION.md:81). The key id selects among keys the verifier
// already holds; one it does not hold fails, however well-formed.
func TestAKeyTheRootDoesNotHoldIsRefused(t *testing.T) {
	signer := testKey(t, DomainCustomerAcceptance, 3)
	other := testKey(t, DomainCustomerAcceptance, 4)

	signed, err := signer.Sign(validStatement())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if err := Verify(rootFor(t, other), signed, DomainCustomerAcceptance, at.Add(time.Hour)); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("a statement signed by an unpinned key verified: %v", err)
	}
}

// Every field is inside the signature. A statement whose any field was
// edited after signing must fail — otherwise the fields exist to be read,
// not to be relied on.
func TestEveryStatementFieldIsInsideTheSignature(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 5)
	root := rootFor(t, k)

	edits := map[string]func(*Statement){
		"Issuer":        func(s *Statement) { s.Issuer = "someone-else" },
		"Audience":      func(s *Statement) { s.Audience = "another-party" },
		"Environment":   func(s *Statement) { s.Environment = "staging" },
		"Schema":        func(s *Statement) { s.Schema = "other/v1" },
		"PayloadDigest": func(s *Statement) { s.PayloadDigest = "def456" },
		"Checkpoint":    func(s *Statement) { s.Checkpoint = "outbox:2" },
		"NotBefore":     func(s *Statement) { s.NotBefore = at.Add(-time.Hour) },
		"NotAfter":      func(s *Statement) { s.NotAfter = at.Add(48 * time.Hour) },
		"KeyID":         func(s *Statement) { s.KeyID = strings.Repeat("0", 32) },
	}

	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			signed, err := k.Sign(validStatement())
			if err != nil {
				t.Fatalf("Sign: %v", err)
			}
			edit(&signed.Statement)

			err = Verify(root, signed, DomainCustomerAcceptance, at.Add(time.Hour))
			if err == nil {
				t.Fatalf("editing %s after signing left the statement verifying; "+
					"the field is outside what the signature covers", name)
			}
		})
	}
}

// The encoding must be injective: two different statements must never
// produce one signable string, or a signature over either attests to both.
// A separator-joined encoding fails exactly here, and several of these
// fields are free text a caller controls.
func TestTheEncodingCannotBeConfusedAcrossFieldBoundaries(t *testing.T) {
	a := validStatement()
	a.Issuer = "ab"
	a.Audience = "c"

	b := validStatement()
	b.Issuer = "a"
	b.Audience = "bc"

	if string(a.signedBytes()) == string(b.signedBytes()) {
		t.Fatal("two different statements produced identical signable bytes; " +
			"a signature over one would attest to the other")
	}
}

func TestAnExpiredStatementIsRefused(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 6)
	signed, err := k.Sign(validStatement())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	for _, when := range []time.Time{at.Add(-time.Second), at.Add(validity + time.Second)} {
		if err := Verify(rootFor(t, k), signed, DomainCustomerAcceptance, when); err == nil {
			t.Fatalf("a statement verified at %s, outside its validity interval", when)
		}
	}
}

// docs/VERIFICATION.md:78-79 lists ten things a signed statement must carry.
// A statement omitting one is not a shorter statement — it is one a verifier
// cannot fully check, so it is refused rather than signed.
func TestAnIncompleteStatementIsNotSigned(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 7)

	omissions := map[string]func(*Statement){
		"issuer":         func(s *Statement) { s.Issuer = "" },
		"audience":       func(s *Statement) { s.Audience = "" },
		"environment":    func(s *Statement) { s.Environment = "" },
		"schema":         func(s *Statement) { s.Schema = "" },
		"payload digest": func(s *Statement) { s.PayloadDigest = "" },
		"checkpoint":     func(s *Statement) { s.Checkpoint = "" },
		"not before":     func(s *Statement) { s.NotBefore = time.Time{} },
		"not after":      func(s *Statement) { s.NotAfter = time.Time{} },
	}
	for name, omit := range omissions {
		t.Run(name, func(t *testing.T) {
			s := validStatement()
			omit(&s)
			if _, err := k.Sign(s); !errors.Is(err, ErrIncomplete) {
				t.Fatalf("a statement with no %s was signed: %v", name, err)
			}
		})
	}
}

func TestAnInvertedValidityIntervalIsRefused(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 8)
	s := validStatement()
	s.NotAfter = s.NotBefore.Add(-time.Second)
	if _, err := k.Sign(s); !errors.Is(err, ErrWindowInverted) {
		t.Fatalf("a statement valid before it starts was signed: %v", err)
	}
}

// The zero Signer signs nothing. A caller that forgot to Load must get a
// refusal, not a panic and not a silent unsigned path.
func TestADeploymentWithNoKeyCannotSign(t *testing.T) {
	var none Signer
	for _, d := range []string{DomainCustomerAcceptance, DomainBillingEvidence, DomainBillingCapabilities} {
		if _, err := none.SignerFor(d); !errors.Is(err, ErrNoKey) {
			t.Fatalf("a keyless deployment produced a signer for %s: %v", d, err)
		}
		if none.CanSign(d) {
			t.Fatalf("a keyless deployment reports it can sign %s", d)
		}
	}
	var noKey Key
	if _, err := noKey.Sign(validStatement()); !errors.Is(err, ErrNoKey) {
		t.Fatalf("the zero Key signed something: %v", err)
	}
}

func TestLoadReadsOnlyTheVariablesItIsGiven(t *testing.T) {
	k := testKey(t, DomainBillingEvidence, 9)
	env := map[string]string{
		EnvBillingEvidenceKey: hex.EncodeToString(k.private),
	}
	s, err := Load(func(name string) string { return env[name] })
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !s.CanSign(DomainBillingEvidence) {
		t.Fatal("the evidence key was provided and did not load")
	}
	if s.CanSign(DomainCustomerAcceptance) || s.CanSign(DomainBillingCapabilities) {
		t.Fatal("a domain with no variable set produced a key")
	}
	if got := s.Domains(); len(got) != 1 || got[0] != DomainBillingEvidence {
		t.Fatalf("Domains() = %v", got)
	}
}

// A variable that is SET but unusable is an error, not a skipped key.
// Skipping it would start the deployment with a smaller set of capabilities
// than whoever provisioned the key configured, and nothing would say so.
func TestMalformedKeyMaterialRefusesToLoad(t *testing.T) {
	for name, value := range map[string]string{
		"not hex":   "zzzz",
		"too short": hex.EncodeToString([]byte("short")),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(func(n string) string {
				if n == EnvCustomerAcceptanceKey {
					return value
				}
				return ""
			})
			if err == nil {
				t.Fatal("malformed key material loaded without complaint")
			}
			// 🔴 The material must not reach the error string. This
			// package is one of the few in the tree that handles a
			// secret, and an error is the most common way one escapes
			// into a log.
			if strings.Contains(err.Error(), value) {
				t.Fatalf("the error quotes the key material: %v", err)
			}
		})
	}
}

// 🔴 The trust root that ships in this repository is EMPTY, and this test
// exists to make that a stated fact rather than an oversight.
//
// While it is empty: nothing verifies, no deployment signs, and any surface
// claiming evidence readiness is claiming something untrue. When a key is
// provisioned, this test is the place the change is declared.
func TestTheRepositoryTrustRootIsEmptyUntilAKeyIsProvisioned(t *testing.T) {
	if n := Repository().Len(); n != 0 {
		t.Fatalf("the pinned trust root has %d keys. If a key has been "+
			"provisioned, update this test to say so and state which "+
			"environment holds the private half — the count is what the "+
			"Capabilities surface reports as evidence-signing readiness.", n)
	}
}

// A pinned key whose id does not match its own bytes is a misconfiguration
// that would otherwise present as every signature failing.
func TestAPinnedKeyMustCarryItsOwnDerivedID(t *testing.T) {
	k := testKey(t, DomainBillingEvidence, 10)
	_, err := NewTrustRoot([]PinnedKey{
		{ID: strings.Repeat("a", 32), Domain: DomainBillingEvidence, PublicKeyHex: k.Public()},
	})
	if !errors.Is(err, ErrIncomplete) {
		t.Fatalf("a key pinned under the wrong id was accepted: %v", err)
	}
}

func TestAnUnknownDomainIsRefusedEverywhere(t *testing.T) {
	if _, err := NewKey("made-up/v1", make([]byte, ed25519.PrivateKeySize)); !errors.Is(err, ErrUnknownDomain) {
		t.Fatalf("NewKey accepted an unknown domain: %v", err)
	}
	if _, err := NewTrustRoot([]PinnedKey{{ID: "x", Domain: "made-up/v1"}}); !errors.Is(err, ErrUnknownDomain) {
		t.Fatalf("NewTrustRoot accepted an unknown domain: %v", err)
	}
	var s Signer
	if _, err := s.SignerFor("made-up/v1"); !errors.Is(err, ErrUnknownDomain) {
		t.Fatalf("SignerFor accepted an unknown domain: %v", err)
	}
}

// A statement naming an algorithm this build does not implement must fail
// rather than being checked with the one it does.
func TestAnUnknownAlgorithmIsRefused(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 11)
	signed, err := k.Sign(validStatement())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	signed.Statement.Algorithm = "rsa-pkcs1"
	if err := Verify(rootFor(t, k), signed, DomainCustomerAcceptance, at.Add(time.Hour)); !errors.Is(err, ErrAlgorithmUnknown) {
		t.Fatalf("a statement naming an unimplemented algorithm was checked anyway: %v", err)
	}
}

func TestAGarbledSignatureIsRefused(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 12)
	signed, err := k.Sign(validStatement())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	root := rootFor(t, k)

	for name, sig := range map[string]string{
		"not base64":              "!!!!",
		"empty":                   "",
		"right size, wrong bytes": base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
	} {
		t.Run(name, func(t *testing.T) {
			bad := signed
			bad.Signature = sig
			if err := Verify(root, bad, DomainCustomerAcceptance, at.Add(time.Hour)); !errors.Is(err, ErrBadSignature) {
				t.Fatalf("a %s signature verified: %v", name, err)
			}
		})
	}
}

// 🔴 The domain separation must be CRYPTOGRAPHIC, not procedural.
//
// TestASignatureFromOneDomainDoesNotVerifyInAnother above is satisfied by
// Verify's own `s.Domain != domain` comparison, so it would still pass if the
// domain were not inside the signed bytes at all. This test removes that
// comparison from the picture: the attacker rewrites Statement.Domain to the
// domain they want, so Verify's check succeeds and the only thing left
// standing is that the domain is covered by the signature.
//
// Without `e.str(s.Domain)` in signedBytes, this test fails and the
// separation is one string comparison away from nothing.
func TestARewrittenDomainDoesNotSurviveTheSignature(t *testing.T) {
	caps := testKey(t, DomainBillingCapabilities, 13)
	signed, err := caps.Sign(validStatement())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	// The forgery: claim the statement was always a customer acceptance.
	signed.Statement.Domain = DomainCustomerAcceptance

	// The worst case for us — the verifier holds this very key pinned for
	// the target domain too, so nothing about key lookup saves it.
	root, err := NewTrustRoot([]PinnedKey{
		{ID: caps.ID(), Domain: DomainCustomerAcceptance, PublicKeyHex: caps.Public()},
	})
	if err != nil {
		t.Fatalf("NewTrustRoot: %v", err)
	}

	if err := Verify(root, signed, DomainCustomerAcceptance, at.Add(time.Hour)); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("a capabilities signature was re-labelled a customer acceptance and verified: %v", err)
	}
}

// The same argument for the key id: Verify uses it to select a key, so a
// rewritten id normally fails as "unknown key". Pin the same material under
// both ids and the lookup succeeds, leaving only the signature.
func TestARewrittenKeyIDDoesNotSurviveTheSignature(t *testing.T) {
	k := testKey(t, DomainBillingEvidence, 14)
	signed, err := k.Sign(validStatement())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	// A second key id is not constructible through NewTrustRoot, which
	// derives and checks it — so the forged root is built directly, the way
	// a compromised deployment's would be.
	forged := TrustRoot{keys: map[rootKey]ed25519.PublicKey{
		{domain: DomainBillingEvidence, id: "deadbeefdeadbeefdeadbeefdeadbeef"}: k.private.Public().(ed25519.PublicKey),
	}}
	signed.Statement.KeyID = "deadbeefdeadbeefdeadbeefdeadbeef"

	if err := Verify(forged, signed, DomainBillingEvidence, at.Add(time.Hour)); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("a statement whose key id was rewritten verified: %v", err)
	}
}
