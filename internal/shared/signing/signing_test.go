package signing

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
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
	k, err := NewKey(domain, s)
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

// expect is what every verifier in these tests requires, matching
// validStatement. A test that wants to prove a MISMATCH varies one field.
func expect(domain string) Expect {
	return Expect{
		Domain:      domain,
		Issuer:      "billing-engine",
		Audience:    "customer",
		Environment: "prod",
	}
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
	if err := Verify(rootFor(t, k), signed, expect(DomainCustomerAcceptance), at.Add(time.Hour)); err != nil {
		t.Fatalf("a statement this deployment just signed did not verify: %v", err)
	}
}

// 🔴 docs/VERIFICATION.md:86-87: "A key valid for `billing-capabilities/v1`
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
	if err := Verify(root, signed, expect(DomainCustomerAcceptance), at.Add(time.Hour)); !errors.Is(err, ErrDomainMismatch) {
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
	if err := Verify(rootFor(t, other), signed, expect(DomainCustomerAcceptance), at.Add(time.Hour)); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("a statement signed by an unpinned key verified: %v", err)
	}
}

// Every field is inside the signature. A statement whose any field was
// edited after signing must fail — otherwise the fields exist to be read,
// not to be relied on.
func TestEveryStatementFieldIsInsideTheSignature(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 5)
	root := rootFor(t, k)

	for name, edit := range statementEdits {
		t.Run(name, func(t *testing.T) {
			signed, err := k.Sign(validStatement())
			if err != nil {
				t.Fatalf("Sign: %v", err)
			}
			edit(&signed.Statement)

			err = Verify(root, signed, expect(DomainCustomerAcceptance), at.Add(time.Hour))
			if err == nil {
				t.Fatalf("editing %s after signing left the statement verifying; "+
					"the field is outside what the signature covers", name)
			}
		})
	}
}

// The encoding must be injective: two different statements must never
// produce one signable string, or a signature over either attests to both.
//
// 🔴 The first version of this test compared ("ab","c") against ("a","bc"),
// and review showed it proves almost nothing: under a separator-joined
// encoding those are "ab|c" and "a|bc", which already differ, so the test
// passed with the very encoder its comment claimed to exclude. It detected a
// bare-concatenation encoder and nothing else.
//
// What actually has to hold is that the code is PREFIX-FREE: reading a
// decimal length, then a colon, then exactly that many bytes consumes each
// field unambiguously, so a value that itself looks like a length prefix
// cannot be reparsed as one. This searches for a collision over an alphabet
// built from the encoding's own metacharacters, which is where a
// separator-joined or naive encoder breaks.
func TestTheEncodingCannotBeConfusedAcrossFieldBoundaries(t *testing.T) {
	adversarial := []string{
		"", "0", "1", ":", "|", "0:", ":0", "1:", "::", "10:", "9:x", "2:ab",
		"ab", "a|b", "1:a", "11:", "|:", ":|",
	}

	seen := map[string][]string{}
	for _, issuer := range adversarial {
		for _, audience := range adversarial {
			for _, schema := range adversarial {
				st := validStatement()
				st.Issuer, st.Audience, st.Schema = issuer, audience, schema
				// signedBytes is taken directly: validate() would refuse the
				// empty values, and it is the ENCODING under test here, not
				// the completeness rule.
				enc := string(st.signedBytes())
				fields := []string{issuer, audience, schema}
				if prev, dup := seen[enc]; dup {
					t.Fatalf("two different statements encode identically:\n  %q\n  %q\n"+
						"a signature over one would attest to the other", prev, fields)
				}
				seen[enc] = fields
			}
		}
	}
	if len(seen) != len(adversarial)*len(adversarial)*len(adversarial) {
		t.Fatalf("expected %d distinct encodings, got %d",
			len(adversarial)*len(adversarial)*len(adversarial), len(seen))
	}
}

// 🔴 The floor under TestEveryStatementFieldIsInsideTheSignature.
//
// That test enumerates Statement's fields by hand, and a hand-written map
// cannot notice what is not in it — the sibling PR in this same wave exists
// because three canonical supersessions each added a sealed field with no
// case and nothing failed. Review proved the same hole here: adding a field
// to Statement and wiring it into neither signedBytes nor validate leaves the
// whole suite green.
//
// So this reflects over Statement and requires every field to be named by
// both the coverage map and the completeness map.
func TestNoStatementFieldEscapesTheSignature(t *testing.T) {
	// Fields set by Sign rather than by the caller, and therefore not in the
	// omission table — Sign overwrites them, so they cannot be omitted.
	setBySign := map[string]bool{"Algorithm": true, "KeyID": true, "Domain": true}

	covered := map[string]bool{}
	for name := range statementEdits {
		covered[name] = true
	}
	required := map[string]bool{}
	for name := range statementOmissions {
		required[name] = true
	}

	st := reflect.TypeOf(Statement{})
	if st.NumField() < 8 {
		t.Fatalf("Statement has %d fields; the reflection target looks wrong", st.NumField())
	}
	for i := 0; i < st.NumField(); i++ {
		f := st.Field(i)
		if !covered[f.Name] {
			t.Errorf("Statement.%s has no case in statementEdits proving it is inside the "+
				"signature. A field the signature does not cover can be rewritten after "+
				"signing and the statement still verifies.", f.Name)
		}
		if setBySign[f.Name] {
			continue
		}
		if !required[f.Name] {
			t.Errorf("Statement.%s has no case in statementOmissions proving it is REQUIRED. "+
				"docs/VERIFICATION.md lists ten things a signed statement must name, and a "+
				"field that may be blank is one a verifier cannot rely on.", f.Name)
		}
	}
	for name := range covered {
		if _, ok := st.FieldByName(name); !ok {
			t.Errorf("statementEdits names %q, which is not a field of Statement", name)
		}
	}
	for name := range required {
		if _, ok := st.FieldByName(name); !ok {
			t.Errorf("statementOmissions names %q, which is not a field of Statement", name)
		}
	}
}

func TestAnExpiredStatementIsRefused(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 6)
	signed, err := k.Sign(validStatement())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	for _, when := range []time.Time{at.Add(-time.Second), at.Add(validity + time.Second)} {
		if err := Verify(rootFor(t, k), signed, expect(DomainCustomerAcceptance), when); err == nil {
			t.Fatalf("a statement verified at %s, outside its validity interval", when)
		}
	}
}

// docs/VERIFICATION.md:85-86 lists ten things a signed statement must carry.
// A statement omitting one is not a shorter statement — it is one a verifier
// cannot fully check, so it is refused rather than signed.
func TestAnIncompleteStatementIsNotSigned(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 7)

	for name, omit := range statementOmissions {
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
		EnvBillingEvidenceKey: hex.EncodeToString(k.private.Seed()),
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
		"not hex":              "zzzz",
		"too short":            hex.EncodeToString([]byte("short")),
		"the 64-byte key form": strings.Repeat("ab", ed25519.PrivateKeySize),
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
func TestTheRepositoryTrustRootPinsTheProvisionedKey(t *testing.T) {
	root, err := Repository()
	if err != nil {
		t.Fatalf("the pinned trust root does not build: %v", err)
	}
	if n := root.Len(); n != 1 {
		t.Fatalf("the pinned trust root has %d keys, want the 1 provisioned for "+
			"billing evidence. The count is what the Capabilities surface reports "+
			"as evidence-signing readiness, so it must not drift silently.", n)
	}

	// The key must be for the domain it claims. A key pinned under the wrong
	// domain verifies documents it was never meant to — the "one key for two
	// domains" failure the Load path refuses at the other end.
	if _, ok := root.PublicKey("7cd37ff8ba25c9d79445918a5eab5d17", DomainBillingEvidence); !ok {
		t.Fatal("the provisioned billing-evidence key is not in the root under its own domain")
	}
	if _, ok := root.PublicKey("7cd37ff8ba25c9d79445918a5eab5d17", DomainCustomerAcceptance); ok {
		t.Fatal("the billing-evidence key resolves under the customer-acceptance domain too; " +
			"one key must not verify two domains")
	}

	// 🔴 Pinning the PUBLIC half is not provisioning the PRIVATE one. Load
	// reads seed material from the environment and finds none here, so this
	// build can verify a signature and cannot produce one. Asserting it keeps
	// the two readinesses from being conflated by a later edit.
	signer, err := Load(func(string) string { return "" })
	if err != nil {
		t.Fatalf("Load with no environment must not error: %v", err)
	}
	if signer.CanSign(DomainBillingEvidence) {
		t.Fatal("a signer materialised with no seed in the environment; pinning a public " +
			"key must not confer the ability to sign")
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
	if _, err := NewKey("made-up/v1", make([]byte, SeedSize)); !errors.Is(err, ErrUnknownDomain) {
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
	if err := Verify(rootFor(t, k), signed, expect(DomainCustomerAcceptance), at.Add(time.Hour)); !errors.Is(err, ErrAlgorithmUnknown) {
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
			if err := Verify(root, bad, expect(DomainCustomerAcceptance), at.Add(time.Hour)); !errors.Is(err, ErrBadSignature) {
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

	if err := Verify(root, signed, expect(DomainCustomerAcceptance), at.Add(time.Hour)); !errors.Is(err, ErrBadSignature) {
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

	if err := Verify(forged, signed, expect(DomainBillingEvidence), at.Add(time.Hour)); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("a statement whose key id was rewritten verified: %v", err)
	}
}

// statementEdits perturbs each Statement field AFTER signing. Keys are the
// Go field names, so TestNoStatementFieldEscapesTheSignature can compare them
// against the type by reflection.
var statementEdits = map[string]func(*Statement){
	"Issuer":        func(s *Statement) { s.Issuer = "someone-else" },
	"Audience":      func(s *Statement) { s.Audience = "another-party" },
	"Environment":   func(s *Statement) { s.Environment = "staging" },
	"Schema":        func(s *Statement) { s.Schema = "other/v1" },
	"PayloadDigest": func(s *Statement) { s.PayloadDigest = "def456" },
	"Checkpoint":    func(s *Statement) { s.Checkpoint = "outbox:2" },
	"NotBefore":     func(s *Statement) { s.NotBefore = at.Add(-time.Hour) },
	"NotAfter":      func(s *Statement) { s.NotAfter = at.Add(48 * time.Hour) },
	"Algorithm":     func(s *Statement) { s.Algorithm = "rsa-pkcs1" },
	"Domain":        func(s *Statement) { s.Domain = DomainBillingEvidence },
	"KeyID":         func(s *Statement) { s.KeyID = strings.Repeat("0", 32) },
}

// statementOmissions blanks each caller-supplied field before signing. Every
// one must be refused: docs/VERIFICATION.md names ten things a signed
// statement must carry, and a field that may be blank is one a verifier
// cannot rely on.
var statementOmissions = map[string]func(*Statement){
	"Issuer":        func(s *Statement) { s.Issuer = "" },
	"Audience":      func(s *Statement) { s.Audience = "" },
	"Environment":   func(s *Statement) { s.Environment = "" },
	"Schema":        func(s *Statement) { s.Schema = "" },
	"PayloadDigest": func(s *Statement) { s.PayloadDigest = "" },
	"Checkpoint":    func(s *Statement) { s.Checkpoint = "" },
	"NotBefore":     func(s *Statement) { s.NotBefore = time.Time{} },
	"NotAfter":      func(s *Statement) { s.NotAfter = time.Time{} },
}

// 🔴 The 64-byte private-key form must be REFUSED, not accepted.
//
// This is the defect that made the first version of this package dangerous.
// An ed25519 private key is seed||public and nothing in the format makes the
// halves agree, so 64 bytes of raw randomness — `openssl rand -hex 64`, the
// most natural thing to paste into SSM — loaded without complaint, reported
// CanSign, signed, and produced signatures that verified against nothing. The
// relying party saw a forgery signal for a deployment that was misconfigured.
func TestThePrivateKeyFormIsRefusedInFavourOfTheSeed(t *testing.T) {
	seed := make([]byte, SeedSize)
	for i := range seed {
		seed[i] = 9
	}
	full := ed25519.NewKeyFromSeed(seed)

	if _, err := NewKey(DomainBillingEvidence, full); !errors.Is(err, ErrKeyInconsistent) {
		t.Fatalf("the 64-byte private-key form was accepted: %v", err)
	}

	// And the specific poison: seed||seed, which the length check alone
	// cannot tell from a real key.
	spliced := append(append([]byte{}, seed...), seed...)
	if _, err := NewKey(DomainBillingEvidence, spliced); !errors.Is(err, ErrKeyInconsistent) {
		t.Fatalf("seed||seed was accepted as a key: %v", err)
	}
}

// A Key built from a seed always signs something its own public half
// verifies. NewKey proves it before returning, so this asserts the property
// end to end rather than trusting the construction.
func TestAKeyAlwaysProducesAVerifiableSignature(t *testing.T) {
	k := testKey(t, DomainBillingEvidence, 21)
	signed, err := k.Sign(validStatement())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if err := Verify(rootFor(t, k), signed, expect(DomainBillingEvidence), at.Add(time.Hour)); err != nil {
		t.Fatalf("a key derived from a seed produced a signature its own public half rejects: %v", err)
	}
}

// 🔴 A statement signed for another environment, issuer or audience must not
// verify here.
//
// All three are inside the signature and all three were compared to nothing,
// so a staging signature verified in production. A field a verifier never
// reads answers no question.
func TestAStatementForSomeoneElseIsRefused(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 22)
	root := rootFor(t, k)

	for name, vary := range map[string]func(*Statement){
		"environment": func(s *Statement) { s.Environment = "staging" },
		"issuer":      func(s *Statement) { s.Issuer = "some-other-service" },
		"audience":    func(s *Statement) { s.Audience = "not-this-verifier" },
	} {
		t.Run(name, func(t *testing.T) {
			st := validStatement()
			vary(&st)
			signed, err := k.Sign(st)
			if err != nil {
				t.Fatalf("Sign: %v", err)
			}
			err = Verify(root, signed, expect(DomainCustomerAcceptance), at.Add(time.Hour))
			if !errors.Is(err, ErrNotAddressedToUs) {
				t.Fatalf("a statement whose %s is not ours verified: %v", name, err)
			}
			// It must NOT read as a forgery: the signature is perfect.
			if errors.Is(err, ErrBadSignature) {
				t.Fatalf("a misaddressed but authentic statement reported as a forgery: %v", err)
			}
		})
	}
}

// A verifier that cannot state what it expects must not verify anything.
func TestAVerifierMustStateWhatItExpects(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 23)
	signed, err := k.Sign(validStatement())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	root := rootFor(t, k)

	full := expect(DomainCustomerAcceptance)
	for name, blank := range map[string]func(*Expect){
		"issuer":      func(e *Expect) { e.Issuer = "" },
		"audience":    func(e *Expect) { e.Audience = "" },
		"environment": func(e *Expect) { e.Environment = "" },
	} {
		t.Run(name, func(t *testing.T) {
			e := full
			blank(&e)
			if err := Verify(root, signed, e, at.Add(time.Hour)); !errors.Is(err, ErrIncomplete) {
				t.Fatalf("a verifier that stated no %s verified anyway: %v", name, err)
			}
		})
	}
}

// 🔴 Expiry is not forgery.
//
// An authentic statement past its window used to return ErrBadSignature, so a
// caller classifying refusals could not tell a stale evidence record from an
// attack. Both are refusals; they are not the same event.
func TestAnExpiredStatementIsNotReportedAsAForgery(t *testing.T) {
	k := testKey(t, DomainCustomerAcceptance, 24)
	signed, err := k.Sign(validStatement())
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	err = Verify(rootFor(t, k), signed, expect(DomainCustomerAcceptance), at.Add(validity+time.Hour))
	if !errors.Is(err, ErrExpired) {
		t.Fatalf("an expired statement did not report as expired: %v", err)
	}
	if errors.Is(err, ErrBadSignature) {
		t.Fatalf("an expired but perfectly signed statement reported as a forgery: %v", err)
	}
}

// 🔴 One key must not serve two domains.
//
// The package's separation claim is that a leaked capabilities key cannot
// mint a customer acceptance. That holds for the TYPE — a Key refuses to sign
// outside its domain — and did not hold for a DEPLOYMENT handed the same
// material twice, which is the easy mistake when three variables are
// provisioned by hand.
func TestOneKeyCannotServeTwoDomains(t *testing.T) {
	k := testKey(t, DomainBillingEvidence, 25)
	material := hex.EncodeToString(k.private.Seed())

	_, err := Load(func(name string) string {
		switch name {
		case EnvBillingEvidenceKey, EnvCapabilitiesKey:
			return material
		}
		return ""
	})
	if !errors.Is(err, ErrKeyReused) {
		t.Fatalf("the same key loaded for two domains: %v", err)
	}
}

// 🔴 Neither a Key nor a Signer may print its material.
//
// fmt prints unexported struct fields, so without String/GoString a single
// %v — in a log line, a panic message, a debugger dump — emits the complete
// private key.
func TestKeyMaterialNeverReachesAFormattedString(t *testing.T) {
	k := testKey(t, DomainBillingEvidence, 26)
	s, err := Load(func(name string) string {
		if name == EnvBillingEvidenceKey {
			return hex.EncodeToString(k.private.Seed())
		}
		return ""
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Every representation the material could take in a formatted string.
	secrets := []string{
		hex.EncodeToString(k.private),
		hex.EncodeToString(k.private.Seed()),
		fmt.Sprint([]byte(k.private)),
		fmt.Sprint([]byte(k.private.Seed())),
	}

	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		for _, subject := range []any{k, s, &k, &s} {
			out := fmt.Sprintf(verb, subject)
			for _, secret := range secrets {
				if strings.Contains(out, secret) {
					t.Fatalf("%s of a %T leaked the key material", verb, subject)
				}
			}
			if !strings.Contains(out, "REDACTED") && !strings.Contains(out, "no keys") {
				t.Fatalf("%s of a %T does not go through the redacting formatter: %s", verb, subject, out)
			}
		}
	}
}

// The pinned trust root must not be reachable for mutation from another
// package. An exported slice is not pinned — anything in the tree could
// append to it and every later Repository() would honour the addition, which
// is the failure the pinning rule exists to prevent.
func TestThePinnedRootIsNotAnExportedVariable(t *testing.T) {
	// One key, for the billing-evidence domain, provisioned 2026-09-01. The
	// count is what the Capabilities surface reports as evidence-signing
	// readiness, so it is asserted exactly rather than as "at least one" — a
	// key arriving unnoticed is the thing a pinned root exists to prevent.
	if PinnedKeyCount() != 1 {
		t.Fatalf("the pinned root holds %d keys, want 1. Adding or removing one is a "+
			"deliberate edit to trustroot.go with a diff attached; say so here and in "+
			"TestTheRepositoryTrustRootPinsTheProvisionedKey", PinnedKeyCount())
	}
	// The compile-time half of this test is that `signing.PinnedKeys` does
	// not exist to be assigned from outside the package. It is unexported,
	// so a mutation attempt from another package does not build — which is a
	// stronger guarantee than any assertion here could make, and this is the
	// note that says why there is no runtime check for it.
}
