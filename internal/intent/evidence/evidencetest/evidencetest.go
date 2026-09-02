// Package evidencetest builds a real Recorder for tests.
//
// It signs with a real, deterministic ed25519 key rather than stubbing the
// signature out. A fake signer would let a test pass with a Recorder that
// cannot actually produce a verifiable record — the same reason
// internal/shared/testutil runs a real Postgres instead of a fake store.
//
// The seed is fixed, so a failure is reproducible and nothing here depends on
// entropy or a clock. It is a TEST key: it is written down in this file, it is
// not in the pinned trust root, and no deployment can be configured with it,
// because Load reads only the environment.
package evidencetest

import (
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/signing"
)

// At is the instant test recorders stamp, so records are comparable.
var At = time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)

// Seed is the fixed test seed. Publishing it here is deliberate: a key in a
// test file that anyone can read is unmistakably not a production key.
func Seed() []byte {
	s := make([]byte, signing.SeedSize)
	for i := range s {
		s[i] = 0x2a
	}
	return s
}

// Signer returns a Signer holding only the evidence key.
func Signer(t *testing.T) signing.Signer {
	t.Helper()
	s, err := signing.Load(func(name string) string {
		if name == signing.EnvBillingEvidenceKey {
			return hexSeed()
		}
		return ""
	})
	if err != nil {
		t.Fatalf("evidencetest: load signer: %v", err)
	}
	return s
}

// Recorder returns a Recorder that signs with the test key.
func Recorder(t *testing.T) *evidence.Recorder {
	t.Helper()
	r, err := evidence.NewRecorder(Signer(t), evidence.Options{
		Issuer:      "billing-engine",
		Audience:    "customer",
		Environment: "test",
		Now:         func() time.Time { return At },
	})
	if err != nil {
		t.Fatalf("evidencetest: new recorder: %v", err)
	}
	return r
}

// TrustRoot returns a root pinning the test key, so a test can VERIFY a record
// it produced rather than only assert that a row exists.
func TrustRoot(t *testing.T) signing.TrustRoot {
	t.Helper()
	key, err := signing.NewKey(signing.DomainBillingEvidence, Seed())
	if err != nil {
		t.Fatalf("evidencetest: new key: %v", err)
	}
	root, err := signing.NewTrustRoot([]signing.PinnedKey{{
		ID:           key.ID(),
		Domain:       signing.DomainBillingEvidence,
		PublicKeyHex: key.Public(),
	}})
	if err != nil {
		t.Fatalf("evidencetest: new trust root: %v", err)
	}
	return root
}

// Expect is what a verifier of a test record requires it to say.
func Expect() signing.Expect {
	return signing.Expect{
		Domain:      signing.DomainBillingEvidence,
		Issuer:      "billing-engine",
		Audience:    "customer",
		Environment: "test",
	}
}

func hexSeed() string {
	const hexDigits = "0123456789abcdef"
	seed := Seed()
	out := make([]byte, 0, len(seed)*2)
	for _, b := range seed {
		out = append(out, hexDigits[b>>4], hexDigits[b&0x0f])
	}
	return string(out)
}
