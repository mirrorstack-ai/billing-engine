package main

import (
	"crypto/rand"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/signing"
)

// 🔴 The guidance this command prints must not drift from what signing.Load
// actually reads.
//
// It tells an operator which variable to store the seed in. If the package
// renamed one, the operator would provision a variable nothing reads, the
// deployment would start with no key, and — because a keyless deployment is a
// legitimate state — nothing would say so. The failure would surface as an
// evidence table that stays empty.
func TestTheGuidanceNamesTheVariableTheLoaderActuallyReads(t *testing.T) {
	for domain, want := range map[string]string{
		signing.DomainCustomerAcceptance:  signing.EnvCustomerAcceptanceKey,
		signing.DomainBillingEvidence:     signing.EnvBillingEvidenceKey,
		signing.DomainBillingCapabilities: signing.EnvCapabilitiesKey,
	} {
		if got := envVarFor(domain); got != want {
			t.Errorf("envVarFor(%q) = %q, the loader reads %q", domain, got, want)
		}
	}
}

// A domain the package does not know must not produce a plausible-looking
// variable name. An operator who provisioned it would be configuring nothing.
func TestAnUnknownDomainGetsNoVariableName(t *testing.T) {
	got := envVarFor("made-up/v1")
	if strings.HasPrefix(got, "BILLING_SIGNING_KEY") {
		t.Fatalf("an unknown domain produced %q, which reads like a real variable", got)
	}
}

// What this command generates must be what the package accepts.
//
// A generator whose output the loader then refuses would be worse than none:
// the operator would have a key, a secret store entry, and a deployment that
// will not start.
func TestGeneratedMaterialIsAcceptedByTheLoader(t *testing.T) {
	for _, domain := range []string{
		signing.DomainCustomerAcceptance,
		signing.DomainBillingEvidence,
		signing.DomainBillingCapabilities,
	} {
		seed := make([]byte, signing.SeedSize)
		if _, err := rand.Read(seed); err != nil {
			t.Fatal(err)
		}

		key, err := signing.NewKey(domain, seed)
		if err != nil {
			t.Fatalf("%s: generated material was refused: %v", domain, err)
		}

		// And the public half it prints must pin: NewTrustRoot derives the id
		// from the material and refuses a key pinned under any other, so a
		// mismatch here would make the printed guidance unusable.
		if _, err := signing.NewTrustRoot([]signing.PinnedKey{{
			ID: key.ID(), Domain: domain, PublicKeyHex: key.Public(),
		}}); err != nil {
			t.Fatalf("%s: the printed public key and id do not pin together: %v", domain, err)
		}
	}
}
