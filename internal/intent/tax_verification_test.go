package intent

import (
	"errors"
	"testing"
)

// 🔴 A sealed tax figure must say HOW it was established.
//
// Before this class existed, a determination the engine recomputed and one
// a vendor merely asserted were BYTE-IDENTICAL once sealed. The execution
// predicate has a clause for tax reproducibility, but nothing in the
// document said which class the figure belonged to — so the clause could
// not tell them apart, and neither could a customer re-deriving the digest
// offline. The only thing preventing substitution was that no vendor
// resolver had been written yet, which is a property of the calendar
// rather than of the design.
//
// INV-004 says an undetermined input must not silently become zero. This
// is the same rule applied to provenance: an unstated class must not
// silently read as the strongest one.
func TestSealRefusesATaxDeterminationThatDoesNotSayHowItWasEstablished(t *testing.T) {
	d := validDraft()
	d.Tax.Verification = TaxUnverified // the zero value

	if _, err := Seal(d); !errors.Is(err, ErrTaxVerificationUnstated) {
		t.Fatalf("Seal with an unstated verification class = %v; want ErrTaxVerificationUnstated.\n"+
			"The zero value must refuse — otherwise every caller that forgets the field "+
			"seals a stronger provenance claim than it earned.", err)
	}
}

// The closed set is a list, not a default branch. A class added to the type
// but not to SealableTaxVerificationClasses must fail to seal, which is the
// direction an unknown value has to fail in.
func TestOnlyTheNamedVerificationClassesCanSeal(t *testing.T) {
	for _, c := range SealableTaxVerificationClasses() {
		d := validDraft()
		d.Tax.Verification = c
		if _, err := Seal(d); err != nil {
			t.Errorf("Seal with verification %q = %v; want it accepted", c, err)
		}
	}

	for _, c := range []TaxVerificationClass{
		TaxUnverified,
		"reproducible",               // near-miss spelling
		"INDEPENDENTLY_REPRODUCIBLE", // case differs
		"anything-else",
	} {
		d := validDraft()
		d.Tax.Verification = c
		if _, err := Seal(d); err == nil {
			t.Errorf("Seal with unlisted verification %q succeeded; want refusal", c)
		}
	}
}

// 🔴 The class must reach the DIGEST, not merely the struct.
//
// A field that is validated but not digested is a field a customer cannot
// verify and an attacker can change without breaking the seal. This is the
// assertion that makes the whole class load-bearing rather than
// decorative: two intents identical except for how their tax was
// established must not share a digest.
func TestTheVerificationClassChangesTheDigest(t *testing.T) {
	seen := map[string]TaxVerificationClass{}

	for _, c := range SealableTaxVerificationClasses() {
		d := validDraft()
		d.Tax.Verification = c
		sealed, err := Seal(d)
		if err != nil {
			t.Fatalf("Seal(%q): %v", c, err)
		}
		digest := sealed.Digest()
		if prior, clash := seen[digest]; clash {
			t.Fatalf("verification classes %q and %q produced the SAME digest %s.\n"+
				"An attested figure and a reproduced one would be indistinguishable "+
				"to anyone verifying the document.", prior, c, digest)
		}
		seen[digest] = c
	}

	if len(seen) != len(SealableTaxVerificationClasses()) {
		t.Fatalf("got %d distinct digests for %d classes", len(seen), len(SealableTaxVerificationClasses()))
	}
}

// The canonical schema tag must move with the layout.
//
// The encoder's own doc says changing the encoding without changing the tag
// would let two different rules produce the same digest. Adding the
// verification class changed the layout, so a v1 intent that omitted it
// must not be able to collide with a v2 one that states it.
func TestCanonicalSchemaTagNamesTheCurrentLayout(t *testing.T) {
	// Bump this deliberately with every encoding change, and only with one.
	//   v2 — the tax determination's verification class
	//   v3 — the sealed funding split (wallet allocation, provider remainder)
	//   v4 — the sealed rail and routing-policy revision
	const current = "mirrorstack.charge-intent/v4"
	if canonicalSchema != current {
		t.Fatalf("canonicalSchema = %q; want %q.\n"+
			"The encoder's own doc says changing the layout without changing the "+
			"tag would let two different rules produce the same digest \u2014 so the tag "+
			"moves with every field added to computeDigest.", canonicalSchema, current)
	}
}
