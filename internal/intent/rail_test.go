package intent

import "testing"

// 🔴 The rail is sealed, so swapping the adapter after disclosure breaks the
// digest.
//
// docs/DESIGN.md:1030-1037 names the abuse directly: "A private caller must
// not select a weaker adapter to bypass notice, authentication, tax, ceilings
// or reconciliation." An UNSEALED rail is exactly that bypass — change it
// after the customer accepted and the document still verifies.
//
// :1282 states the consequence this makes true: "A later rail change requires
// a replacement intent, with a new digest and a new eligibility decision."
func TestTheSealedRailChangesTheDigest(t *testing.T) {
	seen := map[string]string{}

	for _, rail := range []string{"stripe", "stripe-connect", "adyen"} {
		d := validDraft()
		d.SelectedRail = rail
		sealed, err := Seal(d)
		if err != nil {
			t.Fatalf("Seal(rail=%s): %v", rail, err)
		}
		if prior, clash := seen[sealed.Digest()]; clash {
			t.Fatalf("rails %q and %q produced the SAME digest %s — the rail is not "+
				"covered by the seal, so an adapter swap would go undetected",
				prior, rail, sealed.Digest())
		}
		seen[sealed.Digest()] = rail
	}
}

// The routing policy revision is sealed too, and is enumerated.
func TestTheRoutingPolicyRevisionChangesTheDigest(t *testing.T) {
	a := validDraft()
	a.RoutingPolicyRevision = "routing-2026-08"
	sealedA, err := Seal(a)
	if err != nil {
		t.Fatal(err)
	}

	b := validDraft()
	b.RoutingPolicyRevision = "routing-2026-09"
	sealedB, err := Seal(b)
	if err != nil {
		t.Fatal(err)
	}

	if sealedA.Digest() == sealedB.Digest() {
		t.Fatal("two routing policy revisions produced the same digest")
	}
}

// 🔴 A sealed revision MUST be enumerated by UnpublishedRevisions.
//
// That list is what ClausePolicyPublished checks. A revision sealed into the
// digest but absent from the list passes the published check unexamined —
// which is the precise hollowness this predicate was fixed for on 2026-08-30.
//
// This test is the tripwire for the NEXT one: it asserts the routing policy
// participates, so adding a sixth sealed revision without listing it fails
// here rather than shipping a check that silently skips it.
func TestAnUnpublishedRoutingPolicyIsReported(t *testing.T) {
	d := validDraft()
	d.RoutingPolicyRevision = UnpublishedRevisionPrefix + "pending-decision-12"

	sealed, err := Seal(d)
	if err != nil {
		t.Fatal(err)
	}

	var found bool
	for _, name := range UnpublishedRevisions(sealed) {
		if name == "routing_policy" {
			found = true
		}
	}
	if !found {
		t.Fatalf("an unpublished routing policy was not reported; got %v.\n"+
			"A revision sealed into the digest but missing from UnpublishedRevisions "+
			"passes ClausePolicyPublished unexamined.", UnpublishedRevisions(sealed))
	}

	// ...and a published one is not reported.
	d.RoutingPolicyRevision = "routing-2026-08"
	sealed, err = Seal(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range UnpublishedRevisions(sealed) {
		if name == "routing_policy" {
			t.Error("a published routing policy was reported as unpublished")
		}
	}
}
