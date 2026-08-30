package intent

import (
	"errors"
	"testing"
	"time"
)

var (
	authFrom = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	authTill = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	now      = time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
)

const kindWalletTopUp ChargeKind = "wallet.topup"

func standingGrant() AuthorizationGrant {
	return AuthorizationGrant{
		ID:               "auth-1",
		Scope:            ScopeStanding,
		Subject:          Subject{Kind: "org", ID: "org-1"},
		Currency:         "USD",
		Kinds:            []ChargeKind{kindWalletTopUp},
		PerChargeCeiling: 50_000,
		PeriodCeiling:    200_000,
		TermsRevision:    "terms-2026-01",
		PriceBook:        "pb-2026-08",
		NoticePolicy:     "email/v1",
		EffectiveFrom:    authFrom,
		ExpiresAt:        authTill,
		AcceptanceDigest: "accept-1",
	}
}

func sealedFixture(t *testing.T) ChargeIntent {
	t.Helper()
	sealed, err := Seal(validDraft())
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return sealed
}

func TestStandingAuthorizationPermitsAnIntentInsideItsBounds(t *testing.T) {
	auth, err := Authorize(standingGrant())
	if err != nil {
		t.Fatal(err)
	}
	decision := auth.Permits(sealedFixture(t), kindWalletTopUp, now, 0)
	if !decision.Permitted {
		t.Fatalf("refused an intent inside every bound: %v", decision.Refusals)
	}
}

// Every bound must actually bind. A ceiling nobody checks is the
// alert-only budget docs/SECURITY.md §2 warns about: "A displayed
// budget must not be mistaken for a hard authorization cap."
func TestEveryBoundRefuses(t *testing.T) {
	cases := []struct {
		name        string
		grant       func(*AuthorizationGrant)
		draft       func(*Draft)
		kind        ChargeKind
		at          time.Time
		priorSpend  int64
		wantRefusal Refusal
	}{
		{
			name: "a different subject", kind: kindWalletTopUp, at: now,
			grant:       func(g *AuthorizationGrant) { g.Subject = Subject{Kind: "org", ID: "org-2"} },
			wantRefusal: RefusalWrongSubject,
		},
		{
			name: "a different currency", kind: kindWalletTopUp, at: now,
			grant:       func(g *AuthorizationGrant) { g.Currency = "TWD" },
			wantRefusal: RefusalWrongCurrency,
		},
		{
			name: "a charge kind it never permitted", kind: "subscription.increase", at: now,
			wantRefusal: RefusalKindNotPermitted,
		},
		{
			name: "over the per-charge ceiling", kind: kindWalletTopUp, at: now,
			grant:       func(g *AuthorizationGrant) { g.PerChargeCeiling = 1 },
			wantRefusal: RefusalOverPerCharge,
		},
		{
			name: "over the period ceiling", kind: kindWalletTopUp, at: now, priorSpend: 199_999,
			wantRefusal: RefusalOverPeriod,
		},
		{
			name: "the price book moved under it", kind: kindWalletTopUp, at: now,
			draft:       func(d *Draft) { d.PriceBookRevision = "pb-2026-09" },
			wantRefusal: RefusalPriceBookMoved,
		},
		{
			name: "the notice policy moved under it", kind: kindWalletTopUp, at: now,
			draft:       func(d *Draft) { d.NoticePolicy = "sms/v1" },
			wantRefusal: RefusalNoticePolicyMoved,
		},
		{
			name: "the terms moved under it", kind: kindWalletTopUp, at: now,
			draft:       func(d *Draft) { d.TermsRevision = "terms-2026-02" },
			wantRefusal: RefusalTermsMoved,
		},
		{
			name: "before it takes effect", kind: kindWalletTopUp, at: authFrom.Add(-time.Hour),
			wantRefusal: RefusalNotYetEffective,
		},
		{
			name: "after it expires", kind: kindWalletTopUp, at: authTill.Add(time.Hour),
			wantRefusal: RefusalExpired,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := standingGrant()
			if tc.grant != nil {
				tc.grant(&g)
			}
			auth, err := Authorize(g)
			if err != nil {
				t.Fatal(err)
			}

			d := validDraft()
			if tc.draft != nil {
				tc.draft(&d)
			}
			sealed, err := Seal(d)
			if err != nil {
				t.Fatal(err)
			}

			decision := auth.Permits(sealed, tc.kind, tc.at, tc.priorSpend)
			if decision.Permitted {
				t.Fatalf("permitted %s", tc.name)
			}
			if !hasRefusal(decision.Refusals, tc.wantRefusal) {
				t.Errorf("refusals = %v, want to include %v", decision.Refusals, tc.wantRefusal)
			}
		})
	}
}

// An intent seals the authorization it was rated under. Without this
// check any valid standing authorization for the payer would permit an
// intent sealed against a different one, and the AuthorizationID a
// customer reads on their charge bundle would not be the permission
// that was actually consulted.
func TestAuthorizationRefusesAnIntentThatNamesAnotherOne(t *testing.T) {
	auth, err := Authorize(standingGrant())
	if err != nil {
		t.Fatal(err)
	}

	d := validDraft()
	d.AuthorizationID = "auth-somebody-elses"
	elsewhere, err := Seal(d)
	if err != nil {
		t.Fatal(err)
	}

	decision := auth.Permits(elsewhere, kindWalletTopUp, now, 0)
	if decision.Permitted {
		t.Fatal("an authorization permitted an intent that names a different one")
	}
	if !hasRefusal(decision.Refusals, RefusalDifferentAuthorization) {
		t.Errorf("refusals = %v, want %v", decision.Refusals, RefusalDifferentAuthorization)
	}
}

// A customer asking why a charge was refused deserves every reason,
// not the first one found: fixing one and being refused again for the
// next is how a support conversation becomes five.
func TestRefusalsAreReportedTogether(t *testing.T) {
	g := standingGrant()
	g.Subject = Subject{Kind: "org", ID: "org-2"}
	g.Currency = "TWD"
	auth, err := Authorize(g)
	if err != nil {
		t.Fatal(err)
	}

	decision := auth.Permits(sealedFixture(t), "subscription.increase", authTill.Add(time.Hour), 0)

	for _, want := range []Refusal{
		RefusalWrongSubject, RefusalWrongCurrency, RefusalKindNotPermitted, RefusalExpired,
	} {
		if !hasRefusal(decision.Refusals, want) {
			t.Errorf("refusals = %v, missing %v", decision.Refusals, want)
		}
	}
}

// A one-time authorization covers exactly the document it names.
// INV-003: a superseding correction "repeats every notice and
// authorization check", so the replacement is not covered by the
// original's permission.
func TestOneTimeAuthorizationDoesNotCoverASupersedingCorrection(t *testing.T) {
	original := sealedFixture(t)

	g := standingGrant()
	g.Scope = ScopeOneTime
	g.IntentDigest = original.Digest()
	g.Kinds = nil
	g.PerChargeCeiling = 0
	auth, err := Authorize(g)
	if err != nil {
		t.Fatal(err)
	}

	if decision := auth.Permits(original, kindWalletTopUp, now, 0); !decision.Permitted {
		t.Fatalf("refused the very intent it names: %v", decision.Refusals)
	}

	corrected := validDraft()
	corrected.Lines = []Line{NewLine("quiz.render", "quiz-core", "1.4.0", 1_001, 25)}
	replacement, err := original.Supersede(corrected)
	if err != nil {
		t.Fatal(err)
	}

	decision := auth.Permits(replacement, kindWalletTopUp, now, 0)
	if decision.Permitted {
		t.Fatal("a one-time authorization covered a document it never named")
	}
	if !hasRefusal(decision.Refusals, RefusalWrongIntent) {
		t.Errorf("refusals = %v, want %v", decision.Refusals, RefusalWrongIntent)
	}
}

func TestRevokedAuthorizationStopsPermitting(t *testing.T) {
	auth, err := Authorize(standingGrant())
	if err != nil {
		t.Fatal(err)
	}
	sealed := sealedFixture(t)

	revoked := auth.Revoke(now)

	if decision := auth.Permits(sealed, kindWalletTopUp, now, 0); !decision.Permitted {
		t.Fatal("Revoke mutated the original authorization; a bundle already read would change under the reader")
	}
	if decision := revoked.Permits(sealed, kindWalletTopUp, now, 0); decision.Permitted {
		t.Fatal("a revoked authorization still permitted a charge")
	}
	if decision := revoked.Permits(sealed, kindWalletTopUp, now.Add(-time.Hour), 0); !decision.Permitted {
		t.Fatal("revocation applied retroactively; a charge already permitted became unpermitted")
	}
}

// Nothing is defaulted. An authorization with no ceiling is not an
// unlimited one, it is a refused grant.
func TestAuthorizeRefusesRatherThanDefaults(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*AuthorizationGrant)
		wantErr error
	}{
		{"no id", func(g *AuthorizationGrant) { g.ID = "" }, ErrAuthIDMissing},
		{"unknown scope", func(g *AuthorizationGrant) { g.Scope = "forever" }, ErrAuthScopeUnknown},
		{"unknown subject", func(g *AuthorizationGrant) { g.Subject.Kind = "tenant" }, ErrAuthSubjectUnknown},
		{"no currency", func(g *AuthorizationGrant) { g.Currency = "" }, ErrAuthCurrencyMissing},
		{"standing with no kinds", func(g *AuthorizationGrant) { g.Kinds = nil }, ErrAuthKindsMissing},
		{"standing with no ceiling", func(g *AuthorizationGrant) { g.PerChargeCeiling = 0 }, ErrAuthCeilingMissing},
		{"negative ceiling", func(g *AuthorizationGrant) { g.PerChargeCeiling = -1 }, ErrAuthCeilingNegative},
		{"no terms revision", func(g *AuthorizationGrant) { g.TermsRevision = "" }, ErrAuthTermsMissing},
		{"no price book", func(g *AuthorizationGrant) { g.PriceBook = "" }, ErrAuthPriceBookMissing},
		{"no notice policy", func(g *AuthorizationGrant) { g.NoticePolicy = "" }, ErrAuthNoticeMissing},
		{"no window", func(g *AuthorizationGrant) { g.EffectiveFrom = time.Time{} }, ErrAuthWindowMissing},
		{"inverted window", func(g *AuthorizationGrant) { g.ExpiresAt = authFrom.Add(-time.Hour) }, ErrAuthWindowInverted},
		{"no acceptance receipt", func(g *AuthorizationGrant) { g.AcceptanceDigest = "" }, ErrAuthAcceptanceMissing},
		{"one-time naming no intent", func(g *AuthorizationGrant) {
			g.Scope = ScopeOneTime
			g.IntentDigest = ""
		}, ErrAuthDigestMissing},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := standingGrant()
			tc.mutate(&g)
			auth, err := Authorize(g)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Authorize error = %v, want %v", err, tc.wantErr)
			}
			if auth.ID() != "" {
				t.Error("a refused grant still produced an authorization")
			}
		})
	}
}

// A zero authorization permits nothing. It is what a caller holds when
// a lookup missed, and "not found" must never read as "allowed".
func TestZeroAuthorizationPermitsNothing(t *testing.T) {
	var zero BillingAuthorization
	decision := zero.Permits(sealedFixture(t), kindWalletTopUp, now, 0)
	if decision.Permitted {
		t.Fatal("a zero BillingAuthorization permitted a charge")
	}
	if !hasRefusal(decision.Refusals, RefusalNotAuthorized) {
		t.Errorf("refusals = %v, want %v", decision.Refusals, RefusalNotAuthorized)
	}
}

// An unsealed intent is not a document anyone approved.
func TestUnsealedIntentIsNeverPermitted(t *testing.T) {
	auth, err := Authorize(standingGrant())
	if err != nil {
		t.Fatal(err)
	}
	if decision := auth.Permits(ChargeIntent{}, kindWalletTopUp, now, 0); decision.Permitted {
		t.Fatal("an unsealed intent was permitted")
	}
}

func hasRefusal(refusals []Refusal, want Refusal) bool {
	for _, r := range refusals {
		if r == want {
			return true
		}
	}
	return false
}

// Of the two ways to be wrong about a revocation, one refuses charges
// that should have gone through and the other keeps charging a customer
// who asked you to stop. A zero instant must produce the first.
//
// The trap is that "not revoked" is itself represented by a zero time,
// so passing one through would return an authorization that looked
// revoked to its caller and permitted everything.
func TestRevokeWithAZeroInstantStillRevokes(t *testing.T) {
	auth, err := Authorize(standingGrant())
	if err != nil {
		t.Fatal(err)
	}
	sealed := sealedFixture(t)

	revoked := auth.Revoke(time.Time{})

	if decision := revoked.Permits(sealed, kindWalletTopUp, now, 0); decision.Permitted {
		t.Fatal("Revoke with a zero instant silently did nothing")
	}
	if !hasRefusal(revoked.Permits(sealed, kindWalletTopUp, now, 0).Refusals, RefusalRevoked) {
		t.Error("the refusal does not name revocation")
	}
	// Even at the very start of its own effective window.
	if decision := revoked.Permits(sealed, kindWalletTopUp, authFrom, 0); decision.Permitted {
		t.Fatal("a zero-instant revocation left a window in which charges were still permitted")
	}
}

// The persistable form of an authorization is the grant that made it,
// so storage round-trips through Authorize and gets every validation on
// the way back in.
func TestAuthorizationRoundTripsThroughItsGrant(t *testing.T) {
	original, err := Authorize(standingGrant())
	if err != nil {
		t.Fatal(err)
	}

	restored, err := Authorize(original.Grant())
	if err != nil {
		t.Fatalf("an authorization's own grant was refused: %v", err)
	}

	sealed := sealedFixture(t)
	before := original.Permits(sealed, kindWalletTopUp, now, 0)
	after := restored.Permits(sealed, kindWalletTopUp, now, 0)
	if before.Permitted != after.Permitted {
		t.Fatalf("round trip changed the verdict: %v -> %v", before, after)
	}

	// Every bound must survive, not just the happy verdict.
	for name, check := range map[string]func(BillingAuthorization) Decision{
		"over the per-charge ceiling": func(a BillingAuthorization) Decision {
			d := validDraft()
			d.Lines = []Line{NewLine("quiz.render", "quiz-core", "1.4.0", 1_000_000, 25)}
			big, err := Seal(d)
			if err != nil {
				t.Fatal(err)
			}
			return a.Permits(big, kindWalletTopUp, now, 0)
		},
		"a kind it never permitted": func(a BillingAuthorization) Decision {
			return a.Permits(sealed, "subscription.increase", now, 0)
		},
		"after expiry": func(a BillingAuthorization) Decision {
			return a.Permits(sealed, kindWalletTopUp, authTill.Add(time.Hour), 0)
		},
	} {
		t.Run(name, func(t *testing.T) {
			if check(original).Permitted != check(restored).Permitted {
				t.Errorf("round trip changed the %s verdict", name)
			}
		})
	}
}

// Revocation is stored beside the grant, not inside it, so a caller
// reapplies it. This pins that the grant does not silently carry it.
func TestGrantDoesNotCarryRevocation(t *testing.T) {
	auth, err := Authorize(standingGrant())
	if err != nil {
		t.Fatal(err)
	}
	revoked := auth.Revoke(now)

	if revoked.RevokedAt().IsZero() {
		t.Fatal("RevokedAt did not record the revocation")
	}
	restored, err := Authorize(revoked.Grant())
	if err != nil {
		t.Fatal(err)
	}
	if !restored.RevokedAt().IsZero() {
		t.Error("the grant carried a revocation; a caller reapplying it would double-count")
	}
}
