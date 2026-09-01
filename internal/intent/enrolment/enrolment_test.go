package enrolment

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

var at = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

type fakeStore struct {
	owner     intent.Subject
	payerErr  error
	issued    []IssuedAcceptance
	accepted  []string
	saved     []intent.BillingAuthorization
	acceptErr error
	saveErr   error
	issueErr  error
}

func (f *fakeStore) PayerForAccount(_ context.Context, accountID string) (intent.Subject, error) {
	if f.payerErr != nil {
		return intent.Subject{}, f.payerErr
	}
	// Deliberately NOT the account id — the payer is the account's OWNER, and
	// a fixture where the two coincided could not tell a resolved subject from
	// an unresolved one.
	return intent.Subject{Kind: "org", ID: "owner-of-" + accountID}, nil
}

func (f *fakeStore) IssueAcceptance(_ context.Context, a IssuedAcceptance) error {
	if f.issueErr != nil {
		return f.issueErr
	}
	f.issued = append(f.issued, a)
	return nil
}

func (f *fakeStore) AcceptIssuedAcceptance(_ context.Context, authID, digest string, _ time.Time) error {
	if f.acceptErr != nil {
		return f.acceptErr
	}
	f.accepted = append(f.accepted, authID+"|"+digest)
	return nil
}

func (f *fakeStore) SaveAuthorization(_ context.Context, a intent.BillingAuthorization) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	f.saved = append(f.saved, a)
	return nil
}

func (f *fakeStore) LoadAuthorization(context.Context, string) (intent.BillingAuthorization, error) {
	return intent.BillingAuthorization{}, errors.New("not used")
}

type fixedNonces struct{ n int }

func (f *fixedNonces) Next() (string, string, error) {
	f.n++
	return "nonce-1", "replay-1", nil
}

func enroller(t *testing.T, s Store) *Enroller {
	t.Helper()
	e, err := New(s, &fixedNonces{}, "customer", func() time.Time { return at }, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// validTerms is a COMPLETE set of §12 item 1 values. They are a fixture, not a
// recommendation: this package never chooses a ceiling, and neither does this
// test — it only needs one of each to exist.
func validTerms() Terms {
	return Terms{
		AccountID:              "acct-1",
		Kinds:                  []intent.ChargeKind{intent.KindModuleUsage, intent.KindPlatformBase},
		PerChargeCeilingMicros: 50_000_000,
		PeriodCeilingMicros:    500_000_000,
		FrequencyCeiling:       20,
		NoticeLeadTime:         72 * time.Hour,
		Provider:               "stripe",
		MandateReference:       "pm_test_1",
		TermsRevision:          "terms-2026-09",
		PriceBookRevision:      "pb-2026-09",
		NoticePolicy:           "email/v1",
		EffectiveFrom:          at,
		ExpiresAt:              at.AddDate(1, 0, 0),
	}
}

// 🔴 An offer mints NOTHING.
//
// An authorization minted on a customer's behalf would make INV-006 a
// statement about our own records rather than about the customer's decision —
// the failure §12 item 16 describes for relayed acceptance, one step worse.
func TestAnOfferMintsNothing(t *testing.T) {
	store := &fakeStore{}
	offer, err := enroller(t, store).Offer(context.Background(), validTerms())
	if err != nil {
		t.Fatal(err)
	}

	if len(store.saved) != 0 {
		t.Fatal("an offer minted an authorization before the customer answered")
	}
	if len(store.accepted) != 0 {
		t.Fatal("an offer recorded an acceptance the customer never gave")
	}
	if len(store.issued) != 1 {
		t.Fatalf("the engine issued %d challenges for one offer", len(store.issued))
	}
	if offer.DisclosureDigest == "" || offer.Nonce == "" || offer.ExpiresAt.IsZero() {
		t.Fatal("the offer is missing what a customer must be shown")
	}
	if offer.Payer.ID == "acct-1" {
		t.Fatal("the offer names the ACCOUNT as payer; it must name the owner")
	}
}

// The digest the engine issues must be the one Authorize will later derive, or
// no answer can ever mint.
func TestTheOfferedDigestIsTheOneThatMints(t *testing.T) {
	store := &fakeStore{}
	e := enroller(t, store)

	offer, err := e.Offer(context.Background(), validTerms())
	if err != nil {
		t.Fatal(err)
	}
	auth, err := e.Accept(context.Background(), validTerms(), offer.DisclosureDigest, at)
	if err != nil {
		t.Fatalf("the digest the engine offered would not mint: %v", err)
	}
	if auth.AcceptanceDigest() != offer.DisclosureDigest {
		t.Fatal("the minted authorization carries a different acceptance than was offered")
	}
	if auth.Grant().Subject != offer.Payer {
		t.Fatal("the authorization names a different subject than the offer")
	}
}

// 🔴 Answering one set of terms must not mint another.
//
// The guard is intent.Authorize's: it derives the document these terms
// constitute and refuses a grant whose acceptance names anything else. Accept
// carries the answer straight into it rather than re-deriving and comparing
// first, because a second copy of that rule is what drifts — and mutation
// testing confirmed the copy was doing nothing, since Authorize already
// refused.
func TestAnAnswerCannotMintDifferentTerms(t *testing.T) {
	store := &fakeStore{}
	e := enroller(t, store)

	offer, err := e.Offer(context.Background(), validTerms())
	if err != nil {
		t.Fatal(err)
	}

	changed := validTerms()
	changed.PerChargeCeilingMicros = 999_000_000

	_, err = e.Accept(context.Background(), changed, offer.DisclosureDigest, at)
	if !errors.Is(err, intent.ErrAuthAcceptanceMismatch) {
		t.Fatalf("a raised ceiling was minted under the original answer: %v", err)
	}
	if len(store.saved) != 0 {
		t.Fatal("an authorization was saved for terms the customer never saw")
	}
}

// 🔴 The acceptance is recorded BEFORE the authorization is saved.
//
// Reversed, a failed save could leave an authorization whose acceptance the
// predicate never finds — and the predicate refuses on that, so the account
// would be enrolled and permanently uncollectable.
func TestTheAcceptanceIsRecordedBeforeTheAuthorization(t *testing.T) {
	store := &fakeStore{saveErr: errors.New("database down")}
	e := enroller(t, store)

	offer, err := e.Offer(context.Background(), validTerms())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Accept(context.Background(), validTerms(), offer.DisclosureDigest, at); err == nil {
		t.Fatal("a failed save reported success")
	}
	if len(store.accepted) != 1 {
		t.Fatal("the answer was not recorded, so a retry would find no acceptance and the " +
			"account would be enrolled and uncollectable")
	}
}

// Every §12 item 1 value is required. A default here would be this package
// deciding what a customer authorised.
func TestIncompleteTermsAreRefused(t *testing.T) {
	for name, break_ := range map[string]func(*Terms){
		"account":            func(t *Terms) { t.AccountID = "" },
		"kinds":              func(t *Terms) { t.Kinds = nil },
		"per-charge ceiling": func(t *Terms) { t.PerChargeCeilingMicros = 0 },
		"period ceiling":     func(t *Terms) { t.PeriodCeilingMicros = 0 },
		"frequency ceiling":  func(t *Terms) { t.FrequencyCeiling = 0 },
		"notice lead time":   func(t *Terms) { t.NoticeLeadTime = 0 },
		"provider":           func(t *Terms) { t.Provider = "" },
		"mandate":            func(t *Terms) { t.MandateReference = "" },
		"terms revision":     func(t *Terms) { t.TermsRevision = "" },
		"price book":         func(t *Terms) { t.PriceBookRevision = "" },
		"notice policy":      func(t *Terms) { t.NoticePolicy = "" },
		"effective from":     func(t *Terms) { t.EffectiveFrom = time.Time{} },
		"expires at":         func(t *Terms) { t.ExpiresAt = time.Time{} },
	} {
		t.Run(name, func(t2 *testing.T) {
			store := &fakeStore{}
			terms := validTerms()
			break_(&terms)

			if _, err := enroller(t2, store).Offer(context.Background(), terms); !errors.Is(err, ErrIncompleteTerms) {
				t2.Fatalf("an offer with no %s was issued: %v", name, err)
			}
			if len(store.issued) != 0 {
				t2.Fatalf("a challenge was issued for terms with no %s", name)
			}
		})
	}
}

// Enrolling the same account twice must converge on one authorization rather
// than accumulate a second beside it.
func TestEnrollingTwiceIsOneAuthorization(t *testing.T) {
	store := &fakeStore{}
	e := enroller(t, store)

	first, err := e.Offer(context.Background(), validTerms())
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.Offer(context.Background(), validTerms())
	if err != nil {
		t.Fatal(err)
	}
	if first.AuthorizationID != second.AuthorizationID {
		t.Fatal("two offers for one account produced two authorization ids")
	}
	if first.DisclosureDigest != second.DisclosureDigest {
		t.Fatal("the same terms produced two documents, so a customer would be asked " +
			"to re-accept something they already accepted")
	}
}

// An account nothing owns cannot be enrolled. A charge against nobody must not
// become an authorization.
func TestAnUnresolvableAccountCannotBeEnrolled(t *testing.T) {
	store := &fakeStore{payerErr: errors.New("no such account")}
	if _, err := enroller(t, store).Offer(context.Background(), validTerms()); err == nil {
		t.Fatal("an account with no owner was offered an authorization")
	}
	if len(store.issued) != 0 {
		t.Fatal("a challenge was issued for an account nothing owns")
	}
}

// The constructor must refuse a dependency that would make an offer
// unreproducible or an acceptance replayable.
func TestAnEnrollerRefusesAnIncompleteWiring(t *testing.T) {
	for name, build := range map[string]func() (*Enroller, error){
		"no store": func() (*Enroller, error) {
			return New(nil, &fixedNonces{}, "customer", func() time.Time { return at }, time.Hour)
		},
		"no nonces": func() (*Enroller, error) {
			return New(&fakeStore{}, nil, "customer", func() time.Time { return at }, time.Hour)
		},
		"no clock": func() (*Enroller, error) { return New(&fakeStore{}, &fixedNonces{}, "customer", nil, time.Hour) },
		"no audience": func() (*Enroller, error) {
			return New(&fakeStore{}, &fixedNonces{}, "", func() time.Time { return at }, time.Hour)
		},
		"no expiry": func() (*Enroller, error) {
			return New(&fakeStore{}, &fixedNonces{}, "customer", func() time.Time { return at }, 0)
		},
	} {
		t.Run(name, func(t2 *testing.T) {
			if _, err := build(); err == nil {
				t2.Fatalf("an enroller with %s was constructed", name)
			}
		})
	}
}
