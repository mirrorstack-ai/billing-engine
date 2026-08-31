package proposer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence/evidencetest"
)

type recordingStore struct {
	saved []intent.ChargeIntent
	event evidence.Event
	err   error
}

func (s *recordingStore) SaveIntentWithEvidence(
	_ context.Context, sealed intent.ChargeIntent, rec *evidence.Recorder, e evidence.Event,
) error {
	// The proposer must hand down a recorder and a well-formed event. A store
	// that ignored them would let the proposer's wiring rot unnoticed.
	if rec == nil {
		s.err = errors.New("proposer called the store with no recorder")
		return s.err
	}
	s.event = e
	return s.saveIntent(sealed)
}

func (s *recordingStore) saveIntent(sealed intent.ChargeIntent) error {
	if s.err != nil {
		return s.err
	}
	s.saved = append(s.saved, sealed)
	return nil
}

var (
	windowStart = time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	windowEnd   = time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
)

func domainCharge() Charge {
	return Charge{
		Payer:        intent.Subject{Kind: "user", ID: "acct-1"},
		Kind:         intent.KindCustomDomain,
		Currency:     "USD",
		AmountMicros: 5_000_000,
		Description:  "custom domain example.com",
		SourceRef:    "domain:11111111-1111-1111-1111-111111111111",

		AuthorizationID:   "auth-1",
		TermsRevision:     "terms-2026-01",
		PriceBookRevision: "pb-2026-08",
		NoticePolicy:      "email/v1",
		Tax: intent.TaxDetermination{
			Resolved: true, Jurisdiction: "US-OR", RuleRevision: "tax-2026-05",
			Verification: intent.TaxNotApplicable,
		},
		ExecuteNotBefore: windowStart,
		ExecuteNotAfter:  windowEnd,
	}
}

func TestProposingSealsAndStores(t *testing.T) {
	store := &recordingStore{}

	sealed, err := newProposer(t, store).Propose(context.Background(), domainCharge())
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	if !sealed.Sealed() {
		t.Fatal("Propose returned an unsealed intent")
	}
	if got, want := sealed.TotalMicros(), int64(5_000_000); got != want {
		t.Errorf("total = %d, want %d", got, want)
	}
	if len(store.saved) != 1 {
		t.Fatalf("stored %d intents, want 1", len(store.saved))
	}
	if store.saved[0].Digest() != sealed.Digest() {
		t.Error("the stored intent is not the one returned")
	}
}

// Proposing is not charging. A leg that has cut over derives the same
// amount it always did and then stops — which is what makes the cutover
// reversible, and what lets cmd/billing-cycle lose its write port.
func TestProposingCannotCollect(t *testing.T) {
	store := &recordingStore{}
	if _, err := newProposer(t, store).Propose(context.Background(), domainCharge()); err != nil {
		t.Fatal(err)
	}

	// The type has no provider client and no executor to reach. The
	// assertion that matters is structural and lives in
	// internal/architecture; this pins the observable half: nothing
	// left this package except a stored document.
	if len(store.saved) != 1 {
		t.Fatalf("expected exactly one stored document, got %d", len(store.saved))
	}
}

// Re-deriving the same charge proposes the same document rather than a
// second one. The legacy legs get this from deterministic Stripe
// idempotency keys; a cut-over leg gets it from the identity of the
// content.
func TestReproposingTheSameChargeIsTheSameDocument(t *testing.T) {
	store := &recordingStore{}
	p := newProposer(t, store)

	first, err := p.Propose(context.Background(), domainCharge())
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Propose(context.Background(), domainCharge())
	if err != nil {
		t.Fatal(err)
	}

	if first.Digest() != second.Digest() {
		t.Fatalf("re-deriving one charge produced two documents: %s and %s",
			first.Digest(), second.Digest())
	}
}

// A different amount is a different document, or the digest would not
// be an identity.
func TestADifferentAmountIsADifferentDocument(t *testing.T) {
	p := newProposer(t, &recordingStore{})

	first, err := p.Propose(context.Background(), domainCharge())
	if err != nil {
		t.Fatal(err)
	}
	other := domainCharge()
	other.AmountMicros++
	second, err := p.Propose(context.Background(), other)
	if err != nil {
		t.Fatal(err)
	}

	if first.Digest() == second.Digest() {
		t.Fatal("two different amounts sealed to one digest")
	}
}

// A zero or negative charge is not a document worth sealing. Legs
// already skip these; refusing here means the seam does not depend on
// them remembering to.
func TestAZeroOrNegativeChargeIsRefused(t *testing.T) {
	for name, amount := range map[string]int64{"zero": 0, "negative": -1} {
		t.Run(name, func(t *testing.T) {
			c := domainCharge()
			c.AmountMicros = amount

			sealed, err := newProposer(t, &recordingStore{}).Propose(context.Background(), c)
			if !errors.Is(err, ErrNotProposable) {
				t.Fatalf("err = %v, want %v", err, ErrNotProposable)
			}
			if sealed.Sealed() {
				t.Error("a refused charge still produced a sealed intent")
			}
		})
	}
}

// A charge missing something Seal requires is refused with the reason,
// not stored half-formed.
func TestAnUnsealableChargeIsRefusedAndNotStored(t *testing.T) {
	store := &recordingStore{}
	c := domainCharge()
	c.Tax.Resolved = false // an undetermined tax must not seal

	_, err := newProposer(t, store).Propose(context.Background(), c)
	if !errors.Is(err, ErrNotProposable) {
		t.Fatalf("err = %v, want %v", err, ErrNotProposable)
	}
	if !errors.Is(err, intent.ErrTaxUnresolved) {
		t.Errorf("the error does not name the reason: %v", err)
	}
	if len(store.saved) != 0 {
		t.Error("an unsealable charge was stored anyway")
	}
}

// A store failure must not look like a successful proposal.
func TestAStoreFailureIsReported(t *testing.T) {
	store := &recordingStore{err: errors.New("database is down")}

	sealed, err := newProposer(t, store).Propose(context.Background(), domainCharge())
	if err == nil {
		t.Fatal("a failed store returned no error")
	}
	if sealed.Sealed() {
		t.Error("a charge that was never stored was returned as sealed; a leg would " +
			"record a digest pointing at nothing")
	}
}

// newProposer builds a Proposer with a real signing recorder, so a test
// exercises the path a deployment takes rather than a stubbed one.
func newProposer(t *testing.T, store Store) *Proposer {
	t.Helper()
	p, err := New(store, evidencetest.Recorder(t), func() time.Time { return evidencetest.At })
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

// A proposer that cannot record what it seals must not be constructible.
func TestAProposerWithNoRecorderIsRefused(t *testing.T) {
	if _, err := New(&recordingStore{}, nil, func() time.Time { return evidencetest.At }); !errors.Is(err, ErrNoRecorder) {
		t.Fatalf("a proposer with no evidence recorder was constructed: %v", err)
	}
}

// The sealed-intent event must name the document it attests.
func TestProposingRecordsASealedIntentEvent(t *testing.T) {
	store := &recordingStore{}
	sealed, err := newProposer(t, store).Propose(context.Background(), domainCharge())
	if err != nil {
		t.Fatal(err)
	}
	if store.event.Kind != evidence.KindSealedIntent {
		t.Errorf("event kind = %q, want %q", store.event.Kind, evidence.KindSealedIntent)
	}
	if store.event.IntentDigest != sealed.Digest() {
		t.Errorf("the event names %q, the intent is %q", store.event.IntentDigest, sealed.Digest())
	}
	if store.event.Subject != sealed.Payer() {
		t.Errorf("the event names a different payer than the intent")
	}
	if store.event.Detail != string(sealed.Kind()) {
		t.Errorf("detail = %q, want the charge kind %q", store.event.Detail, sealed.Kind())
	}
	if store.event.OccurredAt.IsZero() {
		t.Error("the event has no instant")
	}
}
