package proposer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence/evidencetest"
)

type recordingStore struct {
	groups      []string
	saved       []intent.ChargeIntent
	event       evidence.Event
	resolvedFor string
	payerErr    error
	err         error
}

// PayerForAccount is the resolution the proposer performs. The fixture
// returns an OWNER id that differs from the account id it was given, which is
// the whole point: a test whose owner equalled its account id could not tell
// a resolved subject from an unresolved one.
func (s *recordingStore) PayerForAccount(_ context.Context, accountID string) (intent.Subject, error) {
	if s.payerErr != nil {
		return intent.Subject{}, s.payerErr
	}
	s.resolvedFor = accountID
	return intent.Subject{Kind: "user", ID: "owner-of-" + accountID}, nil
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

// SaveIntentGroupWithEvidence records a grouped save the way the real store
// does it: every member and its grouping, or none of them. The fake keeps the
// group id so a test can assert that one boundary produced ONE group.
func (s *recordingStore) SaveIntentGroupWithEvidence(
	ctx context.Context,
	groupID string,
	sealed []intent.ChargeIntent,
	rec *evidence.Recorder,
	events []evidence.Event,
) error {
	if len(sealed) != len(events) {
		return fmt.Errorf("fake store: %d intents but %d events", len(sealed), len(events))
	}
	for i, in := range sealed {
		if err := s.SaveIntentWithEvidence(ctx, in, rec, events[i]); err != nil {
			return err
		}
		s.groups = append(s.groups, groupID)
		_ = i
	}
	return nil
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
		AccountID: "acct-1",
		Kind:      intent.KindPlatformBase,
		Currency:  "USD",
		Lines: SingleLine(
			"custom domain example.com",
			"domain:11111111-1111-1111-1111-111111111111",
			5_000_000,
		),

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
	other.Lines[0].AmountMicros++
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
			c.Lines = SingleLine("d", "ref", amount)

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

// 🔴 The proposer must SEAL THE OWNER, not the account id it was given.
//
// All three cut-over legs used to seal intent.Subject{Kind:"user", ID:
// <accounts.id>} while the executor resolved a payer by owner_user_id. Those
// are never equal, so every intent this tree could produce was uncollectable.
// Resolving here is what makes the two halves agree; this is the test that
// says so.
func TestTheProposerSealsTheResolvedOwnerNotTheAccountID(t *testing.T) {
	store := &recordingStore{}
	sealed, err := newProposer(t, store).Propose(context.Background(), domainCharge())
	if err != nil {
		t.Fatal(err)
	}
	if store.resolvedFor != "acct-1" {
		t.Fatalf("the proposer resolved %q, the charge named account %q", store.resolvedFor, "acct-1")
	}
	if sealed.Payer().ID == "acct-1" {
		t.Fatal("the intent sealed the ACCOUNT id as its payer. The executor resolves a " +
			"payer by owner id, so this intent could never be collected.")
	}
	if got := (intent.Subject{Kind: "user", ID: "owner-of-acct-1"}); sealed.Payer() != got {
		t.Fatalf("sealed payer = %v, want %v", sealed.Payer(), got)
	}
}

// A charge against an account nothing owns must not seal.
func TestAChargeAgainstAnUnresolvableAccountIsRefused(t *testing.T) {
	store := &recordingStore{payerErr: errors.New("no such account")}
	if _, err := newProposer(t, store).Propose(context.Background(), domainCharge()); !errors.Is(err, ErrNotProposable) {
		t.Fatalf("sealed a charge whose payer could not be resolved: %v", err)
	}
	if len(store.saved) != 0 {
		t.Fatal("an intent was stored for an account nothing owns")
	}
}

// A zero or negative LINE must be refused, not just a zero total.
//
// A charge whose lines cancel out to something positive would seal a document
// with a line the customer is charged nothing for — or, worse, a negative one
// that reads as a refund inside a debit. Legs already skip these; the proposer
// refuses rather than relying on them to.
func TestANonPositiveLineIsRefused(t *testing.T) {
	for name, lines := range map[string][]ChargeLine{
		"a zero line": {
			{Description: "a", SourceRef: "ref-a", AmountMicros: 5_000_000},
			{Description: "b", SourceRef: "ref-b", AmountMicros: 0},
		},
		"a negative line": {
			{Description: "a", SourceRef: "ref-a", AmountMicros: 5_000_000},
			{Description: "b", SourceRef: "ref-b", AmountMicros: -1_000_000},
		},
		"only a zero line": {
			{Description: "a", SourceRef: "ref-a", AmountMicros: 0},
		},
		"no lines at all": {},
	} {
		t.Run(name, func(t *testing.T) {
			store := &recordingStore{}
			c := domainCharge()
			c.Lines = lines
			if _, err := newProposer(t, store).Propose(context.Background(), c); !errors.Is(err, ErrNotProposable) {
				t.Fatalf("sealed a charge with %s: %v", name, err)
			}
			if len(store.saved) != 0 {
				t.Fatalf("an intent was stored for a charge with %s", name)
			}
		})
	}
}

// Every line reaches the sealed document, in order, and each contributes a
// source-fact key. A leg's second line silently dropped would charge the
// customer for it while the document said otherwise.
func TestEveryChargeLineReachesTheSealedIntent(t *testing.T) {
	store := &recordingStore{}
	c := domainCharge()
	c.Lines = []ChargeLine{
		{Description: "usage arrears", SourceRef: "run:1#arrears", AmountMicros: 5_000},
		{Description: "MirrorStack base fee", SourceRef: "run:1#base", AmountMicros: 20_000_000},
		{Description: "custom domain", SourceRef: "run:1#domains", AmountMicros: 2_000_000},
	}

	sealed, err := newProposer(t, store).Propose(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	lines := sealed.Lines()
	if len(lines) != len(c.Lines) {
		t.Fatalf("sealed %d lines for a charge of %d", len(lines), len(c.Lines))
	}
	for i, want := range c.Lines {
		if lines[i].Meter != want.Description {
			t.Errorf("line %d described as %q, want %q", i, lines[i].Meter, want.Description)
		}
		if lines[i].AmountMicros() != want.AmountMicros {
			t.Errorf("line %d is %d micros, want %d", i, lines[i].AmountMicros(), want.AmountMicros)
		}
	}
	if got, want := sealed.SubtotalMicros(), c.TotalMicros(); got != want {
		t.Errorf("the sealed subtotal is %d, the lines add to %d", got, want)
	}
	if got := len(sealed.SourceFactKeys()); got != len(c.Lines) {
		t.Errorf("%d source facts for %d lines; a line with no fact cannot be traced back "+
			"to the row it came from", got, len(c.Lines))
	}
}

// The wallet allocation must reach the seal, or the provider is handed the
// gross and the customer pays twice for the part credit already covered.
func TestTheWalletAllocationReachesTheSealedFundingSplit(t *testing.T) {
	store := &recordingStore{}
	c := domainCharge()
	c.Kind = intent.KindModuleUsage
	c.Lines = SingleLine("module usage", "run:1", 20_000_000)
	c.WalletAllocationMicros = 6_000_000

	sealed, err := newProposer(t, store).Propose(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if got := sealed.WalletAllocationMicros(); got != 6_000_000 {
		t.Fatalf("sealed wallet allocation = %d, want 6000000", got)
	}
	if got, want := sealed.ProviderRemainderMicros(), sealed.TotalMicros()-6_000_000; got != want {
		t.Fatalf("provider remainder = %d, want %d. The adapter is handed this number, so a "+
			"charge that ignored the draw would collect for credit already spent.", got, want)
	}
}
