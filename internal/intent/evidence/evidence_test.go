package evidence

import (
	"errors"
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/signing"
)

var at = time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)

func testRecorder(t *testing.T) *Recorder {
	t.Helper()
	seed := make([]byte, signing.SeedSize)
	for i := range seed {
		seed[i] = 0x2a
	}
	key, err := signing.NewKey(signing.DomainBillingEvidence, seed)
	if err != nil {
		t.Fatal(err)
	}
	r, err := newRecorderWithKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func newRecorderWithKey(k signing.Key) (*Recorder, error) {
	return &Recorder{
		key: k, issuer: "billing-engine", audience: "customer",
		environment: "test", now: func() time.Time { return at },
		validity: DefaultValidity,
	}, nil
}

func validEvent() Event {
	return Event{
		Kind:         KindSealedIntent,
		Subject:      intent.Subject{Kind: "org", ID: "org-1"},
		IntentDigest: "digest-1",
		Detail:       "usage.cycle",
		OccurredAt:   at,
	}
}

// A deployment with no key cannot construct a Recorder, and a nil Recorder
// cannot seal. There is no third state in which evidence is silently skipped.
func TestADeploymentWithNoKeyCannotRecord(t *testing.T) {
	var none signing.Signer
	if _, err := NewRecorder(none, Options{
		Issuer: "billing-engine", Audience: "customer", Environment: "test",
		Now: func() time.Time { return at },
	}); !errors.Is(err, signing.ErrNoKey) {
		t.Fatalf("a keyless deployment built a recorder: %v", err)
	}

	var nilRecorder *Recorder
	if _, err := nilRecorder.Seal(validEvent(), 1); !errors.Is(err, signing.ErrNoKey) {
		t.Fatalf("a nil recorder sealed a record: %v", err)
	}
}

// 🔴 A kind nothing can produce must be refused.
//
// A row of a kind no subsystem mints would be the table asserting that
// subsystem ran. Refusing keeps it an account of what happened rather than of
// what was declared — and internal/architecture measures the claim against
// the tree, so this and that test hold each other up.
func TestANonProducibleKindIsRefused(t *testing.T) {
	r := testRecorder(t)
	for _, k := range []Kind{KindProofResult, KindNoticeEligibilityResult, KindRevocation, KindCorrection} {
		if k.Producible() {
			t.Fatalf("%s is producible now; this test and Producible disagree", k)
		}
		e := validEvent()
		e.Kind = k
		if _, err := r.Seal(e, 1); !errors.Is(err, ErrNotProducible) {
			t.Errorf("sealed a %s record: %v", k, err)
		}
	}
}

func TestAnUnknownKindIsRefused(t *testing.T) {
	e := validEvent()
	e.Kind = "invented"
	if _, err := testRecorder(t).Seal(e, 1); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("sealed a record of an unknown kind: %v", err)
	}
}

// Every field a record must name is required. The schema enforces the same
// things, and both are here because a row can be written by something other
// than this package.
func TestAnIncompleteEventIsNotSealed(t *testing.T) {
	r := testRecorder(t)
	for name, break_ := range map[string]func(*Event){
		"subject kind": func(e *Event) { e.Subject.Kind = "" },
		"subject id":   func(e *Event) { e.Subject.ID = "" },
		"detail":       func(e *Event) { e.Detail = "" },
		"occurred at":  func(e *Event) { e.OccurredAt = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			e := validEvent()
			break_(&e)
			if _, err := r.Seal(e, 1); !errors.Is(err, ErrIncomplete) {
				t.Fatalf("sealed a record with no %s: %v", name, err)
			}
		})
	}
	t.Run("checkpoint", func(t *testing.T) {
		if _, err := r.Seal(validEvent(), 0); !errors.Is(err, ErrIncomplete) {
			t.Fatalf("sealed a record with no checkpoint: %v", err)
		}
	})
}

// The digest must be injective over the event, or one record's signature
// attests to another's contents.
func TestEveryEventFieldChangesTheDigest(t *testing.T) {
	base := PayloadDigestOf(validEvent())

	for name, mutate := range map[string]func(*Event){
		"Kind":         func(e *Event) { e.Kind = KindSettlement },
		"Subject.Kind": func(e *Event) { e.Subject.Kind = "user" },
		"Subject.ID":   func(e *Event) { e.Subject.ID = "org-2" },
		"IntentDigest": func(e *Event) { e.IntentDigest = "digest-2" },
		"Detail":       func(e *Event) { e.Detail = "domain.charge" },
		"OccurredAt":   func(e *Event) { e.OccurredAt = at.Add(time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			e := validEvent()
			mutate(&e)
			if string(PayloadDigestOf(e)) == string(base) {
				t.Errorf("changing %s left the payload digest unchanged", name)
			}
		})
	}
}

// The encoding must not confuse a value with its neighbour's.
func TestTheDigestCannotBeConfusedAcrossFieldBoundaries(t *testing.T) {
	a := validEvent()
	a.IntentDigest, a.Detail = "ab", "c"

	b := validEvent()
	b.IntentDigest, b.Detail = "a", "bc"

	if string(PayloadDigestOf(a)) == string(PayloadDigestOf(b)) {
		t.Fatal("two different events produced one payload digest")
	}
}

// 🔴 The checkpoint must NOT be inside the payload digest.
//
// It was, in the first version, which made the digest unique per ATTEMPT
// rather than per EVENT and silently disabled the schema's idempotency
// constraint: a retry drew a new checkpoint, produced a different digest, and
// appended a second record for one thing that happened.
//
// The signature still covers the checkpoint, so a record cannot be lifted to
// another position in the log. This test is what stops the two being
// conflated again.
func TestTheCheckpointIsNotPartOfTheEventIdentity(t *testing.T) {
	r := testRecorder(t)

	first, err := r.Seal(validEvent(), 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.Seal(validEvent(), 99)
	if err != nil {
		t.Fatal(err)
	}

	if string(first.PayloadDigest) != string(second.PayloadDigest) {
		t.Fatal("the same event at two checkpoints produced two payload digests, so the " +
			"UNIQUE (kind, intent_digest, payload_digest) constraint cannot deduplicate a retry")
	}
	if first.Signed.Signature == second.Signed.Signature {
		t.Fatal("two checkpoints produced one signature, so a record could be lifted to " +
			"another position in the log")
	}
	if first.Signed.Statement.Checkpoint == second.Signed.Statement.Checkpoint {
		t.Fatal("the statement does not carry its own checkpoint")
	}
}

// A recorder must know which deployment it speaks for.
func TestARecorderWithoutItsDeploymentIdentityIsRefused(t *testing.T) {
	seed := make([]byte, signing.SeedSize)
	for i := range seed {
		seed[i] = 7
	}
	signer, err := signing.Load(func(name string) string {
		if name == signing.EnvBillingEvidenceKey {
			return hexOf(seed)
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}

	full := Options{
		Issuer: "billing-engine", Audience: "customer", Environment: "test",
		Now: func() time.Time { return at },
	}
	for name, blank := range map[string]func(*Options){
		"issuer":      func(o *Options) { o.Issuer = "" },
		"audience":    func(o *Options) { o.Audience = "" },
		"environment": func(o *Options) { o.Environment = "" },
		"clock":       func(o *Options) { o.Now = nil },
	} {
		t.Run(name, func(t *testing.T) {
			o := full
			blank(&o)
			if _, err := NewRecorder(signer, o); !errors.Is(err, ErrIncomplete) {
				t.Fatalf("a recorder with no %s was built: %v", name, err)
			}
		})
	}
}

// AllKinds must be INV-014's eight, and Producible a subset of them.
func TestTheKindListIsTheClosedEight(t *testing.T) {
	if len(AllKinds) != 8 {
		t.Fatalf("AllKinds has %d members; docs/DESIGN.md:388 closes the list at eight", len(AllKinds))
	}
	seen := map[Kind]bool{}
	for _, k := range AllKinds {
		if seen[k] {
			t.Errorf("%s appears twice in AllKinds", k)
		}
		seen[k] = true
	}
}

func hexOf(b []byte) string {
	const d = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, x := range b {
		out = append(out, d[x>>4], d[x&0x0f])
	}
	return string(out)
}
