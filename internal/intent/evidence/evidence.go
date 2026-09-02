// Package evidence mints the records INV-014 requires, and refuses to mint one
// it cannot sign.
//
// docs/DESIGN.md:386-399: "A signed, customer-encrypted evidence record must
// commit through a billing-owned transactional outbox. ... The outbox is worth
// building first anyway: it makes your evidence a durable side effect of the
// money moving, rather than a report the relay chooses to render."
//
// The last clause is the design. A record is written INSIDE the transaction
// that changes the billing state it attests, so there is no window in which
// money moved and evidence did not, and no separate reporter that can decline
// to run.
//
// # What this package does NOT do
//
// INV-014 asks for records that are signed AND customer-encrypted. These are
// signed and NOT encrypted: encryption needs a customer key, and
// CustomerReadProof binds "an independently enrolled customer factor that does
// not exist today". §12 decision 16 was answered as option C — build the
// independence that needs no enrolled factor — so this is the reachable half,
// and internal/account/capabilities reports the other half as unsupported
// rather than letting the table imply it.
//
// It also serves nothing. The independent evidence edge of docs/DESIGN.md:392
// is a separate unbuilt component, and it "may serve those records and must
// not create or mutate one".
package evidence

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/signing"
)

// Kind is docs/DESIGN.md:388-390's closed list, verbatim and complete: "a
// sealed intent, a proof result, a notice or eligibility result, a refusal, a
// nonterminal attempt state, a settlement, a revocation and a correction."
type Kind string

const (
	KindSealedIntent            Kind = "sealed_intent"
	KindProofResult             Kind = "proof_result"
	KindNoticeEligibilityResult Kind = "notice_eligibility_result"
	KindRefusal                 Kind = "refusal"
	KindNonterminalAttemptState Kind = "nonterminal_attempt_state"
	KindSettlement              Kind = "settlement"
	KindRevocation              Kind = "revocation"
	KindCorrection              Kind = "correction"
)

// AllKinds is the closed list, in the order docs/DESIGN.md:388 states it.
var AllKinds = []Kind{
	KindSealedIntent,
	KindProofResult,
	KindNoticeEligibilityResult,
	KindRefusal,
	KindNonterminalAttemptState,
	KindSettlement,
	KindRevocation,
	KindCorrection,
}

// Producible reports whether anything in this tree can currently emit a kind.
//
// 🔴 An empty evidence table is consistent with both "nothing has happened"
// and "nothing can happen", and only this distinguishes them. It is what the
// Capabilities surface publishes and what the four unwired members of the
// schema's CHECK are measured against.
//
// It is a claim in source, so internal/architecture SCANS the tree for the
// callers that mint each kind and fails when the two disagree. A kind that
// gains a writer and not an entry here, or an entry here and no writer, is
// the "declared but not implemented" failure this repository keeps finding.
func (k Kind) Producible() bool {
	switch k {
	case KindSealedIntent, KindRefusal, KindNonterminalAttemptState, KindSettlement:
		return true
	}
	// proof_result waits on CustomerProofStream (INV-013, unbuilt).
	// notice_eligibility_result waits on a notifier that delivers anything.
	// revocation and correction have no path at all.
	return false
}

func (k Kind) valid() bool {
	for _, known := range AllKinds {
		if k == known {
			return true
		}
	}
	return false
}

var (
	ErrUnknownKind   = errors.New("evidence: not one of INV-014's eight event kinds")
	ErrIncomplete    = errors.New("evidence: the event omits something a record must name")
	ErrNotProducible = errors.New("evidence: nothing in this tree can produce that kind yet")
)

// Event is what a caller states happened.
//
// Everything here is enum-shaped or an identifier already present elsewhere in
// ms_billing. There is deliberately no free-text field and no payload blob: a
// record must be reconstructable from its own columns so its digest is
// checkable, and nothing customer-written should be duplicated into a table
// whose read path cannot yet be gated.
type Event struct {
	Kind Kind
	// Subject is who the record is about — the payer, for every kind that
	// concerns a charge.
	Subject intent.Subject
	// IntentDigest is the intent this concerns; empty for kinds that concern
	// something else.
	IntentDigest string
	// Detail is the event's own closed-vocabulary content.
	Detail string
	// OccurredAt is when the thing happened, supplied rather than read, so a
	// record is a function of its inputs.
	OccurredAt time.Time
}

// Record is a signed evidence record, ready to commit.
type Record struct {
	Event
	Checkpoint    int64
	PayloadDigest []byte
	Signed        signing.Signed
}

// Recorder seals events into records.
//
// It holds a signing key. A deployment with no key cannot construct one, so
// there is no state in which evidence is silently skipped — the caller gets an
// error at construction, not a quiet no-op at write time.
type Recorder struct {
	key         signing.Key
	issuer      string
	audience    string
	environment string
	now         func() time.Time
	validity    time.Duration
}

// Options are the fields every signed statement must carry that are properties
// of the DEPLOYMENT rather than of the event.
type Options struct {
	Issuer      string
	Audience    string
	Environment string
	Now         func() time.Time
	// Validity is how long a record's statement is checkable for.
	Validity time.Duration
}

// DefaultValidity is ten years.
//
// docs/VERIFICATION.md §4 has a customer rechecking a charge offline, and a
// record whose signature stops verifying before the retention period ends is
// evidence with a fuse in it. Rotation is handled by pinning both the old and
// the new public key, not by expiring records.
const DefaultValidity = 10 * 365 * 24 * time.Hour

// Schema names the shape of what PayloadDigest covers.
const Schema = "billing-evidence-record/v1"

// NewRecorder builds a recorder from whatever key material a deployment has.
//
// It returns signing.ErrNoKey when the deployment holds no evidence key. That
// is the fail-closed direction and it is deliberately not recoverable here: a
// caller that wants to run without evidence has to say so at its own level,
// where the decision is visible, rather than have this package decide for it.
func NewRecorder(s signing.Signer, opts Options) (*Recorder, error) {
	key, err := s.SignerFor(signing.DomainBillingEvidence)
	if err != nil {
		return nil, err
	}
	for _, f := range []struct{ name, value string }{
		{"issuer", opts.Issuer},
		{"audience", opts.Audience},
		{"environment", opts.Environment},
	} {
		if strings.TrimSpace(f.value) == "" {
			return nil, fmt.Errorf("%w: %s", ErrIncomplete, f.name)
		}
	}
	if opts.Now == nil {
		return nil, fmt.Errorf("%w: clock", ErrIncomplete)
	}
	validity := opts.Validity
	if validity <= 0 {
		validity = DefaultValidity
	}
	return &Recorder{
		key:         key,
		issuer:      opts.Issuer,
		audience:    opts.Audience,
		environment: opts.Environment,
		now:         opts.Now,
		validity:    validity,
	}, nil
}

// Seal turns an event and a checkpoint into a signed record.
//
// The checkpoint is supplied because only the transaction about to write the
// row can allocate it, and the signature has to cover it
// (docs/VERIFICATION.md's receipt table asks a receipt to carry "the outbox
// checkpoint"). Seal is therefore called from inside that transaction, which
// is also what makes the record a side effect of the state change rather than
// a report about it.
func (r *Recorder) Seal(e Event, checkpoint int64) (Record, error) {
	if r == nil {
		return Record{}, signing.ErrNoKey
	}
	if !e.Kind.valid() {
		return Record{}, fmt.Errorf("%w: %q", ErrUnknownKind, e.Kind)
	}
	if !e.Kind.Producible() {
		// A record of a kind nothing can produce would be a row asserting
		// that a subsystem ran. Refusing here keeps the table an account of
		// what happened rather than of what was declared.
		return Record{}, fmt.Errorf("%w: %q", ErrNotProducible, e.Kind)
	}
	if strings.TrimSpace(e.Subject.Kind) == "" || strings.TrimSpace(e.Subject.ID) == "" {
		return Record{}, fmt.Errorf("%w: subject", ErrIncomplete)
	}
	if strings.TrimSpace(e.Detail) == "" {
		return Record{}, fmt.Errorf("%w: detail", ErrIncomplete)
	}
	if e.OccurredAt.IsZero() {
		return Record{}, fmt.Errorf("%w: occurred at", ErrIncomplete)
	}
	if checkpoint <= 0 {
		return Record{}, fmt.Errorf("%w: checkpoint", ErrIncomplete)
	}

	digest := e.payloadDigest()
	at := r.now()
	signed, err := r.key.Sign(signing.Statement{
		Issuer:        r.issuer,
		Audience:      r.audience,
		Environment:   r.environment,
		Schema:        Schema,
		PayloadDigest: fmt.Sprintf("%x", digest),
		Checkpoint:    strconv.FormatInt(checkpoint, 10),
		NotBefore:     at,
		NotAfter:      at.Add(r.validity),
	})
	if err != nil {
		return Record{}, fmt.Errorf("sign evidence record: %w", err)
	}
	return Record{Event: e, Checkpoint: checkpoint, PayloadDigest: digest[:], Signed: signed}, nil
}

// payloadDigest is the identity of what the record attests.
//
// Length-prefixed, for the reason the intent digest is: two different events
// must never produce one digest, and Detail is caller-supplied.
//
// 🔴 The checkpoint is deliberately NOT in here, and the first version had it.
//
// Putting it in made the digest unique per ATTEMPT rather than per EVENT, so
// the schema's UNIQUE (kind, intent_digest, payload_digest) — the whole
// idempotency control — could never fire: a retry drew a new checkpoint,
// produced a different digest, and appended a second record for one thing that
// happened. Found by the retry test, which appended three records where it
// asserted one.
//
// Nothing is lost by removing it. The checkpoint is envelope metadata, and the
// SIGNATURE already covers it (signing.Statement.Checkpoint), so a record
// still cannot be lifted to another position in the log — that guarantee lives
// where it belongs, in the statement, rather than being conflated with the
// identity of the event.
//
// It is a function of the event alone, so anyone holding the row can recompute
// it — which is what makes the row's own columns sufficient evidence and a
// stored payload unnecessary.
func (e Event) payloadDigest() [32]byte {
	var b []byte
	add := func(v string) {
		b = strconv.AppendInt(b, int64(len(v)), 10)
		b = append(b, ':')
		b = append(b, v...)
	}
	add(Schema)
	add(string(e.Kind))
	add(e.Subject.Kind)
	add(e.Subject.ID)
	add(e.IntentDigest)
	add(e.Detail)
	add(e.OccurredAt.UTC().Format(time.RFC3339Nano))
	return sha256.Sum256(b)
}

// PayloadDigestOf recomputes a record's digest from its own fields, for a
// verifier holding the row.
func PayloadDigestOf(e Event) []byte {
	d := e.payloadDigest()
	return d[:]
}
