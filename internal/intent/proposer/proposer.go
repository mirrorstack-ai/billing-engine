// Package proposer turns a charge the engine has derived into a sealed
// intent, and stores it.
//
// It is the seam a legacy charge leg cuts over through. Today a leg
// computes an amount and immediately calls the provider; after cutover
// it computes the same amount, proposes it, and stops. Something else —
// cmd/intent-executor, holding the only write port — decides whether to
// collect it.
//
// The split is the migration. docs/VERIFICATION.md §5: "Provider-write
// interfaces may be injected only into the isolated executor
// deployment. The planner, read, usage-ingress, notifier and reconciler
// binaries must not compile against a write port at all." A leg that
// proposes has no write port, so cmd/billing-cycle stops being able to
// charge anyone at all — which is a stronger statement than any check
// over its call graph.
//
// This package holds no provider client and no executor, and cannot
// collect. Proposing is not charging: a sealed intent that nobody
// executes moves no money, which is what makes a cutover reversible.
package proposer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
)

// Store is the durable half. Narrowed so this package cannot advance a
// state, take a claim, or record an outcome — all of which belong to whoever
// executes.
//
// PayerForAccount is a READ, and it is here rather than on each leg because
// the subject a charge seals must agree with what the executor later
// resolves. Those two used to be written independently, in different
// packages, and disagreed.
//
// The one method writes the intent AND its INV-014 evidence record in a
// single transaction. There is deliberately no method here that writes the
// intent alone: sealing an intent is the first of docs/DESIGN.md:388's eight
// evidence events, and a proposer able to seal without recording is a
// proposer that can produce a charge document the customer has no independent
// trace of.
type Store interface {
	PayerForAccount(ctx context.Context, accountID string) (intent.Subject, error)
	SaveIntentWithEvidence(
		ctx context.Context,
		sealed intent.ChargeIntent,
		rec *evidence.Recorder,
		e evidence.Event,
	) error
}

// Charge is a leg's derived charge, ready to be sealed.
//
// AmountMicros is what the leg computed. There is deliberately no way
// to pass a quantity and a price separately: a prorated domain fee or a
// module-overage block is one derived figure, and pretending it
// decomposes into a catalog lookup would be a fiction the digest would
// then attest to.
type Charge struct {
	// AccountID is the ms_billing.accounts row the charge is against.
	//
	// 🔴 It is an ACCOUNT id, and the proposer resolves it to the account's
	// OWNER before sealing. A leg does not construct an intent.Subject.
	//
	// It used to. All three cut-over legs sealed
	// intent.Subject{Kind: "user", ID: <accounts.id>}, while the executor
	// resolved a payer by owner_user_id — and accounts.id is never an
	// owner_user_id, so every intent this tree could produce was
	// uncollectable. Taking an account id here makes the correct subject the
	// only one a leg can express.
	AccountID    string
	Kind         intent.ChargeKind
	Currency     string
	AmountMicros int64
	// Description names what is being charged for, in the customer's
	// terms. It becomes the intent's single line.
	Description string
	// SourceRef ties the intent back to the row the leg derived it
	// from — a domain id, a timer id, a run id — so a reader can walk
	// from a charge to the thing that caused it.
	SourceRef string

	AuthorizationID   string
	TermsRevision     string
	PriceBookRevision string
	NoticePolicy      string
	Tax               intent.TaxDetermination
	// SelectedRail and RoutingPolicyRevision are sealed into the intent
	// (docs/DESIGN.md:1281-1283), so a rail swap after disclosure breaks
	// the digest rather than going unnoticed.
	SelectedRail          string
	RoutingPolicyRevision string

	ExecuteNotBefore time.Time
	ExecuteNotAfter  time.Time
}

// Proposer seals derived charges and stores them.
type Proposer struct {
	store    Store
	recorder *evidence.Recorder
	now      func() time.Time
}

// ErrNoRecorder refuses a Proposer that cannot record what it seals.
//
// A nil recorder is not "evidence off". docs/DESIGN.md:398 makes an evidence
// record a durable side effect of the money moving, so a deployment that
// cannot produce one must not seal charge documents at all — and the caller
// that armed the cutover is the right place to discover that, at startup,
// rather than a leg discovering it mid-run.
var ErrNoRecorder = errors.New("proposer: refusing to construct a proposer with no evidence recorder")

// New returns a Proposer over the given store.
//
// now is injected for the same reason it is everywhere else in this package's
// neighbours: a record that reads a clock cannot be replayed.
func New(store Store, recorder *evidence.Recorder, now func() time.Time) (*Proposer, error) {
	if recorder == nil {
		return nil, ErrNoRecorder
	}
	if now == nil {
		return nil, errors.New("proposer: no clock")
	}
	return &Proposer{store: store, recorder: recorder, now: now}, nil
}

// ErrNotProposable is returned when a charge cannot be sealed.
var ErrNotProposable = errors.New("proposer: charge cannot be sealed")

// Propose seals a charge into an intent and stores it.
//
// It returns the sealed intent so the caller can record its digest
// against whatever row it derived the charge from. That link is what
// makes the cutover auditable in both directions: from a legacy row to
// the intent that replaced its charge, and from an intent back to the
// row that caused it.
//
// Storing is idempotent on the digest, so a leg that re-derives the
// same charge proposes the same document rather than a second one. That
// is what makes a re-run of a cutover leg safe — the property the
// legacy legs get from deterministic Stripe idempotency keys, obtained
// here from the identity of the content instead.
func (p *Proposer) Propose(ctx context.Context, c Charge) (intent.ChargeIntent, error) {
	if c.AmountMicros <= 0 {
		// A zero charge is not a document worth sealing, and a
		// negative one is not a charge. Legs already skip these; this
		// refuses rather than relying on them to.
		return intent.ChargeIntent{}, fmt.Errorf("%w: amount is %d", ErrNotProposable, c.AmountMicros)
	}

	// The payer is RESOLVED, never taken from the caller. See Charge.AccountID.
	payer, err := p.store.PayerForAccount(ctx, c.AccountID)
	if err != nil {
		return intent.ChargeIntent{}, fmt.Errorf("%w: %w", ErrNotProposable, err)
	}

	sealed, err := intent.Seal(intent.Draft{
		Payer:    payer,
		Currency: c.Currency,
		// One line, quantity one, the whole derived amount as its unit
		// price. The line's amount must equal quantity x price, and
		// this is the only decomposition of a single derived figure
		// that satisfies it without inventing factors.
		Lines: []intent.Line{
			intent.NewLine(c.Description, c.SourceRef, "1", 1, c.AmountMicros),
		},
		Kind:              c.Kind,
		PriceBookRevision: c.PriceBookRevision,
		TermsRevision:     c.TermsRevision,
		Tax:               c.Tax,
		AuthorizationID:   c.AuthorizationID,
		NoticePolicy:      c.NoticePolicy,

		SelectedRail:          c.SelectedRail,
		RoutingPolicyRevision: c.RoutingPolicyRevision,
		ExecuteNotBefore:      c.ExecuteNotBefore,
		ExecuteNotAfter:       c.ExecuteNotAfter,
		SourceFactKeys:        []string{c.SourceRef},
	})
	if err != nil {
		return intent.ChargeIntent{}, fmt.Errorf("%w: %w", ErrNotProposable, err)
	}

	// The intent and its evidence record commit together. docs/DESIGN.md:398
	// makes evidence "a durable side effect of the money moving", so a seal
	// this deployment cannot record is a seal it does not perform.
	if err := p.store.SaveIntentWithEvidence(ctx, sealed, p.recorder, evidence.Event{
		Kind:         evidence.KindSealedIntent,
		Subject:      sealed.Payer(),
		IntentDigest: sealed.Digest(),
		// Closed vocabulary, never customer prose: the charge kind is one of
		// docs/DESIGN.md §6's nine, and it is what a reader needs to know
		// which rule of an authorization this document was sealed under.
		Detail:     string(sealed.Kind()),
		OccurredAt: p.now(),
	}); err != nil {
		return intent.ChargeIntent{}, fmt.Errorf("proposer: store sealed intent: %w", err)
	}
	return sealed, nil
}
