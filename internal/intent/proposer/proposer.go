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
// ChargeLine is one line of a charge, in the customer's terms.
//
// 🔴 A Charge carries LINES, not a single amount, and that is what lets a leg
// propose ONE intent where the legacy path issues ONE invoice.
//
// internal/provider/stripeadapter collects per intent: draft, item, finalize,
// pay, keyed on that intent's digest, rounding micros to cents once for that
// intent alone. So a leg that split its charge into several intents would
// issue several invoices and take several card payments where the legacy leg
// took one — a different customer statement, and several roundings instead of
// the single round-on-the-net the boundary collector performs
// (cycle/charge.go:595). One intent with several lines reproduces both.
type ChargeLine struct {
	// Description is what the customer reads.
	Description string
	// SourceRef ties this line back to the row it was derived from — a
	// domain id, a timer id, a run id — so a reader can walk from a charge
	// to the thing that caused it. It becomes a source-fact key.
	SourceRef string
	// AmountMicros is what this line contributes. The proposer refuses a
	// non-positive line for the same reason it refuses a non-positive
	// charge: a zero line is not a line and a negative one is not a charge.
	AmountMicros int64
}

// Charge is a leg's derived charge, ready to be sealed.
//
// There is deliberately no way to pass a quantity and a price separately: a
// prorated domain fee or a module-overage block is one derived figure, and
// pretending it decomposes into a catalog lookup would be a fiction the
// digest would then attest to.
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
	AccountID string

	Kind     intent.ChargeKind
	Currency string

	// Lines are what the charge is made of, in order. At least one.
	Lines []ChargeLine

	// WalletAllocationMicros is the stored-value credit applied to this
	// charge. The provider is handed the REMAINDER, which Seal derives —
	// so a leg that draws on a wallet must state the draw here rather than
	// subtracting it from its lines, or the sealed document will say the
	// customer was charged for less than they owed.
	WalletAllocationMicros int64

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

// SingleLine is the convenience for a leg whose charge is one derived figure.
func SingleLine(description, sourceRef string, amountMicros int64) []ChargeLine {
	return []ChargeLine{{Description: description, SourceRef: sourceRef, AmountMicros: amountMicros}}
}

// TotalMicros is the gross the lines add up to.
func (c Charge) TotalMicros() int64 {
	var total int64
	for _, l := range c.Lines {
		total += l.AmountMicros
	}
	return total
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
	if len(c.Lines) == 0 {
		return intent.ChargeIntent{}, fmt.Errorf("%w: no lines", ErrNotProposable)
	}
	for i, l := range c.Lines {
		if l.AmountMicros <= 0 {
			// A zero line is not a line and a negative one is not a charge.
			// Legs already skip these; this refuses rather than relying on
			// them to.
			return intent.ChargeIntent{}, fmt.Errorf("%w: line %d is %d", ErrNotProposable, i, l.AmountMicros)
		}
	}
	if c.TotalMicros() <= 0 {
		return intent.ChargeIntent{}, fmt.Errorf("%w: total is %d", ErrNotProposable, c.TotalMicros())
	}

	// The payer is RESOLVED, never taken from the caller. See Charge.AccountID.
	payer, err := p.store.PayerForAccount(ctx, c.AccountID)
	if err != nil {
		return intent.ChargeIntent{}, fmt.Errorf("%w: %w", ErrNotProposable, err)
	}

	lines := make([]intent.Line, 0, len(c.Lines))
	facts := make([]string, 0, len(c.Lines))
	for _, l := range c.Lines {
		// Quantity one, the whole derived amount as the unit price. The
		// line's amount must equal quantity x price, and this is the only
		// decomposition of a single derived figure that satisfies it without
		// inventing factors.
		lines = append(lines, intent.NewLine(l.Description, l.SourceRef, "1", 1, l.AmountMicros))
		facts = append(facts, l.SourceRef)
	}

	sealed, err := intent.Seal(intent.Draft{
		Payer:             payer,
		Currency:          c.Currency,
		Lines:             lines,
		Kind:              c.Kind,
		PriceBookRevision: c.PriceBookRevision,
		TermsRevision:     c.TermsRevision,
		Tax:               c.Tax,
		AuthorizationID:   c.AuthorizationID,
		NoticePolicy:      c.NoticePolicy,

		WalletAllocationMicros: c.WalletAllocationMicros,

		SelectedRail:          c.SelectedRail,
		RoutingPolicyRevision: c.RoutingPolicyRevision,
		ExecuteNotBefore:      c.ExecuteNotBefore,
		ExecuteNotAfter:       c.ExecuteNotAfter,
		SourceFactKeys:        facts,
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
