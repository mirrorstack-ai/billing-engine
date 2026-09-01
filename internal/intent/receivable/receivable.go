// Package receivable seals a collect_receivable for what is still owed on an
// intent the rail already raised.
//
// 🔴 THIS LEG WAS LAST FOR A STRUCTURAL REASON, NOT A PRACTICAL ONE.
//
// docs/DESIGN.md §6 defines collect_receivable as a linked intent for "the
// remaining amount only": a receivable NAMES A SOURCE INTENT and collects what
// is left of it. Both stay live — the original is owed until its remainder
// reaches zero, which is what distinguishes a receivable from a supersession.
//
// So it cannot exist without a source, and until migration 069 a settlement
// recorded no provider reference — nothing could answer "which intent raised
// this invoice?". Every unpaid invoice in production predates the rail and has
// no source; those keep the legacy retry, the same guard every other leg
// applies to a charge already at the provider.
package receivable

import (
	"context"
	"fmt"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
)

// Store is the narrow surface this needs.
type Store interface {
	IntentForProviderReference(ctx context.Context, providerReference string) (string, bool, error)
	LoadIntent(ctx context.Context, digest string) (intent.ChargeIntent, error)
	PayerForAccount(ctx context.Context, accountID string) (intent.Subject, error)
	SaveIntentWithEvidence(ctx context.Context, sealed intent.ChargeIntent, rec *evidence.Recorder, e evidence.Event) error
}

// Service answers both halves of the unpaid-retry seam.
type Service struct {
	store    Store
	recorder *evidence.Recorder
}

// New builds the service. The recorder is required for the same reason the
// proposer requires one: a seal this deployment cannot record is a seal it does
// not perform (DESIGN.md:398).
func New(store Store, rec *evidence.Recorder) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("receivable: no store")
	}
	if rec == nil {
		return nil, fmt.Errorf("receivable: no evidence recorder")
	}
	return &Service{store: store, recorder: rec}, nil
}

// SourceIntentFor answers which intent a provider object settled.
func (s *Service) SourceIntentFor(ctx context.Context, providerReference string) (string, bool, error) {
	return s.store.IntentForProviderReference(ctx, providerReference)
}

// ProposeReceivable seals a receivable for the remainder of a source intent
// and stores it with its evidence.
//
// 🔴 THE REMAINDER IS THE PROVIDER'S FIGURE, NOT OURS. It comes from the
// invoice mirror rather than being derived here, because only the provider
// knows what has actually been collected against that invoice. Re-deriving it
// on our side would be INV-001's inversion running backwards — taking the
// amount from a computation instead of from the obligation that exists.
//
// It is BOUNDED by the source: CollectRemainderOf refuses a receivable larger
// than the intent it collects, because that is not a remainder, it is a new
// charge wearing a link. That check lives in the intent package; this returns
// its error rather than duplicating it.
func (s *Service) ProposeReceivable(
	ctx context.Context,
	sourceDigest, accountID string,
	remainderMicros int64,
) (string, error) {
	if remainderMicros <= 0 {
		// Nothing owed. Sealing a zero receivable would put a document in
		// front of a customer for a charge that will never happen.
		return "", fmt.Errorf("receivable: remainder is %d, nothing to collect", remainderMicros)
	}

	source, err := s.store.LoadIntent(ctx, sourceDigest)
	if err != nil {
		return "", fmt.Errorf("load source intent %s: %w", sourceDigest, err)
	}
	// The payer is RESOLVED, never carried over from the source. A source
	// sealed before a funding designation changed names the old payer, and a
	// receivable is a new collection decision about who owes now.
	payer, err := s.store.PayerForAccount(ctx, accountID)
	if err != nil {
		return "", fmt.Errorf("resolve payer: %w", err)
	}

	notBefore, notAfter := source.ExecutionWindow()
	receivable, err := source.CollectRemainderOf(intent.Draft{
		Payer:    payer,
		Currency: source.Currency(),
		Lines: []intent.Line{intent.NewLine(
			"MirrorStack — amount still owed", "receivable:"+sourceDigest, "1", 1, remainderMicros,
		)},
		Kind: intent.KindCollectReceivable,

		// 🔴 walletFunding = 0. Paying a receivable from the wallet would MOVE
		// the obligation rather than collect it: the balance falls by an amount
		// the customer still owes to the same party.
		WalletAllocationMicros: 0,

		// The SOURCE's revisions. A receivable is not a new commercial
		// decision — it collects what was already agreed, so re-pricing it
		// under a later revision would change the terms of a debt after the
		// fact.
		PriceBookRevision:     source.PriceBookRevision(),
		TermsRevision:         source.TermsRevision(),
		NoticePolicy:          source.NoticePolicy(),
		AuthorizationID:       source.AuthorizationID(),
		SelectedRail:          source.SelectedRail(),
		RoutingPolicyRevision: source.RoutingPolicyRevision(),

		// Zero tax, resolved. The tax on this obligation was determined once,
		// on the source. Determining it again would tax one basis twice.
		Tax: intent.TaxDetermination{
			Resolved:     true,
			Jurisdiction: "not-applicable",
			RuleRevision: source.Tax().RuleRevision,
			Verification: intent.TaxNotApplicable,
		},

		// The source's own window. A receivable that could execute outside the
		// period its source could is collecting a debt under different timing
		// than the charge that created it.
		ExecuteNotBefore: notBefore,
		ExecuteNotAfter:  notAfter,
		SourceFactKeys:   []string{"receivable:" + sourceDigest},
	})
	if err != nil {
		return "", fmt.Errorf("seal receivable for %s: %w", sourceDigest, err)
	}

	if err := s.store.SaveIntentWithEvidence(ctx, receivable, s.recorder, evidence.Event{
		Kind:         evidence.KindSealedIntent,
		Subject:      receivable.Payer(),
		IntentDigest: receivable.Digest(),
		Detail:       string(receivable.Kind()),
		OccurredAt:   notBefore,
	}); err != nil {
		return "", fmt.Errorf("store receivable: %w", err)
	}
	return receivable.Digest(), nil
}
