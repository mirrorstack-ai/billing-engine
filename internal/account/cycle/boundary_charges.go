package cycle

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
)

// boundaryComponents is the boundary invoice as the legacy path computes it,
// in micros, before any rounding.
//
// It is the input to the split and nothing else: the fields are exactly the
// four the legacy total sums (charge.go:299) plus the wallet draw, so a reader
// can check the split against the collection it must reproduce without holding
// the whole of RunBillingCycle in their head.
type boundaryComponents struct {
	// ArrearsMicros is the CLOSED period's netted usage.
	ArrearsMicros int64
	// AdvanceBaseMicros, AdvanceOverageMicros and AdvanceDomainsMicros are
	// the NEXT period's recurring fees. Since §12 item 12 folded capacity
	// and domains into the base price they are one charge kind, but they
	// stay separate fields here because the customer is still shown the
	// breakdown — "one collection, two halves shown".
	AdvanceBaseMicros    int64
	AdvanceOverageMicros int64
	AdvanceDomainsMicros int64
	// WalletDrawnMicros is the stored-value credit already allocated to this
	// boundary. The legacy path subtracts it from the total; the split has
	// to place it on the right intents.
	WalletDrawnMicros int64
}

func (b boundaryComponents) advanceMicros() int64 {
	return b.AdvanceBaseMicros + b.AdvanceOverageMicros + b.AdvanceDomainsMicros
}

func (b boundaryComponents) grossMicros() int64 {
	return b.ArrearsMicros + b.advanceMicros()
}

// splitBoundary turns one boundary into the two charges the intent rail seals
// for it: the closed period's usage arrears backward, and the next period's
// subscription forward.
//
// 🔴 TWO INTENTS, ONE COLLECTION, ONE ROUNDING.
//
// The kinds are not a stylistic choice. Migration 054's header says why an
// intent carries exactly one: it "selects which rule of a standing
// authorization applies: a caller that chose it could pick the permission its
// charge happens to fit". Arrears are module_usage and the forward half is
// platform_base, so one intent for the whole boundary would let whichever kind
// it named authorize the other.
//
// It was FOUR kinds until §12 item 12 (module_usage, platform_base,
// module_capacity, custom_domain), and four was the objection: the legacy path
// rounds ONCE on the net (charge.go:595), so four intents rounded four times and
// the totals disagreed. Two dissolves it. The three fee components are exact
// whole-cent multiples, so folding them into one platform_base introduces no
// rounding at all, and the only sub-cent-fractional terms — arrears and the
// wallet draw — now sit in the SAME intent. A group that rounds once over the
// summed remainders therefore takes exactly the cents the legacy path takes.
//
// Nothing is rounded here on purpose. These are micros; the single rounding
// happens at the provider boundary, over the group, where the legacy path's
// also happens.
//
// # The wallet draw is split, not attached
//
// The draw is NOT simply the arrears intent's allocation. The legacy path
// applies it to the total and to the arrears independently, each clamped at
// zero (charge.go:347-356), so a draw LARGER than the arrears spills onto the
// forward half. Attaching all of it to the arrears intent would make that
// intent's funding identity fail — the predicate requires
// wallet + providerRemainder == gross (predicate.go:170) — and would understate
// what the forward intent collects by the spill. So it is allocated arrears
// first, remainder forward, which is the same order the legacy path consumes it.
func splitBoundary(b boundaryComponents, tmpl proposer.Charge) ([]proposer.Charge, error) {
	if b.ArrearsMicros < 0 || b.advanceMicros() < 0 {
		return nil, fmt.Errorf("boundary split: negative component (arrears %d, advance %d)",
			b.ArrearsMicros, b.advanceMicros())
	}
	if b.WalletDrawnMicros < 0 {
		return nil, fmt.Errorf("boundary split: negative wallet draw %d", b.WalletDrawnMicros)
	}
	if b.WalletDrawnMicros > b.grossMicros() {
		// The draw may equal the gross — a boundary fully paid from the
		// wallet — but never exceed it. The legacy path clamps instead,
		// which hides the arithmetic error; a sealed intent must not.
		return nil, fmt.Errorf("boundary split: wallet draw %d exceeds the boundary gross %d",
			b.WalletDrawnMicros, b.grossMicros())
	}

	// Arrears first, remainder forward — the order the legacy path consumes it.
	walletToArrears := b.WalletDrawnMicros
	if walletToArrears > b.ArrearsMicros {
		walletToArrears = b.ArrearsMicros
	}
	walletToAdvance := b.WalletDrawnMicros - walletToArrears

	var charges []proposer.Charge

	// BACKWARD: the closed period's usage.
	if b.ArrearsMicros > 0 {
		c := tmpl
		c.Kind = intent.KindModuleUsage
		c.WalletAllocationMicros = walletToArrears
		c.Lines = proposer.SingleLine(
			"MirrorStack module usage — closed period",
			boundaryArrearsRef(tmpl.AccountID),
			b.ArrearsMicros,
		)
		charges = append(charges, c)
	}

	// FORWARD: the next period's subscription. One kind, three lines — the
	// fold closed the vocabulary, not the disclosure, and §8's answer is one
	// collection with both halves SHOWN.
	if b.advanceMicros() > 0 {
		c := tmpl
		c.Kind = intent.KindPlatformBase
		c.WalletAllocationMicros = walletToAdvance
		c.Lines = advanceLines(b)
		charges = append(charges, c)
	}

	if len(charges) == 0 {
		// A boundary with nothing to charge proposes nothing. Sealing a
		// zero intent would put a document in front of a customer for a
		// charge that was never going to happen.
		return nil, nil
	}
	return charges, nil
}

// advanceLines is the forward half's disclosure: one line per component the
// customer is shown, all under the single folded kind.
//
// A zero component contributes no line rather than a $0 one — an account with
// no custom domain is not shown a domain line.
func advanceLines(b boundaryComponents) []proposer.ChargeLine {
	var lines []proposer.ChargeLine
	if b.AdvanceBaseMicros > 0 {
		lines = append(lines, proposer.ChargeLine{
			Description:  "MirrorStack platform — next period",
			SourceRef:    "advance:base",
			AmountMicros: b.AdvanceBaseMicros,
		})
	}
	if b.AdvanceOverageMicros > 0 {
		lines = append(lines, proposer.ChargeLine{
			Description: fmt.Sprintf("Module capacity above the included %d — next period",
				usage.IncludedModules),
			SourceRef:    "advance:capacity",
			AmountMicros: b.AdvanceOverageMicros,
		})
	}
	if b.AdvanceDomainsMicros > 0 {
		lines = append(lines, proposer.ChargeLine{
			Description:  "Custom domains — next period",
			SourceRef:    "advance:domains",
			AmountMicros: b.AdvanceDomainsMicros,
		})
	}
	return lines
}

// boundaryArrearsRef ties the arrears line back to the account it was derived
// from, the way every other leg's ref ties to its own row.
func boundaryArrearsRef(accountID string) string { return "arrears:" + accountID }

// proposeBoundary seals this boundary as intents instead of collecting it.
//
// 🔴 IT MOVES NO MONEY, AND THAT IS THE POINT — see WithIntentProposer. A leg
// that proposes holds no write port, so once every leg is cut over
// cmd/billing-cycle cannot charge anyone, which is a stronger statement than
// any check over its call graph could make.
//
// The run is marked 'proposed': terminal for this worker, and deliberately
// neither 'invoiced' (no invoice exists, no money moved) nor 'failed'. Both
// would corrupt the measurement the legacy drop depends on.
func (s *Service) proposeBoundary(
	ctx context.Context,
	runID uuid.UUID,
	accountID uuid.UUID,
	summary *ChargeSummary,
	components boundaryComponents,
	periodStart, periodEnd, newPeriodEnd time.Time,
) (*ChargeSummary, error) {
	charges, err := splitBoundary(components, proposer.Charge{
		// The proposer resolves this to the account's FUNDER's owner. A leg
		// that built an intent.Subject here is how the payer and the
		// executor's resolver came to disagree; see proposer.Charge.AccountID.
		AccountID: accountID.String(),
		Currency:  chargeCurrency,

		AuthorizationID:   "boundary:" + accountID.String(),
		TermsRevision:     proposedTermsRevision,
		PriceBookRevision: proposedPriceBookRevision,
		NoticePolicy:      proposedNoticePolicy,
		SelectedRail:      proposedRail,

		RoutingPolicyRevision: proposedRoutingPolicy,
		// 🔴 Zero tax, resolved — the same honest state the other legs record.
		// This leg has never applied tax, so claiming an unresolved
		// determination would quarantine every boundary while claiming a
		// computed one would invent a figure. docs/DESIGN.md §12's tax
		// decisions are what change it.
		Tax: intent.TaxDetermination{
			Resolved:     true,
			Jurisdiction: "not-applicable",
			RuleRevision: proposedTaxRuleRevision,
			Verification: intent.TaxNotApplicable,
		},
		// The window a collection may happen in — NOT the period being billed.
		// Sealing the coverage window as the execution window is what made two
		// earlier legs' intents dead on arrival: the boundary instant is the
		// END of the coverage, so an intent that may only execute inside it
		// could never execute at all.
		ExecuteNotBefore: periodEnd,
		ExecuteNotAfter:  newPeriodEnd,
	})
	if err != nil {
		return nil, billing.Internal("boundary intent split failed", err)
	}
	if len(charges) == 0 {
		// Nothing to charge. The zero-cents short-circuit above already
		// covers this on the legacy path; reaching it here means the split
		// found no positive component, and marking the run invoiced is the
		// same terminal answer with no money either way.
		if err := s.store.MarkBillingRun(ctx, runID, RunStatusInvoiced, "", 0); err != nil {
			return nil, billing.Internal("mark billing run (nothing to propose) failed", err)
		}
		summary.Status = RunStatusInvoiced
		return summary, nil
	}

	sealed, err := s.proposer.ProposeGroup(ctx, charges)
	if err != nil {
		// A failed proposal leaves the run PENDING for the next reclaim. It
		// must not be marked failed: nothing was attempted at a provider, so
		// there is nothing to reconcile and a retry is safe.
		return nil, billing.Internal("boundary intent proposal failed", err)
	}

	if err := s.store.MarkBillingRun(ctx, runID, RunStatusProposed, "", 0); err != nil {
		return nil, billing.Internal("mark billing run proposed failed", err)
	}
	summary.Status = RunStatusProposed
	// 🔴 Nothing was charged, so nothing may report cents as charged.
	//
	// The legacy path sets ChargedCents from the frozen amount before this
	// branch is reached (charge.go:664). Leaving it set would have a proposed
	// run claim it collected money it did not — the same lie the 'proposed'
	// status exists to avoid, arriving by a different field. The amounts are
	// still on the summary as ArrearsMicros / Advance*Micros, which is what a
	// caller comparing the two rails needs.
	summary.ChargedCents = 0
	summary.ProposedDigests = digestsOf(sealed)
	return summary, nil
}

// digestsOf is the link from this run to the intents that replaced its charge.
// It is what makes the cutover auditable in both directions.
func digestsOf(sealed []intent.ChargeIntent) []string {
	out := make([]string, 0, len(sealed))
	for _, in := range sealed {
		out = append(out, in.Digest())
	}
	return out
}
