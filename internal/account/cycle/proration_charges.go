package cycle

import (
	"context"
	"fmt"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
)

// prorationCharge is the combined-proration attempt as one sealed charge.
//
// 🔴 ONE INTENT, AND ONLY BECAUSE OF THE FOLD.
//
// This leg bills two things: an app's prorated base fee for the period it was
// created in, and the prorated module overage for each of its timers. Before
// §12 item 12 those were platform_base and module_capacity — two kinds, so two
// intents, so a group and the rounding argument that comes with one.
//
// The fold made them one kind. A single intent carries both, the authorization
// question does not arise (there is only one rule to select), and the leg needs
// no grouping at all. That is the fold paying for itself in the leg it was not
// aimed at.
//
// 🔴 THE LINES ARE SEALED AT THEIR ALREADY-ROUNDED VALUES, WHICH IS NOT THE
// SAME AS SEALING THE DERIVED MICROS.
//
// The legacy path rounds PER COMPONENT and then sums:
// combinedProrationTotalCents is BaseChargeCents + ModuleChargeCents ×
// timerCount (proration.go:763-770). Sealing the raw micros and rounding once
// at the provider would produce a different number whenever the two components'
// fractional parts sum past a cent — a few cents, on a real charge, silently.
//
// "A cutover must seal EXACTLY what a collection takes" is the rule, so each
// line is sealed at cents×microsPerCent: the intent's total is by construction
// the same integer the legacy invoice would have carried, and the group
// adapter's single rounding over it is a no-op rather than a second opinion.
func prorationCharge(attempt CombinedProrationAttempt, tmpl proposer.Charge) (proposer.Charge, error) {
	shape := attempt.Shape

	c := tmpl
	c.Kind = intent.KindPlatformBase
	c.Currency = shape.Currency

	if shape.BaseChargeCents > 0 {
		c.Lines = append(c.Lines, proposer.ChargeLine{
			Description:  shape.BaseDescription,
			SourceRef:    combinedProrationBaseItemKey,
			AmountMicros: shape.BaseChargeCents * microsPerCent,
		})
	}
	// One line per timer, each at the SAME per-timer figure the legacy
	// invoice items carry. Summing them into a single line would lose the
	// per-timer source ref, and that ref is how a charge is walked back to
	// the timer that caused it.
	for _, timerID := range attempt.TimerIDs {
		if shape.ModuleChargeCents <= 0 {
			break
		}
		c.Lines = append(c.Lines, proposer.ChargeLine{
			Description:  shape.ModuleDescription,
			SourceRef:    combinedProrationTimerItemKey(timerID),
			AmountMicros: shape.ModuleChargeCents * microsPerCent,
		})
	}

	if len(c.Lines) == 0 {
		return proposer.Charge{}, nil
	}

	// Overflow is the only way the legacy total can fail to be computed, and
	// an attempt whose total does not fit must not seal.
	if _, err := combinedProrationTotalCents(attempt); err != nil {
		return proposer.Charge{}, fmt.Errorf("proration charge: %w", err)
	}

	// 🔴 THE SHAPE CARRIES EACH FIGURE TWICE, AND THE TWO MUST AGREE.
	//
	// CombinedProrationChargeShape stores BaseChargeMicros AND BaseChargeCents
	// (and the same pair for the module figure) as separate persisted columns.
	// The lines above are sealed from the CENTS, because that is what the
	// legacy invoice collects. If the stored pair disagrees — a derivation
	// changed on one side, a backfill wrote one and not the other — then the
	// figure the customer was quoted from the micros is not the figure this
	// intent attests to, and the intent is the document they are shown.
	//
	// This is deliberately NOT a comparison of the lines against
	// combinedProrationTotalCents: both read the same cents fields, so that
	// check cannot fail and removing it changes nothing. Mutation testing
	// confirmed exactly that, which is why it is not the check that shipped.
	// This one compares two INDEPENDENTLY STORED values and can.
	if err := requireCentsMatchMicros("base", shape.BaseChargeCents, shape.BaseChargeMicros); err != nil {
		return proposer.Charge{}, err
	}
	if err := requireCentsMatchMicros("module", shape.ModuleChargeCents, shape.ModuleChargeMicros); err != nil {
		return proposer.Charge{}, err
	}
	return c, nil
}

// requireCentsMatchMicros refuses a stored pair whose two representations of
// one figure disagree.
//
// Rounding is the legacy path's own: centsFromMicros. A shape written by that
// path always satisfies this; one that does not was not written by it.
func requireCentsMatchMicros(which string, cents, micros int64) error {
	want, err := centsFromMicros(micros)
	if err != nil {
		return fmt.Errorf("proration charge: %s micros %d: %w", which, micros, err)
	}
	if cents != want {
		return fmt.Errorf(
			"proration charge: the stored %s figure disagrees with itself — %d cents beside %d micros "+
				"(%d cents). A cutover must seal exactly what a collection takes, and these two "+
				"cannot both be it",
			which, cents, micros, want)
	}
	return nil
}

// proposeCombinedProration seals this attempt instead of collecting it.
//
// It moves no money. Like every other cut-over leg it proposes AFTER the
// durable arming claim, never instead of it: the claim is what stops a second
// worker charging beside this one, and a leg that skipped it to propose would
// have removed a guard rather than replaced a charge.
//
// 🔴 IT IS NO LONGER OPTIONAL. The legacy collector this used to stand beside
// is deleted, so an unset proposer is not "the old behaviour" — it is a
// service that cannot bill this charge kind at all. Say so, loudly, at the
// point of use: the alternative is a nil dereference inside a money path, and
// the one after that is a charge that silently never happens.
func (s *Service) proposeCombinedProration(
	ctx context.Context,
	attempt CombinedProrationAttempt,
) (intent.ChargeIntent, error) {
	if s.proposer == nil {
		return intent.ChargeIntent{}, billing.Internal(
			"combined creation-proration has no intent proposer installed, and its direct "+
				"charge path no longer exists; this deployment cannot bill an app's creation period",
			nil,
		)
	}
	charge, err := prorationCharge(attempt, proposer.Charge{
		// Resolved to the account's funder's owner by the proposer, never
		// built here — see proposer.Charge.AccountID.
		AccountID: attempt.Shape.AccountID.String(),

		AuthorizationID:   "proration:" + attempt.Shape.AccountID.String(),
		TermsRevision:     proposedTermsRevision,
		PriceBookRevision: proposedPriceBookRevision,
		NoticePolicy:      proposedNoticePolicy,
		SelectedRail:      proposedRail,

		RoutingPolicyRevision: proposedRoutingPolicy,
		Tax: intent.TaxDetermination{
			Resolved:     true,
			Jurisdiction: "not-applicable",
			RuleRevision: proposedTaxRuleRevision,
			Verification: intent.TaxNotApplicable,
		},
		// The window a collection may happen in, NOT the period being billed.
		// Sealing the coverage window as the execution window is what made two
		// earlier legs' intents dead on arrival.
		ExecuteNotBefore: attempt.Shape.CoverageEnd,
		ExecuteNotAfter:  attempt.Shape.CoverageEnd.AddDate(0, 1, 0),
	})
	if err != nil {
		return intent.ChargeIntent{}, billing.Internal("combined proration intent derivation failed", err)
	}
	if len(charge.Lines) == 0 {
		return intent.ChargeIntent{}, nil
	}

	sealed, err := s.proposer.Propose(ctx, charge)
	if err != nil {
		return intent.ChargeIntent{}, billing.Internal("combined proration intent proposal failed", err)
	}
	return sealed, nil
}
