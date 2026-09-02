package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/stripe/stripetest"
)

// capturingProposer stands in for the real proposer so the cutover can
// be observed without a database.
type capturingProposer struct {
	charges []proposer.Charge
	// groups records each ProposeGroup call as one slice, so a test can tell
	// "two charges in ONE group" from "two separate proposals" — which is the
	// distinction the boundary's single rounding depends on.
	groups [][]proposer.Charge
	err    error
}

// ProposeGroup seals a set that must settle together. It records the SET, not
// just the charges, because a boundary that proposed its two halves separately
// would look identical in p.charges and be collected as two invoices.
func (p *capturingProposer) ProposeGroup(ctx context.Context, charges []proposer.Charge) ([]intent.ChargeIntent, error) {
	if p.err != nil {
		return nil, p.err
	}
	var sealed []intent.ChargeIntent
	for _, c := range charges {
		in, err := p.Propose(ctx, c)
		if err != nil {
			return nil, err
		}
		sealed = append(sealed, in)
	}
	p.groups = append(p.groups, charges)
	return sealed, nil
}

func (p *capturingProposer) Propose(_ context.Context, c proposer.Charge) (intent.ChargeIntent, error) {
	if p.err != nil {
		return intent.ChargeIntent{}, p.err
	}
	p.charges = append(p.charges, c)
	return intent.Seal(intent.Draft{
		Payer:                 intent.Subject{Kind: "user", ID: "owner-of-" + c.AccountID},
		Currency:              c.Currency,
		Lines:                 chargeLines(c),
		Kind:                  c.Kind,
		PriceBookRevision:     c.PriceBookRevision,
		TermsRevision:         c.TermsRevision,
		Tax:                   c.Tax,
		AuthorizationID:       c.AuthorizationID,
		NoticePolicy:          c.NoticePolicy,
		ExecuteNotBefore:      c.ExecuteNotBefore,
		ExecuteNotAfter:       c.ExecuteNotAfter,
		SourceFactKeys:        chargeFacts(c),
		SelectedRail:          "stripe",
		RoutingPolicyRevision: "routing-2026-08",
	})
}

// seedDomain registers a chargeable domain in the fake store and
// returns the candidate the sweep would hand to ChargeDomain.
//
// The account is anchored months back and the domain activated an hour
// ago, so the containing period is open and the charge prorates to a
// real amount — a closed period or a zero charge would exercise the
// early returns rather than the cutover.
func seedDomain(t *testing.T, f *fakeStore) cycle.DomainChargeCandidate {
	t.Helper()

	id, accountID, appID := uuid.New(), uuid.New(), uuid.New()
	activated := time.Now().UTC().Add(-time.Hour)
	accountActivated := time.Now().UTC().AddDate(0, -3, 0)

	f.domains[id] = &fakeDomain{domain: cycle.Domain{
		ID: id, AccountID: accountID, AppID: appID,
		Hostname: "example.com", ActivatedAt: activated,
	}}
	f.hasPM = true
	f.stripeCustomer = "cus_cutover"

	return cycle.DomainChargeCandidate{
		ID: id, AccountID: accountID, AppID: appID,
		Hostname: "example.com", ActivatedAt: activated,
		AccountActivatedAt: accountActivated,
	}
}

// 🔴 The cutover, demonstrated: with a proposer installed the leg
// derives the same charge and reaches NO provider.
//
// This is what docs/VERIFICATION.md §5 asks for — "the planner, read,
// usage-ingress, notifier and reconciler binaries must not compile
// against a write port at all". A leg that proposes needs no write
// port, so once every leg is cut over, cmd/billing-cycle stops being
// able to charge anyone.
func TestDomainLegProposesInsteadOfCharging(t *testing.T) {
	recorder := stripetest.New()
	store := newFakeStore()
	p := &capturingProposer{}
	cand := seedDomain(t, store)

	svc := cycle.NewService(store, recorder).WithIntentProposer(p)
	res, err := svc.ChargeDomain(context.Background(), cand, time.Now().UTC())
	require.NoError(t, err)

	require.Equal(t, cycle.DomainChargeProposed, res.Status,
		"the leg did not take the cutover branch")
	require.NotEmpty(t, res.IntentDigest,
		"a proposed charge carries no digest, so the domain row cannot be walked to its intent")

	// The assertion the migration exists for.
	recorder.RequireNoProviderMutation(t, "a cut-over domain leg")

	require.Len(t, p.charges, 1)
	require.Equal(t, intent.KindPlatformBase, p.charges[0].Kind)
	require.Positive(t, p.charges[0].TotalMicros(),
		"the leg proposed a zero charge; the proration was lost in the cutover")
	require.Contains(t, p.charges[0].Lines[0].Description, cand.Hostname)
}

// The legacy collect path is deleted, so a service built without a proposer
// has no way to charge for a domain. It must say so rather than nil-panic at
// the seal or silently leave the row unresolved forever.
//
// This replaces TestWithoutAProposerTheLegStillCharges, which asserted the
// legacy draft/item/finalize sequence still collected. That sequence no longer
// exists, so the old test was pinning a path rather than a behaviour.
func TestWithoutAProposerTheLegRefusesRatherThanCharging(t *testing.T) {
	recorder := stripetest.New()
	store := newFakeStore()
	cand := seedDomain(t, store)

	_, err := cycle.NewService(store, recorder).ChargeDomain(context.Background(), cand, time.Now().UTC())
	require.Error(t, err,
		"a service with no proposer accepted a domain charge; the leg has no collector left to run")

	recorder.RequireNoProviderMutation(t, "a domain leg with no proposer installed")
}

// TestTheCutoverDoesNotChangeTheAmount was deleted with the legacy collect
// path: its reference figure was the amount the legacy leg handed Stripe, and
// there is no longer a legacy leg to run for comparison.
//
// What it protected survives elsewhere and is stronger:
// recovery_exception_test.go's TestASealedDomainChargeIsAlwaysWholeCents ties
// the sealed micros to the leg's own derived ChargedCents on a deliberately
// FRACTIONAL fixture (122.5806 cents), which the round-number fixture here
// could never have caught. The derivation itself — domainChargeShape,
// centsFromMicros, collectableMicros — is untouched by the cutover.

// chargeLines and chargeFacts mirror what the real proposer builds, so a
// capturing fake cannot drift from the seam it stands in for.
func chargeLines(c proposer.Charge) []intent.Line {
	out := make([]intent.Line, 0, len(c.Lines))
	for _, l := range c.Lines {
		out = append(out, intent.NewLine(l.Description, l.SourceRef, "1", 1, l.AmountMicros))
	}
	return out
}

func chargeFacts(c proposer.Charge) []string {
	out := make([]string, 0, len(c.Lines))
	for _, l := range c.Lines {
		out = append(out, l.SourceRef)
	}
	return out
}
