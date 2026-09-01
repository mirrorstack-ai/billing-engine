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
	err     error
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

// With no proposer the leg behaves exactly as before, which is what
// makes the cutover reversible and lets it be enabled per deployment.
func TestWithoutAProposerTheLegStillCharges(t *testing.T) {
	recorder := stripetest.New()
	store := newFakeStore()
	cand := seedDomain(t, store)

	res, err := cycle.NewService(store, recorder).ChargeDomain(context.Background(), cand, time.Now().UTC())
	require.NoError(t, err)

	require.NotEqual(t, cycle.DomainChargeProposed, res.Status,
		"the legacy path took the cutover branch with no proposer installed")
	require.NotEmpty(t, recorder.CallsWithEffect(stripetest.EffectCollect),
		"the legacy path collected nothing; this test would not notice a broken cutover")
}

// The derived amount must be identical either way. A cutover that
// changed the figure would be a repricing wearing a migration's name,
// and shadow reconciliation would never catch it — it compares the new
// rater against history, not the legacy leg against itself.
func TestTheCutoverDoesNotChangeTheAmount(t *testing.T) {
	now := time.Now().UTC()

	legacyStore := newFakeStore()
	legacyCand := seedDomain(t, legacyStore)
	legacyRes, err := cycle.NewService(legacyStore, stripetest.New()).
		ChargeDomain(context.Background(), legacyCand, now)
	require.NoError(t, err)
	require.Positive(t, legacyRes.ChargedCents,
		"the legacy fixture charged nothing, so this comparison would pass vacuously")

	p := &capturingProposer{}
	proposedStore := newFakeStore()
	proposedCand := seedDomain(t, proposedStore)
	// Same activation instants, so the two runs price the same shape.
	proposedCand.ActivatedAt = legacyCand.ActivatedAt
	proposedCand.AccountActivatedAt = legacyCand.AccountActivatedAt

	_, err = cycle.NewService(proposedStore, stripetest.New()).
		WithIntentProposer(p).
		ChargeDomain(context.Background(), proposedCand, now)
	require.NoError(t, err)
	require.Len(t, p.charges, 1)

	// The legacy path rounds to whole cents at the Stripe boundary; the
	// proposed intent keeps micro-dollars. Comparing at cents compares
	// the same decision.
	proposedCents := (p.charges[0].TotalMicros() + 5_000) / 10_000
	require.Equal(t, legacyRes.ChargedCents, proposedCents,
		"the cutover changed what the customer is charged")
}

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
