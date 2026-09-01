package cycle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
)

func boundaryTemplate() proposer.Charge {
	return proposer.Charge{AccountID: "acct-1", Currency: "USD"}
}

// The property the whole cutover rests on: what the two intents collect
// together is what the legacy path collects.
//
// "A cutover must seal EXACTLY what a collection takes" is a rule this
// repository has already been bitten by, so this is a table over the shapes a
// real boundary takes rather than one happy case.
func TestTheTwoIntentsCollectTheLegacyNet(t *testing.T) {
	for _, tc := range []struct {
		name string
		b    boundaryComponents
	}{
		{"usage only", boundaryComponents{ArrearsMicros: 1_234_567}},
		{"subscription only", boundaryComponents{AdvanceBaseMicros: 20_000_000}},
		{"both halves", boundaryComponents{
			ArrearsMicros: 3_333_333, AdvanceBaseMicros: 20_000_000,
			AdvanceOverageMicros: 5_000_000, AdvanceDomainsMicros: 2_000_000,
		}},
		{"wallet inside the arrears", boundaryComponents{
			ArrearsMicros: 8_000_000, AdvanceBaseMicros: 20_000_000,
			WalletDrawnMicros: 3_000_000,
		}},
		{"wallet EXCEEDS the arrears and spills forward", boundaryComponents{
			ArrearsMicros: 2_000_000, AdvanceBaseMicros: 20_000_000,
			WalletDrawnMicros: 9_000_000,
		}},
		{"wallet covers the whole boundary", boundaryComponents{
			ArrearsMicros: 1_000_000, AdvanceBaseMicros: 20_000_000,
			WalletDrawnMicros: 21_000_000,
		}},
		{"sub-cent arrears", boundaryComponents{
			ArrearsMicros: 4_999, AdvanceBaseMicros: 20_000_000,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			charges, err := splitBoundary(tc.b, boundaryTemplate())
			require.NoError(t, err)
			require.NotEmpty(t, charges, "a boundary with money in it proposed nothing")

			// What the legacy path sends to the provider.
			legacyNet := tc.b.grossMicros() - tc.b.WalletDrawnMicros

			var gross, wallet int64
			for _, c := range charges {
				gross += c.TotalMicros()
				wallet += c.WalletAllocationMicros
			}
			require.Equal(t, tc.b.grossMicros(), gross,
				"the intents' gross is not the boundary's gross — a line was dropped or double-counted")
			require.Equal(t, tc.b.WalletDrawnMicros, wallet,
				"the wallet draw was not fully allocated; the customer's credit is being spent twice or not at all")
			require.Equal(t, legacyNet, gross-wallet,
				"the provider remainder is not what the legacy path collects")
		})
	}
}

// Each intent must satisfy the funding identity the predicate enforces:
// wallet + providerRemainder == gross (predicate.go:170). An intent that fails
// it is refused, so a split that overloads one side is a stall, not an
// overcharge — and it would be invisible until execution.
func TestEachIntentSatisfiesTheFundingIdentity(t *testing.T) {
	b := boundaryComponents{
		ArrearsMicros: 2_000_000, AdvanceBaseMicros: 20_000_000,
		WalletDrawnMicros: 9_000_000, // spills past the arrears
	}
	charges, err := splitBoundary(b, boundaryTemplate())
	require.NoError(t, err)
	require.Len(t, charges, 2)

	for _, c := range charges {
		require.LessOrEqual(t, c.WalletAllocationMicros, c.TotalMicros(),
			"intent %q allocates more wallet credit than it charges, so its provider remainder "+
				"is negative and the funding clause refuses it", c.Kind)
		require.GreaterOrEqual(t, c.WalletAllocationMicros, int64(0))
	}

	// Specifically: the arrears intent takes what it can and the rest spills.
	require.Equal(t, intent.KindModuleUsage, charges[0].Kind)
	require.EqualValues(t, 2_000_000, charges[0].WalletAllocationMicros,
		"the arrears intent did not absorb the draw up to its own size")
	require.Equal(t, intent.KindPlatformBase, charges[1].Kind)
	require.EqualValues(t, 7_000_000, charges[1].WalletAllocationMicros,
		"the spill did not reach the forward intent")
}

// The kinds are the authorization control, not labelling: an intent carries one
// kind and it "selects which rule of a standing authorization applies".
func TestTheBackwardAndForwardHalvesCarryDifferentKinds(t *testing.T) {
	charges, err := splitBoundary(boundaryComponents{
		ArrearsMicros: 1_000_000, AdvanceBaseMicros: 20_000_000,
	}, boundaryTemplate())
	require.NoError(t, err)
	require.Len(t, charges, 2, "a boundary with both halves produced other than two intents")

	require.Equal(t, intent.KindModuleUsage, charges[0].Kind,
		"the CLOSED period's usage must be module_usage")
	require.Equal(t, intent.KindPlatformBase, charges[1].Kind,
		"the NEXT period's subscription must be platform_base")
	require.NotEqual(t, charges[0].Kind, charges[1].Kind,
		"one kind for the whole boundary lets the kind it names authorize the other half")
}

// The fold closed the vocabulary, not the disclosure. §8's answer is one
// collection with both halves SHOWN, so the forward intent still itemises.
func TestTheForwardIntentStillShowsItsComponents(t *testing.T) {
	charges, err := splitBoundary(boundaryComponents{
		AdvanceBaseMicros: 20_000_000, AdvanceOverageMicros: 5_000_000, AdvanceDomainsMicros: 2_000_000,
	}, boundaryTemplate())
	require.NoError(t, err)
	require.Len(t, charges, 1)

	fwd := charges[0]
	require.Equal(t, intent.KindPlatformBase, fwd.Kind)
	require.Len(t, fwd.Lines, 3,
		"the folded kind collapsed the customer-visible breakdown; the fold was a vocabulary "+
			"change and §8 requires both halves to remain shown")
	require.EqualValues(t, 27_000_000, fwd.TotalMicros())

	var refs []string
	for _, l := range fwd.Lines {
		refs = append(refs, l.SourceRef)
		require.NotZero(t, l.AmountMicros, "a zero line was disclosed")
	}
	require.Equal(t, []string{"advance:base", "advance:capacity", "advance:domains"}, refs)
}

// A zero component contributes no line — an account with no custom domain is
// not shown a domain line.
func TestAZeroComponentIsNotDisclosed(t *testing.T) {
	charges, err := splitBoundary(boundaryComponents{AdvanceBaseMicros: 20_000_000}, boundaryTemplate())
	require.NoError(t, err)
	require.Len(t, charges, 1)
	require.Len(t, charges[0].Lines, 1, "a $0 component was shown to the customer as a line")
	require.Equal(t, "advance:base", charges[0].Lines[0].SourceRef)
}

// A boundary with nothing to charge proposes nothing. Sealing a zero intent
// puts a document in front of a customer for a charge that was never going to
// happen.
func TestAnEmptyBoundaryProposesNothing(t *testing.T) {
	charges, err := splitBoundary(boundaryComponents{}, boundaryTemplate())
	require.NoError(t, err)
	require.Empty(t, charges)
}

// Arithmetic that cannot be right must not seal. The legacy path clamps a
// negative to zero, which hides the error; a sealed intent attests to a number
// and must refuse instead.
func TestImpossibleBoundariesRefuseToSplit(t *testing.T) {
	for _, tc := range []struct {
		name string
		b    boundaryComponents
	}{
		{"negative arrears", boundaryComponents{ArrearsMicros: -1}},
		{"negative advance", boundaryComponents{AdvanceBaseMicros: -1}},
		{"negative draw", boundaryComponents{ArrearsMicros: 10, WalletDrawnMicros: -1}},
		{"draw exceeds the gross", boundaryComponents{ArrearsMicros: 10, WalletDrawnMicros: 11}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := splitBoundary(tc.b, boundaryTemplate())
			require.Error(t, err, "an impossible boundary produced charges instead of refusing")
		})
	}
}

// The template's non-kind fields must reach every charge, or a proposal would
// seal without the revisions and window the predicate checks.
func TestTheTemplateIsCarriedOntoBothIntents(t *testing.T) {
	tmpl := boundaryTemplate()
	tmpl.AuthorizationID = "auth-boundary"
	tmpl.TermsRevision = "terms-2026-01"
	tmpl.PriceBookRevision = "pb-2026-08"
	tmpl.NoticePolicy = "email/v1"
	tmpl.SelectedRail = "stripe"

	charges, err := splitBoundary(boundaryComponents{
		ArrearsMicros: 1_000_000, AdvanceBaseMicros: 20_000_000,
	}, tmpl)
	require.NoError(t, err)
	require.Len(t, charges, 2)

	for _, c := range charges {
		require.Equal(t, "acct-1", c.AccountID)
		require.Equal(t, "USD", c.Currency)
		require.Equal(t, "auth-boundary", c.AuthorizationID)
		require.Equal(t, "terms-2026-01", c.TermsRevision)
		require.Equal(t, "pb-2026-08", c.PriceBookRevision)
		require.Equal(t, "email/v1", c.NoticePolicy)
		require.Equal(t, "stripe", c.SelectedRail)
	}
}
