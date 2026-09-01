package cycle

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
)

func prorationAttempt(baseCents, moduleCents int64, timers int) CombinedProrationAttempt {
	ids := make([]uuid.UUID, 0, timers)
	for i := 0; i < timers; i++ {
		ids = append(ids, uuid.New())
	}
	return CombinedProrationAttempt{
		AppID:    uuid.New(),
		TimerIDs: ids,
		Shape: CombinedProrationChargeShape{
			AccountID: uuid.New(),
			Currency:  "usd",
			// Both representations, consistent — which is what the legacy
			// path writes. A fixture that set only the cents would trip the
			// stored-pair guard and hide whatever the test was for.
			BaseChargeCents:    baseCents,
			BaseChargeMicros:   baseCents * microsPerCent,
			ModuleChargeCents:  moduleCents,
			ModuleChargeMicros: moduleCents * microsPerCent,
			CoverageEnd:        time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
			BaseDescription:    "MirrorStack app base fee (prorated) — app",
			ModuleDescription:  "MirrorStack module overage (prorated) — app",
		},
	}
}

// 🔴 THE IDENTITY THE WHOLE LEG RESTS ON.
//
// The legacy path rounds PER COMPONENT and sums: BaseChargeCents +
// ModuleChargeCents × timerCount. Sealing the raw derived micros and rounding
// once at the provider gives a DIFFERENT number whenever the components'
// fractional parts sum past a cent. This asserts the sealed total is the same
// integer the legacy invoice would have carried, across the shapes a real
// attempt takes.
func TestTheProrationIntentSealsTheLegacyTotal(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		baseCents, moduleCents int64
		timers                 int
	}{
		{"base only", 1_234, 0, 0},
		{"base and one timer", 1_234, 77, 1},
		{"base and several timers", 1_234, 77, 4},
		{"timers only", 0, 77, 3},
		{"amounts that would round differently if summed first", 1, 1, 1},
		{"large", 999_999, 12_345, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := prorationAttempt(tc.baseCents, tc.moduleCents, tc.timers)
			c, err := prorationCharge(a, proposer.Charge{})
			require.NoError(t, err)

			want, err := combinedProrationTotalCents(a)
			require.NoError(t, err)
			require.Equal(t, want*microsPerCent, c.TotalMicros(),
				"the sealed intent does not total what the legacy invoice would collect")
		})
	}
}

// The fold is what makes this ONE intent. Before it, the base fee was
// platform_base and the module overage was module_capacity — two kinds, so a
// group. Regressing the kinds would silently reintroduce that.
func TestTheProrationIntentIsASingleFoldedKind(t *testing.T) {
	c, err := prorationCharge(prorationAttempt(1_234, 77, 2), proposer.Charge{})
	require.NoError(t, err)

	require.Equal(t, intent.KindPlatformBase, c.Kind,
		"the prorated base fee and module overage are one kind since §12 item 12")
	require.Len(t, c.Lines, 3, "one base line plus one line per timer")
}

// Each timer keeps its own line and its own source ref. Summing the timers into
// one line would lose the ref, and the ref is how a charge is walked back to
// the timer that caused it.
func TestEachTimerKeepsItsOwnLine(t *testing.T) {
	a := prorationAttempt(1_000, 50, 3)
	c, err := prorationCharge(a, proposer.Charge{})
	require.NoError(t, err)

	require.Equal(t, combinedProrationBaseItemKey, c.Lines[0].SourceRef)
	seen := map[string]bool{}
	for _, l := range c.Lines[1:] {
		require.False(t, seen[l.SourceRef], "two lines share a source ref, so a timer cannot be identified")
		seen[l.SourceRef] = true
		require.EqualValues(t, 50*microsPerCent, l.AmountMicros)
	}
	for _, id := range a.TimerIDs {
		require.True(t, seen[combinedProrationTimerItemKey(id)],
			"timer %s has no line, so its charge vanished in the cutover", id)
	}
}

// A zero component contributes no line, and an attempt with nothing to charge
// produces no charge at all.
func TestAnEmptyProrationProposesNothing(t *testing.T) {
	c, err := prorationCharge(prorationAttempt(0, 0, 2), proposer.Charge{})
	require.NoError(t, err)
	require.Empty(t, c.Lines)

	base, err := prorationCharge(prorationAttempt(500, 0, 2), proposer.Charge{})
	require.NoError(t, err)
	require.Len(t, base.Lines, 1, "a zero per-timer figure produced timer lines")
}

// 🔴 The stored pair guard, driven.
//
// The shape persists each figure TWICE — micros and cents — and the lines are
// sealed from the cents because that is what the legacy invoice collects. If
// the pair disagrees, the amount the customer was quoted from the micros is not
// the amount this intent attests to, and the intent is the document they are
// shown. So it refuses rather than picking one.
//
// This replaced a guard that compared the lines against
// combinedProrationTotalCents. That one could never fail — both sides read the
// same cents fields — and mutation testing proved it: deleting it changed
// nothing. A control that cannot fire is a comment.
func TestAStoredFigureThatDisagreesWithItselfRefusesToSeal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(*CombinedProrationChargeShape)
	}{
		{"base cents disagree with base micros", func(s *CombinedProrationChargeShape) {
			s.BaseChargeMicros = s.BaseChargeCents*microsPerCent + 7_000
		}},
		{"module cents disagree with module micros", func(s *CombinedProrationChargeShape) {
			s.ModuleChargeMicros = s.ModuleChargeCents*microsPerCent + 9_999
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := prorationAttempt(1_000, 50, 2)
			tc.break_(&a.Shape)

			_, err := prorationCharge(a, proposer.Charge{})
			require.Error(t, err,
				"a shape whose two representations of one figure disagree was sealed anyway")
			require.Contains(t, err.Error(), "disagrees with itself")
		})
	}
}

// And a consistent pair must seal, or the guard above would pass by refusing
// everything.
func TestAConsistentStoredPairSeals(t *testing.T) {
	c, err := prorationCharge(prorationAttempt(1_000, 50, 2), proposer.Charge{})
	require.NoError(t, err)
	require.EqualValues(t, 1_100*microsPerCent, c.TotalMicros())
}
