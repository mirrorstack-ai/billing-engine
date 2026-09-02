package creditledger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Settlement asserts that the ROUNDED cents were paid and then credits the RAW
// micros. Those are two different numbers for any amount that is not a whole
// cent, and the difference is credit nobody paid for.
//
// This test is the arithmetic half of the fix. It uses the real
// microsToCentsRoundHalfUp rather than a copy: a transcription of the rounding
// rule into a test proves only that the copy agrees with itself.
func TestCreditedMicrosCanOnlyEqualChargedCentsOnWholeCentAmounts(t *testing.T) {
	// The published amount. $5.00 is charged; $5.004999 was credited.
	const exploit int64 = 5_004_999

	charged := microsToCentsRoundHalfUp(exploit) * microsPerCent
	require.NotEqual(t, exploit, charged,
		"if these were equal the guard would be pointless and this test vacuous")
	require.Equal(t, int64(4_999), exploit-charged,
		"the gap is what the payer received without paying for it")

	// Every amount the entry points now accept is a whole cent, and for those
	// the two computations agree — which is the property the guard buys.
	for _, whole := range []int64{
		5_000_000,     // MinCreditPurchaseMicros
		5_010_000,     // the nearest legal amount above the exploit
		5_000_000_000, // MaxCreditPurchaseMicros
	} {
		require.Equal(t, whole, microsToCentsRoundHalfUp(whole)*microsPerCent,
			"a whole-cent amount must credit exactly what it charges")
	}

	// Sweep the sub-cent digits: every one of them diverges, so the guard is
	// not tuned to the single published value.
	diverged := 0
	for offset := int64(1); offset < microsPerCent; offset++ {
		amount := int64(5_000_000) + offset
		if microsToCentsRoundHalfUp(amount)*microsPerCent != amount {
			diverged++
		}
	}
	require.Equal(t, int(microsPerCent-1), diverged,
		"every non-whole-cent amount must diverge; if some do not, the guard "+
			"is narrower than the defect")
}
