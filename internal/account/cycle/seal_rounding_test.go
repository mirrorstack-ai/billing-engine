package cycle

import "testing"

// A sealed intent must attest to what a collection would actually take.
//
// The legacy legs charge centsFromMicros(derived) cents. Until 2026-08-30 the
// cut-over legs sealed the raw derived micros, so the sealed document named a
// figure the customer was never charged — by up to half a cent, in whichever
// direction the rounding went.
//
// The gap is small and that is exactly what makes it dangerous: it is a
// repricing, and docs/DESIGN.md §11's shadow reconciliation cannot see it,
// because shadow compares the new rater against billing history rather than
// the sealed intent against the legacy leg it replaced.
func TestCollectableMicrosMatchesWhatALegacyCollectionCharges(t *testing.T) {
	for _, tc := range []struct {
		name    string
		derived int64
		cents   int64
		sealed  int64
	}{
		{"already whole cents", 2_000_000, 200, 2_000_000},
		{"rounds down", 2_000_004, 200, 2_000_000},
		{"rounds up at the half", 2_005_000, 201, 2_010_000},
		{"rounds up", 2_009_999, 201, 2_010_000},
		{"sub-cent total rounds to zero", 4_999, 0, 0},
		{"sub-cent total rounds to one cent", 5_000, 1, 10_000},
		{"zero", 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cents, err := centsFromMicros(tc.derived)
			if err != nil {
				t.Fatalf("centsFromMicros(%d): %v", tc.derived, err)
			}
			if cents != tc.cents {
				t.Fatalf("centsFromMicros(%d) = %d, want %d", tc.derived, cents, tc.cents)
			}

			got, err := collectableMicros(tc.derived)
			if err != nil {
				t.Fatalf("collectableMicros(%d): %v", tc.derived, err)
			}
			if got != tc.sealed {
				t.Fatalf("collectableMicros(%d) = %d, want %d", tc.derived, got, tc.sealed)
			}

			// The property that matters, stated directly: the sealed figure
			// and the charged figure are the SAME money.
			if got != cents*microsPerCent {
				t.Fatalf("sealed %d micros but a collection takes %d cents (= %d micros)",
					got, cents, cents*microsPerCent)
			}
		})
	}
}

// The executor re-rounds at the provider boundary. Sealing whole cents makes
// that step a no-op, so the bundle, the sealed total and the card charge are
// one number rather than three that usually agree.
func TestSealingIsIdempotentUnderTheProviderRounding(t *testing.T) {
	for _, derived := range []int64{0, 1, 4_999, 5_000, 2_000_004, 2_005_000, 999_999_999} {
		sealed, err := collectableMicros(derived)
		if err != nil {
			t.Fatalf("collectableMicros(%d): %v", derived, err)
		}
		again, err := collectableMicros(sealed)
		if err != nil {
			t.Fatalf("collectableMicros(%d): %v", sealed, err)
		}
		if again != sealed {
			t.Fatalf("re-rounding %d micros moved it to %d — the provider step is not a no-op",
				sealed, again)
		}
	}
}
