package intent

import (
	"errors"
	"testing"
)

// 🔴 The credit cannot exceed the total.
//
// A charge credited past zero would produce a negative provider remainder —
// money owed back to the customer through a debit path. INV-004's shape
// again: refuse rather than clamp, because a clamped credit silently
// discards value the customer was told they had.
func TestSealRefusesCreditLargerThanTheTotal(t *testing.T) {
	d := validDraft()
	d.WalletAllocationMicros = 1 << 40 // far past any total validDraft produces

	if _, err := Seal(d); !errors.Is(err, ErrCreditExceedsTotal) {
		t.Fatalf("Seal with credit past the total = %v; want ErrCreditExceedsTotal", err)
	}
}

func TestSealRefusesNegativeCredit(t *testing.T) {
	d := validDraft()
	d.WalletAllocationMicros = -1

	if _, err := Seal(d); !errors.Is(err, ErrFundingNegative) {
		t.Fatalf("Seal with negative credit = %v; want ErrFundingNegative", err)
	}
}

// The remainder is DERIVED, and derived exactly once.
//
// Draft carries no ProviderRemainderMicros on purpose: a caller supplying it
// would have to compute the total to do so, which is the second derivation
// INV-002 forbids. This asserts the arithmetic Seal performs instead.
func TestSealDerivesTheRemainderFromTheCredit(t *testing.T) {
	for _, credit := range []int64{0, 1, 1_000} {
		d := validDraft()
		// validDraft() is auto_topup, a stored-value kind that forbids
		// credit. Use a kind that permits it.
		d.Kind = KindModuleUsage
		d.WalletAllocationMicros = credit

		sealed, err := Seal(d)
		if err != nil {
			t.Fatalf("Seal(credit=%d): %v", credit, err)
		}
		if got := sealed.WalletAllocationMicros(); got != credit {
			t.Errorf("credit = %d; want %d", got, credit)
		}
		want := sealed.TotalMicros() - credit
		if got := sealed.ProviderRemainderMicros(); got != want {
			t.Errorf("remainder = %d; want %d (total %d - credit %d)",
				got, want, sealed.TotalMicros(), credit)
		}
		// The property the predicate's balance clause checks.
		if sealed.WalletAllocationMicros()+sealed.ProviderRemainderMicros() != sealed.TotalMicros() {
			t.Errorf("split does not account for the whole obligation")
		}
	}
}

// 🔴 docs/DESIGN.md:493-495 — "credit_purchase and auto_topup create stored
// value, so walletFunding = 0".
//
// A purchase of credit paid for with credit refills a balance out of the
// balance it is refilling. The rule lived only as a comment in the executor
// until this change; it belongs where the kind is sealed.
//
// TWO kinds, not three. subscription_start shares a funding FORMULA with
// them in the executor's by-kind selection but does not create stored value
// (§6's funding table, :1228) — grouping by formula and grouping by
// stored-value are different partitions, and conflating them is how this
// nearly shipped with three.
func TestStoredValueKindsCannotBeFundedFromTheWallet(t *testing.T) {
	for _, kind := range []ChargeKind{KindCreditPurchase, KindAutoTopUp} {
		d := validDraft()
		d.Kind = kind
		d.WalletAllocationMicros = 1

		if _, err := Seal(d); !errors.Is(err, ErrWalletCannotFundItself) {
			t.Errorf("Seal(%s, credit=1) = %v; want ErrWalletCannotFundItself", kind, err)
		}

		// ...and the same kind with no credit seals fine.
		d.WalletAllocationMicros = 0
		if _, err := Seal(d); err != nil {
			t.Errorf("Seal(%s, credit=0) = %v; want it accepted", kind, err)
		}
	}

	// Not forbidden: it does not create stored value.
	d := validDraft()
	d.Kind = KindSubscriptionStart
	d.WalletAllocationMicros = 1
	if _, err := Seal(d); errors.Is(err, ErrWalletCannotFundItself) {
		t.Error("subscription_start was refused wallet funding; §6:1228 funds it " +
			"differently from the stored-value kinds and does not forbid credit")
	}
}

// The split must reach the DIGEST, not merely the struct.
//
// A field validated but not digested is one a customer cannot verify and an
// attacker can change without breaking the seal. Two intents identical except
// for how much credit was applied must not share a digest.
func TestTheFundingSplitChangesTheDigest(t *testing.T) {
	seen := map[string]int64{}

	for _, credit := range []int64{0, 1, 2, 500} {
		d := validDraft()
		d.Kind = KindModuleUsage
		d.WalletAllocationMicros = credit
		sealed, err := Seal(d)
		if err != nil {
			t.Fatalf("Seal(credit=%d): %v", credit, err)
		}
		if prior, clash := seen[sealed.Digest()]; clash {
			t.Fatalf("credits %d and %d produced the SAME digest %s — the amount "+
				"credited is not covered by the seal", prior, credit, sealed.Digest())
		}
		seen[sealed.Digest()] = credit
	}
}
