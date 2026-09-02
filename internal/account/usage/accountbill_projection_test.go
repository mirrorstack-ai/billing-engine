package usage

import "testing"

// The PaaS credit is a CREDIT: it must reduce the projected total, exactly as
// it reduces TotalMicros. ProjectedTotalMicros added it for a while instead,
// because the `-` operator lived on a line that a sibling change deleted, and
// no fixture could catch it: `const subscriptionActive = false` forces the
// credit to 0 on every path that builds a response, so the sign is unobservable
// through GetAccountBill. These assert the sign on the sum directly.
func TestProjectedTotalMicrosSubtractsPaasCredit(t *testing.T) {
	const credit = int64(5)
	total := projectedTotalMicros(100, 20, 30, 40, 50, credit, 6)

	// 100+20+30+40+50-5+6. Adding the credit instead yields 251.
	if want := int64(241); total != want {
		t.Fatalf("projected total = %d, want %d (credit added instead of subtracted?)", total, want)
	}
}

// The arithmetic above pins one point; this pins the DIRECTION, so a future
// edit cannot satisfy the constant by coincidence.
func TestProjectedTotalMicrosFallsAsTheCreditGrows(t *testing.T) {
	base := projectedTotalMicros(100, 20, 30, 40, 50, 0, 6)
	withCredit := projectedTotalMicros(100, 20, 30, 40, 50, 7, 6)

	if withCredit >= base {
		t.Fatalf("a larger credit did not lower the projected total: %d -> %d", base, withCredit)
	}
	if got, want := base-withCredit, int64(7); got != want {
		t.Fatalf("credit moved the total by %d, want exactly %d", got, want)
	}
}
