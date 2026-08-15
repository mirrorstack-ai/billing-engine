package usage

import "testing"

func TestProjectedTotalMicrosSubtractsPaasCredit(t *testing.T) {
	total := projectedTotalMicros(
		100,
		20,
		30,
		40,
		50,
		5,
		6,
	)

	expected := int64(241)
	if total != expected {
		t.Fatalf("projected total mismatch: got %d, want %d", total, expected)
	}
}
