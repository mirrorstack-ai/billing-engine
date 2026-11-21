package einvoice_test

import (
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
)

func TestDefaultVoidWindow(t *testing.T) {
	cases := []struct {
		issuedAt time.Time
		want     time.Time
	}{
		// Jan-Feb period -> deadline March 13.
		{time.Date(2026, time.January, 15, 0, 0, 0, 0, time.UTC), time.Date(2026, time.March, 13, 23, 59, 59, 0, time.UTC)},
		{time.Date(2026, time.February, 28, 0, 0, 0, 0, time.UTC), time.Date(2026, time.March, 13, 23, 59, 59, 0, time.UTC)},
		// Jul-Aug period -> deadline September 13.
		{time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, time.September, 13, 23, 59, 59, 0, time.UTC)},
		// Nov-Dec period wraps into January of the following year.
		{time.Date(2026, time.December, 20, 0, 0, 0, 0, time.UTC), time.Date(2027, time.January, 13, 23, 59, 59, 0, time.UTC)},
	}
	for _, c := range cases {
		got := einvoice.DefaultVoidWindow(c.issuedAt)
		if !got.Equal(c.want) {
			t.Errorf("DefaultVoidWindow(%s) = %s, want %s", c.issuedAt, got, c.want)
		}
	}
}
