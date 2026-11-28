package einvoice_test

import (
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
)

func tpe(y int, m time.Month, d, h, min, s int) time.Time {
	return time.Date(y, m, d, h, min, s, 0, einvoice.Taipei)
}

func TestPeriodStart(t *testing.T) {
	cases := []struct {
		name string
		at   time.Time
		want time.Time
	}{
		{"first month", tpe(2026, time.January, 15, 12, 0, 0), tpe(2026, time.January, 1, 0, 0, 0)},
		{"second month", tpe(2026, time.February, 28, 23, 59, 59), tpe(2026, time.January, 1, 0, 0, 0)},
		{"even-month period opens at 00:00 Taipei", tpe(2026, time.March, 1, 0, 0, 0), tpe(2026, time.March, 1, 0, 0, 0)},
		// 2026-02-28 16:00 UTC is 2026-03-01 00:00 in Taipei: the new
		// period, even though the UTC date is still February.
		{"UTC instant read in Taipei", time.Date(2026, time.February, 28, 16, 0, 0, 0, time.UTC), tpe(2026, time.March, 1, 0, 0, 0)},
		{"one second before, still the old period", time.Date(2026, time.February, 28, 15, 59, 59, 0, time.UTC), tpe(2026, time.January, 1, 0, 0, 0)},
		{"Nov-Dec", tpe(2026, time.December, 31, 23, 0, 0), tpe(2026, time.November, 1, 0, 0, 0)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := einvoice.PeriodStart(c.at); !got.Equal(c.want) {
				t.Errorf("PeriodStart(%s) = %s, want %s", c.at, got, c.want)
			}
		})
	}
}

func TestDefaultVoidWindow(t *testing.T) {
	cases := []struct {
		name     string
		issuedAt time.Time
		want     time.Time
	}{
		{"Jan-Feb -> March 13", tpe(2026, time.January, 15, 9, 0, 0), tpe(2026, time.March, 13, 23, 59, 59)},
		{"last second of Feb", tpe(2026, time.February, 28, 23, 59, 59), tpe(2026, time.March, 13, 23, 59, 59)},
		{"Jul-Aug -> September 13", tpe(2026, time.August, 1, 0, 0, 0), tpe(2026, time.September, 13, 23, 59, 59)},
		{"Nov-Dec wraps into January", tpe(2026, time.December, 20, 0, 0, 0), tpe(2027, time.January, 13, 23, 59, 59)},
		// Issued 2026-03-01 00:30 Taipei, given in UTC (still Feb 28):
		// the Mar-Apr period, so May 13, not March 13.
		{"UTC input read in Taipei", time.Date(2026, time.February, 28, 16, 30, 0, 0, time.UTC), tpe(2026, time.May, 13, 23, 59, 59)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := einvoice.DefaultVoidWindow(c.issuedAt); !got.Equal(c.want) {
				t.Errorf("DefaultVoidWindow(%s) = %s, want %s", c.issuedAt, got, c.want)
			}
		})
	}
}

func TestUploadDeadline(t *testing.T) {
	cases := []struct {
		name string
		date time.Time
		kind einvoice.DocKind
		want time.Time
	}{
		{"B2C: 2 days from the day after", tpe(2026, time.November, 21, 10, 0, 0), einvoice.KindB2C, tpe(2026, time.November, 23, 23, 59, 59)},
		{"B2B: 7 days from the day after", tpe(2026, time.November, 21, 10, 0, 0), einvoice.KindB2B, tpe(2026, time.November, 28, 23, 59, 59)},
		{"B2C across a month end", tpe(2026, time.November, 30, 23, 59, 0), einvoice.KindB2C, tpe(2026, time.December, 2, 23, 59, 59)},
		{"B2B across a year end", tpe(2026, time.December, 30, 8, 0, 0), einvoice.KindB2B, tpe(2027, time.January, 6, 23, 59, 59)},
		// 2026-11-20 17:00 UTC is 11-21 01:00 Taipei: the invoice date is
		// the 21st.
		{"UTC input read in Taipei", time.Date(2026, time.November, 20, 17, 0, 0, 0, time.UTC), einvoice.KindB2C, tpe(2026, time.November, 23, 23, 59, 59)},
		{"unknown kind gets the shorter deadline", tpe(2026, time.November, 21, 10, 0, 0), einvoice.DocKind(""), tpe(2026, time.November, 23, 23, 59, 59)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := einvoice.UploadDeadline(c.date, c.kind); !got.Equal(c.want) {
				t.Errorf("UploadDeadline(%s, %q) = %s, want %s", c.date, c.kind, got, c.want)
			}
		})
	}
}
