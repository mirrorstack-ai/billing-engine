package statement_test

import (
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/account/statement"
)

func TestDueDate_DayOfMonth(t *testing.T) {
	tests := []struct {
		name string
		in   time.Time
		want time.Time
	}{
		{"on the 1st, same month", date(2027, 1, 1), date(2027, 1, 15)},
		{"on the 1st of February", date(2027, 2, 1), date(2027, 2, 15)},
		{"after the 15th, rolls into next year", date(2027, 12, 20), date(2028, 1, 15)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := statement.DueDate(tt.in, statement.NewDueDayOfMonth(15))
			if err != nil {
				t.Fatalf("DueDate: %v", err)
			}
			if !got.Equal(tt.want) {
				t.Errorf("DueDate(%s) = %s, want %s", tt.in.Format("2006-01-02"), got, tt.want)
			}
		})
	}
}

func TestDueDate_AfterDays(t *testing.T) {
	got, err := statement.DueDate(date(2027, 1, 31), statement.NewDueAfterDays(30))
	if err != nil {
		t.Fatalf("DueDate: %v", err)
	}
	want := date(2027, 3, 2)
	if !got.Equal(want) {
		t.Errorf("DueDate(2027-01-31, +30d) = %s, want %s", got, want)
	}
}

func TestDueDate_RefusesBadRule(t *testing.T) {
	if _, err := statement.DueDate(date(2027, 1, 1), statement.DueRule{Kind: "nonsense"}); err == nil {
		t.Error("DueDate with unknown rule kind: want error, got none")
	}
	if _, err := statement.DueDate(date(2027, 1, 1), statement.NewDueAfterDays(0)); err == nil {
		t.Error("DueDate with AfterDays=0: want error, got none")
	}
	if _, err := statement.DueDate(date(2027, 1, 1), statement.NewDueDayOfMonth(0)); err == nil {
		t.Error("DueDate with DayOfMonth=0: want error, got none")
	}
}
