package main

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/mirrorstack-ai/billing-engine/internal/account/statement"
)

// Report is what this command prints: the composed statement plus the
// read-only detail about where its numbers came from. Detail belongs here —
// this is a terminal tool, not a Lambda whose stdout is a shared log group
// (contrast cmd/intent-shadow/lambda.go's Response doc comment).
type Report struct {
	Statement statement.Statement `json:"statement"`

	// OwnerUserID / OwnerOrgID echo which principal was billed.
	OwnerUserID string `json:"owner_user_id,omitempty"`
	OwnerOrgID  string `json:"owner_org_id,omitempty"`
	// PeriodID is the billing_periods id the usage lines were read for.
	PeriodID string `json:"period_id"`
}

func printJSON(w io.Writer, r Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

func printText(w io.Writer, r Report) {
	st := r.Statement
	fmt.Fprintf(w, "STATEMENT — this is a payment notice, not a tax invoice (D433, D494)\n")
	fmt.Fprintf(w, "Statement date:   %s\n", st.StatementDate.Format("2006-01-02"))
	fmt.Fprintf(w, "Usage window:     [%s, %s)\n",
		st.UsageWindowFrom.Format("2006-01-02"), st.UsageWindowTo.Format("2006-01-02"))
	fmt.Fprintf(w, "Due date:         %s\n", st.DueDate.Format("2006-01-02"))
	fmt.Fprintf(w, "FX:               %s (rate date %s, %s)\n",
		st.FX.Rate, st.FX.RateDate.Format("2006-01-02"), st.FX.Source)
	if r.PeriodID != "" {
		fmt.Fprintf(w, "Period ID:        %s\n", r.PeriodID)
	}
	fmt.Fprintln(w)

	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "KIND\tSUBJECT\tGROUP\tAMOUNT (USD)\tNOTE")
	for _, l := range st.Lines {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			l.Kind, l.Subject, l.Group, usdString(l.AmountMicros), l.Note)
	}
	tw.Flush()

	fmt.Fprintln(w)
	fmt.Fprintf(w, "USD subtotal:     %s\n", usdString(st.SubtotalUSDMicros))
	fmt.Fprintf(w, "TWD subtotal:     NT$%d\n", st.SubtotalTWD)
	fmt.Fprintf(w, "Tax:              NT$%d\n", st.TaxTWD)
	fmt.Fprintf(w, "TOTAL:            NT$%d\n", st.TotalTWD)
}

// usdString renders USD micros as a fixed decimal string without ever
// touching a float — the same integer-only posture Compose requires.
func usdString(micros int64) string {
	neg := ""
	if micros < 0 {
		neg = "-"
		micros = -micros
	}
	whole := micros / 1_000_000
	frac := micros % 1_000_000 / 10_000 // cents
	return fmt.Sprintf("%sUSD %d.%02d", neg, whole, frac)
}
