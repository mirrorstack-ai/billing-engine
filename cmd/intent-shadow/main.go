// Command intent-shadow rates closed billing periods through the intent
// rater and reports where it disagrees with what was charged.
//
// docs/DESIGN.md §11 steps 3 and 4: "generate shadow intents from
// current usage that notify nobody and move no money", then "reconcile
// shadow totals against current invoices until every difference is
// explained. Never tune the rater to hide an unexplained difference."
//
// This is the gate every cutover in wave 5 waits on, and it is the one
// piece of that work that is safe to run against production today —
// because it cannot do anything. There is no provider client, no
// notifier, and no writer in this binary or in the Source it reads
// through. "Moves no money" is a property of what was compiled in
// rather than a promise about what the code does.
//
// It exits non-zero when anything is unexplained, so it can gate a
// cutover from CI as well as inform a person reading the report.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/shadow"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
)

func main() {
	periods := flag.Int("periods", 100, "how many recent closed periods to reconcile")
	flag.Parse()

	pool := config.MustPgxPool()

	// Lambda transport: an operator invokes this against production, and the
	// detail comes back as the function RESULT rather than the log. See
	// lambda.go — stdout is CloudWatch, which is a wider audience than
	// lambda:InvokeFunction.
	if config.IsLambda() {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
		startLambda(pool, slog.Default())
		return
	}
	defer pool.Close()

	// CLI: a person at a terminal, against a database they chose. stdout is
	// their screen, not a log group, so nothing is redacted — withholding
	// detail here would only make the tool harder to use without protecting
	// anyone.
	var report shadow.Report
	err := withReadOnlyTx(context.Background(), pool, func(ctx context.Context, tx pgx.Tx) error {
		var runErr error
		report, runErr = run(ctx, shadow.NewSourceFrom(tx), *periods)
		return runErr
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "shadow run failed:", err)
		os.Exit(2)
	}

	fmt.Print(report)

	if !report.Ready() {
		// Non-zero because an unexplained difference BLOCKS a cutover.
		// Printing the report and exiting 0 would let a pipeline treat
		// "we found problems" as "we looked and it was fine".
		fmt.Fprintf(os.Stderr,
			"\nNOT READY: %d unexplained difference(s) over %d period(s).\n"+
				"Each needs a written explanation in internal/intent/shadow/explanations.go\n"+
				"saying which figure is right and why. Do not adjust the rater to close one.\n",
			report.Unexplained(), report.Compared)
		os.Exit(1)
	}
}

// run reconciles the most recent closed periods.
//
// Split from main so it is testable without an environment, and so the
// only thing main adds is a database handle and an exit code.
func run(ctx context.Context, src *shadow.Source, limit int) (shadow.Report, error) {
	closed, err := src.ClosedPeriods(ctx, limit)
	if err != nil {
		return shadow.Report{}, err
	}
	if len(closed) == 0 {
		// Not an error, and not a pass either — Report.Ready() is false
		// when nothing was compared, because "we found no problems" is
		// not the claim "we looked".
		return shadow.Reconcile(nil), nil
	}

	book, err := src.PriceBookFor(ctx, closed[len(closed)-1].Start)
	if err != nil {
		return shadow.Report{}, err
	}

	var differences []shadow.Difference
	for _, p := range closed {
		facts, err := src.FactsFor(ctx, p)
		if err != nil {
			return shadow.Report{}, err
		}

		sealed, rateErr := intent.Rate(intent.RateInput{
			Facts:     facts,
			PriceBook: book,
			Tax:       zeroTax{},
			// These are shadow values. Nothing is executed against this
			// intent — it exists to carry a total and a digest.
			AuthorizationID:  "shadow",
			Kind:             intent.KindModuleUsage,
			TermsRevision:    "shadow",
			NoticePolicy:     "shadow",
			ExecuteNotBefore: p.Start,
			ExecuteNotAfter:  p.End,
			RatedAt:          p.End,
		})

		d := shadow.Difference{
			AccountID:        p.AccountID,
			PeriodID:         p.PeriodID,
			LegacyMicros:     p.LegacyMicros,
			LegacyBaseMicros: p.LegacyBaseMicros,
		}
		if rateErr != nil {
			// A quarantined period is a difference of the whole
			// amount, not a skipped row. Skipping it would remove the
			// periods the rater cannot handle from the very count that
			// is supposed to reveal them.
			d.ShadowMicros = 0
			d.Quarantined = true
			d.IntentDigest = "quarantined: " + rateErr.Error()
		} else {
			d.ShadowMicros = sealed.TotalMicros()
			d.IntentDigest = sealed.Digest()
		}
		differences = append(differences, d)
	}

	return shadow.Reconcile(differences), nil
}

// zeroTax is a resolved zero-tax determination.
//
// Shadow rating compares the USAGE total against what usage was
// charged, and the legacy rollup this reads has no tax in it. Applying
// a tax rule here would introduce a difference on every row that says
// nothing about the rater.
//
// It is resolved rather than absent, because an unresolved
// determination quarantines — correctly, in real rating, and uselessly
// here.
type zeroTax struct{}

func (zeroTax) Determine(intent.Subject, string, int64, time.Time) intent.TaxDetermination {
	return intent.TaxDetermination{
		Resolved:     true,
		Jurisdiction: "shadow",
		RuleRevision: "shadow-zero",
		AmountMicros: 0,
	}
}
