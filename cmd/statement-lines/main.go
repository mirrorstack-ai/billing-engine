// Command statement-lines composes a payer's monthly statement and prints
// it — it does not send it, collect it, or write it anywhere. The owner
// sends the statement by hand (D493); this binary only saves someone from
// typing the numbers from memory.
//
// There is no provider client, no notifier, and no writer in this binary —
// "moves no money" is a property of what is compiled in, the same posture
// cmd/intent-shadow documents for the shadow rater. CLI only: no Lambda
// transport, because a statement's detail (per-app amounts, an FX rate, a
// TWD total) must not go to a shared CloudWatch log group the way
// cmd/intent-shadow/lambda.go's Response comment describes for that binary's
// Lambda path — this command has no such path to begin with.
//
// It reads the payer's usage from usage.GetAccountBill: one usage line per
// app, at 100% of that app's AccountAppBill.TotalMicros (base fee + module
// usage + infra, pre-credit) — never multiplied by a payer price basis. In
// v1 every app's plan is the $0 Hobby stub (usage.PlanStubPriceMicros), so
// TotalMicros is in practice the app's usage; that stops being exactly true
// the day a paid per-app plan ships, and this command does not yet separate
// the two. The account-level Agent bucket, AccountOverageMicros,
// CustomDomainsMicros and the bill's own estimated Tax are read but NOT
// turned into lines — only Apps[].TotalMicros is mapped today.
//
// Fixed lines (platform fee, org seats, app plan fees) are not computed
// here: they come from the -lines JSON file until billing-engine#258 (app
// plan fees) and #228 (org seats) land, per the plan-fee math this issue
// must not copy.
//
// Usage window: GetAccountBill resolves the previous CLOSED anchored
// period (internal/billingperiod's per-account anchor day, all boundaries
// midnight UTC) and this command echoes that window verbatim onto the
// statement. The distributor contract's cut-off wording is the last day
// 24:00 Asia/Taipei (UTC+8) — eight hours later than the engine's midnight
// UTC boundary. This command does NOT reconcile that gap; it prints the
// engine window it actually read, so the gap is visible rather than silently
// absorbed. Closing it needs a usage read for a custom window, which does
// not exist yet.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/mirrorstack-ai/billing-engine/internal/account/statement"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
)

func main() {
	ownerUser := flag.String("owner-user", "", "owner_user_id (uuid) — exactly one of -owner-user / -owner-org")
	ownerOrg := flag.String("owner-org", "", "owner_org_id (uuid) — exactly one of -owner-user / -owner-org")
	period := flag.String("period", "", "a specific past billing_periods id; default: the most recent CLOSED period")
	linesPath := flag.String("lines", "", "required: path to a JSON file of fixed statement.Line objects (platform fee, seats, app plan, manual)")
	statementDateStr := flag.String("statement-date", "", "required: the statement date, YYYY-MM-DD (issued on the 1st)")
	rate := flag.String("rate", "", "required: Bank of Taiwan spot selling rate, decimal string, at most 4 decimals (e.g. 32.1500)")
	rateDateStr := flag.String("rate-date", "", "required: the rate's business day, YYYY-MM-DD, strictly before -statement-date")
	rateSource := flag.String("rate-source", "", "required: citation of where the rate was read")
	taxBps := flag.Int("tax-bps", 0, "business tax rate in basis points; 0 means the default 500 (5%, D502)")
	dueDayOfMonth := flag.Int("due-day-of-month", 15, "due-date day of month (the first distributor's contract value is 15)")
	dueAfterDays := flag.Int("due-after-days", 0, "if > 0, due date is this many days after the statement date instead (the public Terms value is 30), overriding -due-day-of-month")
	flag.Parse()

	if err := run(*ownerUser, *ownerOrg, *period, *linesPath, *statementDateStr, *rate, *rateDateStr, *rateSource, *taxBps, *dueDayOfMonth, *dueAfterDays); err != nil {
		fmt.Fprintln(os.Stderr, "statement-lines failed:", err)
		os.Exit(2)
	}
}

func run(ownerUserStr, ownerOrgStr, periodFlag, linesPath, statementDateStr, rate, rateDateStr, rateSource string, taxBps, dueDayOfMonth, dueAfterDays int) error {
	owner, err := parseOwner(ownerUserStr, ownerOrgStr)
	if err != nil {
		return err
	}
	if linesPath == "" {
		return fmt.Errorf("-lines is required")
	}
	statementDate, err := time.Parse("2006-01-02", statementDateStr)
	if err != nil {
		return fmt.Errorf("-statement-date: %w", err)
	}
	rateDate, err := time.Parse("2006-01-02", rateDateStr)
	if err != nil {
		return fmt.Errorf("-rate-date: %w", err)
	}

	dueRule := statement.NewDueDayOfMonth(dueDayOfMonth)
	if dueAfterDays > 0 {
		dueRule = statement.NewDueAfterDays(dueAfterDays)
	}

	fixedLines, err := loadFixedLines(linesPath)
	if err != nil {
		return err
	}

	if config.IsLambda() {
		return fmt.Errorf("this command is CLI-only and refuses to run under a Lambda transport")
	}

	pool := config.MustPgxPool()
	defer pool.Close()

	ctx := context.Background()
	var rep Report
	err = withReadOnlyTx(ctx, pool, func(ctx context.Context, _ pgx.Tx) error {
		// usage.Service queries go directly against the pool (see main.go's
		// package doc and withReadOnlyTx's doc comment): usage.Store is built
		// on *pgxpool.Pool, not on an externally-held pgx.Tx, the same way
		// every existing GetAccountBill caller in this tree uses it (e.g.
		// internal/account/cycle/org.go, cmd/account-api). The read-only
		// transaction held here still proves, before anything runs, that
		// this binary's database credentials cannot write.
		svc := usage.NewService(usage.NewStore(pool))

		periodID := periodFlag
		if periodID == "" {
			periodID, err = resolvePreviousClosedPeriod(ctx, svc, owner)
			if err != nil {
				return err
			}
		}

		bill, err := svc.GetAccountBill(ctx, usage.GetAccountBillRequest{
			OwnerUserID: owner.userID,
			OwnerOrgID:  owner.orgID,
			PeriodID:    periodID,
		})
		if err != nil {
			return fmt.Errorf("GetAccountBill: %w", err)
		}

		lines := append([]statement.Line{}, fixedLines...)
		for _, app := range bill.Apps {
			subject := app.Name
			if subject == "" {
				subject = app.AppID.String()
			}
			lines = append(lines, statement.Line{
				Kind:         statement.KindUsage,
				Subject:      "App usage: " + subject,
				AmountMicros: app.TotalMicros,
			})
		}

		in := statement.Input{
			StatementDate:   statementDate,
			UsageWindowFrom: bill.PeriodStart,
			UsageWindowTo:   bill.PeriodEnd,
			Lines:           lines,
			FX: statement.FX{
				Rate:     rate,
				RateDate: rateDate,
				Source:   rateSource,
			},
			TaxRateBps: taxBps,
			DueRule:    dueRule,
		}
		st, err := statement.Compose(in)
		if err != nil {
			return fmt.Errorf("compose: %w", err)
		}

		rep = Report{
			Statement:   st,
			OwnerUserID: nonNilUUIDString(owner.userID),
			OwnerOrgID:  nonNilUUIDString(owner.orgID),
			PeriodID:    bill.PeriodID,
		}
		return nil
	})
	if err != nil {
		return err
	}

	if err := printJSON(os.Stdout, rep); err != nil {
		return fmt.Errorf("print json: %w", err)
	}
	fmt.Fprintln(os.Stdout)
	printText(os.Stdout, rep)
	return nil
}

// resolvePreviousClosedPeriod reads the account's period selector and
// returns the most recently CLOSED period id — GetBillingPeriods always
// lists the current live period first, so this is the first entry with
// IsCurrent == false.
func resolvePreviousClosedPeriod(ctx context.Context, svc *usage.Service, owner ownerPrincipal) (string, error) {
	resp, err := svc.GetBillingPeriods(ctx, usage.GetBillingPeriodsRequest{
		OwnerUserID: owner.userID,
		OwnerOrgID:  owner.orgID,
	})
	if err != nil {
		return "", fmt.Errorf("GetBillingPeriods: %w", err)
	}
	for _, p := range resp.Periods {
		if !p.IsCurrent {
			return p.PeriodID, nil
		}
	}
	return "", fmt.Errorf("no closed billing period yet for this account — pass -period explicitly if one exists")
}

type ownerPrincipal struct {
	userID uuid.UUID
	orgID  uuid.UUID
}

func parseOwner(userStr, orgStr string) (ownerPrincipal, error) {
	if (userStr == "") == (orgStr == "") {
		return ownerPrincipal{}, fmt.Errorf("exactly one of -owner-user / -owner-org is required")
	}
	if userStr != "" {
		id, err := uuid.Parse(userStr)
		if err != nil {
			return ownerPrincipal{}, fmt.Errorf("-owner-user: %w", err)
		}
		return ownerPrincipal{userID: id}, nil
	}
	id, err := uuid.Parse(orgStr)
	if err != nil {
		return ownerPrincipal{}, fmt.Errorf("-owner-org: %w", err)
	}
	return ownerPrincipal{orgID: id}, nil
}

func nonNilUUIDString(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
}
