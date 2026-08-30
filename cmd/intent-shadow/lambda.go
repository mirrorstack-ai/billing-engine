package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/shadow"
)

// defaultPeriods matches the CLI's -periods default, so the same question
// asked either way gets the same answer.
const defaultPeriods = 100

// Request is what an operator invokes this function with.
type Request struct {
	// Action is "shadow" (reconcile the rater against billing history),
	// "preconditions" (ask the seven legacy-drop questions), or "census"
	// (how much billing history exists at all).
	Action string `json:"action"`
	// Periods bounds the shadow reconciliation. Ignored by preconditions.
	Periods int `json:"periods"`
}

// Response is the function's RESULT, and the only place detail appears.
//
// 🔴 This is the whole redaction design. Under Lambda, stdout is CloudWatch:
// one week of retention, readable by anyone with logs access and WITHOUT
// lambda:InvokeFunction. So the log has a wider audience than the invoke
// permission implies, and per-account identifiers and money figures must not
// go there.
//
// The result travels back to whoever invoked the function — someone who
// already held the permission to ask. Detail belongs here; the log gets
// counters that reveal nothing about any one customer.
type Response struct {
	Action string `json:"action"`
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`

	// Shadow carries per-account differences. Never logged.
	Shadow *shadow.Report `json:"shadow,omitempty"`
	// Preconditions carry counts and verdicts only, so they are safe
	// either way — but they still travel in the result, so there is one
	// rule rather than two.
	Preconditions []Precondition `json:"preconditions,omitempty"`
	// Census carries table totals — aggregates only, same rule again.
	Census []CensusRow `json:"census,omitempty"`
}

// handler runs one action inside one read-only transaction.
func handler(pool *pgxpool.Pool, log *slog.Logger) func(context.Context, Request) (Response, error) {
	return func(ctx context.Context, req Request) (Response, error) {
		res := Response{Action: req.Action}

		err := withReadOnlyTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
			var err error
			res, err = runAction(ctx, tx, req)
			return err
		})
		if err != nil {
			res.OK = false
			res.Error = err.Error()
			// The error text is returned, not logged: a database error can
			// quote a row.
			log.Error("intent-shadow run failed", "action", req.Action)
			return res, nil
		}

		logSummary(log, res)
		return res, nil
	}
}

// runAction is the ONE place either action is wired to a database handle.
//
// Both transports go through it, so neither can acquire a writable handle or
// skip the read-only transaction by drifting apart.
func runAction(ctx context.Context, tx pgx.Tx, req Request) (Response, error) {
	res := Response{Action: req.Action}

	switch req.Action {
	case "", "shadow":
		res.Action = "shadow"
		periods := req.Periods
		if periods <= 0 {
			periods = defaultPeriods
		}
		report, err := run(ctx, shadow.NewSourceFrom(tx), periods)
		if err != nil {
			return res, err
		}
		res.Shadow = &report
		res.OK = report.Ready()

	case "preconditions":
		answers, err := runPreconditions(ctx, tx)
		if err != nil {
			return res, err
		}
		res.Preconditions = answers
		res.OK = true
		for _, a := range answers {
			if a.Blocked() {
				res.OK = false
			}
		}

	case "census":
		rows, err := runCensus(ctx, tx)
		if err != nil {
			return res, err
		}
		res.Census = rows
		// A census cannot fail a gate — it reports what is there. OK means
		// the questions were answerable, not that the answers are good.
		res.OK = true

	default:
		return res, fmt.Errorf(
			"unknown action %q (want \"shadow\", \"preconditions\" or \"census\")", req.Action)
	}
	return res, nil
}

// logSummary writes AGGREGATES ONLY.
//
// Counts, not rows. Nothing here identifies an account, and nothing here is a
// money figure — a per-account delta is exactly the kind of number that reads
// as harmless and is not.
func logSummary(log *slog.Logger, res Response) {
	switch {
	case res.Shadow != nil:
		log.Info("intent-shadow run complete",
			"action", res.Action,
			"ok", res.OK,
			"compared", res.Shadow.Compared,
			"agreed", res.Shadow.Agreed,
			"unexplained", res.Shadow.Unexplained())
	case res.Preconditions != nil:
		blocked := 0
		for _, p := range res.Preconditions {
			if p.Blocked() {
				blocked++
			}
		}
		log.Info("legacy-drop preconditions complete",
			"action", res.Action,
			"ok", res.OK,
			"questions", len(res.Preconditions),
			"blocked", blocked)
	case res.Census != nil:
		// How many questions were answered, NOT what they answered. A table
		// total is not a customer row, but "how many accounts exist" is a
		// business figure and CloudWatch has a wider audience than the
		// invoke permission — so the detail travels in the result only, and
		// there stays one rule rather than a judgement call per subject.
		log.Info("billing census complete",
			"action", res.Action,
			"ok", res.OK,
			"subjects", len(res.Census))
	default:
		log.Info("intent-shadow run complete", "action", res.Action, "ok", res.OK)
	}
}

// startLambda is the Lambda transport.
func startLambda(pool *pgxpool.Pool, log *slog.Logger) {
	lambda.Start(handler(pool, log))
}
