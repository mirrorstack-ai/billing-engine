// Command pm-default-backfill is a one-shot Stripe-only repair for core#137.
// It finds billing accounts with a usable mirrored payment method but no
// Stripe Customer invoice-settings default, then sets the chosen mirror row as
// that default. Dry-run is enabled by default; the command never writes to the
// billing database, whose mirror converges through customer.updated webhooks.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

func main() {
	dryRun := flag.Bool("dry-run", true, "log Stripe Customer default-PM updates without applying them")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	ctx := context.Background()
	pool := config.MustPgxPool()
	stripeKey := config.MustEnv("STRIPE_SECRET_KEY")
	sc := billingstripe.NewClient(stripeKey)
	q := db.New(pool)

	candidates, err := q.ListPMDefaultBackfillCandidates(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "list payment-method default backfill candidates failed", "error", err)
		pool.Close()
		os.Exit(1)
	}

	failed := 0
	for _, candidate := range candidates {
		customer, err := sc.GetCustomer(ctx, candidate.StripeCustomerID)
		if err != nil {
			failed++
			slog.ErrorContext(ctx, "payment-method default backfill account failed",
				"stripe_customer_id", candidate.StripeCustomerID,
				"chosen_stripe_payment_method_id", candidate.ChosenStripePaymentMethodID,
				"action", "skip", "error", err)
			continue
		}

		if defaultPaymentMethodID(customer) != "" {
			slog.InfoContext(ctx, "payment-method default backfill: skip (already set)",
				"stripe_customer_id", candidate.StripeCustomerID,
				"chosen_stripe_payment_method_id", candidate.ChosenStripePaymentMethodID,
				"action", "skip")
			continue
		}

		if *dryRun {
			slog.InfoContext(ctx, "payment-method default backfill account",
				"stripe_customer_id", candidate.StripeCustomerID,
				"chosen_stripe_payment_method_id", candidate.ChosenStripePaymentMethodID,
				"action", "would-set")
			continue
		}

		if err := sc.SetDefaultPaymentMethod(ctx, candidate.StripeCustomerID, candidate.ChosenStripePaymentMethodID); err != nil {
			failed++
			slog.ErrorContext(ctx, "payment-method default backfill account failed",
				"stripe_customer_id", candidate.StripeCustomerID,
				"chosen_stripe_payment_method_id", candidate.ChosenStripePaymentMethodID,
				"action", "set", "error", err)
			continue
		}

		slog.InfoContext(ctx, "payment-method default backfill account",
			"stripe_customer_id", candidate.StripeCustomerID,
			"chosen_stripe_payment_method_id", candidate.ChosenStripePaymentMethodID,
			"action", "set")
	}

	pool.Close()
	if failed > 0 {
		os.Exit(1)
	}
}

func defaultPaymentMethodID(customer *stripego.Customer) string {
	if customer == nil || customer.InvoiceSettings == nil || customer.InvoiceSettings.DefaultPaymentMethod == nil {
		return ""
	}
	return customer.InvoiceSettings.DefaultPaymentMethod.ID
}
