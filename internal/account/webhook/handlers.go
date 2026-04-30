package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stripe/stripe-go/v85"

	accountdb "github.com/mirrorstack-ai/billing-engine/internal/account/db"
)

// defaultDispatch wires Stripe event types to the real handlers.
// Unknown types fall through to the "no handler" branch in Process().
func defaultDispatch() map[string]HandlerFunc {
	return map[string]HandlerFunc{
		"customer.subscription.created": handleSubscriptionCreated,
		"customer.subscription.updated": handleSubscriptionUpdated,
		"customer.subscription.deleted": handleSubscriptionDeleted,
		"invoice.paid":                  handleInvoicePaid,
		"invoice.payment_failed":        handleInvoicePaymentFailed,
	}
}

// handleSubscriptionCreated mirrors a new Stripe subscription into
// billing_subscriptions and its items.
func handleSubscriptionCreated(ctx context.Context, tx pgx.Tx, event stripe.Event) error {
	return upsertSubscriptionFromEvent(ctx, tx, event)
}

// handleSubscriptionUpdated re-syncs an existing mirror row. Stripe
// does not guarantee strict event ordering, so the create and update
// paths both go through the same upsert — it's the only safe option
// when an `updated` could land before its `created`.
func handleSubscriptionUpdated(ctx context.Context, tx pgx.Tx, event stripe.Event) error {
	return upsertSubscriptionFromEvent(ctx, tx, event)
}

// handleSubscriptionDeleted marks the mirror row as canceled. We don't
// hard-delete because invoices and audit history reference it.
func handleSubscriptionDeleted(ctx context.Context, tx pgx.Tx, event stripe.Event) error {
	sub, err := decodeSubscription(event)
	if err != nil {
		return err
	}
	if sub.ID == "" {
		return errors.New("subscription event missing id")
	}
	if _, err := tx.Exec(ctx, accountdb.UpdateSubscriptionStatusSQL, sub.ID, "canceled"); err != nil {
		return fmt.Errorf("mark subscription canceled: %w", err)
	}
	return nil
}

// handleInvoicePaid mirrors a paid invoice. Stripe's `paid` status is
// terminal for the invoice, so we don't need to chain any subscription
// updates here — the subscription transition (e.g. past_due → active)
// rides on a separate `customer.subscription.updated` event.
func handleInvoicePaid(ctx context.Context, tx pgx.Tx, event stripe.Event) error {
	return upsertInvoiceFromEvent(ctx, tx, event)
}

// handleInvoicePaymentFailed mirrors the invoice and ALSO flips the
// parent subscription to past_due. We don't wait for Stripe's separate
// subscription.updated because that event can arrive minutes later and
// we want the read path to reflect the failed payment immediately.
func handleInvoicePaymentFailed(ctx context.Context, tx pgx.Tx, event stripe.Event) error {
	inv, err := decodeInvoice(event)
	if err != nil {
		return err
	}
	if err := upsertInvoice(ctx, tx, inv); err != nil {
		return err
	}
	if subID := invoiceSubscriptionID(inv); subID != "" {
		if _, err := tx.Exec(ctx, accountdb.UpdateSubscriptionStatusSQL, subID, "past_due"); err != nil {
			return fmt.Errorf("mark subscription past_due: %w", err)
		}
	}
	return nil
}

func upsertSubscriptionFromEvent(ctx context.Context, tx pgx.Tx, event stripe.Event) error {
	sub, err := decodeSubscription(event)
	if err != nil {
		return err
	}
	if sub.ID == "" {
		return errors.New("subscription event missing id")
	}
	if sub.Customer == nil || sub.Customer.ID == "" {
		return errors.New("subscription event missing customer id")
	}

	accountID, err := accountdb.LookupBillingAccountID(ctx, tx, sub.Customer.ID)
	if err != nil {
		// Account-not-found is non-retryable — Stripe redelivery won't
		// conjure a billing_accounts row out of nowhere. Log and treat
		// as success so the dedup row commits.
		if errors.Is(err, accountdb.ErrAccountNotFound) {
			slog.WarnContext(ctx, "no billing account for stripe customer; dropping event",
				"stripe_customer_id", sub.Customer.ID, "stripe_subscription_id", sub.ID)
			return nil
		}
		return err
	}

	periodStart, periodEnd := subscriptionPeriod(sub)
	subscriptionUUID, err := accountdb.UpsertSubscription(ctx, tx, accountdb.SubscriptionUpsert{
		BillingAccountID:     accountID,
		StripeSubscriptionID: sub.ID,
		Status:               string(sub.Status),
		CurrentPeriodStart:   periodStart,
		CurrentPeriodEnd:     periodEnd,
		CancelAtPeriodEnd:    sub.CancelAtPeriodEnd,
	})
	if err != nil {
		return err
	}
	if sub.Items != nil {
		for _, item := range sub.Items.Data {
			if item == nil {
				continue
			}
			if err := upsertSubscriptionItem(ctx, tx, subscriptionUUID, item); err != nil {
				return err
			}
		}
	}
	return nil
}

func upsertInvoiceFromEvent(ctx context.Context, tx pgx.Tx, event stripe.Event) error {
	inv, err := decodeInvoice(event)
	if err != nil {
		return err
	}
	return upsertInvoice(ctx, tx, inv)
}

// upsertInvoice handles the lookup + upsert. Split out because the
// payment_failed handler also needs a subscription-status update.
func upsertInvoice(ctx context.Context, tx pgx.Tx, inv *stripe.Invoice) error {
	if inv.ID == "" {
		return errors.New("invoice event missing id")
	}
	if inv.Customer == nil || inv.Customer.ID == "" {
		return errors.New("invoice event missing customer id")
	}
	accountID, err := accountdb.LookupBillingAccountID(ctx, tx, inv.Customer.ID)
	if err != nil {
		if errors.Is(err, accountdb.ErrAccountNotFound) {
			slog.WarnContext(ctx, "no billing account for stripe customer; dropping invoice",
				"stripe_customer_id", inv.Customer.ID, "stripe_invoice_id", inv.ID)
			return nil
		}
		return err
	}
	return accountdb.UpsertInvoice(ctx, tx, accountdb.InvoiceUpsert{
		BillingAccountID: accountID,
		StripeInvoiceID:  inv.ID,
		Status:           string(inv.Status),
		Total:            inv.Total,
		Currency:         string(inv.Currency),
		HostedInvoiceURL: inv.HostedInvoiceURL,
		PeriodStart:      unixToTime(inv.PeriodStart),
		PeriodEnd:        unixToTime(inv.PeriodEnd),
	})
}

func upsertSubscriptionItem(ctx context.Context, tx pgx.Tx, subscriptionUUID uuid.UUID, item *stripe.SubscriptionItem) error {
	kind, metric, quantity := classifyItem(item)
	in := accountdb.SubscriptionItemUpsert{
		SubscriptionID:           subscriptionUUID,
		Kind:                     kind,
		StripeSubscriptionItemID: item.ID,
	}
	if kind == "meter" {
		m := metric
		in.Metric = &m
	} else {
		q := quantity
		in.Quantity = &q
	}
	if mod := moduleIDFromMetadata(item); mod != "" {
		in.ModuleID = &mod
	}
	return accountdb.UpsertSubscriptionItem(ctx, tx, in)
}

// decodeSubscription unmarshals event.Data.Raw into a Subscription.
// Using Raw (rather than Object) keeps the typed fields populated and
// avoids re-implementing every nested struct's JSON shape.
func decodeSubscription(event stripe.Event) (*stripe.Subscription, error) {
	if event.Data == nil {
		return nil, errors.New("event has no data")
	}
	var sub stripe.Subscription
	if err := json.Unmarshal(event.Data.Raw, &sub); err != nil {
		return nil, fmt.Errorf("decode subscription: %w", err)
	}
	return &sub, nil
}

func decodeInvoice(event stripe.Event) (*stripe.Invoice, error) {
	if event.Data == nil {
		return nil, errors.New("event has no data")
	}
	var inv stripe.Invoice
	if err := json.Unmarshal(event.Data.Raw, &inv); err != nil {
		return nil, fmt.Errorf("decode invoice: %w", err)
	}
	return &inv, nil
}

// subscriptionPeriod derives the current billing period for a
// Subscription. In Stripe API 2024-09-30+ the period fields moved off
// Subscription onto each SubscriptionItem; we take the first item's
// period as the representative because items in a single subscription
// share a billing cycle.
func subscriptionPeriod(sub *stripe.Subscription) (time.Time, time.Time) {
	if sub.Items != nil {
		for _, item := range sub.Items.Data {
			if item == nil {
				continue
			}
			if item.CurrentPeriodStart != 0 || item.CurrentPeriodEnd != 0 {
				return unixToTime(item.CurrentPeriodStart), unixToTime(item.CurrentPeriodEnd)
			}
		}
	}
	// No item period: use start_date as a defensible fallback for the
	// start, and equal-to-start for end. The subscription will be
	// re-synced on the next `updated` event with real period data.
	start := unixToTime(sub.StartDate)
	return start, start
}

// invoiceSubscriptionID extracts the subscription id from the new
// invoice.parent shape introduced in stripe-go v85.
func invoiceSubscriptionID(inv *stripe.Invoice) string {
	if inv.Parent == nil || inv.Parent.SubscriptionDetails == nil {
		return ""
	}
	if inv.Parent.SubscriptionDetails.Subscription == nil {
		return ""
	}
	return inv.Parent.SubscriptionDetails.Subscription.ID
}

// classifyItem decides whether an item should be persisted as a 'seat'
// row (with quantity) or a 'meter' row (with metric). The rule mirrors
// the DB CHECK constraint: metered prices → meter, else seat.
func classifyItem(item *stripe.SubscriptionItem) (kind, metric string, quantity int64) {
	if item.Price != nil && item.Price.Recurring != nil &&
		item.Price.Recurring.UsageType == stripe.PriceRecurringUsageTypeMetered {
		// Prefer the meter id when present — it's the stable handle for
		// usage reporting. Fall back to the literal "metered" so the
		// metric column is always non-empty for metered rows.
		m := item.Price.Recurring.Meter
		if m == "" {
			m = string(stripe.PriceRecurringUsageTypeMetered)
		}
		return "meter", m, 0
	}
	return "seat", "", item.Quantity
}

// moduleIDFromMetadata pulls a MirrorStack module id from Stripe item
// metadata when present. Modules tag themselves by setting
// metadata.module_id on the price; the mirror table preserves it so the
// account-api can join subscriptions to module-specific UI.
func moduleIDFromMetadata(item *stripe.SubscriptionItem) string {
	if item.Metadata != nil {
		if m, ok := item.Metadata["module_id"]; ok {
			return m
		}
	}
	if item.Price != nil && item.Price.Metadata != nil {
		if m, ok := item.Price.Metadata["module_id"]; ok {
			return m
		}
	}
	return ""
}

// unixToTime is a small helper to keep call sites readable.
func unixToTime(s int64) time.Time {
	if s == 0 {
		return time.Time{}
	}
	return time.Unix(s, 0).UTC()
}
