//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditpurchase"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditrecovery"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook/webhooktest"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/stripe/stripetest"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// docs/SECURITY.md §2: "Both HTTPS-webhook and EventBridge callback
// binaries currently wire Stripe-writing auto-top-up and
// credit-purchase executors into their routers. A callback path is not
// read-only merely because its input was authenticated."
//
// This drives the real eventBridgeHandler against a real database with
// a recording provider client, and asserts the property the gap row is
// about: a callback reconciles what already happened, and never
// originates a charge.
//
// The payloads are synthesized rather than captured. Their shape was
// read from the Stripe sandbox's own event history — the envelope
// fields, the invoice object's status/customer/amount/currency, and the
// ms_charge_ref metadata the engine routes on — but this repository is
// public, and a sandbox event carries real customer emails and ids.
// Test mode means no money moves; it does not mean the records are
// fake.
func TestEventBridgeReplayReconcilesAndNeverOriginates(t *testing.T) {
	pool := testutil.NewTestDB(t)
	recorder := stripetest.New()

	customerID := "cus_replay_probe"
	seedWebhookAccount(t, pool, customerID)

	handler := eventBridgeHandler(productionShapedRouter(t, pool, recorder))

	for _, tc := range []struct {
		name      string
		eventType stripego.EventType
		status    string
	}{
		{"paid", stripego.EventTypeInvoicePaid, "paid"},
		{"payment failed", stripego.EventTypeInvoicePaymentFailed, "open"},
		{"marked uncollectible", stripego.EventTypeInvoiceMarkedUncollectible, "uncollectible"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder.Reset()

			err := handler(context.Background(), events.EventBridgeEvent{
				ID:         "eb-" + tc.status,
				Source:     "aws.partner/stripe.com/ed_replay",
				DetailType: string(tc.eventType),
				Detail:     invoiceEventDetail(t, "evt_"+tc.status, tc.eventType, customerID, tc.status),
			})
			require.NoError(t, err)

			// The whole point of the row: authenticated input does not
			// make a path read-only. A callback that can charge is a
			// second way to take money, reachable by anything that can
			// deliver an event.
			recorder.RequireNoCollection(t, "the "+tc.name+" callback")
		})
	}
}

// The row this test exists for is about the credit reconcilers, so one
// case has to actually reach them.
//
// An ordinary invoice deliberately never probes the credit path — there
// is a separate test for that — so a replay carrying no credit anchors
// leaves the Stripe-writing executors untouched, and asserting it did
// not collect would describe a code path that never ran. These events
// carry the anchors internal/shared/stripe stamps on a credit invoice,
// so the reconcilers run.
func TestCreditCallbackReachesTheReconcilersAndStillCannotCollect(t *testing.T) {
	pool := testutil.NewTestDB(t)
	recorder := stripetest.New()

	customerID := "cus_replay_credit"
	seedWebhookAccount(t, pool, customerID)
	handler := eventBridgeHandler(productionShapedRouter(t, pool, recorder))

	for _, tc := range []struct {
		name      string
		operation string
		eventType stripego.EventType
		status    string
	}{
		{"auto top-up paid", "auto_topup", stripego.EventTypeInvoicePaid, "paid"},
		{"auto top-up failed", "auto_topup", stripego.EventTypeInvoicePaymentFailed, "open"},
		{"purchase paid", "purchase", stripego.EventTypeInvoicePaid, "paid"},
		{"purchase failed", "purchase", stripego.EventTypeInvoicePaymentFailed, "open"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder.Reset()
			eventID := "evt_credit_" + tc.operation + "_" + tc.status

			err := handler(context.Background(), events.EventBridgeEvent{
				ID: "eb-" + eventID, Source: "aws.partner/stripe.com/ed_replay",
				DetailType: string(tc.eventType),
				Detail: invoiceEventDetailWithMetadata(
					t, eventID, tc.eventType, customerID, tc.status,
					creditInvoiceMetadata(tc.operation, eventID),
				),
			})

			// The reconciler is reached and fails: the ledger row the
			// anchors name does not exist, because seeding a real
			// credit lot is a larger fixture than this test needs. The
			// error is the evidence that the credit path ran — an
			// ordinary invoice returns nil here without touching it.
			//
			// That makes this an assertion about the ERROR path, and
			// the error path is worth asserting on its own: a
			// reconciler that collected while failing to find what it
			// was reconciling would be a serious defect, and nothing
			// else in this repository would notice.
			//
			// 🔴 What is still untested is the happy path — a
			// reconciler that finds its ledger row and completes. That
			// needs seeded credit state and is the honest gap in this
			// test.
			require.Error(t, err, "the credit anchors did not route to the reconcilers; "+
				"this case would assert over a code path that never ran")

			recorder.RequireNoCollection(t, "the "+tc.name+" callback")
		})
	}
}

// A partner bus can deliver the same event more than once. Replay must
// be idempotent, or a redelivery becomes a second settlement.
//
// 🔴 Read what this asserts carefully. The row count is 1 because
// event_id is the PRIMARY KEY of webhook_events_processed
// (003_webhook_events_processed.up.sql:15), so it would be 1 even if
// the handler had run its side effects twice. What is actually shown is
// that the idempotency RECORD is unique and that neither delivery
// collected.
//
// Proving side-effects-once needs a side effect to observe, and an
// ordinary invoice.paid has none — the recorder sees zero provider
// calls on both passes. The stronger version of this test belongs with
// the credit cases, where a reconciler runs, and it needs seeded ledger
// state to reach the branch that would double-apply.
func TestEventBridgeReplayIsIdempotent(t *testing.T) {
	pool := testutil.NewTestDB(t)
	recorder := stripetest.New()

	customerID := "cus_replay_idem"
	seedWebhookAccount(t, pool, customerID)

	handler := eventBridgeHandler(productionShapedRouter(t, pool, recorder))

	detail := invoiceEventDetail(t, "evt_replay_once", stripego.EventTypeInvoicePaid, customerID, "paid")
	evt := events.EventBridgeEvent{
		ID: "eb-1", Source: "aws.partner/stripe.com/ed_replay",
		DetailType: "invoice.paid", Detail: detail,
	}

	require.NoError(t, handler(context.Background(), evt))
	require.NoError(t, handler(context.Background(), evt))

	var processed int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.webhook_events_processed WHERE event_id = $1`,
		"evt_replay_once").Scan(&processed))
	require.Equal(t, 1, processed, "a redelivered event was recorded twice")
	require.NotZero(t, processed, "the event was never recorded at all; both deliveries were dropped "+
		"before reaching the idempotency marker, and this test would assert nothing")

	recorder.RequireNoCollection(t, "a redelivered callback")
}

// 🔴 What this test cannot show, stated so nobody reads more into it.
//
// ProcessTrusted performs no signature verification by construction —
// EventBridge partner events arrive pre-trusted because only Stripe's
// AWS account can publish to the bus. So a replay here proves the
// dispatch and the ledger effect, and proves nothing about
// authentication. The verifier is deliberately one that always errors:
// a correctly wired handler must never call it, and this asserts that
// it does not.
func TestEventBridgeHandlerNeverVerifies(t *testing.T) {
	pool := testutil.NewTestDB(t)
	recorder := stripetest.New()

	customerID := "cus_replay_noverify"
	seedWebhookAccount(t, pool, customerID)

	verifier := &countingVerifier{}
	router := webhook.NewRouter(verifier, webhook.NewStore(pool), recorder, recorder, webhooktest.SilentLogger())

	err := eventBridgeHandler(router)(context.Background(), events.EventBridgeEvent{
		ID: "eb-noverify", Source: "aws.partner/stripe.com/ed_replay",
		DetailType: "invoice.paid",
		Detail:     invoiceEventDetail(t, "evt_noverify", stripego.EventTypeInvoicePaid, customerID, "paid"),
	})

	require.NoError(t, err)
	require.Zero(t, verifier.calls, "the EventBridge path called the signature verifier")
}

// productionShapedRouter wires the router the way cmd's buildRouter
// does, including the Stripe-WRITING reconcilers.
//
// Without them the no-collection assertion would be ceremony: a router
// holding no writer obviously cannot write. docs/SECURITY.md §2's row
// is specifically that "both HTTPS-webhook and EventBridge callback
// binaries currently wire Stripe-writing auto-top-up and
// credit-purchase executors into their routers", so the executors have
// to be present for the test to say anything.
//
// Both executors are constructed against the recorder, so any provider
// call they make during a replay is observed and classified.
func productionShapedRouter(t *testing.T, pool *pgxpool.Pool, recorder *stripetest.Recorder) *webhook.Router {
	t.Helper()

	autoTopUpExecutor := autotopup.NewExecutor(
		autotopup.NewStore(pool), creditledger.NewStore(pool), recorder,
	)
	manualPurchaseExecutor := creditpurchase.NewExecutor(
		creditpurchase.NewStore(pool), creditledger.NewStore(pool), recorder,
	)
	capability := creditrecovery.NewRuntimeCapability(
		func(ctx context.Context) (bool, error) {
			return config.CreditRecoverySchemaReady(ctx, pool)
		},
	)
	autoTopUpRecovery := creditrecovery.GuardWebhookReconciler(capability, autoTopUpExecutor)
	manualRecovery := creditrecovery.GuardWebhookReconciler(capability, manualPurchaseExecutor)

	return webhook.NewRouter(
		refusingVerifier(t), webhook.NewStore(pool), recorder, recorder, webhooktest.SilentLogger(),
	).
		WithCreditPaidReconciler(autoTopUpRecovery).
		WithManualCreditPaidReconciler(manualRecovery).
		WithCreditFailureReconciler(autoTopUpRecovery).
		WithManualCreditFailureReconciler(manualRecovery)
}

// refusingVerifier fails the test if it is called at all. ProcessTrusted
// must never verify a signature: EventBridge partner events arrive
// pre-trusted because only Stripe's AWS account can publish to the bus,
// and a handler that verified would reject every real delivery.
func refusingVerifier(t *testing.T) billingstripe.Verifier {
	t.Helper()
	return verifierFunc(func([]byte, string) (stripego.Event, error) {
		t.Error("the EventBridge path called the signature verifier")
		return stripego.Event{}, errors.New("must not be called")
	})
}

type verifierFunc func([]byte, string) (stripego.Event, error)

func (f verifierFunc) Verify(payload []byte, signature string) (stripego.Event, error) {
	return f(payload, signature)
}

type countingVerifier struct{ calls int }

func (c *countingVerifier) Verify([]byte, string) (stripego.Event, error) {
	c.calls++
	return stripego.Event{}, errors.New("must not be called")
}

// invoiceEventDetail builds an event whose shape matches what Stripe
// actually delivers: the envelope carries id/type/created/livemode, and
// the invoice object carries the status, customer, amounts, currency
// and the ms_charge_ref metadata the engine routes on.
func invoiceEventDetail(t *testing.T, eventID string, eventType stripego.EventType, customerID, status string) []byte {
	t.Helper()
	return invoiceEventDetailWithMetadata(t, eventID, eventType, customerID, status,
		map[string]string{"ms_charge_ref": "run:" + eventID})
}

// creditInvoiceMetadata carries the anchors internal/shared/stripe
// stamps on a credit invoice (client.go:268-270, :318-320), which is
// what routes a callback to the credit reconcilers rather than treating
// it as an ordinary invoice.
//
// Without these the replay never reaches the Stripe-writing executors
// at all, and asserting that it did not collect would be a statement
// about a code path that never ran.
func creditInvoiceMetadata(operation, eventID string) map[string]string {
	return map[string]string{
		"ms_charge_ref":        "credit:" + eventID,
		"ms_credit_operation":  operation,
		"ms_credit_account_id": uuid.NewSHA1(uuid.Nil, []byte("acct-"+eventID)).String(),
		"ms_credit_ledger_id":  uuid.NewSHA1(uuid.Nil, []byte("ledger-"+eventID)).String(),
	}
}

func invoiceEventDetailWithMetadata(
	t *testing.T, eventID string, eventType stripego.EventType,
	customerID, status string, metadata map[string]string,
) []byte {
	t.Helper()

	invoice := map[string]any{
		"id":                "in_" + eventID,
		"object":            "invoice",
		"status":            status,
		"customer":          customerID,
		"amount_due":        2_000,
		"amount_paid":       0,
		"currency":          "usd",
		"collection_method": "charge_automatically",
		"billing_reason":    "manual",
		"attempt_count":     1,
		"metadata":          metadata,
	}
	if status == "paid" {
		invoice["amount_paid"] = 2_000
	}

	raw, err := json.Marshal(invoice)
	require.NoError(t, err)

	event := stripego.Event{
		ID:       eventID,
		Type:     eventType,
		Created:  time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC).Unix(),
		Livemode: false,
		Data:     &stripego.EventData{Raw: raw},
	}
	detail, err := json.Marshal(event)
	require.NoError(t, err)
	return detail
}

func seedWebhookAccount(t *testing.T, pool *pgxpool.Pool, stripeCustomerID string) uuid.UUID {
	t.Helper()
	accountID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, stripe_customer_id)
		 VALUES ($1, 'user', $2, $3)`,
		accountID, uuid.New(), stripeCustomerID,
	)
	require.NoError(t, err)
	return accountID
}
