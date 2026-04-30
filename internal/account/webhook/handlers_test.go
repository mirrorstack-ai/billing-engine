package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stripe/stripe-go/v85"
)

// recordingTx is a pgx.Tx mock that records every Exec/QueryRow call so
// the handler tests can assert on the SQL fingerprint and parameters
// without standing up Postgres.
type recordingTx struct {
	execs    []recordedExec
	queries  []recordedQuery
	queryRow func(sql string, args []any) pgx.Row
}

type recordedExec struct {
	sql  string
	args []any
}

type recordedQuery struct {
	sql  string
	args []any
}

func (t *recordingTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.execs = append(t.execs, recordedExec{sql: sql, args: args})
	return pgconn.NewCommandTag("UPSERT 1"), nil
}

func (t *recordingTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	t.queries = append(t.queries, recordedQuery{sql: sql, args: args})
	if t.queryRow != nil {
		return t.queryRow(sql, args)
	}
	return staticRow{}
}

// Unused methods.
func (t *recordingTx) Begin(_ context.Context) (pgx.Tx, error)    { panic("Begin not used") }
func (t *recordingTx) Commit(_ context.Context) error             { return nil }
func (t *recordingTx) Rollback(_ context.Context) error           { return nil }
func (t *recordingTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("CopyFrom not used")
}
func (t *recordingTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	panic("SendBatch not used")
}
func (t *recordingTx) LargeObjects() pgx.LargeObjects { panic("LargeObjects not used") }
func (t *recordingTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	panic("Prepare not used")
}
func (t *recordingTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	panic("Query not used")
}
func (t *recordingTx) Conn() *pgx.Conn { panic("Conn not used") }

// staticRow returns a pre-baked uuid for the LookupBillingAccountID
// QueryRow path. It's the simplest mock that satisfies the pgx.Row
// interface used by `Scan`.
type staticRow struct {
	val uuid.UUID
	err error
}

func (r staticRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) == 0 {
		return errors.New("scan: no destinations")
	}
	if u, ok := dest[0].(*uuid.UUID); ok {
		if r.val == uuid.Nil {
			// Hand a deterministic non-nil uuid so subsequent inserts
			// have a real billing_account_id.
			*u = uuid.MustParse("11111111-1111-1111-1111-111111111111")
		} else {
			*u = r.val
		}
	}
	return nil
}

func TestHandleSubscriptionCreated_InsertsSubscriptionAndItems(t *testing.T) {
	tx := &recordingTx{
		queryRow: func(_ string, _ []any) pgx.Row { return staticRow{} },
	}

	event := mustEvent(t, "customer.subscription.created", map[string]any{
		"id":                   "sub_123",
		"object":               "subscription",
		"status":               "active",
		"start_date":           1_700_000_000,
		"cancel_at_period_end": false,
		"customer":             "cus_abc",
		"items": map[string]any{
			"object": "list",
			"data": []map[string]any{
				{
					"id":                   "si_seat",
					"object":               "subscription_item",
					"quantity":             5,
					"current_period_start": 1_700_000_000,
					"current_period_end":   1_702_592_000,
					"price": map[string]any{
						"id":     "price_seat",
						"object": "price",
						"recurring": map[string]any{
							"usage_type": "licensed",
						},
					},
				},
				{
					"id":     "si_meter",
					"object": "subscription_item",
					"price": map[string]any{
						"id":     "price_meter",
						"object": "price",
						"recurring": map[string]any{
							"usage_type": "metered",
							"meter":      "mtr_tokens",
						},
					},
				},
			},
		},
	})

	if err := handleSubscriptionCreated(context.Background(), tx, event); err != nil {
		t.Fatalf("handler error: %v", err)
	}

	// 1 lookup query + 1 subscription upsert (also a QueryRow) + 2 item upserts (Exec)
	if len(tx.queries) != 2 {
		t.Fatalf("expected 2 QueryRow calls (account lookup + subscription RETURNING); got %d", len(tx.queries))
	}
	if len(tx.execs) != 2 {
		t.Fatalf("expected 2 item upsert Execs; got %d", len(tx.execs))
	}

	// Check that one Exec is a meter row (metric set, quantity nil) and
	// the other is a seat row (quantity set, metric nil).
	var sawMeter, sawSeat bool
	for _, e := range tx.execs {
		if !strings.Contains(e.sql, "billing_subscription_items") {
			t.Fatalf("expected item-upsert SQL; got %q", e.sql)
		}
		kind, _ := e.args[1].(string)
		switch kind {
		case "meter":
			sawMeter = true
			if q, _ := e.args[5].(*int64); q != nil {
				t.Errorf("meter row should pass quantity=nil; got %#v", e.args[5])
			}
			metric, _ := e.args[4].(*string)
			if metric == nil || *metric != "mtr_tokens" {
				t.Errorf("expected meter metric=mtr_tokens; got %#v", e.args[4])
			}
		case "seat":
			sawSeat = true
			if m, _ := e.args[4].(*string); m != nil {
				t.Errorf("seat row should pass metric=nil; got %#v", e.args[4])
			}
			quantity, _ := e.args[5].(*int64)
			if quantity == nil || *quantity != 5 {
				t.Errorf("expected seat quantity=5; got %#v", e.args[5])
			}
		default:
			t.Errorf("unexpected kind: %q", kind)
		}
	}
	if !sawMeter || !sawSeat {
		t.Fatalf("expected one meter and one seat Exec; meter=%v seat=%v", sawMeter, sawSeat)
	}
}

func TestHandleSubscriptionUpdated_GoesThroughUpsertPath(t *testing.T) {
	tx := &recordingTx{}
	event := mustEvent(t, "customer.subscription.updated", map[string]any{
		"id":         "sub_upd",
		"object":     "subscription",
		"status":     "active",
		"customer":   "cus_xyz",
		"start_date": 1_700_000_000,
		"items": map[string]any{
			"object": "list",
			"data":   []map[string]any{},
		},
	})
	if err := handleSubscriptionUpdated(context.Background(), tx, event); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if len(tx.queries) != 2 {
		t.Fatalf("expected lookup + upsert RETURNING; got %d", len(tx.queries))
	}
}

func TestHandleSubscriptionDeleted_FlipsStatusCanceled(t *testing.T) {
	tx := &recordingTx{}
	event := mustEvent(t, "customer.subscription.deleted", map[string]any{
		"id":     "sub_gone",
		"object": "subscription",
	})

	if err := handleSubscriptionDeleted(context.Background(), tx, event); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if len(tx.execs) != 1 {
		t.Fatalf("expected one UPDATE Exec; got %d", len(tx.execs))
	}
	if !strings.Contains(tx.execs[0].sql, "billing_subscriptions") {
		t.Fatalf("unexpected SQL: %q", tx.execs[0].sql)
	}
	if tx.execs[0].args[0] != "sub_gone" || tx.execs[0].args[1] != "canceled" {
		t.Fatalf("unexpected args: %#v", tx.execs[0].args)
	}
}

func TestHandleInvoicePaid_UpsertsInvoice(t *testing.T) {
	tx := &recordingTx{}
	event := mustEvent(t, "invoice.paid", map[string]any{
		"id":                 "in_1",
		"object":             "invoice",
		"status":             "paid",
		"customer":           "cus_inv",
		"total":              1999,
		"currency":           "usd",
		"hosted_invoice_url": "https://pay.example/in_1",
		"period_start":       1_700_000_000,
		"period_end":         1_702_592_000,
	})

	if err := handleInvoicePaid(context.Background(), tx, event); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if len(tx.queries) != 1 {
		t.Fatalf("expected 1 lookup QueryRow; got %d", len(tx.queries))
	}
	if len(tx.execs) != 1 {
		t.Fatalf("expected 1 invoice upsert Exec; got %d", len(tx.execs))
	}
	if !strings.Contains(tx.execs[0].sql, "billing_invoices") {
		t.Fatalf("unexpected SQL: %q", tx.execs[0].sql)
	}
	if tx.execs[0].args[2] != "paid" {
		t.Fatalf("expected status=paid; got %#v", tx.execs[0].args[2])
	}
}

func TestHandleInvoicePaymentFailed_UpsertsAndMarksPastDue(t *testing.T) {
	tx := &recordingTx{}
	event := mustEvent(t, "invoice.payment_failed", map[string]any{
		"id":           "in_fail",
		"object":       "invoice",
		"status":       "open",
		"customer":     "cus_inv",
		"total":        9900,
		"currency":     "usd",
		"period_start": 1_700_000_000,
		"period_end":   1_702_592_000,
		"parent": map[string]any{
			"type": "subscription_details",
			"subscription_details": map[string]any{
				"subscription": "sub_owner",
			},
		},
	})

	if err := handleInvoicePaymentFailed(context.Background(), tx, event); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if len(tx.execs) != 2 {
		t.Fatalf("expected 2 Execs (invoice + sub status); got %d", len(tx.execs))
	}
	// Second exec is the past_due update.
	last := tx.execs[1]
	if !strings.Contains(last.sql, "billing_subscriptions") {
		t.Fatalf("expected subscription update; got %q", last.sql)
	}
	if last.args[0] != "sub_owner" || last.args[1] != "past_due" {
		t.Fatalf("unexpected past_due args: %#v", last.args)
	}
}

func TestHandleInvoicePaymentFailed_NoSubscription_StillUpsertsInvoice(t *testing.T) {
	tx := &recordingTx{}
	event := mustEvent(t, "invoice.payment_failed", map[string]any{
		"id":           "in_no_sub",
		"object":       "invoice",
		"status":       "open",
		"customer":     "cus_inv",
		"currency":     "usd",
		"period_start": 1,
		"period_end":   2,
	})
	if err := handleInvoicePaymentFailed(context.Background(), tx, event); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if len(tx.execs) != 1 {
		t.Fatalf("expected just the invoice upsert; got %d", len(tx.execs))
	}
}

func TestHandleSubscription_AccountNotFound_ReturnsNil(t *testing.T) {
	// Lookup miss must NOT bubble up as an error; the dedup row commits
	// and we drop the event with a warning. Stripe retrying won't help.
	tx := &recordingTx{
		queryRow: func(_ string, _ []any) pgx.Row {
			return staticRow{err: pgx.ErrNoRows}
		},
	}
	event := mustEvent(t, "customer.subscription.created", map[string]any{
		"id":         "sub_orphan",
		"object":     "subscription",
		"status":     "active",
		"customer":   "cus_unknown",
		"start_date": 1_700_000_000,
		"items": map[string]any{
			"object": "list",
			"data":   []map[string]any{},
		},
	})

	if err := handleSubscriptionCreated(context.Background(), tx, event); err != nil {
		t.Fatalf("expected nil error on account-not-found; got %v", err)
	}
	if len(tx.execs) != 0 {
		t.Fatalf("must not write subscription rows when account is unknown; got %d execs", len(tx.execs))
	}
}

func TestHandleSubscription_MissingCustomer_ReturnsError(t *testing.T) {
	tx := &recordingTx{}
	event := mustEvent(t, "customer.subscription.created", map[string]any{
		"id":     "sub_nocust",
		"object": "subscription",
	})
	err := handleSubscriptionCreated(context.Background(), tx, event)
	if err == nil {
		t.Fatalf("expected error when customer id missing")
	}
}

func TestClassifyItem_SeatVsMeter(t *testing.T) {
	seat := &stripe.SubscriptionItem{
		Quantity: 3,
		Price: &stripe.Price{
			Recurring: &stripe.PriceRecurring{UsageType: stripe.PriceRecurringUsageTypeLicensed},
		},
	}
	if k, _, q := classifyItem(seat); k != "seat" || q != 3 {
		t.Errorf("seat: got kind=%q q=%d", k, q)
	}

	meter := &stripe.SubscriptionItem{
		Price: &stripe.Price{
			Recurring: &stripe.PriceRecurring{
				UsageType: stripe.PriceRecurringUsageTypeMetered,
				Meter:     "mtr_x",
			},
		},
	}
	if k, m, _ := classifyItem(meter); k != "meter" || m != "mtr_x" {
		t.Errorf("meter: got kind=%q metric=%q", k, m)
	}

	// Metered without explicit meter id falls back to the literal.
	meterFallback := &stripe.SubscriptionItem{
		Price: &stripe.Price{
			Recurring: &stripe.PriceRecurring{UsageType: stripe.PriceRecurringUsageTypeMetered},
		},
	}
	if k, m, _ := classifyItem(meterFallback); k != "meter" || m != "metered" {
		t.Errorf("meter fallback: got kind=%q metric=%q", k, m)
	}
}

// mustEvent builds a stripe.Event from a typed body map. Going through
// JSON keeps the test honest against stripe-go's UnmarshalJSON quirks
// (e.g. Customer being either a bare id or a full object).
func mustEvent(t *testing.T, eventType string, body map[string]any) stripe.Event {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	envelope := map[string]any{
		"id":     "evt_test",
		"object": "event",
		"type":   eventType,
		"data":   map[string]any{"object": body},
	}
	envBytes, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	var ev stripe.Event
	if err := json.Unmarshal(envBytes, &ev); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	// Sanity: event.Data.Raw should now be the inner body.
	if len(ev.Data.Raw) == 0 || string(ev.Data.Raw) == "{}" {
		t.Fatalf("event.Data.Raw not populated; got %q (body=%s)", string(ev.Data.Raw), string(raw))
	}
	return ev
}
