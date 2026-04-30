package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stripe/stripe-go/v85"
	stripewebhook "github.com/stripe/stripe-go/v85/webhook"
)

// fakeDB is a minimal EventDB stub. It records the most recent Exec
// call and returns a configurable CommandTag/error pair.
type fakeDB struct {
	calls   atomic.Int32
	lastSQL string
	lastID  string
	tag     pgconn.CommandTag
	err     error
}

func (f *fakeDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.calls.Add(1)
	f.lastSQL = sql
	if len(args) > 0 {
		if s, ok := args[0].(string); ok {
			f.lastID = s
		}
	}
	return f.tag, f.err
}

// signedFixture builds a real, valid Stripe webhook payload + signature
// header using the SDK's test helper. Using the real signer (not a
// hand-rolled HMAC) keeps the test honest against any future signature
// scheme changes inside stripe-go.
func signedFixture(t *testing.T, secret, eventID, eventType string) (payload []byte, header string) {
	t.Helper()
	body := fmt.Sprintf(`{"id":%q,"object":"event","type":%q,"api_version":"2099-01-01.preview","data":{"object":{}}}`, eventID, eventType)
	signed := stripewebhook.GenerateTestSignedPayload(&stripewebhook.UnsignedPayload{
		Payload: []byte(body),
		Secret:  secret,
	})
	return signed.Payload, signed.Header
}

func TestProcess_BadSignature_Returns400(t *testing.T) {
	db := &fakeDB{}
	h := NewHandler("whsec_correct", db)

	// Sign with the wrong secret; the handler verifies with the right one.
	payload, header := signedFixture(t, "whsec_wrong", "evt_bad", "customer.subscription.created")

	got := h.Process(context.Background(), payload, header)

	if got.StatusCode != 400 {
		t.Fatalf("status: want 400, got %d (body=%q)", got.StatusCode, got.Body)
	}
	if db.calls.Load() != 0 {
		t.Fatalf("DB should not be touched on bad signature; got %d calls", db.calls.Load())
	}
}

func TestProcess_ValidEvent_FirstTime_DispatchesAndInsertsRow(t *testing.T) {
	const secret = "whsec_test"
	db := &fakeDB{tag: pgconn.NewCommandTag("INSERT 0 1")}
	h := NewHandler(secret, db)

	var called atomic.Int32
	h.dispatch["customer.subscription.created"] = func(_ context.Context, _ stripe.Event) error {
		called.Add(1)
		return nil
	}

	payload, header := signedFixture(t, secret, "evt_first", "customer.subscription.created")
	got := h.Process(context.Background(), payload, header)

	if got.StatusCode != 200 {
		t.Fatalf("status: want 200, got %d (body=%q)", got.StatusCode, got.Body)
	}
	if called.Load() != 1 {
		t.Fatalf("handler should run exactly once on first delivery; got %d", called.Load())
	}
	if db.calls.Load() != 1 {
		t.Fatalf("DB should be hit once for dedup insert; got %d", db.calls.Load())
	}
	if !strings.Contains(db.lastSQL, "webhook_events_processed") {
		t.Fatalf("unexpected SQL: %s", db.lastSQL)
	}
	if db.lastID != "evt_first" {
		t.Fatalf("expected event id evt_first; got %q", db.lastID)
	}
}

func TestProcess_ValidEvent_Replay_SkipsDispatch(t *testing.T) {
	const secret = "whsec_test"
	// RowsAffected==0 simulates ON CONFLICT DO NOTHING firing.
	db := &fakeDB{tag: pgconn.NewCommandTag("INSERT 0 0")}
	h := NewHandler(secret, db)

	var called atomic.Int32
	h.dispatch["customer.subscription.updated"] = func(_ context.Context, _ stripe.Event) error {
		called.Add(1)
		return nil
	}

	payload, header := signedFixture(t, secret, "evt_replay", "customer.subscription.updated")
	got := h.Process(context.Background(), payload, header)

	if got.StatusCode != 200 {
		t.Fatalf("status: want 200, got %d (body=%q)", got.StatusCode, got.Body)
	}
	if called.Load() != 0 {
		t.Fatalf("handler must NOT run on replay; got %d calls", called.Load())
	}
	if got.Body != "duplicate" {
		t.Fatalf("expected duplicate body; got %q", got.Body)
	}
}

func TestProcess_UnknownEventType_Returns200_NoHandler(t *testing.T) {
	const secret = "whsec_test"
	db := &fakeDB{tag: pgconn.NewCommandTag("INSERT 0 1")}
	h := NewHandler(secret, db)

	// Pre-flight: assert the type really is unregistered, otherwise
	// the test gives a false negative.
	if _, ok := h.dispatch["customer.created"]; ok {
		t.Fatalf("test fixture assumes customer.created is NOT registered")
	}

	payload, header := signedFixture(t, secret, "evt_unknown", "customer.created")
	got := h.Process(context.Background(), payload, header)

	if got.StatusCode != 200 {
		t.Fatalf("status: want 200, got %d (body=%q)", got.StatusCode, got.Body)
	}
	if got.Body != "ignored" {
		t.Fatalf("expected 'ignored' body; got %q", got.Body)
	}
	if db.calls.Load() != 1 {
		t.Fatalf("dedup row should still be inserted; got %d DB calls", db.calls.Load())
	}
}

func TestProcess_DBError_Returns500(t *testing.T) {
	const secret = "whsec_test"
	db := &fakeDB{err: errors.New("connection refused")}
	h := NewHandler(secret, db)

	payload, header := signedFixture(t, secret, "evt_dberr", "invoice.paid")
	got := h.Process(context.Background(), payload, header)

	if got.StatusCode != 500 {
		t.Fatalf("status: want 500 on DB error so Stripe retries; got %d", got.StatusCode)
	}
}

func TestDefaultDispatch_CoversIssue8EventTypes(t *testing.T) {
	want := []string{
		"customer.subscription.created",
		"customer.subscription.updated",
		"customer.subscription.deleted",
		"invoice.paid",
		"invoice.payment_failed",
	}
	got := defaultDispatch()
	for _, name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("default dispatch missing handler for %q", name)
		}
	}
}
