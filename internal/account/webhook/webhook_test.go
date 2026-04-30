package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stripe/stripe-go/v85"
	stripewebhook "github.com/stripe/stripe-go/v85/webhook"
)

// fakeDB is the minimal EventDB stub. Every Process() call begins a tx,
// so we hand back a fakeTx that records everything done within it.
type fakeDB struct {
	beginErr error
	tx       *fakeTx
	begins   atomic.Int32
}

func (f *fakeDB) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	f.begins.Add(1)
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	if f.tx == nil {
		f.tx = &fakeTx{}
	}
	return f.tx, nil
}

// fakeTx implements pgx.Tx well enough for the webhook tests. Only the
// methods Process() and the dedup insert path actually call are
// non-trivial; the rest panic so a stray call surfaces loudly.
type fakeTx struct {
	execCalls   atomic.Int32
	commitCalls atomic.Int32
	rollback    atomic.Int32
	lastSQL     string
	lastID      string
	dedupTag    pgconn.CommandTag
	execErr     error
	commitErr   error
}

func (t *fakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.execCalls.Add(1)
	t.lastSQL = sql
	if len(args) > 0 {
		if s, ok := args[0].(string); ok {
			t.lastID = s
		}
	}
	if t.execErr != nil {
		return pgconn.CommandTag{}, t.execErr
	}
	return t.dedupTag, nil
}

func (t *fakeTx) Commit(_ context.Context) error {
	t.commitCalls.Add(1)
	return t.commitErr
}

func (t *fakeTx) Rollback(_ context.Context) error {
	t.rollback.Add(1)
	return nil
}

// Unused pgx.Tx methods — webhook code never reaches these.
func (t *fakeTx) Begin(_ context.Context) (pgx.Tx, error)         { panic("Begin not used") }
func (t *fakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("CopyFrom not used")
}
func (t *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { panic("SendBatch not used") }
func (t *fakeTx) LargeObjects() pgx.LargeObjects                          { panic("LargeObjects not used") }
func (t *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	panic("Prepare not used")
}
func (t *fakeTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	panic("Query not used")
}
func (t *fakeTx) QueryRow(context.Context, string, ...any) pgx.Row { panic("QueryRow not used") }
func (t *fakeTx) Conn() *pgx.Conn                                  { panic("Conn not used") }

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

// newHandlerWithStubs builds a Handler whose dispatch entries are
// replaced with no-op stubs so the webhook tests focus on Process()
// orchestration rather than handler internals (covered separately in
// handlers_test.go).
func newHandlerWithStubs(secret string, db EventDB) *Handler {
	h := NewHandler(secret, db)
	stub := func(_ context.Context, _ pgx.Tx, _ stripe.Event) error { return nil }
	for k := range h.dispatch {
		h.dispatch[k] = stub
	}
	return h
}

func TestProcess_BadSignature_Returns400(t *testing.T) {
	db := &fakeDB{}
	h := newHandlerWithStubs("whsec_correct", db)

	// Sign with the wrong secret; the handler verifies with the right one.
	payload, header := signedFixture(t, "whsec_wrong", "evt_bad", "customer.subscription.created")

	got := h.Process(context.Background(), payload, header)

	if got.StatusCode != 400 {
		t.Fatalf("status: want 400, got %d (body=%q)", got.StatusCode, got.Body)
	}
	if db.begins.Load() != 0 {
		t.Fatalf("DB tx should not be opened on bad signature; got %d begins", db.begins.Load())
	}
}

func TestProcess_ValidEvent_FirstTime_DispatchesAndCommits(t *testing.T) {
	const secret = "whsec_test"
	db := &fakeDB{tx: &fakeTx{dedupTag: pgconn.NewCommandTag("INSERT 0 1")}}
	h := newHandlerWithStubs(secret, db)

	var called atomic.Int32
	h.dispatch["customer.subscription.created"] = func(_ context.Context, _ pgx.Tx, _ stripe.Event) error {
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
	if db.tx.execCalls.Load() != 1 {
		t.Fatalf("dedup insert should fire once; got %d Exec calls", db.tx.execCalls.Load())
	}
	if !strings.Contains(db.tx.lastSQL, "billing_webhook_events_processed") {
		t.Fatalf("unexpected SQL: %s", db.tx.lastSQL)
	}
	if db.tx.lastID != "evt_first" {
		t.Fatalf("expected event id evt_first; got %q", db.tx.lastID)
	}
	if db.tx.commitCalls.Load() != 1 {
		t.Fatalf("expected exactly one Commit; got %d", db.tx.commitCalls.Load())
	}
}

func TestProcess_ValidEvent_Replay_SkipsDispatch_Rollback(t *testing.T) {
	const secret = "whsec_test"
	// RowsAffected==0 simulates ON CONFLICT DO NOTHING firing.
	db := &fakeDB{tx: &fakeTx{dedupTag: pgconn.NewCommandTag("INSERT 0 0")}}
	h := newHandlerWithStubs(secret, db)

	var called atomic.Int32
	h.dispatch["customer.subscription.updated"] = func(_ context.Context, _ pgx.Tx, _ stripe.Event) error {
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
	if db.tx.commitCalls.Load() != 0 {
		t.Fatalf("replay path must not commit; got %d commits", db.tx.commitCalls.Load())
	}
	if db.tx.rollback.Load() == 0 {
		t.Fatalf("replay path must roll back; got %d rollbacks", db.tx.rollback.Load())
	}
}

func TestProcess_UnknownEventType_Returns200_CommitsDedup(t *testing.T) {
	const secret = "whsec_test"
	db := &fakeDB{tx: &fakeTx{dedupTag: pgconn.NewCommandTag("INSERT 0 1")}}
	h := newHandlerWithStubs(secret, db)

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
	if db.tx.commitCalls.Load() != 1 {
		t.Fatalf("dedup row for ignored event must be committed; got %d commits", db.tx.commitCalls.Load())
	}
}

func TestProcess_DedupInsertError_Returns500_NoCommit(t *testing.T) {
	const secret = "whsec_test"
	db := &fakeDB{tx: &fakeTx{execErr: errors.New("connection refused")}}
	h := newHandlerWithStubs(secret, db)

	payload, header := signedFixture(t, secret, "evt_dberr", "invoice.paid")
	got := h.Process(context.Background(), payload, header)

	if got.StatusCode != 500 {
		t.Fatalf("status: want 500 on DB error so Stripe retries; got %d", got.StatusCode)
	}
	if db.tx.commitCalls.Load() != 0 {
		t.Fatalf("must not commit when dedup insert errored; got %d commits", db.tx.commitCalls.Load())
	}
}

func TestProcess_BeginTxError_Returns500(t *testing.T) {
	db := &fakeDB{beginErr: errors.New("pool exhausted")}
	h := newHandlerWithStubs("whsec_test", db)

	payload, header := signedFixture(t, "whsec_test", "evt_begin", "invoice.paid")
	got := h.Process(context.Background(), payload, header)

	if got.StatusCode != 500 {
		t.Fatalf("status: want 500, got %d", got.StatusCode)
	}
}

func TestProcess_HandlerError_RollsBackDedup(t *testing.T) {
	const secret = "whsec_test"
	db := &fakeDB{tx: &fakeTx{dedupTag: pgconn.NewCommandTag("INSERT 0 1")}}
	h := newHandlerWithStubs(secret, db)

	h.dispatch["invoice.paid"] = func(_ context.Context, _ pgx.Tx, _ stripe.Event) error {
		return errors.New("boom")
	}

	payload, header := signedFixture(t, secret, "evt_handler_fail", "invoice.paid")
	got := h.Process(context.Background(), payload, header)

	// Critical assertion: handler error returns 500 so Stripe retries
	// AND the dedup row is rolled back so the retry is not a no-op.
	if got.StatusCode != 500 {
		t.Fatalf("handler error must return 500 to trigger Stripe retry; got %d", got.StatusCode)
	}
	if db.tx.commitCalls.Load() != 0 {
		t.Fatalf("must not commit when handler errored; got %d commits", db.tx.commitCalls.Load())
	}
	if db.tx.rollback.Load() == 0 {
		t.Fatalf("must roll back when handler errored; got %d rollbacks", db.tx.rollback.Load())
	}
}

func TestProcess_CommitError_Returns500(t *testing.T) {
	const secret = "whsec_test"
	db := &fakeDB{tx: &fakeTx{
		dedupTag:  pgconn.NewCommandTag("INSERT 0 1"),
		commitErr: errors.New("network blip"),
	}}
	h := newHandlerWithStubs(secret, db)

	payload, header := signedFixture(t, secret, "evt_commit_fail", "invoice.paid")
	got := h.Process(context.Background(), payload, header)

	if got.StatusCode != 500 {
		t.Fatalf("commit error must surface as 500; got %d", got.StatusCode)
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
