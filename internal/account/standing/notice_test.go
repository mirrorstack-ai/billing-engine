package standing_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/standing"
)

// memState mimics ms_billing.standing_state semantics with a fake clock: the
// row serializes under a mutex (the real store's row lock), changed_at moves
// only when blocked flips.
type memState struct {
	mu    sync.Mutex
	rows  map[string]*row
	clock int64
	fail  error
}
type row struct {
	blocked   bool
	reason    string
	changedAt time.Time
}

func newMemState() *memState { return &memState{rows: map[string]*row{}, clock: 1794000000} }

func (m *memState) tick() { m.mu.Lock(); m.clock += 60; m.mu.Unlock() }

func (m *memState) Observe(_ context.Context, key string, blocked bool, reason string, _ time.Time) (*standing.PriorState, time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return nil, time.Time{}, m.fail
	}
	now := time.Unix(m.clock, 0)
	r, ok := m.rows[key]
	if !ok {
		m.rows[key] = &row{blocked: blocked, reason: reason, changedAt: now}
		return nil, now, nil
	}
	prior := &standing.PriorState{Blocked: r.blocked, Reason: r.reason}
	if r.blocked != blocked {
		r.changedAt = now
	}
	r.blocked, r.reason = blocked, reason
	return prior, r.changedAt, nil
}

// endpoint is a fake api-platform: serving-block always 200, standing-notice
// answers noticeStatus; both recorded in order.
type endpoint struct {
	mu           sync.Mutex
	noticeStatus int
	blockBodies  []string
	notices      []string
}

func (e *endpoint) server(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(r.Body)
		e.mu.Lock()
		defer e.mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/standing-notice") {
			e.notices = append(e.notices, buf.String())
			w.WriteHeader(e.noticeStatus)
			return
		}
		e.blockBodies = append(e.blockBodies, buf.String())
		w.WriteHeader(http.StatusOK)
	}))
}

func newNoticeNotifier(srv *httptest.Server, st *fakeStatus, owner standing.Owner, state standing.StateStore, on bool, log *slog.Logger) *standing.Notifier {
	return standing.NewNotifier(srv.URL, "shhh", st, &fakeOwners{owner: owner, found: true}, log).
		WithSuspensionNotice(state, on)
}

func TestSuspensionNotice_TransitionTable(t *testing.T) {
	org := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	ep := &endpoint{noticeStatus: 200}
	srv := ep.server(t)
	defer srv.Close()
	state := newMemState()
	st := &fakeStatus{blocked: false, reason: "ELIGIBLE"}
	n := newNoticeNotifier(srv, st, standing.Owner{OrgID: org}, state, true, slog.Default())
	ctx := context.Background()
	step := func(blocked bool, reason string) {
		st.blocked, st.reason = blocked, reason
		state.tick()
		n.NotifyStripeCustomer(ctx, "cus")
	}

	step(false, "ELIGIBLE")
	require.Empty(t, ep.notices, "first observation, eligible: nothing")
	step(true, "UNPAID_INVOICES")
	require.Len(t, ep.notices, 1, "eligible -> blocked sends exactly one")
	first := ep.notices[0]
	require.Contains(t, first, `"kind":"suspended"`)
	require.Contains(t, first, `"reason_code":"UNPAID_INVOICES"`)
	require.Contains(t, first, `"owner_org_id":"`+org.String()+`"`)
	step(true, "UNPAID_INVOICES")
	step(true, "TOO_MANY_FAILURES")
	require.Len(t, ep.notices, 1, "blocked -> blocked (repeat, reason change) sends none")
	step(false, "ELIGIBLE")
	require.Len(t, ep.notices, 1, "blocked -> eligible sends none")
	step(true, "UNPAID_INVOICES")
	require.Len(t, ep.notices, 2, "eligible -> blocked again sends one more")
	require.NotEqual(t, first, ep.notices[1], "a new transition gets a new idempotency key")
	require.Len(t, ep.blockBodies, 6, "serving-block pushed on every event, untouched")
}

func TestSuspensionNotice_FirstObservation(t *testing.T) {
	for _, tc := range []struct {
		reason string
		want   int
	}{{"NO_USABLE_CARD", 0}, {"UNPAID_INVOICES", 1}, {"OUT_OF_CREDITS", 1}} {
		ep := &endpoint{noticeStatus: 200}
		srv := ep.server(t)
		st := &fakeStatus{blocked: true, reason: tc.reason}
		n := newNoticeNotifier(srv, st, standing.Owner{UserID: uuid.New()}, newMemState(), true, slog.Default())
		n.NotifyStripeCustomer(context.Background(), "cus")
		require.Len(t, ep.notices, tc.want, tc.reason)
		srv.Close()
	}
}

func TestSuspensionNotice_GraceNeverSends(t *testing.T) {
	ep := &endpoint{noticeStatus: 200}
	srv := ep.server(t)
	defer srv.Close()
	n := newNoticeNotifier(srv, &fakeStatus{blocked: false, reason: "GRACE"}, standing.Owner{UserID: uuid.New()}, newMemState(), true, slog.Default())
	n.NotifyStripeCustomer(context.Background(), "cus")
	require.Empty(t, ep.notices)
}

func TestSuspensionNotice_FlagOffRecordsButNeverMails(t *testing.T) {
	ep := &endpoint{noticeStatus: 200}
	srv := ep.server(t)
	defer srv.Close()
	state := newMemState()
	st := &fakeStatus{blocked: true, reason: "UNPAID_INVOICES"}
	owner := standing.Owner{UserID: uuid.New()}
	off := newNoticeNotifier(srv, st, owner, state, false, slog.Default())
	off.NotifyStripeCustomer(context.Background(), "cus")
	require.Empty(t, ep.notices)
	require.Len(t, state.rows, 1, "state is recorded while the flag is off")
	// Enabling later does not mail an account that was already blocked.
	on := newNoticeNotifier(srv, st, owner, state, true, slog.Default())
	on.NotifyStripeCustomer(context.Background(), "cus")
	require.Empty(t, ep.notices)
}

func TestSuspensionNotice_NilStoreIsTheOldNotifier(t *testing.T) {
	ep := &endpoint{noticeStatus: 200}
	srv := ep.server(t)
	defer srv.Close()
	n := newNoticeNotifier(srv, &fakeStatus{blocked: true, reason: "UNPAID_INVOICES"}, standing.Owner{UserID: uuid.New()}, nil, true, slog.Default())
	n.NotifyStripeCustomer(context.Background(), "cus")
	require.Empty(t, ep.notices)
	require.Len(t, ep.blockBodies, 1)
}

func TestSuspensionNotice_FailingEndpointChangesNothingElse(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusBadGateway} {
		ep := &endpoint{noticeStatus: code}
		srv := ep.server(t)
		var logs bytes.Buffer
		log := slog.New(slog.NewTextHandler(&logs, nil))
		state := newMemState()
		owner := standing.Owner{UserID: uuid.New()}
		n := newNoticeNotifier(srv, &fakeStatus{blocked: true, reason: "UNPAID_INVOICES"}, owner, state, true, log)

		blocked, err := n.NotifyOwner(context.Background(), owner.UserID, uuid.Nil)
		require.NoError(t, err, "a failed mail never changes the return")
		require.True(t, blocked)
		require.Len(t, ep.blockBodies, 1, "the serving-block push still happened")
		require.Len(t, state.rows, 1, "the state row is not rolled back")
		out := logs.String()
		require.Equal(t, 1, strings.Count(out, "suspension mail NOT sent"))
		require.Contains(t, out, "idempotency_key=blocked:")
		require.NotContains(t, out, "@", "no address in logs")
		srv.Close()
	}
}

func TestSuspensionNotice_StateErrorSkipsMailKeepsPush(t *testing.T) {
	ep := &endpoint{noticeStatus: 200}
	srv := ep.server(t)
	defer srv.Close()
	state := newMemState()
	state.fail = errors.New("db down")
	n := newNoticeNotifier(srv, &fakeStatus{blocked: true, reason: "UNPAID_INVOICES"}, standing.Owner{UserID: uuid.New()}, state, true, slog.Default())
	n.NotifyStripeCustomer(context.Background(), "cus")
	require.Empty(t, ep.notices)
	require.Len(t, ep.blockBodies, 1)
}

func TestSuspensionNotice_ConcurrentSameTransitionSendsOnce(t *testing.T) {
	ep := &endpoint{noticeStatus: 200}
	srv := ep.server(t)
	defer srv.Close()
	state := newMemState()
	st := &fakeStatus{blocked: false, reason: "ELIGIBLE"}
	owner := standing.Owner{UserID: uuid.New()}
	n := newNoticeNotifier(srv, st, owner, state, true, slog.Default())
	n.NotifyStripeCustomer(context.Background(), "cus") // eligible row exists
	st.blocked, st.reason = true, "UNPAID_INVOICES"
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); n.NotifyStripeCustomer(context.Background(), "cus") }()
	}
	wg.Wait()
	require.Len(t, ep.notices, 1)
}

// The body is pinned by a golden file that api-platform's
// handler/testdata/standing_notice_suspended.request.json shape matches
// (owner_*, kind, reason_code, idempotency_key; no reason_text/limit_at).
func TestSuspensionNotice_GoldenBody(t *testing.T) {
	org := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	ep := &endpoint{noticeStatus: 200}
	srv := ep.server(t)
	defer srv.Close()
	state := newMemState()
	state.clock = 1793999940 // tick() inside the test lands changed_at on 1794000000
	st := &fakeStatus{blocked: true, reason: "UNPAID_INVOICES"}
	n := newNoticeNotifier(srv, st, standing.Owner{OrgID: org}, state, true, slog.Default())
	state.tick()
	n.NotifyStripeCustomer(context.Background(), "cus")
	want, err := os.ReadFile("testdata/standing_notice_suspended.request.json")
	require.NoError(t, err)
	require.Len(t, ep.notices, 1)
	require.Equal(t, string(want), ep.notices[0])
}
