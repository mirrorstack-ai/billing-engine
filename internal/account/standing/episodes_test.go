package standing_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/eligibility"
	"github.com/mirrorstack-ai/billing-engine/internal/account/standing"
)

// graceClock is a settable fake clock.
type graceClock struct{ t time.Time }

func (c *graceClock) Now() time.Time          { return c.t }
func (c *graceClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// fakeEpisodes is an in-memory EpisodeStore mirroring the SQL semantics of
// ms_billing.unpaid_episodes (one open episode per account). failOnCall makes
// every method fail the test: the flag-off assertion.
type fakeEpisodes struct {
	t          *testing.T
	acct       uuid.UUID
	owner      standing.Owner
	eps        []*fakeEp
	failOnCall bool
	calls      int
}

type fakeEp struct {
	id       uuid.UUID
	started  time.Time
	notice   *time.Time
	closed   *time.Time
	attempts int
}

func (f *fakeEpisodes) hit(name string) {
	f.calls++
	if f.failOnCall {
		f.t.Fatalf("episode store called (%s) with the flag off", name)
	}
}

func (f *fakeEpisodes) open() *fakeEp {
	for _, e := range f.eps {
		if e.closed == nil {
			return e
		}
	}
	return nil
}

func (f *fakeEpisodes) AccountForOwner(context.Context, standing.Owner) (uuid.UUID, bool, error) {
	f.hit("AccountForOwner")
	return f.acct, true, nil
}

func (f *fakeEpisodes) OpenUnpaidEpisodeIfNone(_ context.Context, _ uuid.UUID, at time.Time) (billing.UnpaidEpisode, error) {
	f.hit("OpenUnpaidEpisodeIfNone")
	e := f.open()
	if e == nil {
		e = &fakeEp{id: uuid.New(), started: at}
		f.eps = append(f.eps, e)
	}
	return billing.UnpaidEpisode{ID: e.id, AccountID: f.acct, StartedAt: e.started, NoticeSentAt: e.notice}, nil
}

func (f *fakeEpisodes) CloseUnpaidEpisode(_ context.Context, _ uuid.UUID, at time.Time) (bool, error) {
	f.hit("CloseUnpaidEpisode")
	if e := f.open(); e != nil {
		e.closed = &at
		return true, nil
	}
	return false, nil
}

func (f *fakeEpisodes) SetUnpaidEpisodeNotice(_ context.Context, _ uuid.UUID, at time.Time) (bool, error) {
	f.hit("SetUnpaidEpisodeNotice")
	if e := f.open(); e != nil && e.notice == nil {
		e.notice = &at
		return true, nil
	}
	return false, nil
}

func (f *fakeEpisodes) RecordNoticeFailure(_ context.Context, id uuid.UUID) (int, error) {
	f.hit("RecordNoticeFailure")
	for _, e := range f.eps {
		if e.id == id {
			e.attempts++
			return e.attempts, nil
		}
	}
	return 0, nil
}

func (f *fakeEpisodes) DueUnpaidEpisodes(_ context.Context, now time.Time, after uuid.UUID, limit int) ([]standing.DueEpisode, error) {
	f.hit("DueUnpaidEpisodes")
	var out []standing.DueEpisode
	for _, e := range f.eps {
		if e.closed != nil || e.id.String() <= after.String() {
			continue
		}
		if e.notice == nil || !e.notice.Add(eligibility.UnpaidNoticePeriod).After(now) {
			out = append(out, standing.DueEpisode{ID: e.id, Owner: f.owner})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// graceStatus computes the verdict with the REAL eligibility.Evaluate (the same
// rule billing.Service.GetServiceStatus applies behind UNPAID_GRACE) over the
// fake episode and the fake clock.
type graceStatus struct {
	eps    *fakeEpisodes
	clock  *graceClock
	sig    eligibility.Signals
	wallet bool // the wallet gate reports OUT_OF_CREDITS
}

func (g *graceStatus) GetServiceStatus(context.Context, billing.GetServiceStatusRequest) (*billing.GetServiceStatusResponse, error) {
	s := g.sig
	s.GraceEnabled = true
	s.Now = g.clock.Now()
	if e := g.eps.open(); e != nil {
		s.UnpaidNoticeSentAt = e.notice
	}
	v := eligibility.Evaluate(s)
	resp := &billing.GetServiceStatusResponse{Blocked: v.Blocked, Reason: string(v.Reason)}
	for _, r := range v.Reasons {
		resp.Reasons = append(resp.Reasons, string(r))
	}
	if g.wallet && !v.Blocked {
		resp.Blocked = true
		resp.Reason = string(eligibility.ReasonOutOfCredits)
	}
	return resp, nil
}

// graceServer records serving-block pushes and standing-notice POSTs.
type graceServer struct {
	mu          sync.Mutex
	pushes      []bool
	notices     []map[string]string
	noticeCode  int
	noticeCalls int
}

func newGraceServer(t *testing.T) (*graceServer, *httptest.Server) {
	g := &graceServer{noticeCode: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		g.mu.Lock()
		defer g.mu.Unlock()
		switch r.URL.Path {
		case "/internal/apps/serving-block":
			var body struct {
				Blocked bool `json:"blocked"`
			}
			require.NoError(t, json.Unmarshal(b, &body))
			g.pushes = append(g.pushes, body.Blocked)
		case "/internal/apps/standing-notice":
			g.noticeCalls++
			require.Equal(t, "secret", r.Header.Get("X-MS-Internal-Secret"))
			if g.noticeCode == http.StatusOK {
				var body map[string]string
				require.NoError(t, json.Unmarshal(b, &body))
				g.notices = append(g.notices, body)
			}
			w.WriteHeader(g.noticeCode)
			return
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return g, srv
}

type graceRig struct {
	n      *standing.Notifier
	eps    *fakeEpisodes
	status *graceStatus
	clock  *graceClock
	srv    *graceServer
	logs   *bytes.Buffer
	user   uuid.UUID
}

func newGraceRig(t *testing.T, sig eligibility.Signals) *graceRig {
	t.Helper()
	clock := &graceClock{t: time.Date(2026, 11, 8, 3, 0, 0, 0, time.UTC)}
	user := uuid.New()
	eps := &fakeEpisodes{t: t, acct: uuid.New(), owner: standing.Owner{UserID: user}}
	status := &graceStatus{eps: eps, clock: clock, sig: sig}
	srv, hs := newGraceServer(t)
	logs := &bytes.Buffer{}
	n := standing.NewNotifier(hs.URL, "secret", status,
		&fakeOwners{owner: eps.owner, found: true},
		slog.New(slog.NewTextHandler(logs, nil))).
		WithEpisodes(eps).WithUnpaidGrace(true).WithClock(clock.Now)
	return &graceRig{n: n, eps: eps, status: status, clock: clock, srv: srv, logs: logs, user: user}
}

// unpaidSignals: a card, and gate 4 failing (two unpaid invoices).
func unpaidSignals() eligibility.Signals {
	return eligibility.Signals{UsableNonFraudCardCount: 1, UnpaidInvoiceCount: 2}
}

func (r *graceRig) event() { r.n.NotifyStripeInvoice(context.Background(), "in_1") }

func TestGrace_FirstEventOpensOneEpisodeAndSendsOneNotice(t *testing.T) {
	r := newGraceRig(t, unpaidSignals())
	start := r.clock.Now()

	r.event()
	require.Len(t, r.eps.eps, 1)
	require.Len(t, r.srv.notices, 1)
	n := r.srv.notices[0]
	require.Equal(t, "unpaid_notice", n["kind"])
	require.Equal(t, "UNPAID_INVOICES", n["reason_code"])
	require.Equal(t, r.user.String(), n["owner_user_id"])
	require.Equal(t, "unpaid:"+r.eps.eps[0].id.String(), n["idempotency_key"])
	// notice_sent_at is the very instant the email's limit_at is built from.
	require.NotNil(t, r.eps.eps[0].notice)
	require.Equal(t, start.Truncate(time.Second), *r.eps.eps[0].notice)
	require.Equal(t, r.eps.eps[0].notice.Add(14*24*time.Hour).Format(time.RFC3339), n["limit_at"])
	require.Equal(t, []bool{false}, r.srv.pushes, "inside the grace the verdict is not blocked")

	// A second event the same day: nothing new.
	r.clock.advance(5 * time.Hour)
	r.event()
	require.Len(t, r.eps.eps, 1)
	require.Len(t, r.srv.notices, 1)

	// Paying clears the gate: the episode closes and the block never happens.
	r.status.sig.UnpaidInvoiceCount = 0
	r.event()
	require.NotNil(t, r.eps.eps[0].closed)
	require.Nil(t, r.eps.open())
	for _, blocked := range r.srv.pushes {
		require.False(t, blocked)
	}
	r.clock.advance(30 * 24 * time.Hour)
	require.Empty(t, r.n.SweepUnpaidEpisodes(context.Background()), "closed episode: nothing to sweep")
}

func TestGrace_FailedNoticeFailsTowardServiceThenSweepRetries(t *testing.T) {
	r := newGraceRig(t, unpaidSignals())
	r.srv.noticeCode = http.StatusBadGateway

	r.event()
	require.Len(t, r.eps.eps, 1)
	require.Nil(t, r.eps.eps[0].notice, "a failed send leaves notice_sent_at null")
	require.Equal(t, []bool{false}, r.srv.pushes, "and the account is not blocked")
	require.NotContains(t, r.logs.String(), "ALERT")

	r.clock.advance(24 * time.Hour)
	res := r.n.SweepUnpaidEpisodes(context.Background())
	require.Equal(t, 1, res.Candidates)
	require.Nil(t, r.eps.eps[0].notice)
	require.NotContains(t, r.logs.String(), "ALERT", "two failures so far")

	r.clock.advance(24 * time.Hour)
	r.n.SweepUnpaidEpisodes(context.Background())
	require.Equal(t, 3, r.eps.eps[0].attempts)
	require.Contains(t, r.logs.String(), "ALERT", "third failure raises the alert")
	require.NotContains(t, r.logs.String(), r.user.String(), "no raw owner id in logs")

	// The platform recovers: the next sweep sends and stamps the notice.
	r.srv.noticeCode = http.StatusOK
	r.clock.advance(24 * time.Hour)
	r.n.SweepUnpaidEpisodes(context.Background())
	require.NotNil(t, r.eps.eps[0].notice)
	require.Len(t, r.srv.notices, 1)
	require.Equal(t, r.eps.eps[0].notice.Add(14*24*time.Hour).Format(time.RFC3339), r.srv.notices[0]["limit_at"])
}

func TestGrace_DayBoundaryAndSweepPush(t *testing.T) {
	r := newGraceRig(t, unpaidSignals())
	r.event()
	sent := *r.eps.eps[0].notice
	ctx := context.Background()
	pushes := func() int { return len(r.srv.pushes) }

	r.clock.t = sent.Add(14*24*time.Hour - time.Minute)
	res := r.n.SweepUnpaidEpisodes(ctx)
	require.Zero(t, res.Candidates, "13d23h59m: not due, sweep skips it")
	v, err := r.status.GetServiceStatus(ctx, billing.GetServiceStatusRequest{UserID: r.user})
	require.NoError(t, err)
	require.False(t, v.Blocked)

	r.clock.t = sent.Add(14 * 24 * time.Hour)
	before := pushes()
	res = r.n.SweepUnpaidEpisodes(ctx)
	require.Equal(t, standing.SweepResult{Candidates: 1, Blocked: 1}, res)
	require.Equal(t, before+1, pushes(), "exactly one push per run")
	require.True(t, r.srv.pushes[len(r.srv.pushes)-1], "blocked: true at sent + 14d")

	// A second run changes nothing: same episode, same stamp, same verdict.
	stamp := *r.eps.eps[0].notice
	res = r.n.SweepUnpaidEpisodes(ctx)
	require.Equal(t, standing.SweepResult{Candidates: 1, Blocked: 1}, res)
	require.Len(t, r.eps.eps, 1)
	require.Equal(t, stamp, *r.eps.eps[0].notice)
	require.Len(t, r.srv.notices, 1, "no second notice")
}

func TestGrace_Gate1AndWalletNeverOpenAnEpisode(t *testing.T) {
	t.Run("gate 1 blocks at once", func(t *testing.T) {
		r := newGraceRig(t, eligibility.Signals{UsableNonFraudCardCount: 0})
		r.event()
		require.Empty(t, r.eps.eps)
		require.Zero(t, r.srv.noticeCalls)
		require.Equal(t, []bool{true}, r.srv.pushes)
	})
	t.Run("gate 1 plus an unpaid gate is still gate 1", func(t *testing.T) {
		r := newGraceRig(t, eligibility.Signals{UnpaidInvoiceCount: 2})
		r.event()
		require.Empty(t, r.eps.eps)
		require.Zero(t, r.srv.noticeCalls)
		require.Equal(t, []bool{true}, r.srv.pushes)
	})
	t.Run("wallet gate blocks at once", func(t *testing.T) {
		r := newGraceRig(t, eligibility.Signals{UsableNonFraudCardCount: 1})
		r.status.wallet = true
		r.event()
		require.Empty(t, r.eps.eps)
		require.Zero(t, r.srv.noticeCalls)
		require.Equal(t, []bool{true}, r.srv.pushes)
	})
	t.Run("wallet gate with an unpaid bill sends no notice", func(t *testing.T) {
		r := newGraceRig(t, unpaidSignals())
		r.status.wallet = true
		r.event()
		require.Empty(t, r.eps.eps)
		require.Zero(t, r.srv.noticeCalls)
	})
}

func TestGrace_FlagOffDoesNothing(t *testing.T) {
	r := newGraceRig(t, unpaidSignals())
	r.n.WithUnpaidGrace(false)
	r.eps.failOnCall = true

	r.event()
	res := r.n.SweepUnpaidEpisodes(context.Background())
	require.Zero(t, res.Candidates)
	require.Zero(t, r.eps.calls)
	require.Zero(t, r.srv.noticeCalls)
	require.Equal(t, 1, len(r.srv.pushes), "the plain serving-block push is unchanged")
}

func TestGrace_NoEpisodeStoreIsInert(t *testing.T) {
	r := newGraceRig(t, unpaidSignals())
	r.n.WithEpisodes(nil)
	r.event()
	require.Zero(t, r.srv.noticeCalls)
	require.Zero(t, r.n.SweepUnpaidEpisodes(context.Background()).Candidates)
}
