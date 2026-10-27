package cloudflare

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests exercise the SQL API request/response mapping against a LOCAL
// httptest server — never the real Cloudflare API (hard rule). The server
// stands in for api.cloudflare.com so the auth header, request body (raw
// SQL text), and response decoding are all verified without leaving the
// process.

// newTestClient points a realClient at a local httptest server instead of the
// real CF endpoint by swapping the http.Client's transport to rewrite the host.
func newTestClient(t *testing.T, srv *httptest.Server) *realClient {
	t.Helper()
	srvURL, err := url.Parse(srv.URL)
	require.NoError(t, err)
	return &realClient{
		apiToken:  "test-token",
		accountID: "acct-123",
		http: &http.Client{
			Timeout:   5 * time.Second,
			Transport: rewriteTransport{base: srv.Client().Transport, host: srvURL.Host, scheme: srvURL.Scheme},
		},
	}
}

// rewriteTransport redirects every request to the test server's host while
// preserving the original method, path, body, and headers, so realClient's
// hard-coded sqlEndpointFormat is transparently routed to httptest. It mutates
// only req.URL.Scheme/Host — a clean redirect that cannot lose the path and has
// no error to discard (unlike rebuilding the request from scratch).
type rewriteTransport struct {
	base   http.RoundTripper
	host   string
	scheme string
}

func (rt rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = rt.scheme
	req.URL.Host = rt.host
	base := rt.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(req)
}

func TestQueryEgressWindow_ParsesGroupedRows(t *testing.T) {
	var gotAuth, gotContentType, gotPath, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)

		_, _ = w.Write([]byte(`{"meta":[{"name":"app_id","type":"String"},{"name":"module_id","type":"String"},{"name":"bytes","type":"Float64"}],"data":[
			{"app_id":"app-a","module_id":"mod-x","bytes":1024},
			{"app_id":"app-b","module_id":"","bytes":2048},
			{"app_id":"","module_id":"","org_id":"org-c","bytes":4096}
		],"rows":3}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	start := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	rows, err := c.QueryEgressWindow(context.Background(), "cdn_egress", start, end)
	require.NoError(t, err)
	require.Equal(t, []EgressRow{
		{AppID: "app-a", ModuleID: "mod-x", Bytes: 1024},
		{AppID: "app-b", ModuleID: "", Bytes: 2048},
		{AppID: "", ModuleID: "", OrgID: "org-c", Bytes: 4096},
	}, rows)

	// Bearer auth + the SQL API's raw-SQL-body / account-scoped-path shape.
	require.Equal(t, "Bearer test-token", gotAuth)
	require.Equal(t, "text/plain", gotContentType)
	require.Equal(t, "/client/v4/accounts/acct-123/analytics_engine/sql", gotPath)
	require.Contains(t, gotBody, "FROM cdn_egress")
	require.Contains(t, gotBody, "SUM(_sample_interval * double1)")
	require.Contains(t, gotBody, "blob3 AS org_id")
	require.Contains(t, gotBody, "GROUP BY blob1, blob2, blob3")
	require.Contains(t, gotBody, "toDateTime('2026-06-15T11:00:00')")
	require.Contains(t, gotBody, "toDateTime('2026-06-15T12:00:00')")
	require.NotContains(t, gotBody, "Z')", "ClickHouse's toDateTime() rejects a trailing 'Z' (HTTP 422) — must never regress")
	require.Contains(t, gotBody, fmt.Sprintf("LIMIT %d", queryRowLimit))
}

func TestQueryEgressWindow_InvalidDatasetNameIsRejected(t *testing.T) {
	// datasetName is interpolated directly into the raw SQL body (the SQL API
	// has no parameter binding), so an unexpected value must be rejected
	// before ever making a request.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("must not make a request for an invalid dataset name")
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	_, err := c.QueryEgressWindow(context.Background(), "cdn_egress'; DROP TABLE x --", time.Now(), time.Now().Add(time.Hour))
	require.ErrorContains(t, err, "invalid cloudflare analytics dataset name")
}

func TestQueryEgressWindow_ErrorEnvelopeIsFatal(t *testing.T) {
	// Cloudflare error responses (auth failure, bad query, etc.) come back as
	// a non-200 status with the standard v4 {errors: [...]} envelope — this
	// must surface as a hard error with the underlying message, not a silent
	// zero-row result.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"success":false,"errors":[{"code":10000,"message":"authentication error"}]}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	_, err := c.QueryEgressWindow(context.Background(), "cdn_egress", time.Now(), time.Now().Add(time.Hour))
	require.ErrorContains(t, err, "authentication error")
}

func TestQueryEgressWindow_TruncatedResultIsFatal(t *testing.T) {
	// The query caps at queryRowLimit distinct (app_id, module_id) groups via
	// an explicit SQL LIMIT, and this puller does not paginate, so a result
	// set AT the limit must fail loudly rather than silently under-bill the
	// dropped overflow groups.
	var sb strings.Builder
	sb.WriteString(`{"data":[`)
	for i := 0; i < queryRowLimit; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"app_id":"app-%d","module_id":"m","bytes":1}`, i)
	}
	sb.WriteString(`],"rows":` + fmt.Sprint(queryRowLimit) + `}`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, sb.String())
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	_, err := c.QueryEgressWindow(context.Background(), "cdn_egress", time.Now(), time.Now().Add(time.Hour))
	require.ErrorIs(t, err, ErrResultTruncated)
}

func TestQueryEgressWindow_Non200IsFatal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("upstream boom"))
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	_, err := c.QueryEgressWindow(context.Background(), "cdn_egress", time.Now(), time.Now().Add(time.Hour))
	require.ErrorContains(t, err, "status 500")
	require.ErrorContains(t, err, "upstream boom")
}

func TestQueryRequestWindow_ParsesTierAndStageGroups(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		_, _ = w.Write([]byte(`{"meta":[],"data":[
			{"app_id":"app-a","module_id":"","tier":"edge-hit","stage":"prod","requests":120},
			{"app_id":"app-a","module_id":"","tier":"r2-hit","stage":"prod","requests":30},
			{"app_id":"app-b","module_id":"","tier":"","stage":"","requests":0}
		],"rows":3}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	start := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)
	rows, err := c.QueryRequestWindow(context.Background(), "cdn_egress", start, start.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, []RequestRow{
		{AppID: "app-a", ModuleID: "", Tier: "edge-hit", Stage: "prod", Requests: 120},
		{AppID: "app-a", ModuleID: "", Tier: "r2-hit", Stage: "prod", Requests: 30},
		{AppID: "app-b", ModuleID: "", Tier: "", Stage: "", Requests: 0},
	}, rows)
	// double2 weighted by the sampling interval, grouped by the two new blobs.
	require.Contains(t, gotBody, "SUM(_sample_interval * double2)")
	require.Contains(t, gotBody, "blob4 AS tier")
	require.Contains(t, gotBody, "blob5 AS stage")
	require.Contains(t, gotBody, "GROUP BY blob1, blob2, blob4, blob5")
	require.NotContains(t, gotBody, "Z')")

	_, err = c.QueryRequestWindow(context.Background(), "bad name", start, start.Add(time.Hour))
	require.Error(t, err, "the dataset name is interpolated into raw SQL and must be validated")
}
