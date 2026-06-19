package cloudflare

import (
	"context"
	"encoding/json"
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

// These tests exercise the GraphQL request/response mapping against a LOCAL
// httptest server — never the real Cloudflare API (hard rule). The server
// stands in for api.cloudflare.com so the auth header, query variables, and
// response decoding are all verified without leaving the process.

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
// hard-coded graphqlEndpoint is transparently routed to httptest. It mutates
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
	var gotAuth string
	var gotVars map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		var req graphqlRequest
		_ = json.Unmarshal(body, &req)
		gotVars = req.Variables

		_, _ = w.Write([]byte(`{"data":{"viewer":{"accounts":[{"egress":[
			{"sum":{"double1":1024},"dimensions":{"blob1":"app-a","blob2":"mod-x"}},
			{"sum":{"double1":2048},"dimensions":{"blob1":"app-b","blob2":""}}
		]}]}}}`))
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
	}, rows)

	// Bearer auth + the dataset/window/account variables are wired correctly.
	require.Equal(t, "Bearer test-token", gotAuth)
	require.Equal(t, "cdn_egress", gotVars["dataset"])
	require.Equal(t, "acct-123", gotVars["account"])
	require.Equal(t, "2026-06-15T11:00:00Z", gotVars["start"])
	require.Equal(t, "2026-06-15T12:00:00Z", gotVars["end"])
}

func TestQueryEgressWindow_GraphQLErrorsAreFatal(t *testing.T) {
	// CF reports auth/query failures with HTTP 200 + an errors[] array — these
	// must surface as a hard error, not a silent zero-row result.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":null,"errors":[{"message":"authentication error"}]}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	_, err := c.QueryEgressWindow(context.Background(), "cdn_egress", time.Now(), time.Now().Add(time.Hour))
	require.ErrorContains(t, err, "authentication error")
}

func TestQueryEgressWindow_TruncatedResultIsFatal(t *testing.T) {
	// CF AE returns at most queryRowLimit rows with no pagination cursor and no
	// truncation flag, so a result set AT the limit must fail loudly rather than
	// silently under-bill the dropped overflow groups.
	var sb strings.Builder
	sb.WriteString(`{"data":{"viewer":{"accounts":[{"egress":[`)
	for i := 0; i < queryRowLimit; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"sum":{"double1":1},"dimensions":{"blob1":"app-%d","blob2":"m"}}`, i)
	}
	sb.WriteString(`]}]}}}`)

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
}
