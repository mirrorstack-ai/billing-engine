// Package cloudflare is the thin wrapper around the Cloudflare GraphQL
// Analytics API that billing-engine's egress puller depends on. It is the
// FIRST outbound HTTP consumer in billing-engine — every other external call
// (Stripe) rides stripe-go's own transport — so it deliberately mirrors the
// internal/shared/stripe shape: a small AnalyticsQuerier interface +
// realClient struct + NewClient constructor, with a fake supplied by tests.
//
// Direction is billing-engine → Cloudflare (an OUTBOUND pull). Cloudflare
// never calls back. The edge (cdn-worker) holds only the Analytics Engine
// write binding and no platform/CF secret; the read-only CF API token lives
// HERE, in billing-engine (design §3a / §5 PR #10c).
//
// The dataset contract (shared with cdn-worker's writeDataPoint) is:
//
//	dataset "cdn_egress"
//	blob1 = app_id, blob2 = module_id
//	double1 = bytes_served
//	index1 = app_id (sampling key)
//
// The pull query is SUM(double1) AS bytes GROUP BY blob1 (app_id),
// blob2 (module_id) over a FULLY-CLOSED [windowStart, windowEnd) window.
package cloudflare

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ErrResultTruncated is returned when a single window's query hits the CF
// Analytics Engine row limit (queryRowLimit). CF AE has no pagination and no
// truncation flag, so a result set AT the limit must be treated as truncated:
// the run fails loudly rather than silently under-billing the overflow groups.
var ErrResultTruncated = errors.New("cloudflare egress query hit the row limit; result is truncated and would under-bill (no pagination available)")

// graphqlEndpoint is the Cloudflare GraphQL Analytics API endpoint. The pull
// query POSTs here with a Bearer read-only API token.
const graphqlEndpoint = "https://api.cloudflare.com/client/v4/graphql"

// queryRowLimit is the CF Analytics Engine maximum rows per query. CF AE does
// NOT support cursor pagination and returns NO truncation indicator: a window
// with more than this many distinct (app_id, module_id) groups silently returns
// only the first queryRowLimit and drops the rest. This becomes relevant only
// when a single closed-hour window has > queryRowLimit distinct (app, module)
// pairs (e.g. > 2000 apps each with 5+ active modules in one hour). Because we
// cannot paginate, the only safe response when the limit is hit is to fail the
// run loudly (see the ErrResultTruncated check in QueryEgressWindow) rather than
// silently under-bill the overflow groups — the deterministic event_id would
// otherwise make every retry re-record the same truncated set forever.
const queryRowLimit = 10000

// defaultTimeout bounds a single GraphQL pull. Generous (the Analytics API can
// be slow for wide windows) but finite so a hung request can never wedge the
// scheduled run.
const defaultTimeout = 30 * time.Second

// EgressRow is one aggregated egress group from the CF Analytics dataset: the
// SUM(double1) bytes for a single (app_id, module_id) over the queried window.
// AppID / ModuleID are the raw blob strings as Cloudflare returns them — the
// caller parses + validates them (an empty or unparseable app_id is skipped at
// ingest, not here).
type EgressRow struct {
	AppID    string
	ModuleID string
	Bytes    float64
}

// AnalyticsQuerier is the CF Analytics surface the egress puller uses. Kept as
// an interface so the cmd binary and the tests use a fake — tests MUST NEVER
// call the real Cloudflare API (hard rule). Implementations:
//
//   - Production: NewClient(apiToken, accountID) — POSTs the real GraphQL query.
//   - Tests: a fake satisfying this interface.
type AnalyticsQuerier interface {
	// QueryEgressWindow returns the per-(app, module) summed egress bytes for
	// the dataset over the FULLY-CLOSED [windowStart, windowEnd) window. The
	// window MUST already be closed (the caller never passes the current
	// partial bucket) so the SUM is stable across re-runs — which, paired with
	// the deterministic event_id at ingest, makes the whole pull idempotent.
	QueryEgressWindow(ctx context.Context, datasetName string, windowStart, windowEnd time.Time) ([]EgressRow, error)
}

// NewClient returns an AnalyticsQuerier backed by the real Cloudflare GraphQL
// Analytics API. apiToken is the READ-ONLY CF API token (Account Analytics
// Read); accountID is the CF account the dataset lives under. Both are required
// — an empty token makes every query fail with an auth error at request time;
// callers should fail-fast at startup (the cmd reads them via config.MustEnv).
//
// The http.Client carries a finite timeout set HERE at construction so a single
// pull can never hang the scheduled run.
func NewClient(apiToken, accountID string) AnalyticsQuerier {
	return &realClient{
		apiToken:  apiToken,
		accountID: accountID,
		http:      &http.Client{Timeout: defaultTimeout},
	}
}

type realClient struct {
	apiToken  string
	accountID string
	http      *http.Client
}

// graphqlQuery is the CF Analytics GraphQL document. It selects the
// httpRequestsAdaptiveGroups-equivalent for an Analytics Engine dataset
// (workersAnalyticsEngineAdaptiveGroups): SUM(double1) AS bytes GROUP BY the
// two dimensions blob1 (app_id) + blob2 (module_id), filtered to the
// [windowStart, windowEnd) window. The dataset name is bound at the variable
// $dataset; the window + account are variables too so the document is static
// (no string interpolation of untrusted values into the query).
//
// limit is bound to queryRowLimit (the CF AE per-query maximum). CF AE provides
// no pagination, so a result AT the limit is treated as truncated and fails the
// run — see queryRowLimit and the ErrResultTruncated check below.
var graphqlQuery = fmt.Sprintf(`query EgressWindow($account: String!, $dataset: String!, $start: Time!, $end: Time!) {
  viewer {
    accounts(filter: {accountTag: $account}) {
      egress: workersAnalyticsEngineAdaptiveGroups(
        limit: %d
        filter: {dataset: $dataset, datetime_geq: $start, datetime_lt: $end}
      ) {
        sum { double1 }
        dimensions { blob1 blob2 }
      }
    }
  }
}`, queryRowLimit)

// graphqlRequest is the POST body for the CF GraphQL endpoint.
type graphqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

// graphqlResponse is the slice of the CF GraphQL response the puller reads. The
// shape mirrors the query: viewer → accounts[] → egress[] groups, each with a
// summed double1 + the two grouping dimensions. errors[] carries GraphQL-level
// failures (auth, bad query) that the HTTP layer reports as 200.
type graphqlResponse struct {
	Data struct {
		Viewer struct {
			Accounts []struct {
				Egress []struct {
					Sum struct {
						Double1 float64 `json:"double1"`
					} `json:"sum"`
					Dimensions struct {
						Blob1 string `json:"blob1"`
						Blob2 string `json:"blob2"`
					} `json:"dimensions"`
				} `json:"egress"`
			} `json:"accounts"`
		} `json:"viewer"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// QueryEgressWindow POSTs the GraphQL pull and maps the grouped result into
// EgressRows. The window is passed straight through as RFC3339 UTC bounds; the
// caller guarantees it is fully closed.
func (c *realClient) QueryEgressWindow(ctx context.Context, datasetName string, windowStart, windowEnd time.Time) ([]EgressRow, error) {
	body, err := json.Marshal(graphqlRequest{
		Query: graphqlQuery,
		Variables: map[string]any{
			"account": c.accountID,
			"dataset": datasetName,
			"start":   windowStart.UTC().Format(time.RFC3339),
			"end":     windowEnd.UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal graphql request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, graphqlEndpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build graphql request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cloudflare graphql request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("read graphql response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cloudflare graphql status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var parsed graphqlResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("decode graphql response: %w", err)
	}
	// CF reports GraphQL-level failures (auth/bad-query) with HTTP 200 + an
	// errors[] array, so a clean status is not enough — surface them as a hard
	// error so the run fails cleanly rather than silently recording zero rows.
	if len(parsed.Errors) > 0 {
		msgs := make([]string, 0, len(parsed.Errors))
		for _, e := range parsed.Errors {
			msgs = append(msgs, e.Message)
		}
		return nil, fmt.Errorf("cloudflare graphql errors: %s", strings.Join(msgs, "; "))
	}

	var rows []EgressRow
	for _, acct := range parsed.Data.Viewer.Accounts {
		for _, g := range acct.Egress {
			rows = append(rows, EgressRow{
				AppID:    g.Dimensions.Blob1,
				ModuleID: g.Dimensions.Blob2,
				Bytes:    g.Sum.Double1,
			})
		}
	}
	// Silent-truncation guard: CF AE returns at most queryRowLimit rows with no
	// pagination cursor and no truncation flag, so a result set AT the limit is
	// indistinguishable from one that overflowed it. Fail the run loudly rather
	// than silently under-bill the dropped groups (which the deterministic
	// event_id would otherwise cause every retry to re-skip forever).
	if len(rows) >= queryRowLimit {
		return nil, fmt.Errorf("%w: %d rows for window [%s, %s)", ErrResultTruncated,
			len(rows), windowStart.UTC().Format(time.RFC3339), windowEnd.UTC().Format(time.RFC3339))
	}
	return rows, nil
}
