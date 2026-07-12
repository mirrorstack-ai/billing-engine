// Package cloudflare is the thin wrapper around the Cloudflare Analytics
// Engine SQL API that billing-engine's egress puller depends on. It is the
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
// The pull query is SUM(_sample_interval * double1) AS bytes GROUP BY blob1
// (app_id), blob2 (module_id) over a FULLY-CLOSED [windowStart, windowEnd)
// window.
//
// # Why this reads via the SQL API, not GraphQL
//
// This package originally queried the GraphQL Analytics API's
// workersAnalyticsEngineAdaptiveGroups field with a `sum { double1 }`
// selection. That query has NEVER worked against the real Cloudflare API: CF
// rejects it with a GraphQL schema error, "unknown field \"sum\"" — confirmed
// directly against the live API, not inferred. The failure was masked until
// now by a separate (since-fixed) auth/token issue on the same code path, so
// the query-shape bug never surfaced on its own.
//
// A live schema introspection of AccountWorkersAnalyticsEngineAdaptiveGroups
// shows why: the type exposes only `count`, `dimensions`, and an ALPHA
// `confidence(...)` field — there is no `sum`, `avg`, `quantiles`, or any
// other aggregate. Its `dimensions` sub-type is likewise limited to `dataset`
// and the built-in date/time buckets (date, datetime, datetimeHour, ...) —
// it does NOT expose blob1/blob2/double1 (or any blob/double/index field) at
// all. So there is no field-name fix and no "read raw dimensions and sum
// client-side" fallback available on this GraphQL field for a custom
// Analytics Engine dataset: the payload data literally cannot be read back
// through it, in any shape. Cloudflare's own current documentation and
// reference material route this use case away from GraphQL entirely and
// exclusively via the separate Analytics Engine SQL API
// (POST .../accounts/{account_id}/analytics_engine/sql, raw SQL body).
//
// Hence the switch below. The public QueryEgressWindow contract (and the
// one production call site, cmd/infra-egress-sync/main.go) is unchanged —
// this is an internal transport swap, not an API change.
package cloudflare

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// ErrResultTruncated is returned when a single window's query hits the CF
// Analytics Engine row limit (queryRowLimit). CF AE has no pagination and no
// truncation flag, so a result set AT the limit must be treated as truncated:
// the run fails loudly rather than silently under-billing the overflow groups.
var ErrResultTruncated = errors.New("cloudflare egress query hit the row limit; result is truncated and would under-bill (no pagination available)")

// sqlEndpointFormat is the Cloudflare Analytics Engine SQL API endpoint. The
// account ID is path-scoped (unlike the old GraphQL endpoint, which took the
// account as a query variable), so it is formatted in per-request.
const sqlEndpointFormat = "https://api.cloudflare.com/client/v4/accounts/%s/analytics_engine/sql"

// queryRowLimit bounds the number of (app_id, module_id) groups a single
// window query may return, via an explicit SQL LIMIT. The Analytics Engine
// SQL API documents no server-side cap of its own and supports OFFSET-based
// pagination, but this puller does not paginate — it treats a result AT the
// limit as truncated and fails the run (see ErrResultTruncated below) rather
// than risk silently under-billing overflow groups, exactly as it did under
// the old (broken) GraphQL query. This becomes relevant only when a single
// closed-hour window has > queryRowLimit distinct (app, module) pairs.
const queryRowLimit = 10000

// defaultTimeout bounds a single SQL pull. Generous (the Analytics Engine can
// be slow for wide windows) but finite so a hung request can never wedge the
// scheduled run.
const defaultTimeout = 30 * time.Second

// datasetNamePattern restricts datasetName to a safe SQL identifier. The SQL
// API takes a raw SQL string body (no parameter binding), so datasetName is
// interpolated directly into the query text; this guards against injection
// even though today's only caller passes a hardcoded constant.
var datasetNamePattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// sqlQueryTemplate is the Analytics Engine SQL API query for one closed-hour
// egress window. _sample_interval weights each row by its inverse sampling
// probability, so SUM(_sample_interval * double1) is the correct total under
// Analytics Engine's adaptive sampling — a plain SUM(double1) would silently
// under-count once sampling kicks in (sampling is keyed on index1 = app_id,
// so this would bite hardest on exactly the highest-traffic single app_id).
// %[1]s = dataset name (pre-validated against datasetNamePattern), %[2]s /
// %[3]s = RFC3339 UTC window bounds, %[4]d = queryRowLimit.
const sqlQueryTemplate = `SELECT blob1 AS app_id, blob2 AS module_id, SUM(_sample_interval * double1) AS bytes FROM %[1]s WHERE timestamp >= toDateTime('%[2]s') AND timestamp < toDateTime('%[3]s') GROUP BY blob1, blob2 LIMIT %[4]d FORMAT JSON`

// EgressRow is one aggregated egress group from the CF Analytics dataset: the
// summed bytes for a single (app_id, module_id) over the queried window.
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
//   - Production: NewClient(apiToken, accountID) — POSTs the real SQL query.
//   - Tests: a fake satisfying this interface.
type AnalyticsQuerier interface {
	// QueryEgressWindow returns the per-(app, module) summed egress bytes for
	// the dataset over the FULLY-CLOSED [windowStart, windowEnd) window. The
	// window MUST already be closed (the caller never passes the current
	// partial bucket) so the SUM is stable across re-runs — which, paired with
	// the deterministic event_id at ingest, makes the whole pull idempotent.
	QueryEgressWindow(ctx context.Context, datasetName string, windowStart, windowEnd time.Time) ([]EgressRow, error)
}

// NewClient returns an AnalyticsQuerier backed by the real Cloudflare
// Analytics Engine SQL API. apiToken is the READ-ONLY CF API token (Account
// Analytics Read — the same scope the old GraphQL query used); accountID is
// the CF account the dataset lives under. Both are required — an empty token
// makes every query fail with an auth error at request time; callers should
// fail-fast at startup (the cmd reads them via config.MustEnv).
//
// The http.Client carries a finite timeout set HERE at construction so a
// single pull can never hang the scheduled run.
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

// buildSQLQuery renders the SQL API query body for one closed-hour window,
// validating datasetName first since it is interpolated directly into raw
// SQL text (the SQL API has no parameter-binding mechanism).
func buildSQLQuery(datasetName string, windowStart, windowEnd time.Time) (string, error) {
	if !datasetNamePattern.MatchString(datasetName) {
		return "", fmt.Errorf("invalid cloudflare analytics dataset name %q: must match %s", datasetName, datasetNamePattern.String())
	}
	return fmt.Sprintf(sqlQueryTemplate,
		datasetName,
		windowStart.UTC().Format(time.RFC3339),
		windowEnd.UTC().Format(time.RFC3339),
		queryRowLimit,
	), nil
}

// sqlQueryResponse is the Analytics Engine SQL API's success response shape
// (FORMAT JSON): a flat ClickHouse-style envelope, not the standard
// Cloudflare v4 {success, result, errors} wrapper.
type sqlQueryResponse struct {
	Data []struct {
		AppID    string  `json:"app_id"`
		ModuleID string  `json:"module_id"`
		Bytes    float64 `json:"bytes"`
	} `json:"data"`
}

// cfErrorEnvelope is the standard Cloudflare v4 API error shape, used to
// extract a human-readable message from a non-200 response when one is
// available; if the body doesn't match this shape, the raw body is used
// as-is.
type cfErrorEnvelope struct {
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// sqlErrorMessage extracts the best available error message from a non-200
// SQL API response body: the standard CF error envelope's messages if the
// body decodes as one, else the raw trimmed body.
func sqlErrorMessage(raw []byte) string {
	var env cfErrorEnvelope
	if err := json.Unmarshal(raw, &env); err == nil && len(env.Errors) > 0 {
		msgs := make([]string, 0, len(env.Errors))
		for _, e := range env.Errors {
			msgs = append(msgs, e.Message)
		}
		return strings.Join(msgs, "; ")
	}
	return strings.TrimSpace(string(raw))
}

// QueryEgressWindow POSTs the SQL pull and maps the grouped result into
// EgressRows. The window is passed straight through as RFC3339 UTC bounds; the
// caller guarantees it is fully closed.
func (c *realClient) QueryEgressWindow(ctx context.Context, datasetName string, windowStart, windowEnd time.Time) ([]EgressRow, error) {
	query, err := buildSQLQuery(datasetName, windowStart, windowEnd)
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf(sqlEndpointFormat, url.PathEscape(c.accountID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(query))
	if err != nil {
		return nil, fmt.Errorf("build sql request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	req.Header.Set("Content-Type", "text/plain")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cloudflare sql request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("read sql response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cloudflare sql query status %d: %s", resp.StatusCode, sqlErrorMessage(raw))
	}

	var parsed sqlQueryResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("decode sql response: %w", err)
	}

	rows := make([]EgressRow, 0, len(parsed.Data))
	for _, d := range parsed.Data {
		rows = append(rows, EgressRow{
			AppID:    d.AppID,
			ModuleID: d.ModuleID,
			Bytes:    d.Bytes,
		})
	}
	// Silent-truncation guard: the query above caps at queryRowLimit rows via
	// an explicit SQL LIMIT, and this puller does not paginate (OFFSET-based
	// pagination is supported by the SQL API but not wired here), so a result
	// set AT the limit is indistinguishable from one that overflowed it. Fail
	// the run loudly rather than silently under-bill the dropped groups (which
	// the deterministic event_id would otherwise cause every retry to
	// re-skip forever). Rows here are already grouped by (app_id, module_id)
	// server-side (GROUP BY in the query), so this counts distinct groups —
	// the same semantics the old GraphQL-based guard enforced.
	if len(rows) >= queryRowLimit {
		return nil, fmt.Errorf("%w: %d rows for window [%s, %s)", ErrResultTruncated,
			len(rows), windowStart.UTC().Format(time.RFC3339), windowEnd.UTC().Format(time.RFC3339))
	}
	return rows, nil
}
