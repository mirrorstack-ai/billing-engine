//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/shadow"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

var uuidPattern = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// 🔴 Under Lambda, stdout is CloudWatch: one week of retention, readable by
// anyone with logs access and WITHOUT lambda:InvokeFunction. So the log has a
// WIDER audience than the permission to run this function.
//
// Per-account identifiers and money figures must therefore reach the function
// RESULT — which goes back to whoever already held the permission to ask — and
// never the log.
func TestTheLambdaKeepsAccountDetailOutOfTheLog(t *testing.T) {
	pool := testutil.NewTestDB(t)

	var logged bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logged, nil))

	// A difference carrying an account id and a money delta — exactly what
	// must not be logged.
	res := Response{
		Action: "shadow",
		OK:     false,
		Shadow: reportWithAccount(t, "ef577b3a-5b9c-4a48-92c4-06ff578ebf14", 7_654_321),
	}
	logSummary(log, res)

	out := logged.String()
	require.NotRegexp(t, uuidPattern, out, "an account id reached the log")
	require.NotContains(t, out, "7654321", "a money figure reached the log")
	require.NotContains(t, out, "delta", "a per-account delta reached the log")

	// And the aggregates a reader legitimately needs ARE there.
	require.Contains(t, out, "compared")
	require.Contains(t, out, "unexplained")

	// The detail must survive in the RESULT, or redaction has become
	// deletion and the function answers nothing.
	payload, err := json.Marshal(res)
	require.NoError(t, err)
	require.Regexp(t, uuidPattern, string(payload),
		"the account id was stripped from the result too — the function now reports nothing")

	_ = pool
}

// A failure must report through the payload, not the log: a database error can
// quote a row.
func TestAFailureReportsThroughThePayloadNotTheLog(t *testing.T) {
	var logged bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logged, nil))

	res := Response{
		Action: "shadow",
		OK:     false,
		Error:  `pq: duplicate key value violates unique constraint on account ef577b3a-5b9c-4a48-92c4-06ff578ebf14`,
	}
	logSummary(log, res)

	require.NotRegexp(t, uuidPattern, logged.String(),
		"an error message quoting a row reached the log")
}

func reportWithAccount(t *testing.T, accountID string, delta int64) *shadow.Report {
	t.Helper()
	r := shadow.Reconcile([]shadow.Difference{{
		AccountID:        accountID,
		PeriodID:         "566be22f-98bb-4565-95ee-bec47a842b8d",
		LegacyBaseMicros: delta,
		LegacyMicros:     delta,
		ShadowMicros:     0,
		Quarantined:      true,
		IntentDigest:     "quarantined: no price",
	}})
	return &r
}

func TestTheCLIPathIsNotRedacted(t *testing.T) {
	// A person at a terminal against a database they chose. Withholding
	// detail here protects nobody and makes the tool harder to use.
	r := reportWithAccount(t, "ef577b3a-5b9c-4a48-92c4-06ff578ebf14", 7_654_321)
	require.Regexp(t, uuidPattern, r.String(),
		"the CLI report was redacted; that is not what redaction is for")
	_ = context.Background()
}
