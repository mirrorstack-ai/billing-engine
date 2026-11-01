//go:build integration

package cycle_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// internal/account/cycle/testdata/ai_model_prices.json is the shared fixture
// api-platform's CI reads from this repo's `main` to check its model roster
// against this repo's priced rows (billing-engine#253, mirrorstack-core-v2#1824).
// This repo is public source and api-platform is not, so the fixture has to
// live here, not there.
//
// Two rules keep the fixture honest:
//
//   - A price change is a new UPDATE migration plus a fixture edit in the SAME
//     PR. Editing 092 in place changes nothing on an already-seeded database —
//     092 seeds with ON CONFLICT (metric, model) DO NOTHING.
//   - A new roster model needs its migration AND its fixture entry merged here
//     FIRST. api-platform's CI reads this file from `main`, not from a branch.
//
// A missing or unreadable fixture is a test failure here, never a skip: that
// failure mode is exactly what would let a roster model ship with no price
// rows and get metered at 0.
type aiModelPriceFixture struct {
	Unit   string `json:"unit"`
	Markup struct {
		Num int64 `json:"num"`
		Den int64 `json:"den"`
	} `json:"markup"`
	Models map[string]map[string]int64 `json:"models"`
}

const aiModelPriceFixturePath = "testdata/ai_model_prices.json"

// loadAIModelPriceFixture is shared with migration092_integration_test.go, so
// this repo keeps exactly one copy of the 16 seeded numbers.
func loadAIModelPriceFixture(t *testing.T) aiModelPriceFixture {
	t.Helper()
	body, err := os.ReadFile(aiModelPriceFixturePath)
	require.NoError(t, err, "%s must exist and be readable", aiModelPriceFixturePath)
	var fixture aiModelPriceFixture
	require.NoError(t, json.Unmarshal(body, &fixture), "%s must be valid JSON", aiModelPriceFixturePath)
	return fixture
}

var aiModelPriceMetrics = []string{mIn, mOut, mCW, mCR}

func TestAIModelPriceFixture_MatchesTheSeededRows(t *testing.T) {
	fixture := loadAIModelPriceFixture(t)
	require.Equal(t, int64(usage.InfraMarkupNum), fixture.Markup.Num, "fixture markup.num must match usage.InfraMarkupNum")
	require.Equal(t, int64(usage.InfraMarkupDen), fixture.Markup.Den, "fixture markup.den must match usage.InfraMarkupDen")
	require.NotEmpty(t, fixture.Models, "fixture must list at least one model")

	pool := testutil.NewTestDB(t) // the FULL chain

	for model, prices := range fixture.Models {
		require.Len(t, prices, len(aiModelPriceMetrics), "model %s must have exactly the 4 token metrics", model)
		for _, metric := range aiModelPriceMetrics {
			want, ok := prices[metric]
			require.True(t, ok, "model %s is missing metric %s", model, metric)

			got, active, found := m092Read(t, pool, m092Key{metric, model})
			require.True(t, found, "no price row for metric=%s model=%s", metric, model)
			require.True(t, active, "price row for metric=%s model=%s is not active", metric, model)
			require.Equal(t, want, got, "metric=%s model=%s", metric, model)
		}
	}
}
