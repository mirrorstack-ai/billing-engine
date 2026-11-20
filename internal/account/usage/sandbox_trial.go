package usage

import (
	"context"
	"regexp"
	"strings"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// SourceSandboxTrial tags usage a capped authoring-sandbox trial produced
// (billing-engine#256, migration 095): infra.sandbox.* compute and infra.ai.*
// tokens. MirrorStack pays for the trial (D441), so such an event is RECORDED and
// PRICED and NEVER CHARGED; only the `sandbox` budget counts it. A closed set:
// a new source is a migration, not a producer string.
const SourceSandboxTrial = "sandbox_trial"

// sandboxRunIDShape is the opaque run id a tagged event carries.
var sandboxRunIDShape = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

const sandboxMetricPrefix = "infra.sandbox."

// validateSandboxTag checks the source tag of a RecordInfraUsage request: the
// source is empty or the one known value; infra.sandbox.* MUST be tagged
// (untagged it would be charged to the org as ordinary compute); a tagged event
// is one of the two trial families, so the tag cannot waive any other metric's
// charge; and a tagged event names its run, which never rides an untagged one.
func validateSandboxTag(req RecordInfraUsageRequest) error {
	switch {
	case req.Source != "" && req.Source != SourceSandboxTrial:
		return billing.InvalidInput("unknown usage source: " + req.Source)
	case req.Source == "" && req.RunID != "":
		return billing.InvalidInput("run_id requires a source")
	case req.Source == "" && strings.HasPrefix(req.Metric, sandboxMetricPrefix):
		return billing.InvalidInput(req.Metric + " is trial compute and requires source " + SourceSandboxTrial)
	case req.Source == "":
		return nil
	case !strings.HasPrefix(req.Metric, sandboxMetricPrefix) && !strings.HasPrefix(req.Metric, "infra.ai."):
		return billing.InvalidInput("source " + SourceSandboxTrial + " applies to infra.sandbox.* and infra.ai.* metrics only")
	case !sandboxRunIDShape.MatchString(req.RunID):
		return billing.InvalidInput("run_id required for a sandbox_trial event (1-128 chars of [A-Za-z0-9._:-])")
	}
	return nil
}

// stampSandboxUnitPrice resolves the RAW unit price (micro-dollars) a
// sandbox_trial event is frozen at: the exact (metric, model) row when the model
// has one, else the platform-infra catalog row — the sources the live AI budget
// reads, resolved once so a later price edit cannot move the cap. It fails
// CLOSED, since a stamped 0 would let spend slip under the cap: no catalog row,
// an unpriced one, or a token metric whose model has no exact row (the catalog
// fallback is the cheapest model) refuses the event. A catalog price of exactly
// 0 (infra.ai.requests) is allowed: it cannot under-count.
func (s *Service) stampSandboxUnitPrice(ctx context.Context, metric, model string) (int64, error) {
	if model != "" {
		lookup, ok := s.store.(modelPriceLookup)
		if !ok {
			return 0, billing.Internal("model price lookup is unavailable; refusing the sandbox event", nil)
		}
		price, found, err := lookup.LookupModelPrice(ctx, metric, model)
		if err != nil {
			return 0, billing.Internal("sandbox model price lookup failed", err)
		}
		if found { // a retired row still prices at its own rate: this is a cap
			return price.UnitPriceMicros, nil
		}
	}
	def, found, err := s.store.LookupMetricDefinition(ctx, platformInfraModuleID, metric)
	if err != nil {
		return 0, billing.Internal("sandbox catalog price lookup failed", err)
	}
	if !found || !def.Priced {
		return 0, billing.InvalidInput("no price for " + metric + "; refusing the sandbox event")
	}
	if strings.HasPrefix(metric, "infra.ai.") && def.UnitPriceMicros > 0 {
		return 0, billing.InvalidInput(metric + " needs a model with an exact price row for a sandbox_trial event")
	}
	return def.UnitPriceMicros, nil
}
