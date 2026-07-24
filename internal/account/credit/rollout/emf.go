package rollout

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

const metricsNamespace = "MirrorStack/Billing/CreditWalletRollout"

// Observation is one selected shadow/enforce evaluation. The caller supplies
// invariant flags explicitly; this package never infers or changes behavior.
type Observation struct {
	Decision              Decision
	Duration              time.Duration
	Diverged              bool
	EvaluatorError        bool
	MoneyInvariantFailure bool
	ShadowMutation        bool
	DuplicateMutation     bool
}

// Reporter emits CloudWatch Embedded Metric Format JSON. Component, mode, and
// the bounded release/config-derived RolloutID are dimensions. Exact release
// identities, basis points, and cohort bucket remain structured properties,
// and account UUID is deliberately absent.
type Reporter struct {
	mu    sync.Mutex
	out   io.Writer
	nowFn func() time.Time
}

func NewReporter(out io.Writer) *Reporter {
	return &Reporter{out: out, nowFn: time.Now}
}

func (r *Reporter) WithNow(nowFn func() time.Time) *Reporter {
	if nowFn != nil {
		r.nowFn = nowFn
	}
	return r
}

// Emit writes one complete EMF event. A nil reporter/writer is a safe no-op;
// telemetry can never change the billing decision or money path.
func (r *Reporter) Emit(observation Observation) error {
	if r == nil || r.out == nil {
		return nil
	}
	event := emfEvent{
		AWS: emfMetadata{
			Timestamp: r.nowFn().UTC().UnixMilli(),
			CloudWatchMetrics: []emfDirective{{
				Namespace:  metricsNamespace,
				Dimensions: [][]string{{"Component", "Mode", "RolloutID"}},
				Metrics: []emfMetric{
					{Name: "EvaluationCount", Unit: "Count"},
					{Name: "DivergenceCount", Unit: "Count"},
					{Name: "EvaluatorErrorCount", Unit: "Count"},
					{Name: "LatencyMs", Unit: "Milliseconds"},
					{Name: "MoneyInvariantFailureCount", Unit: "Count"},
					{Name: "ShadowMutationCount", Unit: "Count"},
					{Name: "DuplicateMutationCount", Unit: "Count"},
				},
			}},
		},
		Component: observation.Decision.Component,
		Mode:      observation.Decision.Mode,
		RolloutID: observation.Decision.RolloutID,

		BasisPoints:     observation.Decision.BasisPoints,
		CohortBucket:    observation.Decision.CohortBucket,
		Selected:        observation.Decision.Selected,
		Allowlisted:     observation.Decision.Allowlisted,
		CoreManifestSHA: observation.Decision.CoreManifestSHA,
		BillingSHA:      observation.Decision.BillingSHA,

		EvaluationCount:            1,
		DivergenceCount:            count(observation.Diverged),
		EvaluatorErrorCount:        count(observation.EvaluatorError),
		LatencyMs:                  float64(observation.Duration) / float64(time.Millisecond),
		MoneyInvariantFailureCount: count(observation.MoneyInvariantFailure),
		ShadowMutationCount:        count(observation.ShadowMutation),
		DuplicateMutationCount:     count(observation.DuplicateMutation),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return json.NewEncoder(r.out).Encode(event)
}

type emfEvent struct {
	AWS emfMetadata `json:"_aws"`

	Component Component `json:"Component"`
	Mode      Mode      `json:"Mode"`
	RolloutID string    `json:"RolloutID"`

	BasisPoints     uint16 `json:"BasisPoints"`
	CohortBucket    uint16 `json:"CohortBucket"`
	Selected        bool   `json:"Selected"`
	Allowlisted     bool   `json:"Allowlisted"`
	CoreManifestSHA string `json:"CoreManifestSHA"`
	BillingSHA      string `json:"BillingEngineSHA"`

	EvaluationCount            int     `json:"EvaluationCount"`
	DivergenceCount            int     `json:"DivergenceCount"`
	EvaluatorErrorCount        int     `json:"EvaluatorErrorCount"`
	LatencyMs                  float64 `json:"LatencyMs"`
	MoneyInvariantFailureCount int     `json:"MoneyInvariantFailureCount"`
	ShadowMutationCount        int     `json:"ShadowMutationCount"`
	DuplicateMutationCount     int     `json:"DuplicateMutationCount"`
}

type emfMetadata struct {
	Timestamp         int64          `json:"Timestamp"`
	CloudWatchMetrics []emfDirective `json:"CloudWatchMetrics"`
}

type emfDirective struct {
	Namespace  string      `json:"Namespace"`
	Dimensions [][]string  `json:"Dimensions"`
	Metrics    []emfMetric `json:"Metrics"`
}

type emfMetric struct {
	Name string `json:"Name"`
	Unit string `json:"Unit"`
}

func count(value bool) int {
	if value {
		return 1
	}
	return 0
}
