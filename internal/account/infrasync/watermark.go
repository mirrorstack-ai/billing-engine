// Package infrasync gives the scheduled infra pullers (cmd/infra-egress-sync,
// cmd/infra-ssr-compute-sync) a durable memory of what they have recorded.
//
// 🔴 WHY THIS EXISTS (billing-engine#225). Both pullers used to sweep a fixed
// 3 closed hours back from the trigger time and remember nothing. Their retry
// horizon therefore equalled their lookback: any outage longer than 3 h lost
// every window inside it except the last three, silently. It happened —
// infra-egress-sync took Cloudflare 401s on five runs on 2026-09-13 and came
// back sweeping 05:00–08:00Z, so 04:00–05:00Z was never asked for again; and
// infra-ssr-compute-sync recovered nothing from a day of 403s until it was
// backfilled by hand. A longer fixed lookback would only move that cliff.
//
// The shape here instead:
//
//   - a per-source WATERMARK (ms_billing.infra_sync_watermarks, migration 098):
//     the end of the longest run of windows confirmed recorded, oldest first;
//   - each run sweeps from max(watermark, latest − BackfillFloor), always
//     including the newest RecentWindows (the old fixed lookback, so steady
//     state is unchanged), at most MaxWindowsPerRun windows per run;
//   - the deterministic event_id each puller already mints keeps a re-sweep a
//     no-op, so re-asking for a window that half-landed is free;
//   - a CloudWatch EMF metric, WatermarkLagSeconds per source, emitted on every
//     run that can read its watermark — the infra stack alarms on it.
package infrasync

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"time"
)

const (
	// BackfillFloor is how far back a run will still go for a window that was
	// never confirmed. Past it a window is abandoned (logged, counted, and the
	// lag alarm has been red for days by then). Seven days sits well inside
	// both sources' retention: Cloudflare Analytics Engine keeps three months,
	// CloudWatch keeps 1-hour datapoints for 455 days.
	BackfillFloor = 7 * 24 * time.Hour

	// RecentWindows is the old fixed lookback, kept as the minimum sweep: the
	// newest three closed windows are re-asked for on every run, exactly as
	// before this package existed, whatever the watermark says.
	RecentWindows = 3

	// MaxWindowsPerRun caps one run's work. infra-egress-sync makes two
	// Cloudflare queries per window inside a 60 s Lambda timeout, so a run
	// sweeps at most twelve windows: the oldest unconfirmed nine plus the
	// newest three. A full seven-day backlog drains in under a day of hourly
	// runs (net eight hours of backlog per run).
	MaxWindowsPerRun = 12

	// LagAlarmThreshold is the lag the infra stack's alarm fires above. The
	// alarm itself lives in mirrorstack-infra (constructs/scheduled_job_alarms.go);
	// this constant is the contract the metric is emitted against.
	LagAlarmThreshold = 3 * time.Hour

	metricsNamespace = "MirrorStack/Billing/InfraSync"
	lagMetricName    = "WatermarkLagSeconds"
)

// Window is a half-open [Start, End) one-hour bucket.
type Window struct {
	Start time.Time
	End   time.Time
}

// Plan is one run's sweep.
type Plan struct {
	// Windows to sweep, oldest first. When the backlog exceeds
	// MaxWindowsPerRun there is a gap: the oldest unconfirmed windows, then
	// the newest RecentWindows.
	Windows []Window
	// Abandoned counts hours between the stored watermark and the backfill
	// floor: never confirmed, and never to be asked for again.
	Abandoned int
}

// PlanSweep decides which windows a run sweeps. latestEnd is the exclusive end
// of the newest closed, sweepable window (a top of hour); watermark is the
// stored confirmed-through time, zero when the source has none yet.
//
// A source with no watermark sweeps RecentWindows — exactly the pre-#225
// behaviour — and its first run seeds the watermark. It is deliberately not a
// seven-day backfill on first deploy: what was or wasn't recorded before the
// watermark existed is not this run's to guess at.
func PlanSweep(watermark, latestEnd time.Time) Plan {
	latestEnd = latestEnd.UTC().Truncate(time.Hour)
	recentStart := latestEnd.Add(-RecentWindows * time.Hour)
	start := recentStart
	var abandoned int
	if !watermark.IsZero() {
		watermark = watermark.UTC().Truncate(time.Hour)
		floor := latestEnd.Add(-BackfillFloor)
		start = watermark
		if start.Before(floor) {
			abandoned = int(floor.Sub(start) / time.Hour)
			start = floor
		}
		if start.After(recentStart) {
			start = recentStart
		}
	}

	var all []Window
	for s := start; s.Before(latestEnd); s = s.Add(time.Hour) {
		all = append(all, Window{Start: s, End: s.Add(time.Hour)})
	}
	if len(all) > MaxWindowsPerRun {
		oldest := all[:MaxWindowsPerRun-RecentWindows]
		newest := all[len(all)-RecentWindows:]
		all = append(append([]Window{}, oldest...), newest...)
	}
	return Plan{Windows: all, Abandoned: abandoned}
}

// Advance returns the new watermark: the end of the contiguous run of
// confirmed windows that starts at (or before) the current watermark. A
// window that failed, or a gap in the plan, stops it — the next run re-sweeps
// from there. It never moves backwards: a window already behind the watermark
// that fails a re-sweep does not un-confirm it.
//
// When the watermark is zero (first run) or behind the plan's first window
// (the backfill floor cut it), the run starts at that first window.
func Advance(watermark time.Time, windows []Window, confirmed func(Window) bool) time.Time {
	if len(windows) == 0 {
		return watermark
	}
	cur := windows[0].Start
	if watermark.After(cur) {
		cur = watermark
	}
	for _, w := range windows {
		if !w.End.After(cur) {
			continue
		}
		if w.Start.After(cur) || !confirmed(w) {
			break
		}
		cur = w.End
	}
	return cur
}

// Store is the durable watermark, one row per source.
type Store interface {
	// Watermark returns the stored confirmed-through time, zero if none.
	Watermark(ctx context.Context, source string) (time.Time, error)
	// Advance stores through, never moving the stored value backwards.
	Advance(ctx context.Context, source string, through time.Time) error
}

// Tracker runs one source's sweep against its watermark.
type Tracker struct {
	Store  Store
	Source string
	// Metrics receives one EMF line per run (os.Stdout in Lambda, where
	// CloudWatch Logs turns it into the metric). nil emits nothing.
	Metrics io.Writer
	Now     func() time.Time
}

// Outcome is what one tracked run did to the watermark.
type Outcome struct {
	Windows   []Window
	Abandoned int
	Watermark time.Time     // confirmed-through after the run; zero if unknown
	Lag       time.Duration // latestEnd − Watermark; meaningful only if Watermark is set
	Err       error         // a watermark read or write failed (the sweep still ran)
}

// Run reads the watermark, sweeps the planned windows, advances the watermark
// over what the sweep confirmed, and emits the lag. sweep returns, for each
// window it was given, whether that window is confirmed recorded.
//
// A failed watermark read does not stop collection: the run falls back to the
// RecentWindows sweep (the pre-#225 behaviour), stores nothing and emits no
// lag — a missing datapoint is what the alarm treats as breaching.
func (t Tracker) Run(ctx context.Context, latestEnd time.Time, sweep func([]Window) func(Window) bool) Outcome {
	latestEnd = latestEnd.UTC().Truncate(time.Hour)
	watermark, err := t.Store.Watermark(ctx, t.Source)
	if err != nil {
		slog.ErrorContext(ctx, "infra sync watermark read failed; sweeping the recent windows only",
			"source", t.Source, "error", err)
		plan := PlanSweep(time.Time{}, latestEnd)
		sweep(plan.Windows)
		return Outcome{Windows: plan.Windows, Err: err}
	}

	plan := PlanSweep(watermark, latestEnd)
	if plan.Abandoned > 0 {
		slog.ErrorContext(ctx, "infra sync abandoned windows older than the backfill floor",
			"source", t.Source, "watermark", watermark, "abandoned_hours", plan.Abandoned,
			"floor", latestEnd.Add(-BackfillFloor))
	}
	confirmed := sweep(plan.Windows)
	next := Advance(watermark, plan.Windows, confirmed)

	out := Outcome{Windows: plan.Windows, Abandoned: plan.Abandoned, Watermark: watermark}
	if next.After(watermark) {
		if err := t.Store.Advance(ctx, t.Source, next); err != nil {
			slog.ErrorContext(ctx, "infra sync watermark write failed",
				"source", t.Source, "through", next, "error", err)
			out.Err = err
		} else {
			out.Watermark = next
		}
	}
	if out.Watermark.IsZero() {
		return out
	}
	out.Lag = latestEnd.Sub(out.Watermark)
	if out.Lag < 0 {
		out.Lag = 0
	}
	if out.Lag > LagAlarmThreshold {
		slog.WarnContext(ctx, "infra sync watermark is lagging",
			"source", t.Source, "confirmed_through", out.Watermark, "lag", out.Lag.String())
	}
	t.emit(out)
	return out
}

type emfEvent struct {
	AWS              emfMetadata `json:"_aws"`
	Source           string      `json:"Source"`
	Lag              float64     `json:"WatermarkLagSeconds"`
	ConfirmedThrough string      `json:"confirmed_through"`
	Windows          int         `json:"windows"`
	Abandoned        int         `json:"abandoned_hours"`
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

// emit writes one EMF line. Telemetry never changes the run: a write error is
// logged and dropped.
func (t Tracker) emit(out Outcome) {
	if t.Metrics == nil {
		return
	}
	now := time.Now
	if t.Now != nil {
		now = t.Now
	}
	ev := emfEvent{
		AWS: emfMetadata{
			Timestamp: now().UTC().UnixMilli(),
			CloudWatchMetrics: []emfDirective{{
				Namespace:  metricsNamespace,
				Dimensions: [][]string{{"Source"}},
				Metrics:    []emfMetric{{Name: lagMetricName, Unit: "Seconds"}},
			}},
		},
		Source:           t.Source,
		Lag:              out.Lag.Seconds(),
		ConfirmedThrough: out.Watermark.UTC().Format(time.RFC3339),
		Windows:          len(out.Windows),
		Abandoned:        out.Abandoned,
	}
	if err := json.NewEncoder(t.Metrics).Encode(ev); err != nil {
		slog.Error("infra sync lag metric write failed", "source", t.Source, "error", err)
	}
}
