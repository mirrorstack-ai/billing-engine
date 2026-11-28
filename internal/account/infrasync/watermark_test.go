package infrasync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var latest = time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

func starts(ws []Window) []time.Time {
	out := make([]time.Time, len(ws))
	for i, w := range ws {
		out[i] = w.Start
	}
	return out
}

func hoursBefore(n ...int) []time.Time {
	out := make([]time.Time, len(n))
	for i, h := range n {
		out[i] = latest.Add(time.Duration(-h) * time.Hour)
	}
	return out
}

func TestPlanSweep_NoWatermarkIsTheOldThreeHourSweep(t *testing.T) {
	p := PlanSweep(time.Time{}, latest)
	require.Equal(t, hoursBefore(3, 2, 1), starts(p.Windows))
	require.Zero(t, p.Abandoned)
}

func TestPlanSweep_CaughtUpStillResweepsTheRecentWindows(t *testing.T) {
	p := PlanSweep(latest, latest)
	require.Equal(t, hoursBefore(3, 2, 1), starts(p.Windows))
}

// The 2026-09-13 shape: confirmed through 04:00Z, broken until 08:00Z. The old
// sweep at 09:xx asked for 06:00-09:00 and never for 04:00-06:00 again.
func TestPlanSweep_OutageLongerThanThreeHoursSweepsFromTheWatermark(t *testing.T) {
	p := PlanSweep(latest.Add(-8*time.Hour), latest)
	require.Equal(t, hoursBefore(8, 7, 6, 5, 4, 3, 2, 1), starts(p.Windows))
	require.Zero(t, p.Abandoned)
}

func TestPlanSweep_BacklogOverTheCapKeepsOldestAndNewest(t *testing.T) {
	p := PlanSweep(latest.Add(-30*time.Hour), latest)
	require.Len(t, p.Windows, MaxWindowsPerRun)
	require.Equal(t, hoursBefore(30, 29, 28, 27, 26, 25, 24, 23, 22, 3, 2, 1), starts(p.Windows))
}

func TestPlanSweep_WatermarkBehindTheFloorIsAbandonedToTheFloor(t *testing.T) {
	p := PlanSweep(latest.Add(-BackfillFloor-5*time.Hour), latest)
	require.Equal(t, 5, p.Abandoned)
	require.Equal(t, latest.Add(-BackfillFloor), p.Windows[0].Start)
}

func confirmAllBut(failed ...time.Time) func(Window) bool {
	return func(w Window) bool {
		for _, f := range failed {
			if w.Start.Equal(f) {
				return false
			}
		}
		return true
	}
}

func TestAdvance_StopsAtTheFirstUnconfirmedWindow(t *testing.T) {
	wm := latest.Add(-8 * time.Hour)
	p := PlanSweep(wm, latest)
	got := Advance(wm, p.Windows, confirmAllBut(latest.Add(-5*time.Hour)))
	require.Equal(t, latest.Add(-5*time.Hour), got)
}

func TestAdvance_StopsAtTheCapGap(t *testing.T) {
	wm := latest.Add(-30 * time.Hour)
	p := PlanSweep(wm, latest)
	require.Equal(t, latest.Add(-21*time.Hour), Advance(wm, p.Windows, confirmAllBut()))
}

func TestAdvance_NeverMovesBackwards(t *testing.T) {
	p := PlanSweep(latest, latest)
	got := Advance(latest, p.Windows, confirmAllBut(latest.Add(-3*time.Hour)))
	require.Equal(t, latest, got)
}

func TestAdvance_FirstRunThatFailsSeedsAtItsFirstWindow(t *testing.T) {
	p := PlanSweep(time.Time{}, latest)
	got := Advance(time.Time{}, p.Windows, func(Window) bool { return false })
	require.Equal(t, latest.Add(-3*time.Hour), got)
}

func runOnce(t *testing.T, tr Tracker, failed ...time.Time) (Outcome, []Window) {
	t.Helper()
	var swept []Window
	out := tr.Run(context.Background(), latest.Add(17*time.Minute), func(ws []Window) func(Window) bool {
		swept = ws
		return confirmAllBut(failed...)
	})
	return out, swept
}

func TestTrackerRun_RecoversAWindowAFixedLookbackWouldHaveLost(t *testing.T) {
	store := NewMemoryStore()
	store.Set("cdn-egress", latest.Add(-8*time.Hour))
	var metrics bytes.Buffer
	tr := Tracker{Store: store, Source: "cdn-egress", Metrics: &metrics, Now: func() time.Time { return latest }}

	out, swept := runOnce(t, tr)
	require.Equal(t, hoursBefore(8, 7, 6, 5, 4, 3, 2, 1), starts(swept))
	require.Equal(t, latest, out.Watermark)
	require.Zero(t, out.Lag)

	got, err := store.Watermark(context.Background(), "cdn-egress")
	require.NoError(t, err)
	require.Equal(t, latest, got)

	var ev map[string]any
	require.NoError(t, json.Unmarshal(metrics.Bytes(), &ev))
	require.Equal(t, "cdn-egress", ev["Source"])
	require.Equal(t, float64(0), ev["WatermarkLagSeconds"])
	aws := ev["_aws"].(map[string]any)
	directive := aws["CloudWatchMetrics"].([]any)[0].(map[string]any)
	require.Equal(t, metricsNamespace, directive["Namespace"])
}

func TestTrackerRun_FailedWindowHoldsTheWatermarkAndReportsTheLag(t *testing.T) {
	store := NewMemoryStore()
	store.Set("ssr-compute", latest.Add(-5*time.Hour))
	var metrics bytes.Buffer
	tr := Tracker{Store: store, Source: "ssr-compute", Metrics: &metrics}

	out, _ := runOnce(t, tr, latest.Add(-5*time.Hour))
	require.Equal(t, latest.Add(-5*time.Hour), out.Watermark)
	require.Equal(t, 5*time.Hour, out.Lag)

	var ev map[string]any
	require.NoError(t, json.Unmarshal(metrics.Bytes(), &ev))
	require.Equal(t, (5 * time.Hour).Seconds(), ev["WatermarkLagSeconds"])
}

func TestTrackerRun_ReadErrorStillSweepsRecentWindowsAndEmitsNothing(t *testing.T) {
	store := NewMemoryStore()
	store.ReadErr = errors.New("db down")
	var metrics bytes.Buffer
	tr := Tracker{Store: store, Source: "cdn-egress", Metrics: &metrics}

	out, swept := runOnce(t, tr)
	require.Error(t, out.Err)
	require.Equal(t, hoursBefore(3, 2, 1), starts(swept))
	require.Zero(t, metrics.Len(), "no lag datapoint without a watermark: the alarm reads missing as breaching")
}

func TestTrackerRun_WriteErrorReportsTheStoredLag(t *testing.T) {
	store := NewMemoryStore()
	store.Set("cdn-egress", latest.Add(-4*time.Hour))
	store.WriteErr = errors.New("db down")
	tr := Tracker{Store: store, Source: "cdn-egress"}

	out, _ := runOnce(t, tr)
	require.Error(t, out.Err)
	require.Equal(t, latest.Add(-4*time.Hour), out.Watermark)
	require.Equal(t, 4*time.Hour, out.Lag)
}
