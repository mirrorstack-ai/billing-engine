package cycle

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

// Freeze snapshots (billing-engine#218, core-v2#1485; migration 087).
//
// A boundary freeze keeps a whole-cent figure; the rollup under it re-runs on
// every cycle attempt and UpsertUsageAggregate overwrites each line in place.
// So when a reclaim re-derived 11202¢ against the 2026-09-11 freeze of 11390¢,
// nothing could say which line moved: the lines behind the 11390 were gone.
//
// Everything here is DIAGNOSTIC. The snapshot is written after the freeze has
// already won, the drift is read only to be logged, and neither can fail the
// run or change what it charges — errors are logged and dropped. The refreeze
// and the split refusal in charge.go decide the money exactly as before.

// maxLoggedDriftLines bounds the per-line drift log for one run: enough to
// attribute a drift, not enough to flood a cycle's logs.
const maxLoggedDriftLines = 50

// liveFreezeDerivation is the boundary as live state derives it now: the
// whole cents a fresh freeze would take — the boundary total net of this
// attempt's wallet draw, which is what stripeTotal holds before a frozen
// figure replaces it — and the components behind them. ok=false only when
// the cents conversion fails, and then there is nothing to compare.
func liveFreezeDerivation(usageChargedMicros, allowanceMicros, boundaryTotalMicros int64, summary *ChargeSummary) (FreezeDerivation, bool) {
	net := boundaryTotalMicros - summary.WalletDrawnMicros
	if net < 0 {
		net = 0
	}
	cents, err := centsFromMicros(net)
	if err != nil {
		return FreezeDerivation{}, false
	}
	return FreezeDerivation{
		FrozenCents:          cents,
		UsageChargedMicros:   usageChargedMicros,
		AllowanceMicros:      allowanceMicros,
		ArrearsMicros:        summary.ArrearsMicros,
		AdvanceBaseMicros:    summary.AdvanceBaseMicros,
		AdvanceOverageMicros: summary.AdvanceOverageMicros,
		AdvanceDomainsMicros: summary.AdvanceDomainsMicros,
		MembersMicros:        summary.MembersMicros,
		WalletDrawnMicros:    summary.WalletDrawnMicros,
	}, true
}

// snapshotFreezeDerivation records the derivation a fresh freeze was just
// taken from. Best-effort: a failure is logged and the run continues exactly
// as it would have without it.
func (s *Service) snapshotFreezeDerivation(ctx context.Context, runID uuid.UUID, d FreezeDerivation) {
	if _, err := s.store.SnapshotFreezeDerivation(ctx, runID, d); err != nil {
		slog.WarnContext(ctx, "freeze snapshot not written; a later drift on this run cannot be attributed to a line (continuing)",
			"run_id", runID, "frozen_cents", d.FrozenCents, "error", err)
	}
}

// logFreezeDrift prints, for a run whose frozen figure differs from its live
// derivation, which components and which aggregate lines moved since the
// freeze. Best-effort, like the snapshot.
func (s *Service) logFreezeDrift(ctx context.Context, runID, accountID uuid.UUID, frozenCents int64, live FreezeDerivation) {
	snap, drift, found, err := s.store.FreezeDerivationDrift(ctx, runID)
	if err != nil {
		slog.WarnContext(ctx, "freeze drift: reading the freeze snapshot failed (continuing)",
			"run_id", runID, "account_id", accountID, "error", err)
		return
	}
	if !found {
		slog.WarnContext(ctx, "freeze drift: the frozen figure differs from the live derivation and no snapshot exists to attribute it (frozen before migration 087, or its snapshot was refused)",
			"run_id", runID, "account_id", accountID,
			"frozen_cents", frozenCents, "live_cents", live.FrozenCents,
			"live_usage_charged_micros", live.UsageChargedMicros)
		return
	}
	slog.WarnContext(ctx, "freeze drift: frozen derivation vs live",
		"run_id", runID, "account_id", accountID,
		"frozen_cents", frozenCents, "snapshot_cents", snap.FrozenCents, "live_cents", live.FrozenCents,
		"usage_charged_micros_frozen", snap.UsageChargedMicros, "usage_charged_micros_live", live.UsageChargedMicros,
		"allowance_micros_frozen", snap.AllowanceMicros, "allowance_micros_live", live.AllowanceMicros,
		"arrears_micros_frozen", snap.ArrearsMicros, "arrears_micros_live", live.ArrearsMicros,
		"advance_base_micros_frozen", snap.AdvanceBaseMicros, "advance_base_micros_live", live.AdvanceBaseMicros,
		"advance_overage_micros_frozen", snap.AdvanceOverageMicros, "advance_overage_micros_live", live.AdvanceOverageMicros,
		"advance_domains_micros_frozen", snap.AdvanceDomainsMicros, "advance_domains_micros_live", live.AdvanceDomainsMicros,
		"members_micros_frozen", snap.MembersMicros, "members_micros_live", live.MembersMicros,
		"wallet_drawn_micros_frozen", snap.WalletDrawnMicros, "wallet_drawn_micros_live", live.WalletDrawnMicros,
		"lines_moved", len(drift))
	for i, d := range drift {
		if i == maxLoggedDriftLines {
			slog.WarnContext(ctx, "freeze drift: line log truncated",
				"run_id", runID, "lines_not_logged", len(drift)-maxLoggedDriftLines)
			break
		}
		slog.WarnContext(ctx, "freeze drift: line",
			"run_id", runID,
			"app_id", d.AppID, "module_id", d.ModuleID, "metric", d.Metric, "model", d.Model,
			"module_version", d.ModuleVersion, "aggregation_key", d.AggregationKey,
			"in_frozen", d.InFrozen, "in_live", d.InLive,
			"quantity_frozen", d.FrozenQuantity, "quantity_live", d.LiveQuantity,
			"unit_price_micros_frozen", d.FrozenUnitPriceMicros, "unit_price_micros_live", d.LiveUnitPriceMicros,
			"markup_frozen", fmt.Sprintf("%d/%d", d.FrozenMarkupNum, d.FrozenMarkupDen),
			"markup_live", fmt.Sprintf("%d/%d", d.LiveMarkupNum, d.LiveMarkupDen),
			"charged_micros_frozen", d.FrozenChargedMicros, "charged_micros_live", d.LiveChargedMicros,
			"charged_micros_delta", d.LiveChargedMicros-d.FrozenChargedMicros)
	}
}
