package usage

import (
	"context"
	"log/slog"
	"sort"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// MaxInstalledModuleIDs bounds GetAppBillRequest.InstalledModuleIDs. An app that
// installs more modules than this is not one this bill can render anyway; the
// cap keeps the unnest join in ModuleInfraPriceCatalog bounded.
const MaxInstalledModuleIDs = 500

// The platform-infra markup, the ONE Go definition: the customer is charged 1.2x
// the raw COGS. cycle.infraMarkupNum/Den alias these (cycle imports usage), and
// TestInfraMarkup_MatchesTheSQLLiteral ties them to the `* 12 / 10` literals in
// usage.sql, so the display unit prices below cannot drift from the charge. The
// charged amounts still come from SQL and are untouched by this file.
const (
	InfraMarkupNum = 12
	InfraMarkupDen = 10
)

// customerUnitPrice is raw x 12/10, fractional on purpose (37 -> 44.4): a
// per-GiB-month display built on a rounded 44 would read $0.0321, not $0.0324.
func customerUnitPrice(rawMicros int64) float64 {
	return float64(rawMicros*InfraMarkupNum) / InfraMarkupDen
}

func customerUnitPricePtr(rawMicros *int64) *float64 {
	if rawMicros == nil {
		return nil
	}
	p := customerUnitPrice(*rawMicros)
	return &p
}

// withCustomerPrices copies the residual infra lines with their customer unit
// price set. A copy, because the store may hand back a slice it still owns.
func withCustomerPrices(lines []AppInfraUsage) []AppInfraUsage {
	out := make([]AppInfraUsage, len(lines))
	for i, l := range lines {
		l.CustomerUnitPriceMicros = customerUnitPrice(l.UnitPriceMicros)
		out[i] = l
	}
	return out
}

// infraPriceSource says which price bills a (module, metric). override is the
// module's stored price (nil = no row); absorbsAll is whether the module holds a
// stored override on EVERY active infra metric, the only shape
// AbsorbAllInfraPriceOverrides writes.
//
// 🔴 "absorbed" is a HEURISTIC: the ms.AbsorbInfra() declaration is not persisted,
// so it is inferred from the stored rows. A stored 0 reads absorbed only when the
// module is overridden on every active metric. Two known misclassifications:
//  1. a module that declares an explicit Price(0) on EVERY active infra metric
//     without AbsorbInfra reads "absorbed" (same stored shape);
//  2. a newly added sentinel metric has no row for a module until it republishes,
//     so that module's zeros read "override", not "absorbed", in the meantime.
//
// Persisting the declaration would remove both; this is display only and moves no
// charge.
func infraPriceSource(override *int64, absorbsAll bool) PriceSource {
	switch {
	case override == nil:
		return PriceSourceDefault
	case *override == 0 && absorbsAll:
		return PriceSourceAbsorbed
	default:
		return PriceSourceOverride
	}
}

// normalizeInstalledModuleIDs drops nil and duplicate ids, keeping order.
func normalizeInstalledModuleIDs(ids []uuid.UUID) ([]uuid.UUID, error) {
	if len(ids) > MaxInstalledModuleIDs {
		return nil, billing.InvalidInput("installed_module_ids exceeds the maximum")
	}
	seen := make(map[uuid.UUID]bool, len(ids))
	out := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if id == uuid.Nil || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out, nil
}

// priceModuleInfra adds customer unit prices and PriceSource to every per-module
// infra line (both dev_served partitions) and appends, to the CHARGED partition
// only, a zero-quantity row for each (installed module x active infra metric) that
// has no usage line. Totals are untouched: every added row is qty 0, charged 0,
// and the dev_served half stays a term of no total and grows no rows.
//
// Usage rows keep their order and content (the new fields aside); zero rows follow,
// sorted by module, display group, metric. With no module to look up the lines
// come back unchanged and the catalog is not read.
//
// The catalog read is display-only and NON-FATAL: on error it is logged and the
// lines come back undecorated (no zero rows, no price_source) rather than failing
// the bill. Only GetAppBill calls this (see computeAppBill's withDisplayPrices).
func (s *Service) priceModuleInfra(ctx context.Context, charged, devServed []AppModuleInfraUsage, installed []uuid.UUID) ([]AppModuleInfraUsage, []AppModuleInfraUsage) {
	ids := append([]uuid.UUID(nil), installed...)
	seen := make(map[uuid.UUID]bool, len(installed))
	for _, id := range installed {
		seen[id] = true
	}
	for _, part := range [][]AppModuleInfraUsage{charged, devServed} {
		for _, l := range part {
			if !seen[l.ModuleID] {
				seen[l.ModuleID] = true
				ids = append(ids, l.ModuleID)
			}
		}
	}
	if len(ids) == 0 {
		// Never null on the wire, whatever the store handed back.
		return append([]AppModuleInfraUsage{}, charged...), append([]AppModuleInfraUsage{}, devServed...)
	}

	cells, err := s.store.ModuleInfraPriceCatalog(ctx, ids)
	if err != nil {
		slog.ErrorContext(ctx, "module infra price catalog read failed (bill rendered without customer prices)",
			"module_count", len(ids), "error", err)
		return append([]AppModuleInfraUsage{}, charged...), append([]AppModuleInfraUsage{}, devServed...)
	}
	total, covered := map[uuid.UUID]int{}, map[uuid.UUID]int{}
	for _, c := range cells {
		total[c.ModuleID]++
		if c.ModuleUnitPriceMicros != nil {
			covered[c.ModuleID]++
		}
	}
	absorbsAll := func(m uuid.UUID) bool { return total[m] > 0 && covered[m] == total[m] }

	decorate := func(lines []AppModuleInfraUsage) []AppModuleInfraUsage {
		out := make([]AppModuleInfraUsage, len(lines))
		for i, l := range lines {
			l.CustomerUnitPriceMicros = customerUnitPrice(l.DefaultUnitPriceMicros)
			l.ModuleCustomerUnitPriceMicros = customerUnitPricePtr(l.ModuleUnitPriceMicros)
			l.PriceSource = infraPriceSource(l.ModuleUnitPriceMicros, absorbsAll(l.ModuleID))
			out[i] = l
		}
		return out
	}
	outCharged, outDev := decorate(charged), decorate(devServed)

	type key struct {
		module uuid.UUID
		metric string
	}
	hasUsage := make(map[key]bool, len(charged))
	for _, l := range charged {
		hasUsage[key{l.ModuleID, l.Metric}] = true
	}
	want := make(map[uuid.UUID]bool, len(installed))
	for _, id := range installed {
		want[id] = true
	}
	var zero []AppModuleInfraUsage
	for _, c := range cells {
		if !want[c.ModuleID] || hasUsage[key{c.ModuleID, c.Metric}] {
			continue
		}
		zero = append(zero, AppModuleInfraUsage{
			ModuleID:                      c.ModuleID,
			Metric:                        c.Metric,
			Label:                         c.Metric, // same as the usage rows: no label registry here
			Kind:                          c.Kind,
			Unit:                          c.Unit,
			Group:                         c.Group,
			DefaultUnitPriceMicros:        c.DefaultUnitPriceMicros,
			ModuleUnitPriceMicros:         c.ModuleUnitPriceMicros,
			CustomerUnitPriceMicros:       customerUnitPrice(c.DefaultUnitPriceMicros),
			ModuleCustomerUnitPriceMicros: customerUnitPricePtr(c.ModuleUnitPriceMicros),
			PriceSource:                   infraPriceSource(c.ModuleUnitPriceMicros, absorbsAll(c.ModuleID)),
		})
	}
	sort.SliceStable(zero, func(i, j int) bool {
		a, b := zero[i], zero[j]
		if a.ModuleID != b.ModuleID {
			return a.ModuleID.String() < b.ModuleID.String()
		}
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		return a.Metric < b.Metric
	})
	return append(outCharged, zero...), outDev
}
