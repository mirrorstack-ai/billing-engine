package usage

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
)

// Tax ESTIMATE on the account bill read (core-v2#250, owner 2026-07-21):
// list prices are NET; tax is a separate itemized line computed per the
// paying entity's rail and jurisdiction at invoice issuance. This file is the
// forecast of that line for the bill card, not the determination the invoice
// seals (internal/intent's TaxDetermination stays the authority).
//
// Jurisdiction today = the default card's issuing country (migration 074).
// Rules are a closed table: a jurisdiction without a rule is NOT "0% tax", it
// is not_configured, and the response says so.

const (
	// TaxStatusEstimated: a rule applied; TaxMicros is a forecast.
	TaxStatusEstimated = "estimated"
	// TaxStatusNotConfigured: no jurisdiction on file, or no rule for it yet.
	TaxStatusNotConfigured = "not_configured"

	// twVATRateBps is Taiwan 營業稅 5% on the net amount (core-v2#250 acceptance
	// criterion: "TW customers … charged net + 5% 營業稅").
	twVATRateBps int64 = 500
	// twVATRuleRevision names the rule so a displayed figure traces to it.
	twVATRuleRevision = "tw-vat-5pct/core-v2#250"

	bpsDenominator int64 = 10_000
)

// taxRuleFor is the closed rule table. ok=false means not configured.
func taxRuleFor(jurisdiction string) (rateBps int64, revision string, ok bool) {
	switch jurisdiction {
	case "TW":
		return twVATRateBps, twVATRuleRevision, true
	default:
		// Stripe-rail regions wait for Stripe Tax (core-v2#250); until then
		// the estimate is explicitly not configured rather than a guess.
		return 0, "", false
	}
}

// estimateAccountBillTax forecasts the tax line for a NET basis. The basis is
// clamped at 0 (a credit-driven negative projection owes no tax); the tax is
// rounded half-up in integer micros.
func estimateAccountBillTax(jurisdiction string, taxableMicros int64) AccountBillTax {
	if taxableMicros < 0 {
		taxableMicros = 0
	}
	rateBps, revision, ok := taxRuleFor(jurisdiction)
	if !ok {
		return AccountBillTax{
			Status:        TaxStatusNotConfigured,
			Jurisdiction:  jurisdiction,
			TaxableMicros: taxableMicros,
		}
	}
	return AccountBillTax{
		Status:        TaxStatusEstimated,
		Jurisdiction:  jurisdiction,
		RateBps:       rateBps,
		RuleRevision:  revision,
		TaxableMicros: taxableMicros,
		TaxMicros:     (taxableMicros*rateBps + bpsDenominator/2) / bpsDenominator,
	}
}

// taxEstimate resolves the payer's jurisdiction and forecasts the line. A
// store error is logged and degrades to not_configured: the bill must render
// even when the jurisdiction read fails, and not_configured is the honest
// state for "we could not tell".
func (s *Service) taxEstimate(ctx context.Context, accountID uuid.UUID, accountFound bool, taxableMicros int64) AccountBillTax {
	if !accountFound {
		return estimateAccountBillTax("", taxableMicros)
	}
	country, found, err := s.store.DefaultCardCountry(ctx, accountID)
	if err != nil {
		slog.WarnContext(ctx, "tax estimate: default card country read failed; reporting not_configured",
			"account_id", accountID, "error", err)
		return estimateAccountBillTax("", taxableMicros)
	}
	if !found {
		return estimateAccountBillTax("", taxableMicros)
	}
	return estimateAccountBillTax(country, taxableMicros)
}
