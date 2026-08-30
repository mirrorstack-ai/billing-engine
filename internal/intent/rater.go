package intent

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// Quarantine is why a set of usage facts could not be rated.
//
// INV-004: "Missing or conflicting usage provenance, price policy,
// module manifest, authorization, tax, notification evidence, rail
// capability or build identity must quarantine the intent."
//
// A quarantine is not an error in the ordinary sense — nothing went
// wrong with the machinery. It says the engine was asked to price
// something it has no rule for, and refused. The facts are named so an
// operator can see exactly which ones need a rule rather than being
// told the batch failed.
type Quarantine struct {
	Reason string
	Facts  []string
}

func (q Quarantine) Error() string {
	return fmt.Sprintf("intent: quarantined (%s): %v", q.Reason, q.Facts)
}

var (
	ErrRaterNoFacts       = errors.New("intent: nothing to rate")
	ErrRaterBookNotLoaded = errors.New("intent: no price book revision loaded")
	ErrRaterMixedSubjects = errors.New("intent: facts belong to more than one payer")
)

// TaxResolver determines tax for a subtotal.
//
// It is an interface so that the tax rule set can be replaced without
// touching the rater, and it returns a TaxDetermination whose Resolved
// flag distinguishes "no determination" from "determined as zero".
// docs/DESIGN.md §7 is built on refusing to guess, and a resolver that
// could only return a number would have no way to say it does not know.
type TaxResolver interface {
	Determine(payer Subject, currency string, subtotalMicros int64, at time.Time) TaxDetermination
}

// RateRequest is everything the rater needs. It carries no clock and no
// store: every input arrives as a value, which is what makes the same
// call reproducible by the offline verifier.
type RateRequest struct {
	Facts            []UsageFact
	PriceBook        PriceBookRevision
	Tax              TaxResolver
	AuthorizationID  string
	NoticePolicy     string
	ExecuteNotBefore time.Time
	ExecuteNotAfter  time.Time
	RatedAt          time.Time
}

// Rate turns reported usage into a sealed charge intent.
//
// This is the single derivation of INV-002: "DescribeCharge,
// ProposeChargeIntent, invoice presentation, ledger posting and the
// offline verifier must use the same pure rating model and the same
// canonical encoding. No frontend formula and no per-provider formula
// may exist."
//
// "Two implementations of one question drift, and the looser one is the
// one that charges." So preview and settlement call this, and neither
// is allowed its own arithmetic.
//
// Facts with the same idempotency key are one fact: re-delivery is
// harmless, and a caller that retries does not double a bill.
func Rate(req RateRequest) (ChargeIntent, error) {
	if len(req.Facts) == 0 {
		return ChargeIntent{}, ErrRaterNoFacts
	}
	if !req.PriceBook.Loaded() {
		return ChargeIntent{}, ErrRaterBookNotLoaded
	}
	if req.Tax == nil {
		// A nil resolver would otherwise be indistinguishable from a
		// zero-tax jurisdiction, which is the exact substitution
		// INV-004 forbids.
		return ChargeIntent{}, Quarantine{Reason: "no tax resolver"}
	}

	deduped := make(map[string]UsageFact, len(req.Facts))
	var invalid, unpriced []string

	for _, fact := range req.Facts {
		if err := fact.Validate(); err != nil {
			invalid = append(invalid, fact.IdempotencyKey+": "+err.Error())
			continue
		}
		if _, seen := deduped[fact.IdempotencyKey]; seen {
			continue
		}
		deduped[fact.IdempotencyKey] = fact
	}
	if len(invalid) > 0 {
		sort.Strings(invalid)
		return ChargeIntent{}, Quarantine{Reason: "usage provenance", Facts: invalid}
	}

	// Ordering by idempotency key makes the resulting lines — and so
	// the digest — independent of the order facts arrived in. Two
	// deliveries of the same batch in different orders must produce the
	// same document, or the digest is not an identity.
	keys := make([]string, 0, len(deduped))
	for key := range deduped {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var (
		payer    Subject
		lines    []Line
		sourceOf = make([]string, 0, len(keys))
	)

	for i, key := range keys {
		fact := deduped[key]
		if i == 0 {
			payer = fact.Subject
		} else if fact.Subject != payer {
			return ChargeIntent{}, fmt.Errorf("%w: %s and %s",
				ErrRaterMixedSubjects, payer, fact.Subject)
		}

		priceKey := PriceKey{
			Meter:         fact.Meter,
			Module:        fact.Module,
			ModuleVersion: fact.ModuleVersion,
		}
		unitPrice, priced := req.PriceBook.UnitPriceMicros(priceKey)
		if !priced {
			// Not an error and not a zero. The engine has no rule for
			// this meter at this module version, so it declines to
			// invent one.
			unpriced = append(unpriced, key+" ("+priceKey.String()+")")
			continue
		}

		lines = append(lines, NewLine(
			fact.Meter, fact.Module, fact.ModuleVersion, fact.Quantity, unitPrice,
		))
		sourceOf = append(sourceOf, key)
	}

	if len(unpriced) > 0 {
		sort.Strings(unpriced)
		return ChargeIntent{}, Quarantine{Reason: "price policy", Facts: unpriced}
	}

	var subtotal int64
	for _, line := range lines {
		subtotal += line.AmountMicros()
	}

	tax := req.Tax.Determine(payer, req.PriceBook.Currency(), subtotal, req.RatedAt)
	if !tax.Resolved {
		return ChargeIntent{}, Quarantine{Reason: "tax", Facts: sourceOf}
	}

	return Seal(Draft{
		Payer:             payer,
		Currency:          req.PriceBook.Currency(),
		Lines:             lines,
		PriceBookRevision: req.PriceBook.Revision(),
		Tax:               tax,
		AuthorizationID:   req.AuthorizationID,
		NoticePolicy:      req.NoticePolicy,
		ExecuteNotBefore:  req.ExecuteNotBefore,
		ExecuteNotAfter:   req.ExecuteNotAfter,
		SourceFactKeys:    sourceOf,
	})
}
