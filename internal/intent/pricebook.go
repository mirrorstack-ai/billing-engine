package intent

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// PriceKey identifies what a price applies to.
//
// The module version is part of the key on purpose. docs/SECURITY.md §2
// records that per-version module metric prices already have immutable
// snapshots, and that is the property worth keeping: a fact priced
// under one version stays reproducible after the module moves on. A
// price keyed only on the meter silently re-rates history the next time
// a module ships.
type PriceKey struct {
	Meter         string
	Module        string
	ModuleVersion string
}

func (k PriceKey) String() string {
	return k.Module + "@" + k.ModuleVersion + "/" + k.Meter
}

// PriceBookRevision is one immutable, effective-dated set of prices.
//
// docs/SECURITY.md §2 records the gap it answers: "pricing is not yet
// one complete, effective-dated, customer-disclosed policy and legacy
// fallback paths remain". The two words that matter are *complete* and
// *effective-dated*. Complete, because a book with holes needs a
// fallback, and a fallback is a second pricing implementation — the
// thing INV-002 exists to forbid. Effective-dated, because a charge is
// only reproducible if the revision that priced it can be named and
// re-read.
//
// Fields are unexported: a revision that can be edited after something
// was priced under it makes every past charge unverifiable.
type PriceBookRevision struct {
	revision      string
	effectiveFrom time.Time
	currency      string
	prices        map[PriceKey]int64
}

// PriceBookDefinition is the input to NewPriceBookRevision.
type PriceBookDefinition struct {
	Revision      string
	EffectiveFrom time.Time
	Currency      string
	Prices        map[PriceKey]int64
}

var (
	ErrPriceBookRevisionMissing = errors.New("intent: price book has no revision id")
	ErrPriceBookDateMissing     = errors.New("intent: price book has no effective date")
	ErrPriceBookCurrencyMissing = errors.New("intent: price book names no currency")
	ErrPriceBookEmpty           = errors.New("intent: price book has no prices")
	ErrPriceNegative            = errors.New("intent: price book contains a negative price")
	ErrPriceKeyIncomplete       = errors.New("intent: price book key is missing a meter, module or version")
)

// NewPriceBookRevision validates a definition and freezes it.
func NewPriceBookRevision(def PriceBookDefinition) (PriceBookRevision, error) {
	if strings.TrimSpace(def.Revision) == "" {
		return PriceBookRevision{}, ErrPriceBookRevisionMissing
	}
	if def.EffectiveFrom.IsZero() {
		return PriceBookRevision{}, ErrPriceBookDateMissing
	}
	if strings.TrimSpace(def.Currency) == "" {
		return PriceBookRevision{}, ErrPriceBookCurrencyMissing
	}
	if len(def.Prices) == 0 {
		return PriceBookRevision{}, ErrPriceBookEmpty
	}

	prices := make(map[PriceKey]int64, len(def.Prices))
	for key, price := range def.Prices {
		if strings.TrimSpace(key.Meter) == "" ||
			strings.TrimSpace(key.Module) == "" ||
			strings.TrimSpace(key.ModuleVersion) == "" {
			return PriceBookRevision{}, fmt.Errorf("%w: %s", ErrPriceKeyIncomplete, key)
		}
		if price < 0 {
			return PriceBookRevision{}, fmt.Errorf("%w: %s at %d", ErrPriceNegative, key, price)
		}
		prices[key] = price
	}

	return PriceBookRevision{
		revision:      def.Revision,
		effectiveFrom: def.EffectiveFrom.UTC(),
		currency:      strings.ToUpper(strings.TrimSpace(def.Currency)),
		prices:        prices,
	}, nil
}

// UnitPriceMicros returns the price for a key, and whether the book
// has one.
//
// The second return value is the whole interface. INV-004: an unknown
// input must quarantine the intent, and "must never silently become
// zero". A lookup returning a bare int64 makes "not priced" and "priced
// at zero" the same value, and the caller that forgets to check gets a
// free charge rather than a refusal.
func (r PriceBookRevision) UnitPriceMicros(key PriceKey) (int64, bool) {
	price, ok := r.prices[key]
	return price, ok
}

// Revision identifies this book.
func (r PriceBookRevision) Revision() string { return r.revision }

// Currency is what its prices are denominated in.
func (r PriceBookRevision) Currency() string { return r.currency }

// EffectiveFrom is when this revision takes effect.
func (r PriceBookRevision) EffectiveFrom() time.Time { return r.effectiveFrom }

// Loaded reports whether this is a real revision rather than a zero
// value, so that a missed lookup cannot be mistaken for an empty book.
func (r PriceBookRevision) Loaded() bool { return r.revision != "" }
