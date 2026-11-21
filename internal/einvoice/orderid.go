package einvoice

import (
	"errors"
	"fmt"
	"regexp"
)

const (
	// DefaultMaxOrderNumberLen is the limit for a provider that states
	// none: 20 characters, the tightest limit a remote invoicing service
	// is known to impose. A future vendor adapter keeps it (or its own).
	DefaultMaxOrderNumberLen = 20
	// SelfNumberingMaxOrderNumberLen is the limit the Turnkey provider and
	// the fake in self-numbering mode declare. Under D627 the invoice
	// number comes from our own ledger, so the order number is only our
	// internal idempotency key and never leaves the engine; 64 leaves room
	// for a seller prefix plus a payment-fact id.
	SelfNumberingMaxOrderNumberLen = 64
	// MaxOrderNumberLenCeiling bounds what any provider may declare, so a
	// key can always be stored in a fixed-width column.
	MaxOrderNumberLenCeiling = 64
)

// orderNumberPattern is the character set every provider shares. The
// length is checked separately because it is per provider.
var orderNumberPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// ErrInvalidOrderNumber is returned by ValidateOrderNumber.
var ErrInvalidOrderNumber = errors.New("einvoice: invalid order number")

var (
	errSelfNumberingPending = errors.New("einvoice: capabilities: SelfNumbering and SupportsAsyncPending are mutually exclusive")
	errOrderNumberLenRange  = fmt.Errorf("einvoice: capabilities: MaxOrderNumberLen must be 0 (default) or %d..%d", DefaultMaxOrderNumberLen, MaxOrderNumberLenCeiling)
)

// OrderNumberLimit is the longest order number this provider accepts.
func (c Capabilities) OrderNumberLimit() int {
	if c.MaxOrderNumberLen == 0 {
		return DefaultMaxOrderNumberLen
	}
	return c.MaxOrderNumberLen
}

// ValidateOrderNumber checks the shape against this provider's limit:
// 1 to OrderNumberLimit() characters of [A-Za-z0-9_]. It does not check
// uniqueness — that is a Provider's Issue, keyed on the same value.
func (c Capabilities) ValidateOrderNumber(n ProviderOrderNumber) error {
	if len(n) == 0 || len(n) > c.OrderNumberLimit() || !orderNumberPattern.MatchString(string(n)) {
		return fmt.Errorf("%w: must be 1-%d characters of [A-Za-z0-9_]", ErrInvalidOrderNumber, c.OrderNumberLimit())
	}
	return nil
}

// ValidateOrderNumber checks n against the default limit, for a caller
// that builds a key before it knows the provider. A key that passes here
// passes every provider.
func ValidateOrderNumber(n ProviderOrderNumber) error {
	return Capabilities{}.ValidateOrderNumber(n)
}
