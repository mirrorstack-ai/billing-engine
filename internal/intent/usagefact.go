// Package intent holds the objects that must exist before money moves.
//
// docs/DESIGN.md §1 lists five facts that today's code keeps confusing
// with one another, and gives each its own record:
//
//	what happened                  UsageFact
//	which rules apply              PriceBookRevision, TaxDetermination
//	what you authorized            BillingAuthorization
//	what effect is proposed        ChargeIntent
//	what an external rail did      PaymentAttempt, ChargeReceipt
//
// No one record may substitute for another. A billing run created
// moments before a provider request is an execution record, not an
// intent; a mutable draft invoice nobody can see is not notice; a
// post-charge badge is not a pre-charge control.
//
// This package is pure. It performs no I/O, reads no clock, and holds
// no provider client — the discipline of internal/account/eligibility,
// which is the shape docs/DESIGN.md asks for and the opposite of the
// coordinator, whose woven call sites are where the capability leak
// lives. Purity is what lets one implementation serve preview,
// settlement and the offline verifier alike (INV-002); a rater that can
// reach a database can answer differently on two calls, and then the
// preview is related to the charge by hope.
package intent

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// UsageFact is what the caller is permitted to tell the engine about
// something that happened.
//
// INV-001 draws the line: usage ingress may carry a payer or app
// subject, a declared meter, a module and its immutable version, an
// integer quantity, an occurrence time, and an idempotency key. It must
// not carry amount, price, rate, currency, subtotal, tax, discount,
// credit, total, invoiceLine, paymentMethod, provider, executeAt, or a
// notice or authorization status.
//
// The struct is that sentence made structural. There is no field for a
// price because there is nowhere to put one — a caller cannot propose a
// number the engine did not derive, and a reviewer can see that by
// reading the type rather than by auditing the validator.
//
// Quantity is an integer for the same reason money is. A float quantity
// multiplied by a price is a number whose last digits depend on the
// order of operations, and two implementations that disagree in the
// last digit disagree about a bill.
type UsageFact struct {
	// Subject is whose usage this is — the payer or the app. The engine
	// treats it as opaque and resolves authority elsewhere; see INV-006
	// on why that is a trust assumption rather than a control.
	Subject Subject

	// Meter names the declared metric. It must already exist in the
	// price book; an unknown meter quarantines the fact rather than
	// pricing it at zero (INV-004).
	Meter string

	// Module and ModuleVersion identify what produced the usage.
	// The version is immutable by contract, so a fact priced under one
	// version stays reproducible after the module moves on.
	Module        string
	ModuleVersion string

	// Quantity is a non-negative integer count in the meter's own unit.
	Quantity int64

	// OccurredAt is when the usage happened, which decides the period
	// it falls in and the price revision that applies. It is supplied
	// by the caller because only the caller was there; it is bounded
	// elsewhere, since a caller that can date usage freely can choose
	// its own price revision.
	OccurredAt time.Time

	// IdempotencyKey makes re-delivery harmless. Two facts with the
	// same key are the same fact.
	IdempotencyKey string
}

// Subject identifies whose usage or charge this is.
type Subject struct {
	// Kind is "user", "org", or "app".
	Kind string
	// ID is the caller's identifier for that subject, treated as
	// opaque.
	ID string
}

// subjectKinds is the closed set. INV-004: an unknown input must
// quarantine rather than dispatch an effect, so an unrecognised kind is
// refused instead of being treated as some default.
var subjectKinds = map[string]bool{"user": true, "org": true, "app": true}

// Valid reports whether the subject names a kind the engine knows.
func (s Subject) Valid() bool { return subjectKinds[s.Kind] && s.ID != "" }

func (s Subject) String() string { return s.Kind + ":" + s.ID }

// Errors returned by Validate. They are distinguishable because a
// caller integrating against this engine has to be able to tell a
// malformed request from a refused one.
var (
	ErrSubjectUnknown   = errors.New("intent: subject kind is not one the engine knows")
	ErrMeterMissing     = errors.New("intent: no meter named")
	ErrModuleMissing    = errors.New("intent: no module named")
	ErrVersionMissing   = errors.New("intent: no module version named")
	ErrQuantityNegative = errors.New("intent: quantity is negative")
	ErrOccurredAtUnset  = errors.New("intent: no occurrence time")
	ErrIdempotencyUnset = errors.New("intent: no idempotency key")
)

// Validate reports why a usage fact cannot be accepted, or nil.
//
// It refuses rather than repairs. INV-004: "It must never silently
// become zero, fall back to a mutable default, guess a jurisdiction, or
// call a provider with a partial total." A validator that fills in a
// missing field has decided something on the customer's behalf, and the
// resulting charge is no longer derived from what was reported.
func (f UsageFact) Validate() error {
	switch {
	case !f.Subject.Valid():
		return fmt.Errorf("%w: %q", ErrSubjectUnknown, f.Subject.Kind)
	case strings.TrimSpace(f.Meter) == "":
		return ErrMeterMissing
	case strings.TrimSpace(f.Module) == "":
		return ErrModuleMissing
	case strings.TrimSpace(f.ModuleVersion) == "":
		return ErrVersionMissing
	case f.Quantity < 0:
		return fmt.Errorf("%w: %d", ErrQuantityNegative, f.Quantity)
	case f.OccurredAt.IsZero():
		return ErrOccurredAtUnset
	case strings.TrimSpace(f.IdempotencyKey) == "":
		return ErrIdempotencyUnset
	}
	return nil
}
