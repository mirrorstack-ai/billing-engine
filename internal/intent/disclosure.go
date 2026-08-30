package intent

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Disclosure is what a customer accepts before a credit purchase.
//
// docs/DESIGN.md §6 states the contents exactly: "your acceptance of
// engine-signed disclosure bytes naming currency, amount, credit received,
// restrictions, expiry, refund terms, rail and intent digest".
//
// Every field is required. A disclosure with a blank restrictions or refund
// terms is not a shorter disclosure — it is one that failed to say something
// §6 requires it to say, and the customer accepted a document with a hole in
// it. So this type refuses to produce a digest at all rather than digest an
// incomplete one: an acceptance is only meaningful if what was accepted is
// fully determined.
type Disclosure struct {
	// Currency and AmountMicros are what the customer pays.
	Currency     string
	AmountMicros int64
	// CreditMicros is what they receive. It is NOT the same number as the
	// amount — a promotion, a bonus tier or a fee makes them differ, and
	// collapsing them would hide exactly the term a customer most needs to
	// see.
	CreditMicros int64
	// Restrictions and RefundTerms are the prose the customer is agreeing
	// to. They are digested verbatim, so changing a word changes the
	// document.
	Restrictions string
	RefundTerms  string
	// ExpiresAt is when the purchased credit expires.
	ExpiresAt time.Time
	// Rail is the provider the money moves over.
	Rail string
	// IntentDigest binds this disclosure to the exact charge it describes.
	// Without it an acceptance floats free and could be replayed against a
	// different purchase.
	IntentDigest string
}

var (
	ErrDisclosureIncomplete    = errors.New("intent: the disclosure omits something §6 requires it to name")
	ErrDisclosureNonPositive   = errors.New("intent: a disclosure amount must be positive")
	ErrDisclosureUnboundIntent = errors.New("intent: the disclosure names no intent digest")
)

// Digest returns the identity of the disclosure bytes.
//
// It is what the customer's acceptance references, so the encoding must be
// injective: two different disclosures must never produce one digest, or an
// acceptance of one would attest to the other. It uses the same
// length-prefixed canonical encoder as the intent digest, for the same reason
// — a separator-joined encoding collides on values containing the separator,
// and "restrictions" is free prose that can contain anything.
func (d Disclosure) Digest() (string, error) {
	if err := d.validate(); err != nil {
		return "", err
	}
	e := &canonicalEncoder{}
	e.string("disclosure/v1")
	e.string(strings.ToUpper(strings.TrimSpace(d.Currency)))
	e.int(d.AmountMicros)
	e.int(d.CreditMicros)
	e.string(d.Restrictions)
	e.string(d.RefundTerms)
	e.time(d.ExpiresAt)
	e.string(d.Rail)
	e.string(d.IntentDigest)
	return e.digest(), nil
}

func (d Disclosure) validate() error {
	for _, f := range []struct {
		name  string
		value string
	}{
		{"currency", d.Currency},
		{"restrictions", d.Restrictions},
		{"refund terms", d.RefundTerms},
		{"rail", d.Rail},
	} {
		if strings.TrimSpace(f.value) == "" {
			return fmt.Errorf("%w: %s", ErrDisclosureIncomplete, f.name)
		}
	}
	if d.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: expiry", ErrDisclosureIncomplete)
	}
	if strings.TrimSpace(d.IntentDigest) == "" {
		return ErrDisclosureUnboundIntent
	}
	// A zero or negative purchase is not a purchase, and a zero credit is
	// a purchase that gives nothing. Both are refused rather than digested,
	// because either would be a document the customer could accept and get
	// nothing from.
	if d.AmountMicros <= 0 || d.CreditMicros <= 0 {
		return fmt.Errorf("%w: amount=%d credit=%d", ErrDisclosureNonPositive, d.AmountMicros, d.CreditMicros)
	}
	return nil
}
