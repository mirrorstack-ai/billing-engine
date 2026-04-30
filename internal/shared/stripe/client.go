// Package stripe wraps stripe-go/v85 with billing-engine conventions:
// env-driven construction, mapped sentinel errors, idempotency keys, and
// retry-with-backoff for transient failures.
package stripe

import (
	"errors"
	"os"

	stripego "github.com/stripe/stripe-go/v85"
)

// envSecretKey is the env var holding the Stripe secret API key. We refuse to
// boot without it so misconfiguration fails loudly at startup, not on the
// first billing request.
const envSecretKey = "STRIPE_SECRET_KEY"

// ErrMissingSecretKey is returned by NewClient when STRIPE_SECRET_KEY is unset.
var ErrMissingSecretKey = errors.New("stripe: STRIPE_SECRET_KEY env var is required")

// Client wraps stripe-go's *stripe.Client so callers reach v85 services via
// c.API.V1Customers, c.API.V1Subscriptions, etc.
type Client struct {
	API *stripego.Client
}

// NewClient reads STRIPE_SECRET_KEY from the environment and constructs a
// stripe-go client. Returns ErrMissingSecretKey if the env var is unset or
// empty.
func NewClient() (*Client, error) {
	key := os.Getenv(envSecretKey)
	if key == "" {
		return nil, ErrMissingSecretKey
	}
	return &Client{API: stripego.NewClient(key)}, nil
}
