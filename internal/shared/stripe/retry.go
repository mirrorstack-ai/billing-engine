package stripe

import (
	"context"
	"errors"
	"net"
	"time"

	stripego "github.com/stripe/stripe-go/v85"
)

// retryDelays are the backoff intervals between attempts: total of three
// attempts (the initial call + 2 retries). Tuned for Stripe's typical 5xx
// blip duration; longer waits would exceed Lambda timeouts in callers.
var retryDelays = []time.Duration{
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
}

// Do runs fn with retries on transient failures: 5xx Stripe responses and
// network errors. 4xx responses (validation, card declines, not-found)
// return immediately — retrying them would just re-fail. Honors ctx
// cancellation between attempts.
func Do(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < len(retryDelays); attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelays[attempt-1]):
			}
		}
		err = fn()
		if err == nil {
			return nil
		}
		if !shouldRetry(err) {
			return err
		}
	}
	return err
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	var se *stripego.Error
	if errors.As(err, &se) {
		return se.HTTPStatusCode >= 500
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return false
}
