package stripe

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	stripego "github.com/stripe/stripe-go/v85"
)

func withFastRetries(t *testing.T) {
	t.Helper()
	orig := retryDelays
	retryDelays = []time.Duration{1 * time.Millisecond, 1 * time.Millisecond, 1 * time.Millisecond}
	t.Cleanup(func() { retryDelays = orig })
}

func TestDo_SucceedsFirstTry(t *testing.T) {
	withFastRetries(t)
	var calls int32
	err := Do(context.Background(), func() error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestDo_RetriesOn5xx(t *testing.T) {
	withFastRetries(t)
	var calls int32
	err := Do(context.Background(), func() error {
		n := atomic.AddInt32(&calls, 1)
		if n < 2 {
			return &stripego.Error{HTTPStatusCode: 503, Type: stripego.ErrorTypeAPI}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
}

func TestDo_GivesUpAfterThreeAttempts(t *testing.T) {
	withFastRetries(t)
	var calls int32
	want := &stripego.Error{HTTPStatusCode: 500, Type: stripego.ErrorTypeAPI}
	err := Do(context.Background(), func() error {
		atomic.AddInt32(&calls, 1)
		return want
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var se *stripego.Error
	if !errors.As(err, &se) || se != want {
		t.Fatalf("err = %v, want stripe.Error 500", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
}

func TestDo_NoRetryOn4xx(t *testing.T) {
	withFastRetries(t)
	var calls int32
	err := Do(context.Background(), func() error {
		atomic.AddInt32(&calls, 1)
		return &stripego.Error{HTTPStatusCode: 402, Type: stripego.ErrorTypeCard, Code: stripego.ErrorCodeCardDeclined}
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls = %d, want 1 (no retry on 4xx)", got)
	}
}

type fakeNetErr struct{}

func (fakeNetErr) Error() string   { return "fake net error" }
func (fakeNetErr) Timeout() bool   { return true }
func (fakeNetErr) Temporary() bool { return true }

var _ net.Error = fakeNetErr{}

func TestDo_RetriesOnNetworkError(t *testing.T) {
	withFastRetries(t)
	var calls int32
	err := Do(context.Background(), func() error {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return fakeNetErr{}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
}

func TestDo_RespectsCtxCancel(t *testing.T) {
	// Real (slow) delays so cancel happens during the inter-attempt sleep.
	ctx, cancel := context.WithCancel(context.Background())
	var calls int32
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := Do(ctx, func() error {
		atomic.AddInt32(&calls, 1)
		return &stripego.Error{HTTPStatusCode: 500, Type: stripego.ErrorTypeAPI}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := atomic.LoadInt32(&calls); got > 2 {
		t.Fatalf("calls = %d, want <=2 (ctx should have cancelled before all retries)", got)
	}
}

func TestDo_NonRetryableNonStripe(t *testing.T) {
	withFastRetries(t)
	var calls int32
	want := errors.New("plain error")
	err := Do(context.Background(), func() error {
		atomic.AddInt32(&calls, 1)
		return want
	})
	if err != want {
		t.Fatalf("err = %v, want %v", err, want)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}
