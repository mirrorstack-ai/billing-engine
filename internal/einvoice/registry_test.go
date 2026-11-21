package einvoice_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
)

// stubProvider is the minimum Provider implementation needed to drive
// the registry tests without pulling in fakeinvoice, which has its own
// richer tests.
type stubProvider struct{ name string }

func (s stubProvider) Name() string                        { return s.name }
func (s stubProvider) Capabilities() einvoice.Capabilities { return einvoice.Capabilities{} }
func (s stubProvider) Issue(context.Context, einvoice.Document) (einvoice.Result, error) {
	return einvoice.Result{}, nil
}
func (s stubProvider) Void(context.Context, string, einvoice.Mode) (einvoice.Result, error) {
	return einvoice.Result{}, nil
}
func (s stubProvider) Allowance(context.Context, string, []einvoice.Line, einvoice.Mode) (einvoice.Result, error) {
	return einvoice.Result{}, nil
}
func (s stubProvider) VoidAllowance(context.Context, string, einvoice.Mode) (einvoice.Result, error) {
	return einvoice.Result{}, nil
}
func (s stubProvider) QueryByOrderNumber(context.Context, einvoice.ProviderOrderNumber, einvoice.Mode) (einvoice.Result, error) {
	return einvoice.Result{}, nil
}
func (s stubProvider) QueryByInvoiceNumber(context.Context, string, einvoice.Mode) (einvoice.Result, error) {
	return einvoice.Result{}, nil
}
func (s stubProvider) ValidateCarrier(einvoice.CarrierKind, string) error { return nil }
func (s stubProvider) ValidateLoveCode(string) error                      { return nil }
func (s stubProvider) LookupTrack(context.Context, string, string, einvoice.Mode) (einvoice.TrackStatus, error) {
	return einvoice.TrackStatus{}, nil
}
func (s stubProvider) VoidWindow(issuedAt time.Time) time.Time { return issuedAt }
func (s stubProvider) RegisterCounterparty(context.Context, einvoice.Counterparty, einvoice.Mode) error {
	return einvoice.ErrUnsupported
}

func TestRegistryGetUnknownName(t *testing.T) {
	r := einvoice.NewRegistry()
	_, err := r.Get("nope")
	if !errors.Is(err, einvoice.ErrUnknownProvider) {
		t.Fatalf("got %v, want ErrUnknownProvider", err)
	}
}

func TestRegistryRegisterAndGet(t *testing.T) {
	r := einvoice.NewRegistry()
	r.Register("fake", stubProvider{name: "fake"})

	p, err := r.Get("fake")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Name() != "fake" {
		t.Fatalf("got provider %q, want fake", p.Name())
	}
}

func TestRegistryRegisterTwicePanics(t *testing.T) {
	r := einvoice.NewRegistry()
	r.Register("fake", stubProvider{name: "fake"})

	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic registering a duplicate name")
		}
	}()
	r.Register("fake", stubProvider{name: "fake"})
}
