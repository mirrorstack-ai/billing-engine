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
type stubProvider struct {
	name string
	caps einvoice.Capabilities
}

func (s stubProvider) Name() string                        { return s.name }
func (s stubProvider) Capabilities() einvoice.Capabilities { return s.caps }
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
	r.Register(einvoice.ProviderFake, stubProvider{name: einvoice.ProviderFake})
	r.Register(einvoice.ProviderMOFTurnkey, stubProvider{
		name: einvoice.ProviderMOFTurnkey,
		caps: einvoice.Capabilities{SelfNumbering: true, MaxOrderNumberLen: einvoice.SelfNumberingMaxOrderNumberLen},
	})

	for _, name := range []string{einvoice.ProviderFake, einvoice.ProviderMOFTurnkey} {
		p, err := r.Get(name)
		if err != nil {
			t.Fatalf("Get(%q): %v", name, err)
		}
		if p.Name() != name {
			t.Fatalf("got provider %q, want %q", p.Name(), name)
		}
	}
}

// The vendor route is gone (D627): its names resolve to nothing.
func TestRegistryHasNoVendorNames(t *testing.T) {
	if einvoice.ProviderMOFTurnkey != "mof_turnkey" || einvoice.ProviderFake != "fake" {
		t.Fatalf("provider names changed: %q, %q", einvoice.ProviderMOFTurnkey, einvoice.ProviderFake)
	}
	r := einvoice.NewRegistry()
	r.Register(einvoice.ProviderFake, stubProvider{name: einvoice.ProviderFake})
	for _, name := range []string{"systemlead", "ecpay"} {
		if _, err := r.Get(name); !errors.Is(err, einvoice.ErrUnknownProvider) {
			t.Fatalf("Get(%q) = %v, want ErrUnknownProvider", name, err)
		}
	}
}

func TestRegistryRefusesMisnamedProvider(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic registering a provider under another name")
		}
	}()
	einvoice.NewRegistry().Register(einvoice.ProviderMOFTurnkey, stubProvider{name: einvoice.ProviderFake})
}

func TestRegistryRefusesContradictoryCapabilities(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic registering SelfNumbering together with SupportsAsyncPending")
		}
	}()
	einvoice.NewRegistry().Register(einvoice.ProviderFake, stubProvider{
		name: einvoice.ProviderFake,
		caps: einvoice.Capabilities{SelfNumbering: true, SupportsAsyncPending: true},
	})
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
