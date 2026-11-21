package einvoice

import "fmt"

// Registered provider names under D627. "systemlead" and "ecpay" are
// gone with the vendor route; a future provider adds its own name here.
const (
	// ProviderMOFTurnkey is our own MOF Turnkey, reached through the
	// adapter in mirrorstack-ai/billing-engine#341 (not built yet).
	ProviderMOFTurnkey = "mof_turnkey"
	// ProviderFake is the in-memory fake. Sandbox payments go here and
	// nowhere else (D524).
	ProviderFake = "fake"
)

// Registry resolves a provider name to one Provider. A seller sits on
// exactly one provider at a time: a 字軌 range cannot live in two invoice
// systems, so switching is a per-seller migration, not failover. This
// type deliberately offers no multi-provider fallback or load-balancing
// — callers resolve a seller to a provider name elsewhere and ask this
// Registry for that one name.
type Registry struct {
	providers map[string]Provider
}

// NewRegistry returns an empty registry. Register before Get.
func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]Provider)}
}

// Register adds a provider under a name. Registering the same name
// twice is a programming error, not a runtime condition to swallow: it
// means two adapters both believe they are "mof_turnkey", and silently
// keeping the second would resolve some sellers to the wrong system.
// A provider whose Name differs from the key, or whose Capabilities
// contradict themselves (Capabilities.Validate), is refused the same way.
func (r *Registry) Register(name string, p Provider) {
	if _, exists := r.providers[name]; exists {
		panic(fmt.Sprintf("einvoice: provider %q already registered", name))
	}
	if p.Name() != name {
		panic(fmt.Sprintf("einvoice: provider named %q registered as %q", p.Name(), name))
	}
	if err := p.Capabilities().Validate(); err != nil {
		panic(fmt.Sprintf("einvoice: provider %q: %v", name, err))
	}
	r.providers[name] = p
}

// Get resolves a name, or ErrUnknownProvider. It never returns a zero
// Provider for an unknown name: a caller that does not check the error
// must not be able to proceed with a nil interface value and panic
// somewhere unrelated later.
func (r *Registry) Get(name string) (Provider, error) {
	p, ok := r.providers[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownProvider, name)
	}
	return p, nil
}

// Names lists every registered provider name.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}
