package einvoice

import "fmt"

// Registry resolves a provider name — "systemlead", "ecpay", "fake" —
// to one Provider. Item 11 of mirrorstack-ai/billing-engine#286: a
// seller sits on exactly one provider at a time, and switching is a
// per-seller migration, not failover, so this type deliberately offers
// no multi-provider fallback or load-balancing — callers resolve a
// seller to a provider name elsewhere and ask this Registry for that
// one name.
type Registry struct {
	providers map[string]Provider
}

// NewRegistry returns an empty registry. Register before Get.
func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]Provider)}
}

// Register adds a provider under a name. Registering the same name
// twice is a programming error, not a runtime condition to swallow: it
// means two adapters both believe they are "systemlead", and silently
// keeping the second would resolve some sellers to the wrong vendor.
func (r *Registry) Register(name string, p Provider) {
	if _, exists := r.providers[name]; exists {
		panic(fmt.Sprintf("einvoice: provider %q already registered", name))
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
