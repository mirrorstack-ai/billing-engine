package architecture

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// knownReads are the provider methods that observe without changing
// anything, and so are deliberately absent from providerEffects.
//
// Listed rather than inferred. The whole inventory rests on the
// read/mutate split being right, and a rule that guessed — "anything
// starting with Get or List" — would classify a hypothetical
// GetOrCreateCustomer as harmless.
var knownReads = map[string]bool{
	"GetCustomer":         true,
	"GetInvoice":          true,
	"FindInvoiceByRef":    true,
	"ListInvoiceItems":    true,
	"ListInvoicePayments": true,
	"RetrieveCharge":      true,
}

// TestEveryProviderMethodIsClassified ties providerEffects to the
// interfaces it is meant to describe.
//
// providerEffects is a hand-written name list, and the scan that uses
// it silently ignores any method not in it. So a provider method added
// to internal/shared/stripe would be invisible to the mutation
// inventory — the check would still pass, over a smaller surface than
// it claims to cover.
//
// Deriving the expectation from the interface types by reflection makes
// that impossible: adding a method breaks this test until someone
// decides whether it reads or changes something.
func TestEveryProviderMethodIsClassified(t *testing.T) {
	ifaces := []reflect.Type{
		reflect.TypeOf((*billingstripe.Client)(nil)).Elem(),
		reflect.TypeOf((*billingstripe.CombinedProrationClient)(nil)).Elem(),
		reflect.TypeOf((*billingstripe.CreditPurchaseClient)(nil)).Elem(),
		reflect.TypeOf((*billingstripe.AutoTopUpClient)(nil)).Elem(),
	}

	methods := map[string]bool{}
	for _, it := range ifaces {
		for i := 0; i < it.NumMethod(); i++ {
			methods[it.Method(i).Name] = true
		}
	}
	if len(methods) == 0 {
		t.Fatal("reflected zero provider methods; the interfaces moved and this check is inert")
	}

	var unclassified []string
	for name := range methods {
		if _, mutates := providerEffects[name]; mutates {
			continue
		}
		if knownReads[name] {
			continue
		}
		unclassified = append(unclassified, name)
	}
	sort.Strings(unclassified)
	if len(unclassified) > 0 {
		t.Errorf("%d provider method(s) are neither classified as state-changing nor listed as reads.\n"+
			"An unclassified method is invisible to the mutation inventory, so the check would keep "+
			"passing over a smaller surface than it claims:\n  %s",
			len(unclassified), strings.Join(unclassified, "\n  "))
	}

	// The other direction: an entry describing a method that no longer
	// exists means the inventory is describing a surface that is gone.
	var stale []string
	for name := range providerEffects {
		if !methods[name] {
			stale = append(stale, name)
		}
	}
	for name := range knownReads {
		if !methods[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d classified method(s) are on no provider interface; remove them:\n  %s",
			len(stale), strings.Join(stale, "\n  "))
	}
}
