package stripe

import (
	"net/http"
	"net/http/httptest"
	"testing"

	stripego "github.com/stripe/stripe-go/v85"
)

// docs/SECURITY.md §2: "One apparent SDK mutation may submit multiple
// HTTP writes after an ambiguous transport failure."
//
// stripe-go's default is two retries, so this must be asserted rather
// than assumed. A future SDK bump that changed the default would
// otherwise reopen the gap silently, which is exactly how the gap came
// to exist.
func TestBackendsMakeNoAutomaticRetries(t *testing.T) {
	backends := newBackends()

	for name, backend := range map[string]stripego.Backend{
		"API":     backends.API,
		"Uploads": backends.Uploads,
	} {
		impl, ok := backend.(*stripego.BackendImplementation)
		if !ok {
			t.Fatalf("%s backend is %T, not the implementation whose retry count can be read", name, backend)
		}
		if impl.MaxNetworkRetries != 0 {
			t.Errorf("%s backend retries %d times; one call would put %d requests on the wire",
				name, impl.MaxNetworkRetries, impl.MaxNetworkRetries+1)
		}
	}
}

// The default this overrides is nonzero, so the assertion above is
// meaningful. If stripe-go ever ships zero as its default, this test
// starts failing and whoever sees it can simplify with confidence
// rather than guessing whether the override still does anything.
func TestTheSDKDefaultIsStillNonZero(t *testing.T) {
	if stripego.DefaultMaxNetworkRetries == 0 {
		t.Skip("stripe-go now defaults to zero retries; the explicit override is redundant but harmless")
	}
	if stripego.DefaultMaxNetworkRetries != 2 {
		t.Logf("stripe-go's default retry count is now %d, was 2 when the override was written",
			stripego.DefaultMaxNetworkRetries)
	}
}

// A followed redirect re-sends the request body to a host the code
// never named. For a write that is a mutation dispatched somewhere
// nobody reviewed.
func TestHTTPClientRefusesRedirects(t *testing.T) {
	var landed int

	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		landed++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"redirected":true}`))
	}))
	defer final.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	resp, err := newHTTPClient().Post(redirector.URL, "application/json", http.NoBody)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if landed != 0 {
		t.Errorf("the request was re-sent to the redirect target %d time(s)", landed)
	}
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Errorf("status = %d, want the redirect itself (%d) rather than the followed response",
			resp.StatusCode, http.StatusTemporaryRedirect)
	}
}

// Stating the transport explicitly must not quietly change how long a
// call waits, so the timeout matches stripe-go's own default.
func TestTimeoutMatchesTheSDKDefault(t *testing.T) {
	if got := newHTTPClient().Timeout; got != stripeHTTPTimeout {
		t.Fatalf("client timeout = %v, want %v", got, stripeHTTPTimeout)
	}
}
