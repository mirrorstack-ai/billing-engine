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

	// Every backend the struct has. A nil field is filled in from the
	// SDK's defaults, so a backend left unset is a backend still
	// retrying — this test existed while two of the four were.
	for name, backend := range map[string]stripego.Backend{
		"API":         backends.API,
		"Connect":     backends.Connect,
		"Uploads":     backends.Uploads,
		"MeterEvents": backends.MeterEvents,
	} {
		if backend == nil {
			t.Errorf("%s backend is nil; stripe-go will fill it in with its own defaults, "+
				"including the retry count this test exists to pin", name)
			continue
		}
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

// The default this overrides is nonzero, which is what makes the
// assertion above worth making. Written as a failure rather than a skip
// or a log: an earlier version did both, and a test that cannot fail
// tells nobody anything, whatever its comment claims.
func TestTheSDKDefaultIsStillNonZero(t *testing.T) {
	if stripego.DefaultMaxNetworkRetries == 0 {
		t.Fatal("stripe-go now defaults to zero retries. The explicit override is no longer " +
			"doing anything; either delete it or keep it deliberately, and update this test.")
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
