package architecture

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// providerSDKPath is the provider SDK whose reach this check bounds.
const providerSDKPath = "github.com/stripe/stripe-go"

// ScanProviderSDKImporters returns every non-test file under the given
// roots that imports the provider SDK.
//
// docs/VERIFICATION.md §5 calls this provider SDK confinement: the SDK
// may appear only in adapter and enclave packages. The reason is not
// tidiness. A package that imports the SDK can construct a provider
// request directly, so it is outside whatever the typed client
// interfaces guarantee — the mutation inventory in this package sees
// method calls on those interfaces, and an SDK call made by hand is
// invisible to it.
func ScanProviderSDKImporters(repoRoot string, roots ...string) ([]string, error) {
	var out []string
	fset := token.NewFileSet()

	for _, root := range roots {
		err := filepath.Walk(filepath.Join(repoRoot, root), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				if info.Name() == "testdata" || info.Name() == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, parseErr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if parseErr != nil {
				return parseErr
			}
			for _, imp := range file.Imports {
				value, unquoteErr := strconv.Unquote(imp.Path.Value)
				if unquoteErr != nil {
					continue
				}
				if strings.HasPrefix(value, providerSDKPath) {
					rel, relErr := filepath.Rel(repoRoot, path)
					if relErr != nil {
						return relErr
					}
					out = append(out, filepath.ToSlash(rel))
					break
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// providerSDKImporters records every file allowed to import the
// provider SDK, and why.
//
// The target shape has two kinds of holder: the adapter that speaks the
// provider's protocol, and the callback transports that must decode the
// provider's own event envelope. Everything else should reach the
// provider through a typed interface, so that the mutation inventory
// above can see what it does.
var providerSDKImporters = map[string]string{
	"internal/shared/stripe/types.go":  "ADAPTER: the interfaces and the projections they return",
	"internal/shared/stripe/client.go": "ADAPTER: the one implementation that speaks to Stripe",

	"internal/account/webhook/router.go":      "TRANSPORT: decodes the provider's event envelope to dispatch on its type",
	"internal/account/webhook/handlers.go":    "TRANSPORT: reads provider event payloads",
	"cmd/account-webhook-eventbridge/main.go": "TRANSPORT: unmarshals the EventBridge detail into a provider event",

	"internal/shared/stripe/stripetest/recorder.go": "TEST SUPPORT: returns provider types from the recording double; not a _test.go file so other packages can use it",
	"internal/account/webhook/webhooktest/fakes.go": "TEST SUPPORT: fake store and verifier over provider types, likewise reusable",

	// Debt. Each of these constructs provider requests outside the
	// adapter, which is exactly the reach VERIFICATION §5 wants closed.
	"internal/account/autotopup/executor.go":      "DEBT: uses provider error types directly to classify a failed charge; belongs behind the adapter",
	"internal/account/creditpurchase/executor.go": "DEBT: same, for the purchase path",
	"cmd/pm-default-backfill/main.go":             "DEBT: a one-off backfill binary talking to the SDK directly",
}
