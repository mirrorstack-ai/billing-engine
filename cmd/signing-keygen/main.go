// Command signing-keygen generates one signing key for internal/shared/signing.
//
// It exists because provisioning a key was otherwise a research task: the
// package takes a 32-byte SEED (not the 64-byte private-key form, which is the
// mistake it was built to refuse), derives the key id from the material, and
// needs the public half pinned in the repository — four facts that had to be
// reconstructed from doc comments before anyone could set a variable.
//
// 🔴 IT PRINTS A SECRET. The seed on stdout is the private key. Nothing else
// in this repository prints key material, and this command exists only because
// generating a key is the one moment somebody has to hold it.
//
//	go run ./cmd/signing-keygen -domain billing-evidence/v1
//
// It writes the secret to STDOUT and everything else to STDERR, so a caller
// can pipe the seed straight into a secret store without the guidance text:
//
//	go run ./cmd/signing-keygen -domain billing-evidence/v1 2>/dev/null | \
//	  aws ssm put-parameter --type SecureString --name /billing/evidence-key --value file:///dev/stdin
//
// It touches nothing. It reads no environment, contacts no service, and writes
// no file — so running it has no effect until somebody takes the output
// somewhere.
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/signing"
)

func main() {
	domain := flag.String("domain", "", "signature domain this key may sign for (required)")
	flag.Parse()

	if strings.TrimSpace(*domain) == "" {
		fail("a -domain is required. A key carries its domain, so that a leaked "+
			"capabilities key cannot mint a customer acceptance.\n\nKnown domains:\n  %s\n  %s\n  %s",
			signing.DomainCustomerAcceptance, signing.DomainBillingEvidence, signing.DomainBillingCapabilities)
	}

	seed := make([]byte, signing.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		fail("could not read %d bytes of randomness: %v", signing.SeedSize, err)
	}

	// Built through the real constructor, so a key this command prints is one
	// the package would accept — including its self-check that the key can
	// produce a signature its own public half verifies. A generator that
	// emitted material the loader then refuses would be worse than none.
	key, err := signing.NewKey(*domain, seed)
	if err != nil {
		fail("generated material was refused by signing.NewKey: %v", err)
	}

	// The SECRET, alone, on stdout.
	fmt.Println(hex.EncodeToString(seed))

	env := envVarFor(*domain)
	fmt.Fprintf(os.Stderr, `
Generated an ed25519 signing key for %s.

  key id      %s
  public key  %s

🔴 The line on stdout is the PRIVATE SEED. Treat it as a password: put it in a
   secret store and do not paste it anywhere that keeps history.

To provision it:

  1. Store the seed as %s
     in the deployment's secret store. It is the 32-byte SEED, hex-encoded —
     NOT the 64-byte private-key form. signing.NewKey refuses that form on
     purpose: its two halves need not agree, so 64 bytes of raw randomness
     loads, reports ready, signs, and produces signatures nothing can verify.

  2. Pin the PUBLIC half in internal/shared/signing/trustroot.go by adding to
     pinnedKeys:

         {ID: %q,
          Domain: %q,
          PublicKeyHex: %q},

     The trust root ships in the repository on purpose: docs/VERIFICATION.md
     says a verifier that learns its root from the service it is checking has
     checked nothing. The id is DERIVED from the material, so NewTrustRoot
     refuses a key pinned under any other id.

  3. Update TestTheRepositoryTrustRootIsEmptyUntilAKeyIsProvisioned, which
     currently asserts the root is empty. It is the place that change is
     declared.

Nothing was written. This command has no side effects.
`, *domain, key.ID(), key.Public(), env, key.ID(), *domain, key.Public())
}

// envVarFor names the variable a domain's key is read from, so the guidance
// cannot drift from signing.Load's actual contract.
func envVarFor(domain string) string {
	switch domain {
	case signing.DomainCustomerAcceptance:
		return signing.EnvCustomerAcceptanceKey
	case signing.DomainBillingEvidence:
		return signing.EnvBillingEvidenceKey
	case signing.DomainBillingCapabilities:
		return signing.EnvCapabilitiesKey
	}
	return "(no environment variable is defined for this domain)"
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// Keep the ed25519 import honest: SeedSize is the package's, and this pins
// that the two agree rather than assuming it.
var _ = [1]struct{}{}[ed25519.SeedSize-signing.SeedSize]
