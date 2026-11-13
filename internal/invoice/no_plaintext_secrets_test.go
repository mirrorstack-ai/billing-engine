package invoice

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/architecture"
)

// plaintextKeyColumnNames are the column-name shapes a vendor credential
// would take if someone stored it directly instead of behind secret_ref.
// This is deliberately a denylist of SPECIFIC vendor-shaped names (fix
// direction #2: e首發票 apiKey/hash salt/CompanyID, ECPay MerchantID/HashKey/
// HashIV, ECPay platform PlatformID/platform keys) rather than every column
// containing "key" — key_version is a legitimate, non-secret column.
var plaintextKeyColumnNames = []string{
	"api_key", "apikey",
	"hash_key", "hashkey",
	"hash_iv", "hashiv",
	"hash_salt",
	"merchant_key",
	"secret_key",
	"private_key",
	"platform_key",
	"password",
}

// TestMigrationHasNoPlaintextKeyColumn is the git-grep proof the acceptance
// criteria ask for, run as a Go test so it is part of `go test ./...`
// instead of a shell script nobody runs. It reads the actual migration file
// rather than re-deriving column names, so a future column addition is
// caught by the same scan without this test needing an update.
func TestMigrationHasNoPlaintextKeyColumn(t *testing.T) {
	root, err := architecture.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "migrations", "billing", "093_invoice_seller_identity.up.sql")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(body))
	for _, name := range plaintextKeyColumnNames {
		if strings.Contains(lower, name) {
			t.Errorf("migration 093 contains a plaintext-credential-shaped column name %q; "+
				"vendor keys must be sealed and referenced only via secret_ref", name)
		}
	}

	// The only columns containing "key" at all must be the two sanctioned
	// non-secret ones.
	keyColumn := regexp.MustCompile(`(?m)^\s*(\w*key\w*)\s+`)
	allowed := map[string]bool{"key_version": true}
	for _, line := range strings.Split(lower, "\n") {
		m := keyColumn.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if !allowed[m[1]] {
			t.Errorf("migration 093 declares column %q; only key_version may contain \"key\" (got line: %q)", m[1], strings.TrimSpace(line))
		}
	}
}

// TestNoInvoiceKeyEnvVars is the "no key in Lambda env" half of the
// acceptance criteria — this feature must not copy the existing
// provider-secret-as-env-var pattern (see README.md's deploy docs). It greps
// every cmd/ file for an os.Getenv/os.LookupEnv call naming the env var,
// rather than a raw substring search of the whole file: a blind substring
// search false-positives on ordinary identifiers (StripeInvoiceID contains
// "einvoice" case-insensitively) without ever looking at an actual env var
// read.
func TestNoInvoiceKeyEnvVars(t *testing.T) {
	root, err := architecture.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	forbidden := []string{"SYSTEMLEAD", "ECPAY", "INVOICE_API_KEY", "INVOICE_SECRET", "INVOICE_HASH"}
	envRead := regexp.MustCompile(`os\.(?:Getenv|LookupEnv)\("([^"]+)"\)`)

	err = filepath.Walk(filepath.Join(root, "cmd"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		body, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		for _, m := range envRead.FindAllStringSubmatch(string(body), -1) {
			name := strings.ToUpper(m[1])
			for _, forbid := range forbidden {
				if strings.Contains(name, forbid) {
					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s reads env var %q: invoice provider credentials must never be Lambda env vars", rel, m[1])
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestSellerSettingsNeverCarriesKeyMaterial is a struct test: reflect over
// SellerSettings and fail if any field looks like it could carry a secret —
// a []byte, or a name matching a credential shape. KeyVersion and Status
// (KeyStatus, a closed enum of unset/active/revoked) are explicitly fine.
func TestSellerSettingsNeverCarriesKeyMaterial(t *testing.T) {
	allowedKeyFields := map[string]bool{
		"KeyVersion": true,
	}
	typ := reflect.TypeOf(SellerSettings{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Uint8 {
			t.Errorf("SellerSettings.%s is a []byte field; a status read must never carry key bytes", f.Name)
		}
		lower := strings.ToLower(f.Name)
		if strings.Contains(lower, "secret") || strings.Contains(lower, "hint") || strings.Contains(lower, "last4") {
			t.Errorf("SellerSettings.%s looks like it could carry key material or a hint", f.Name)
		}
		if strings.Contains(lower, "key") && !allowedKeyFields[f.Name] {
			t.Errorf("SellerSettings.%s contains \"key\" and is not in the allowed set %v", f.Name, allowedKeyFields)
		}
	}
}

// TestSealedPayloadNeverPrintsItsBytes guards the %v/%#v backstop in
// broker.go: even if a SealedPayload ends up embedded in a struct passed to
// a logger, the default formatting must not reproduce the bytes.
func TestSealedPayloadNeverPrintsItsBytes(t *testing.T) {
	p := SealedPayload("super-secret-canary-" + strconv.Itoa(12345))
	if got := p.String(); strings.Contains(got, "canary") {
		t.Errorf("SealedPayload.String() leaks the payload: %s", got)
	}
	if got := p.GoString(); strings.Contains(got, "canary") {
		t.Errorf("SealedPayload.GoString() leaks the payload: %s", got)
	}
}
