package architecture

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestTapPayHostReachIsConfined is billing-engine#310 item 9: only
// internal/provider/tappaygateway may name tappaysdk.com. Nothing else
// in the engine should be able to construct a TapPay request by hand —
// by analogy with TestProviderSDKReachIsBounded above, a package that
// names the host outside the gateway package is a package that could
// talk to TapPay without going through the typed client this repo
// reviews.
func TestTapPayHostReachIsConfined(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	namers, err := ScanTapPayHostNamers(root, "internal", "cmd")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(namers) == 0 {
		t.Fatal("scanned zero files naming the TapPay host; the scanner is broken, not the tree")
	}

	var leaked []string
	for _, f := range namers {
		if f == tappayHostConfinementScannerFile {
			continue
		}
		if !strings.HasPrefix(f, tappayHostConfinedPackage) {
			leaked = append(leaked, f)
		}
	}
	sort.Strings(leaked)
	if len(leaked) > 0 {
		t.Errorf("%d file(s) outside %s name the TapPay host; route the call through the gateway client instead:\n  %s",
			len(leaked), tappayHostConfinedPackage, strings.Join(leaked, "\n  "))
	}
}

// TestTapPayHostReachIsConfined_CatchesASecondPackage proves the check
// above actually fails closed, by planting a throwaway file outside the
// confined package that names the host and asserting the scan reports
// it.
func TestTapPayHostReachIsConfined_CatchesASecondPackage(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}

	plantDir := filepath.Join(root, "internal", "architecture", "tappaytestplant")
	if err := os.MkdirAll(plantDir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(plantDir) })

	planted := filepath.Join(plantDir, "planted.go")
	src := "package tappaytestplant\n\nconst host = \"https://sandbox." + tappayHost + "\"\n"
	if err := os.WriteFile(planted, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}

	namers, err := ScanTapPayHostNamers(root, "internal")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	rel, err := filepath.Rel(root, planted)
	if err != nil {
		t.Fatal(err)
	}
	rel = filepath.ToSlash(rel)

	found := false
	for _, f := range namers {
		if f == rel {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("scan did not catch planted file %s naming the TapPay host: %v", rel, namers)
	}
}
