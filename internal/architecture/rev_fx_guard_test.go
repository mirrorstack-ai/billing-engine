package architecture

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 🔴 NP-D4: no Go charge path may read ms_billing.rev_fx_rates.
//
// It is a reporting-only table (migration 094, billing-engine#311): every
// charge in this codebase prices and collects in its native currency, never
// through a market FX figure, and that must stay true even as the NewebPay
// arm (billing-engine#294) lands. This greps every non-test, non-migration Go
// file in the tree for the table's name; a hit means something started
// reading it as a charge input.
func TestNoGoChargePathReadsRevFXRates(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatalf("find repo root: %v", err)
	}

	var offenders []string
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "migrations", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(body), "rev_fx_rates") {
			offenders = append(offenders, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repo: %v", err)
	}

	if len(offenders) > 0 {
		t.Fatalf("non-test Go file(s) reference ms_billing.rev_fx_rates, which no charge path "+
			"may read (owner NP-D4 — TWD-priced only, no FX in code): %v", offenders)
	}
}
