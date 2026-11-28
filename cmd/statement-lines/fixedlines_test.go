package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFixedLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lines.json")
	if err := os.WriteFile(path, []byte(`[
		{"kind":"platform_fee","subject":"Platform fee","amount_micros":100000000},
		{"kind":"org_seats","subject":"Seats","quantity":5,"unit_micros":10000000,"amount_micros":50000000}
	]`), 0o600); err != nil {
		t.Fatal(err)
	}

	lines, err := loadFixedLines(path)
	if err != nil {
		t.Fatalf("loadFixedLines: %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	if lines[1].Quantity == nil || *lines[1].Quantity != 5 {
		t.Errorf("seats line quantity = %v, want 5", lines[1].Quantity)
	}
}

func TestLoadFixedLines_RejectsUsageKind(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lines.json")
	if err := os.WriteFile(path, []byte(`[{"kind":"usage","subject":"should not be here","amount_micros":1}]`), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := loadFixedLines(path); err == nil {
		t.Error("loadFixedLines: want error for a usage-kind line in the fixed-lines file, got none")
	}
}

func TestLoadFixedLines_MissingFile(t *testing.T) {
	if _, err := loadFixedLines("/no/such/file.json"); err == nil {
		t.Error("loadFixedLines: want error for a missing file, got none")
	}
}

func TestUSDString(t *testing.T) {
	tests := []struct {
		micros int64
		want   string
	}{
		{200_000_000, "USD 200.00"},
		{-50_000_000, "-USD 50.00"},
		{1_234_567, "USD 1.23"},
	}
	for _, tt := range tests {
		if got := usdString(tt.micros); got != tt.want {
			t.Errorf("usdString(%d) = %q, want %q", tt.micros, got, tt.want)
		}
	}
}
