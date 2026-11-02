package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/mirrorstack-ai/billing-engine/internal/account/statement"
)

// loadFixedLines reads the -lines JSON file: an array of statement.Line
// objects (same wire shape Compose takes and prints), carrying the fixed
// fees of the month — platform fee, org seats, app plan fees, and any manual
// line. This is the stand-in until billing-engine#258 (app plan fees) and
// #228 (org seats) land with their own price functions; once they do, this
// command should read those instead of a hand-typed file for the kinds they
// cover.
//
// Usage lines are NOT read from this file — main.go synthesizes them from
// the bill read, one per app, so a kind of "usage" here would double the
// line on the printed statement.
func loadFixedLines(path string) ([]statement.Line, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read -lines file %q: %w", path, err)
	}
	var lines []statement.Line
	if err := json.Unmarshal(data, &lines); err != nil {
		return nil, fmt.Errorf("parse -lines file %q: %w", path, err)
	}
	for i, l := range lines {
		if l.Kind == statement.KindUsage {
			return nil, fmt.Errorf(
				"-lines file %q: line %d is kind %q — usage lines come from the bill read, not the fixed-lines file",
				path, i, statement.KindUsage)
		}
	}
	return lines, nil
}
