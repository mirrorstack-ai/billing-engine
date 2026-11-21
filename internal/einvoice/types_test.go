package einvoice_test

import (
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
)

func TestRoundHalfUpNTD(t *testing.T) {
	cases := []struct {
		net  int64
		want int64
	}{
		{0, 0},
		{100, 5},
		{1, 0},  // 1 * 0.05 = 0.05 -> rounds to 0
		{10, 1}, // 10 * 0.05 = 0.5 -> rounds half-up to 1
		{30, 2}, // 30 * 0.05 = 1.5 -> rounds half-up to 2
		{-10, -1},
	}
	for _, c := range cases {
		got := einvoice.RoundHalfUpNTD(c.net)
		if got != c.want {
			t.Errorf("RoundHalfUpNTD(%d) = %d, want %d", c.net, got, c.want)
		}
	}
}

func TestDocumentTotalNTD(t *testing.T) {
	doc := einvoice.Document{
		Lines: []einvoice.Line{
			{NetNTD: 100, TaxNTD: 5, GrossNTD: 105},
			{NetNTD: 200, TaxNTD: 10, GrossNTD: 210},
		},
	}
	net, tax, gross := doc.TotalNTD()
	if net != 300 || tax != 15 || gross != 315 {
		t.Fatalf("TotalNTD() = (%d, %d, %d), want (300, 15, 315)", net, tax, gross)
	}
}

func TestTransmissionStateAdvances(t *testing.T) {
	const (
		none     = einvoice.TransmissionState("")
		queued   = einvoice.TransmissionQueued
		sent     = einvoice.TransmissionSent
		accepted = einvoice.TransmissionAccepted
		rejected = einvoice.TransmissionRejected
	)
	cases := []struct {
		from, to einvoice.TransmissionState
		want     bool
	}{
		{none, queued, true},
		{queued, sent, true},
		{sent, accepted, true},
		{sent, rejected, true},
		{queued, accepted, true}, // a poll may skip a state
		{queued, rejected, true},
		{queued, queued, false}, // a reread is not an advance
		{sent, queued, false},   // a late "queued" never moves it back
		{accepted, rejected, false},
		{rejected, accepted, false},
		{rejected, queued, false}, // a resend is a new transmission, not this one
		{accepted, sent, false},
		{queued, einvoice.TransmissionState("lost"), false},
		{queued, none, false},
	}
	for _, c := range cases {
		if got := c.from.Advances(c.to); got != c.want {
			t.Errorf("%q.Advances(%q) = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestTransmissionStateTerminal(t *testing.T) {
	for s, want := range map[einvoice.TransmissionState]bool{
		"":                            false,
		einvoice.TransmissionQueued:   false,
		einvoice.TransmissionSent:     false,
		einvoice.TransmissionAccepted: true,
		einvoice.TransmissionRejected: true,
	} {
		if got := s.Terminal(); got != want {
			t.Errorf("%q.Terminal() = %v, want %v", s, got, want)
		}
	}
	if einvoice.TransmissionState("").Valid() {
		t.Error(`"" is Valid; "not reported" must not be a reachable state`)
	}
}
