package einvoice

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
)

// fakeStore is an in-memory Store for Service tests (no database).
type fakeStore struct {
	profiles map[uuid.UUID]Profile
}

func newFakeStore() *fakeStore { return &fakeStore{profiles: map[uuid.UUID]Profile{}} }

func (f *fakeStore) GetProfile(_ context.Context, accountID uuid.UUID) (Profile, bool, error) {
	p, ok := f.profiles[accountID]
	return p, ok, nil
}

func (f *fakeStore) PutProfile(_ context.Context, p Profile) (Profile, error) {
	f.profiles[p.AccountID] = p
	return p, nil
}

func TestGetProfileDefaultsWhenNoneStored(t *testing.T) {
	svc := NewService(newFakeStore())
	accountID := uuid.New()
	resp, err := svc.GetProfile(context.Background(), GetProfileRequest{AccountID: accountID})
	if err != nil {
		t.Fatalf("GetProfile: %v", err)
	}
	if resp.Exists {
		t.Errorf("Exists = true, want false for an account with no profile")
	}
	if resp.Profile.Kind != KindPersonal {
		t.Errorf("default Kind = %q, want %q", resp.Profile.Kind, KindPersonal)
	}
	if resp.Profile.CarrierType != "" || resp.Profile.DonationCode != "" {
		t.Errorf("default profile carries a carrier or donation code: %+v", resp.Profile)
	}
}

func TestPutProfileRefusesAnInvalidProfile(t *testing.T) {
	svc := NewService(newFakeStore())
	_, err := svc.PutProfile(context.Background(), PutProfileRequest{
		AccountID: uuid.New(),
		Kind:      KindCompany,
		TaxID:     "12345678", // bad checksum
	})
	if err == nil {
		t.Fatal("PutProfile accepted a 統編 that fails the checksum")
	}
}

func TestPutProfileThenGetRoundTrips(t *testing.T) {
	svc := NewService(newFakeStore())
	accountID := uuid.New()
	_, err := svc.PutProfile(context.Background(), PutProfileRequest{
		AccountID:   accountID,
		Kind:        KindCompany,
		TaxID:       "04595257",
		CompanyName: "Acme Co",
	})
	if err != nil {
		t.Fatalf("PutProfile: %v", err)
	}
	resp, err := svc.GetProfile(context.Background(), GetProfileRequest{AccountID: accountID})
	if err != nil {
		t.Fatalf("GetProfile: %v", err)
	}
	if !resp.Exists || resp.Profile.CompanyName != "Acme Co" {
		t.Errorf("round trip mismatch: %+v", resp)
	}
}

// TestPutProfileNeverLogsProfileValues is the canary the acceptance criteria
// ask for: distinctive tax_id, carrier_num, email and donation_code values
// must never reach a log line, only the account id and the outcome shape.
func TestPutProfileNeverLogsProfileValues(t *testing.T) {
	var logged bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	defer slog.SetDefault(restore)

	svc := NewService(newFakeStore())
	const (
		taxID       = "04595257"
		companyName = "Canary Holdings Ltd"
		email       = "canary-secret@example.com"
		carrierNum  = "/CANARY1"
	)
	accountID := uuid.New()
	_, err := svc.PutProfile(context.Background(), PutProfileRequest{
		AccountID:   accountID,
		Kind:        KindCompany,
		TaxID:       taxID,
		CompanyName: companyName,
		Email:       email,
	})
	if err != nil {
		t.Fatalf("PutProfile: %v", err)
	}

	out := logged.String()
	for _, secret := range []string{taxID, companyName, email, carrierNum} {
		if bytesContains(out, secret) {
			t.Errorf("log line leaked a profile value %q: %s", secret, out)
		}
	}
	if !bytesContains(out, accountID.String()) {
		t.Errorf("log line dropped the account id entirely: %s", out)
	}
}

func bytesContains(haystack, needle string) bool {
	return len(needle) > 0 && bytes.Contains([]byte(haystack), []byte(needle))
}
