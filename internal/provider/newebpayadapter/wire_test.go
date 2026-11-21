package newebpayadapter

import (
	"errors"
	"testing"
)

func TestBuildTradeInfoFixedOrder(t *testing.T) {
	req := TradeInfoRequest{
		MerchantID:      "MS00000001",
		RespondType:     "JSON",
		TimeStamp:       1700000000,
		Version:         "2.0",
		MerchantOrderNo: "TEST000000000001",
		Amt:             100,
		ItemDesc:        "Test Item",
		NotifyURL:       "https://example.test/notify",
		ReturnURL:       "https://example.test/return",
	}
	got := BuildTradeInfo(req)
	if got != vecQueryPlain {
		t.Fatalf("BuildTradeInfo mismatch:\n got  %s\n want %s", got, vecQueryPlain)
	}

	// Reproducible: building the same logical request twice gives byte-identical output.
	if got2 := BuildTradeInfo(req); got2 != got {
		t.Fatalf("BuildTradeInfo is not reproducible: %s != %s", got2, got)
	}
}

func TestParseQueryFieldsRoundTrip(t *testing.T) {
	fields, err := ParseQueryFields(vecQueryPlain)
	if err != nil {
		t.Fatalf("ParseQueryFields: %v", err)
	}
	want := map[string]string{
		"MerchantID":      "MS00000001",
		"RespondType":     "JSON",
		"TimeStamp":       "1700000000",
		"Version":         "2.0",
		"MerchantOrderNo": "TEST000000000001",
		"Amt":             "100",
		"ItemDesc":        "Test Item",
		"NotifyURL":       "https://example.test/notify",
		"ReturnURL":       "https://example.test/return",
	}
	for k, v := range want {
		if fields[k] != v {
			t.Errorf("field %q: got %q, want %q", k, fields[k], v)
		}
	}
}

func TestParseQueryFieldsRejectsDuplicates(t *testing.T) {
	_, err := ParseQueryFields("MerchantID=A&MerchantID=B")
	if !errors.Is(err, ErrDuplicateField) {
		t.Fatalf("got %v, want error wrapping ErrDuplicateField", err)
	}
}
