package newebpayadapter

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

func vecCreds(t *testing.T) Credentials {
	return Credentials{
		MerchantID: vecMerchantID,
		HashKey:    vecKey(t),
		HashIV:     vecIV(t),
	}
}

// noticeBody builds a NewebPay NotifyURL POST body from fields, letting
// each test override exactly the field it wants to forge.
func noticeBody(fields map[string]string) []byte {
	v := url.Values{}
	for k, val := range fields {
		v.Set(k, val)
	}
	return []byte(v.Encode())
}

func validNoticeFields() map[string]string {
	return map[string]string{
		"Status":     "SUCCESS",
		"MerchantID": vecMerchantID,
		"TradeInfo":  vecNoticeCipherHex,
		"TradeSha":   vecNoticeTradeSha,
		"Version":    "2.0",
	}
}

func TestVerifyNoticeHappyPath(t *testing.T) {
	obs, err := VerifyNotice(vecCreds(t), noticeBody(validNoticeFields()))
	if err != nil {
		t.Fatalf("VerifyNotice: %v", err)
	}
	want := Observation{
		Status:          "SUCCESS",
		MerchantID:      vecMerchantID,
		MerchantOrderNo: vecMerchantOrderNo,
		TradeNo:         "25110200000001",
		Amt:             100,
		PaymentType:     "CREDIT",
		PayTime:         "2026-11-02 12:00:00",
	}
	if obs != want {
		t.Fatalf("Observation mismatch:\n got  %+v\n want %+v", obs, want)
	}
}

func TestVerifyNoticeIsStateless(t *testing.T) {
	body := noticeBody(validNoticeFields())
	first, err1 := VerifyNotice(vecCreds(t), body)
	second, err2 := VerifyNotice(vecCreds(t), body)
	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected errors: %v, %v", err1, err2)
	}
	if first != second {
		t.Fatalf("verifying the same notice twice gave different results: %+v vs %+v", first, second)
	}
}

func TestVerifyNoticeForgedCalls(t *testing.T) {
	cases := []struct {
		name    string
		fields  func() map[string]string
		body    func() []byte
		wantErr error
	}{
		{
			name: "wrong TradeSha",
			fields: func() map[string]string {
				f := validNoticeFields()
				f["TradeSha"] = strings.Repeat("0", len(vecNoticeTradeSha))
				return f
			},
			wantErr: ErrBadTradeSha,
		},
		{
			name: "valid TradeSha under another store's key",
			fields: func() map[string]string {
				f := validNoticeFields()
				f["TradeSha"] = vecNoticeOtherStoreTradeSha
				return f
			},
			wantErr: ErrBadTradeSha,
		},
		{
			name: "flipped hex digit in TradeInfo with the old TradeSha",
			fields: func() map[string]string {
				f := validNoticeFields()
				b := []byte(f["TradeInfo"])
				if b[0] == '0' {
					b[0] = '1'
				} else {
					b[0] = '0'
				}
				f["TradeInfo"] = string(b)
				return f
			},
			wantErr: ErrBadTradeSha,
		},
		{
			name: "non-hex TradeInfo",
			fields: func() map[string]string {
				f := validNoticeFields()
				f["TradeInfo"] = "not-hex!!"
				f["TradeSha"] = TradeSha(vecKey(t), vecIV(t), "not-hex!!")
				return f
			},
			wantErr: ErrInvalidHex,
		},
		{
			name: "odd-length hex TradeInfo",
			fields: func() map[string]string {
				f := validNoticeFields()
				odd := vecNoticeCipherHex[:len(vecNoticeCipherHex)-1]
				f["TradeInfo"] = odd
				f["TradeSha"] = TradeSha(vecKey(t), vecIV(t), odd)
				return f
			},
			wantErr: ErrInvalidHex,
		},
		{
			name: "length not a multiple of 16",
			fields: func() map[string]string {
				f := validNoticeFields()
				short := vecNoticeCipherHex[:len(vecNoticeCipherHex)-2]
				f["TradeInfo"] = short
				f["TradeSha"] = TradeSha(vecKey(t), vecIV(t), short)
				return f
			},
			wantErr: ErrInvalidCiphertextLength,
		},
		{
			name: "oversize body",
			body: func() []byte {
				v := url.Values{}
				for k, val := range validNoticeFields() {
					v.Set(k, val)
				}
				v.Set("Padding", strings.Repeat("a", MaxNoticeBodySize))
				return []byte(v.Encode())
			},
			wantErr: ErrBodyTooLarge,
		},
		{
			name: "Status not SUCCESS",
			fields: func() map[string]string {
				f := validNoticeFields()
				f["TradeInfo"] = vecNoticeFailCipherHex
				f["TradeSha"] = vecNoticeFailTradeSha
				return f
			},
			wantErr: ErrNoticeStatusNotSuccess,
		},
		{
			name: "inner MerchantID different from outer",
			fields: func() map[string]string {
				f := validNoticeFields()
				f["TradeInfo"] = vecNoticeMismatchCipherHex
				f["TradeSha"] = vecNoticeMismatchTradeSha
				return f
			},
			wantErr: ErrMerchantIDMismatch,
		},
		{
			name: "outer MerchantID different from credentials",
			fields: func() map[string]string {
				f := validNoticeFields()
				f["MerchantID"] = "MS00000002"
				return f
			},
			wantErr: ErrMerchantIDMismatch,
		},
		{
			name: "EncryptType=1 (GCM) refused",
			fields: func() map[string]string {
				f := validNoticeFields()
				f["EncryptType"] = "1"
				return f
			},
			wantErr: ErrEncryptTypeUnsupported,
		},
		{
			name: "duplicate field",
			body: func() []byte {
				return []byte("MerchantID=A&MerchantID=B&TradeInfo=" + vecNoticeCipherHex + "&TradeSha=" + vecNoticeTradeSha)
			},
			wantErr: ErrDuplicateField,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body []byte
			if tc.body != nil {
				body = tc.body()
			} else {
				body = noticeBody(tc.fields())
			}
			_, err := VerifyNotice(vecCreds(t), body)
			if err == nil {
				t.Fatalf("VerifyNotice: want error, got nil")
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("VerifyNotice: got %v, want error wrapping %v", err, tc.wantErr)
			}
		})
	}
}

func TestObservationMatches(t *testing.T) {
	obs := Observation{MerchantID: vecMerchantID, MerchantOrderNo: vecMerchantOrderNo, Amt: 100}

	if err := obs.Matches(Expected{MerchantID: vecMerchantID, MerchantOrderNo: vecMerchantOrderNo, Amt: 100}); err != nil {
		t.Fatalf("Matches: unexpected error: %v", err)
	}

	if err := obs.Matches(Expected{MerchantID: "someone-else", MerchantOrderNo: vecMerchantOrderNo, Amt: 100}); !errors.Is(err, ErrMerchantIDNoMatch) {
		t.Fatalf("got %v, want error wrapping ErrMerchantIDNoMatch", err)
	}
	if err := obs.Matches(Expected{MerchantID: vecMerchantID, MerchantOrderNo: "different-order", Amt: 100}); !errors.Is(err, ErrMerchantOrderNoNoMatch) {
		t.Fatalf("got %v, want error wrapping ErrMerchantOrderNoNoMatch", err)
	}
	if err := obs.Matches(Expected{MerchantID: vecMerchantID, MerchantOrderNo: vecMerchantOrderNo, Amt: 200}); !errors.Is(err, ErrAmtNoMatch) {
		t.Fatalf("got %v, want error wrapping ErrAmtNoMatch", err)
	}
}
