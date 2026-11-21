package newebpayadapter

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

func validMPGRequest() MPGRequest {
	return MPGRequest{
		Environment:     Sandbox,
		TimeStamp:       1700000000,
		MerchantOrderNo: "M_TEST0001",
		Amt:             100,
		ItemDesc:        "Credits",
		NotifyURL:       "https://billing.example.test/newebpay/sandbox/notify",
		ReturnURL:       "https://account.example.test/billing/return/p1",
	}
}

func TestSignMPGFormGolden(t *testing.T) {
	creds := vecCreds(t)
	form, err := SignMPGForm(creds, validMPGRequest())
	if err != nil {
		t.Fatalf("SignMPGForm: %v", err)
	}
	u, err := url.Parse(form.ActionURL)
	if err != nil || u.Host != "ccore.newebpay.com" || u.Scheme != "https" {
		t.Fatalf("sandbox action URL = %q, want https on ccore.newebpay.com", form.ActionURL)
	}
	if form.MerchantID != creds.MerchantID || form.Version != MPGVersion {
		t.Fatalf("posted MerchantID/Version = %q/%q", form.MerchantID, form.Version)
	}
	if got := TradeSha(creds.HashKey, creds.HashIV, form.TradeInfo); got != form.TradeSha {
		t.Fatalf("TradeSha does not verify: %s != %s", got, form.TradeSha)
	}
	plain, err := Decrypt(creds.HashKey, creds.HashIV, form.TradeInfo)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	fields, err := ParseQueryFields(plain)
	if err != nil {
		t.Fatalf("ParseQueryFields: %v", err)
	}
	want := map[string]string{
		"MerchantID":      creds.MerchantID,
		"RespondType":     "JSON",
		"TimeStamp":       "1700000000",
		"Version":         MPGVersion,
		"LangType":        LangZhTW,
		"MerchantOrderNo": "M_TEST0001",
		"Amt":             "100",
		"ItemDesc":        "Credits",
		"NotifyURL":       "https://billing.example.test/newebpay/sandbox/notify",
		"ReturnURL":       "https://account.example.test/billing/return/p1",
		"CREDIT":          "1",
	}
	if len(fields) != len(want) {
		t.Fatalf("TradeInfo has %d fields, want %d: %v", len(fields), len(want), fields)
	}
	for k, v := range want {
		if fields[k] != v {
			t.Errorf("TradeInfo %s = %q, want %q", k, fields[k], v)
		}
	}
	vals := form.Values()
	if len(vals) != 4 || vals.Get("TradeInfo") != form.TradeInfo || vals.Get("TradeSha") != form.TradeSha {
		t.Fatalf("posted fields = %v", vals)
	}
}

func TestSignMPGFormLiveHost(t *testing.T) {
	r := validMPGRequest()
	r.Environment = Live
	form, err := SignMPGForm(vecCreds(t), r)
	if err != nil {
		t.Fatalf("SignMPGForm: %v", err)
	}
	if u, _ := url.Parse(form.ActionURL); u.Host != "core.newebpay.com" {
		t.Fatalf("live action URL = %q", form.ActionURL)
	}
}

func TestSignMPGFormRefusals(t *testing.T) {
	long := "https://billing.example.test/" + strings.Repeat("a", 200)
	cases := []struct {
		name   string
		mutate func(*MPGRequest, *Credentials)
		want   error
	}{
		{"unknown environment", func(r *MPGRequest, _ *Credentials) { r.Environment = "off" }, ErrUnknownEnvironment},
		{"empty merchant id", func(_ *MPGRequest, c *Credentials) { c.MerchantID = "" }, ErrInvalidMerchantID},
		{"order no too long", func(r *MPGRequest, _ *Credentials) { r.MerchantOrderNo = strings.Repeat("A", 31) }, ErrInvalidMerchantOrderNo},
		{"order no bad char", func(r *MPGRequest, _ *Credentials) { r.MerchantOrderNo = "M-1" }, ErrInvalidMerchantOrderNo},
		{"order no empty", func(r *MPGRequest, _ *Credentials) { r.MerchantOrderNo = "" }, ErrInvalidMerchantOrderNo},
		{"zero amount", func(r *MPGRequest, _ *Credentials) { r.Amt = 0 }, ErrInvalidAmount},
		{"negative amount", func(r *MPGRequest, _ *Credentials) { r.Amt = -1 }, ErrInvalidAmount},
		{"zero timestamp", func(r *MPGRequest, _ *Credentials) { r.TimeStamp = 0 }, ErrInvalidTimeStamp},
		{"empty item", func(r *MPGRequest, _ *Credentials) { r.ItemDesc = "" }, ErrInvalidItemDesc},
		{"item over 50", func(r *MPGRequest, _ *Credentials) { r.ItemDesc = strings.Repeat("點", 51) }, ErrInvalidItemDesc},
		{"bad lang", func(r *MPGRequest, _ *Credentials) { r.LangType = "jp" }, ErrInvalidLangType},
		{"http notify", func(r *MPGRequest, _ *Credentials) { r.NotifyURL = "http://billing.example.test/n" }, ErrInvalidCallbackURL},
		{"notify port 8443", func(r *MPGRequest, _ *Credentials) { r.NotifyURL = "https://billing.example.test:8443/n" }, ErrInvalidCallbackURL},
		{"notify with userinfo", func(r *MPGRequest, _ *Credentials) { r.NotifyURL = "https://u:p@billing.example.test/n" }, ErrInvalidCallbackURL},
		{"relative return", func(r *MPGRequest, _ *Credentials) { r.ReturnURL = "/billing/return" }, ErrInvalidCallbackURL},
		{"return over 200", func(r *MPGRequest, _ *Credentials) { r.ReturnURL = long }, ErrInvalidCallbackURL},
		{"return equals notify", func(r *MPGRequest, _ *Credentials) { r.ReturnURL = r.NotifyURL }, ErrReturnURLIsNotifyURL},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, c := validMPGRequest(), vecCreds(t)
			tc.mutate(&r, &c)
			if _, err := SignMPGForm(c, r); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSignMPGFormAcceptsExplicit443AndEnglish(t *testing.T) {
	r := validMPGRequest()
	r.NotifyURL = "https://billing.example.test:443/newebpay/sandbox/notify"
	r.LangType = LangEn
	if _, err := SignMPGForm(vecCreds(t), r); err != nil {
		t.Fatalf("SignMPGForm: %v", err)
	}
}

func TestBuildTradeInfoOptionalFieldsAppend(t *testing.T) {
	base := TradeInfoRequest{MerchantID: "MS1", RespondType: "JSON", TimeStamp: 1, Version: "2.0", MerchantOrderNo: "O1", Amt: 1, ItemDesc: "x", NotifyURL: "n", ReturnURL: "r"}
	plain := BuildTradeInfo(base)
	with := base
	with.LangType, with.Credit = "en", true
	if got, want := BuildTradeInfo(with), plain+"&LangType=en&CREDIT=1"; got != want {
		t.Fatalf("BuildTradeInfo = %q, want %q", got, want)
	}
}
