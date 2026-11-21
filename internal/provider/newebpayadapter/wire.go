package newebpayadapter

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// tradeInfoFieldOrder is the order BuildTradeInfo writes fields in. Fixed
// once so the same logical request always serializes to the same bytes:
// a reproducible TradeInfo is what makes the known-answer vectors in
// testdata, and any later diff against a captured request, mean anything.
var tradeInfoFieldOrder = []string{
	"MerchantID",
	"RespondType",
	"TimeStamp",
	"Version",
	"MerchantOrderNo",
	"Amt",
	"ItemDesc",
	"NotifyURL",
	"ReturnURL",
}

// TradeInfoRequest is the set of fields BuildTradeInfo encodes into the
// TradeInfo query string ahead of encryption. N9 (the payment form,
// billing-engine#294) is the caller.
type TradeInfoRequest struct {
	MerchantID      string
	RespondType     string
	TimeStamp       int64
	Version         string
	MerchantOrderNo string
	Amt             int64
	ItemDesc        string
	NotifyURL       string
	ReturnURL       string
}

// BuildTradeInfo serializes r as the URL-encoded query string NewebPay
// expects to receive before encryption, in BuildTradeInfo's own fixed
// field order rather than map iteration order.
func BuildTradeInfo(r TradeInfoRequest) string {
	values := map[string]string{
		"MerchantID":      r.MerchantID,
		"RespondType":     r.RespondType,
		"TimeStamp":       strconv.FormatInt(r.TimeStamp, 10),
		"Version":         r.Version,
		"MerchantOrderNo": r.MerchantOrderNo,
		"Amt":             strconv.FormatInt(r.Amt, 10),
		"ItemDesc":        r.ItemDesc,
		"NotifyURL":       r.NotifyURL,
		"ReturnURL":       r.ReturnURL,
	}
	parts := make([]string, 0, len(tradeInfoFieldOrder))
	for _, name := range tradeInfoFieldOrder {
		parts = append(parts, name+"="+url.QueryEscape(values[name]))
	}
	return strings.Join(parts, "&")
}

// ParseQueryFields parses a decrypted TradeInfo, or a raw notice body, as
// an application/x-www-form-urlencoded query string, rejecting any field
// that repeats. NewebPay's own format gives a repeated field no defined
// meaning, and url.Values would otherwise silently keep every value while
// every reader here only ever wants one.
func ParseQueryFields(s string) (map[string]string, error) {
	raw, err := url.ParseQuery(s)
	if err != nil {
		return nil, fmt.Errorf("newebpayadapter: parse query fields: %w", err)
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		if len(v) != 1 {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateField, k)
		}
		out[k] = v[0]
	}
	return out, nil
}
