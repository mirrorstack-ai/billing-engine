package newebpayadapter

import (
	"encoding/json"
	"fmt"
)

// Credentials is one store's NewebPay identity: the MerchantID NewebPay
// knows it by, and the HashKey/HashIV it signs with.
type Credentials struct {
	MerchantID string
	HashKey    HashKey
	HashIV     HashIV
}

// Observation is what a verified notice says happened, typed rather than
// left as the map ParseQueryFields or json.Unmarshal would otherwise hand
// back.
type Observation struct {
	Status          string
	MerchantID      string
	MerchantOrderNo string
	TradeNo         string
	Amt             int64
	PaymentType     string
	PayTime         string
}

// Expected is what a caller believes a notice ought to say, for Matches.
type Expected struct {
	MerchantID      string
	MerchantOrderNo string
	Amt             int64
}

// Matches compares o against expected, returning a typed error naming the
// first field that disagrees: MerchantID, then MerchantOrderNo, then Amt.
func (o Observation) Matches(expected Expected) error {
	if o.MerchantID != expected.MerchantID {
		return fmt.Errorf("%w: observed %q, expected %q", ErrMerchantIDNoMatch, o.MerchantID, expected.MerchantID)
	}
	if o.MerchantOrderNo != expected.MerchantOrderNo {
		return fmt.Errorf("%w: observed %q, expected %q", ErrMerchantOrderNoNoMatch, o.MerchantOrderNo, expected.MerchantOrderNo)
	}
	if o.Amt != expected.Amt {
		return fmt.Errorf("%w: observed %d, expected %d", ErrAmtNoMatch, o.Amt, expected.Amt)
	}
	return nil
}

// noticePayload is the JSON shape TradeInfo decrypts to when RespondType
// is JSON, per NDNF-1.2.5. Only the fields this library exposes as
// Observation are named; the rest of Result is method-specific and is N10's
// concern, not this package's.
type noticePayload struct {
	Status  string `json:"Status"`
	Message string `json:"Message"`
	Result  struct {
		MerchantID      string `json:"MerchantID"`
		Amt             string `json:"Amt"`
		TradeNo         string `json:"TradeNo"`
		MerchantOrderNo string `json:"MerchantOrderNo"`
		PaymentType     string `json:"PaymentType"`
		PayTime         string `json:"PayTime"`
	} `json:"Result"`
}

// VerifyNotice checks a raw NotifyURL POST body against creds and returns
// what it observed.
//
// The checks run in this fixed order, and the order is load-bearing:
//
//  1. Body size, against MaxNoticeBodySize — bound the work done before
//     anything else runs.
//  2. Parse the body as form fields, rejecting a repeated field.
//  3. TradeInfo length, against MaxTradeInfoLen.
//  4. EncryptType: "1" (AES-GCM) is refused, never silently misread as CBC.
//  5. The outer MerchantID form field against creds.MerchantID.
//  6. TradeSha, by constant-time comparison, over the RECEIVED TradeInfo
//     hex — BEFORE that hex is decrypted. A forged TradeInfo with a stale
//     or substituted TradeSha is rejected here, before a single byte of
//     attacker-controlled ciphertext reaches AES.
//  7. Only then: decrypt, and parse the result as the NDNF-1.2.5 JSON
//     shape.
//  8. Status must be "SUCCESS".
//  9. The MerchantID decrypted out of TradeInfo against the outer field
//     (and therefore against creds.MerchantID, already checked equal to
//     it in step 5).
//  10. Amt, strictly (see ParseAmt).
//
// Statelessness. VerifyNotice holds no state and consults none: the same
// notice verified twice returns the same Observation or the same error,
// every time. It does not know whether a MerchantOrderNo has been seen
// before, so it cannot detect a replayed notice, and it does not know
// whether a MerchantOrderNo was ever opened, so it cannot detect a notice
// for an order nobody placed. Both questions need durable state this
// package deliberately does not carry, and are the notice worker's job
// (N10, billing-engine#296) — along with the tests for them.
func VerifyNotice(creds Credentials, rawBody []byte) (Observation, error) {
	if len(rawBody) > MaxNoticeBodySize {
		return Observation{}, fmt.Errorf("%w: %d bytes", ErrBodyTooLarge, len(rawBody))
	}

	fields, err := ParseQueryFields(string(rawBody))
	if err != nil {
		return Observation{}, err
	}

	tradeInfoHex := fields["TradeInfo"]
	if len(tradeInfoHex) > MaxTradeInfoLen {
		return Observation{}, fmt.Errorf("%w: %d bytes", ErrTradeInfoTooLarge, len(tradeInfoHex))
	}

	if fields["EncryptType"] == "1" {
		return Observation{}, ErrEncryptTypeUnsupported
	}

	if fields["MerchantID"] != creds.MerchantID {
		return Observation{}, fmt.Errorf("%w: form field %q != credential %q", ErrMerchantIDMismatch, fields["MerchantID"], creds.MerchantID)
	}

	expectedSha := TradeSha(creds.HashKey, creds.HashIV, tradeInfoHex)
	if !constantTimeEqualHex(expectedSha, fields["TradeSha"]) {
		return Observation{}, ErrBadTradeSha
	}

	plaintext, err := Decrypt(creds.HashKey, creds.HashIV, tradeInfoHex)
	if err != nil {
		return Observation{}, err
	}

	var payload noticePayload
	if err := json.Unmarshal([]byte(plaintext), &payload); err != nil {
		return Observation{}, fmt.Errorf("%w: %v", ErrMalformedPayload, err)
	}

	if payload.Status != "SUCCESS" {
		return Observation{}, fmt.Errorf("%w: %q (%s)", ErrNoticeStatusNotSuccess, payload.Status, payload.Message)
	}

	if payload.Result.MerchantID != fields["MerchantID"] {
		return Observation{}, fmt.Errorf("%w: inner %q != outer %q", ErrMerchantIDMismatch, payload.Result.MerchantID, fields["MerchantID"])
	}

	amt, err := ParseAmt(payload.Result.Amt)
	if err != nil {
		return Observation{}, err
	}

	return Observation{
		Status:          payload.Status,
		MerchantID:      payload.Result.MerchantID,
		MerchantOrderNo: payload.Result.MerchantOrderNo,
		TradeNo:         payload.Result.TradeNo,
		Amt:             amt,
		PaymentType:     payload.Result.PaymentType,
		PayTime:         payload.Result.PayTime,
	}, nil
}
