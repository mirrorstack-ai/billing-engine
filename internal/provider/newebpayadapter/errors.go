package newebpayadapter

import "errors"

// Crypto and wire-format errors.
var (
	// ErrInvalidHex is returned for hex input that is not an even number
	// of hex digits (0-9, a-f, A-F).
	ErrInvalidHex = errors.New("newebpayadapter: invalid hex encoding")
	// ErrInvalidCiphertextLength is returned when decoded ciphertext is
	// empty or not a multiple of the AES block size (16 bytes).
	ErrInvalidCiphertextLength = errors.New("newebpayadapter: ciphertext length is not a multiple of the AES block size")
	// ErrInvalidPadding is returned when PKCS7 unpadding fails.
	ErrInvalidPadding = errors.New("newebpayadapter: invalid PKCS7 padding")
	// ErrDuplicateField is returned when a query string repeats a field.
	ErrDuplicateField = errors.New("newebpayadapter: duplicate field in query string")
	// ErrUnknownEnvironment is returned for any Environment value other
	// than Sandbox or Live. There is no default environment.
	ErrUnknownEnvironment = errors.New("newebpayadapter: unknown environment")
	// ErrInvalidAmount is returned by ParseAmt for anything other than
	// 1-10 unsigned decimal digits.
	ErrInvalidAmount = errors.New("newebpayadapter: invalid Amt")
)

// Notice verification errors.
var (
	// ErrBodyTooLarge is returned when the raw notice body exceeds
	// MaxNoticeBodySize.
	ErrBodyTooLarge = errors.New("newebpayadapter: notice body exceeds the size limit")
	// ErrTradeInfoTooLarge is returned when the TradeInfo field exceeds
	// MaxTradeInfoLen.
	ErrTradeInfoTooLarge = errors.New("newebpayadapter: TradeInfo exceeds the size limit")
	// ErrEncryptTypeUnsupported is returned when the notice declares
	// EncryptType=1 (AES-GCM). v1 speaks CBC only.
	ErrEncryptTypeUnsupported = errors.New("newebpayadapter: EncryptType=1 (AES-GCM) is not supported by this library")
	// ErrBadTradeSha is returned when the received TradeSha does not
	// match the one computed over the received TradeInfo.
	ErrBadTradeSha = errors.New("newebpayadapter: TradeSha does not match TradeInfo")
	// ErrMerchantIDMismatch is returned when the outer form MerchantID,
	// the credential's MerchantID, or the MerchantID decrypted out of
	// TradeInfo disagree with one another.
	ErrMerchantIDMismatch = errors.New("newebpayadapter: MerchantID mismatch")
	// ErrNoticeStatusNotSuccess is returned when the decrypted payload's
	// Status is anything other than "SUCCESS".
	ErrNoticeStatusNotSuccess = errors.New("newebpayadapter: notice Status is not SUCCESS")
	// ErrMalformedPayload is returned when the decrypted TradeInfo is
	// not the expected JSON shape.
	ErrMalformedPayload = errors.New("newebpayadapter: TradeInfo payload is not valid JSON")
)

// Observation.Matches errors.
var (
	// ErrMerchantIDNoMatch is returned when an Observation's MerchantID
	// differs from what the caller expected.
	ErrMerchantIDNoMatch = errors.New("newebpayadapter: observed MerchantID does not match expected")
	// ErrMerchantOrderNoNoMatch is returned when an Observation's
	// MerchantOrderNo differs from what the caller expected.
	ErrMerchantOrderNoNoMatch = errors.New("newebpayadapter: observed MerchantOrderNo does not match expected")
	// ErrAmtNoMatch is returned when an Observation's Amt differs from
	// what the caller expected.
	ErrAmtNoMatch = errors.New("newebpayadapter: observed Amt does not match expected")
)
