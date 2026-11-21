package newebpayadapter

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strings"
)

// decodeHex is strict where encoding/hex.DecodeString alone is not: an
// odd-length input to hex.DecodeString already errors, but this also
// rejects anything containing a byte outside 0-9a-fA-F with the same
// typed error hex.DecodeString's own ErrLength and generic error would
// not give callers a stable way to match on.
func decodeHex(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("%w: odd length (%d)", ErrInvalidHex, len(s))
	}
	for i := 0; i < len(s); i++ {
		if !isHexDigit(s[i]) {
			return nil, fmt.Errorf("%w: non-hex byte %q at offset %d", ErrInvalidHex, s[i], i)
		}
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidHex, err)
	}
	return b, nil
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func pkcs7Pad(data []byte, blockSize int) []byte {
	padLen := blockSize - len(data)%blockSize
	out := make([]byte, len(data)+padLen)
	copy(out, data)
	for i := len(data); i < len(out); i++ {
		out[i] = byte(padLen) //nolint:gosec // padLen is in [1, blockSize], blockSize is the AES block size (16)
	}
	return out
}

func pkcs7Unpad(data []byte, blockSize int) ([]byte, error) {
	n := len(data)
	if n == 0 || n%blockSize != 0 {
		return nil, ErrInvalidPadding
	}
	padLen := int(data[n-1])
	if padLen == 0 || padLen > blockSize || padLen > n {
		return nil, ErrInvalidPadding
	}
	for _, b := range data[n-padLen:] {
		if int(b) != padLen {
			return nil, ErrInvalidPadding
		}
	}
	return data[:n-padLen], nil
}

// Encrypt returns the lower-case hex TradeInfo ciphertext for plaintext,
// AES-256-CBC with PKCS7 padding under key and iv.
func Encrypt(key HashKey, iv HashIV, plaintext string) (string, error) {
	block, err := aes.NewCipher(key.Reveal())
	if err != nil {
		return "", err
	}
	padded := pkcs7Pad([]byte(plaintext), block.BlockSize())
	ciphertext := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv.Reveal()).CryptBlocks(ciphertext, padded)
	return hex.EncodeToString(ciphertext), nil
}

// Decrypt reverses Encrypt. It is strict at every stage: tradeInfoHex
// must be well-formed hex, the decoded ciphertext must be a non-empty
// multiple of the AES block size, and the recovered padding must be
// valid PKCS7 — any of those failing is a typed, non-panicking error
// rather than garbage plaintext.
func Decrypt(key HashKey, iv HashIV, tradeInfoHex string) (string, error) {
	ciphertext, err := decodeHex(tradeInfoHex)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key.Reveal())
	if err != nil {
		return "", err
	}
	bs := block.BlockSize()
	if len(ciphertext) == 0 || len(ciphertext)%bs != 0 {
		return "", fmt.Errorf("%w: %d bytes", ErrInvalidCiphertextLength, len(ciphertext))
	}
	plaintext := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, iv.Reveal()).CryptBlocks(plaintext, ciphertext)
	unpadded, err := pkcs7Unpad(plaintext, bs)
	if err != nil {
		return "", err
	}
	return string(unpadded), nil
}

// TradeSha returns the upper-case hex SHA-256 NewebPay expects alongside
// a TradeInfo value: SHA-256("HashKey=<key>&<tradeInfoHex>&HashIV=<iv>").
func TradeSha(key HashKey, iv HashIV, tradeInfoHex string) string {
	input := "HashKey=" + string(key.Reveal()) + "&" + tradeInfoHex + "&HashIV=" + string(iv.Reveal())
	return upperHexSHA256(input)
}

// CheckValue returns the upper-case hex SHA-256 NewebPay's QueryTradeInfo
// requires: SHA-256 over IV, Amt, MerchantID and MerchantOrderNo (in that
// order) wrapping the shared key, per NDNF-1.2.6.
func CheckValue(key HashKey, iv HashIV, amt int64, merchantID, merchantOrderNo string) string {
	input := fmt.Sprintf("IV=%s&Amt=%d&MerchantID=%s&MerchantOrderNo=%s&Key=%s",
		string(iv.Reveal()), amt, merchantID, merchantOrderNo, string(key.Reveal()))
	return upperHexSHA256(input)
}

// constantTimeEqualHex compares two hex-digest strings without leaking
// which byte first differed. Case-insensitive: NewebPay's own TradeSha is
// always upper-case, but nothing about the comparison should depend on a
// forged caller matching that convention exactly.
func constantTimeEqualHex(a, b string) bool {
	au, bu := strings.ToUpper(a), strings.ToUpper(b)
	if len(au) != len(bu) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(au), []byte(bu)) == 1
}

func upperHexSHA256(s string) string {
	sum := sha256.Sum256([]byte(s))
	return strings.ToUpper(hex.EncodeToString(sum[:]))
}
