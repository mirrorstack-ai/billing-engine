package newebpayadapter

import (
	"errors"
	"testing"
)

func TestEncryptKnownAnswer(t *testing.T) {
	got, err := Encrypt(vecKey(t), vecIV(t), vecQueryPlain)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if got != vecQueryCipherHex {
		t.Fatalf("Encrypt mismatch:\n got  %s\n want %s", got, vecQueryCipherHex)
	}
}

func TestDecryptKnownAnswer(t *testing.T) {
	got, err := Decrypt(vecKey(t), vecIV(t), vecQueryCipherHex)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if got != vecQueryPlain {
		t.Fatalf("Decrypt mismatch:\n got  %s\n want %s", got, vecQueryPlain)
	}
}

func TestTradeShaKnownAnswer(t *testing.T) {
	got := TradeSha(vecKey(t), vecIV(t), vecQueryCipherHex)
	if got != vecQueryTradeSha {
		t.Fatalf("TradeSha mismatch:\n got  %s\n want %s", got, vecQueryTradeSha)
	}
}

func TestCheckValueKnownAnswer(t *testing.T) {
	got := CheckValue(vecKey(t), vecIV(t), vecAmt, vecMerchantID, vecMerchantOrderNo)
	if got != vecCheckValue {
		t.Fatalf("CheckValue mismatch:\n got  %s\n want %s", got, vecCheckValue)
	}
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	key, iv := vecKey(t), vecIV(t)
	for _, plain := range []string{
		"",
		"a",
		"exactly-16-bytes",
		"MerchantID=MS1&Amt=1",
		"this plaintext is deliberately longer than two whole AES blocks to exercise multi-block CBC chaining",
	} {
		ciphertext, err := Encrypt(key, iv, plain)
		if err != nil {
			t.Fatalf("Encrypt(%q): %v", plain, err)
		}
		got, err := Decrypt(key, iv, ciphertext)
		if err != nil {
			t.Fatalf("Decrypt(Encrypt(%q)): %v", plain, err)
		}
		if got != plain {
			t.Fatalf("round trip mismatch: got %q want %q", got, plain)
		}
	}
}

func TestDecryptForgedCalls(t *testing.T) {
	key, iv := vecKey(t), vecIV(t)

	cases := []struct {
		name    string
		hexIn   string
		wantErr error
	}{
		{
			name:    "non-hex input",
			hexIn:   "zz" + vecQueryCipherHex[2:],
			wantErr: ErrInvalidHex,
		},
		{
			name:    "odd-length hex",
			hexIn:   vecQueryCipherHex[:len(vecQueryCipherHex)-1],
			wantErr: ErrInvalidHex,
		},
		{
			name:    "length not a multiple of 16",
			hexIn:   vecQueryCipherHex[:len(vecQueryCipherHex)-2],
			wantErr: ErrInvalidCiphertextLength,
		},
		{
			name:    "empty ciphertext",
			hexIn:   "",
			wantErr: ErrInvalidCiphertextLength,
		},
		{
			name: "flipped hex digit produces bad padding or garbage",
			hexIn: func() string {
				b := []byte(vecQueryCipherHex)
				// Flip a digit in the last block, which is what PKCS7
				// unpadding inspects.
				if b[len(b)-1] == '0' {
					b[len(b)-1] = '1'
				} else {
					b[len(b)-1] = '0'
				}
				return string(b)
			}(),
			wantErr: ErrInvalidPadding,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decrypt(key, iv, tc.hexIn)
			if err == nil {
				t.Fatalf("Decrypt(%q): want error, got nil", tc.hexIn)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Decrypt(%q): got %v, want error wrapping %v", tc.hexIn, err, tc.wantErr)
			}
		})
	}
}

func TestConstantTimeEqualHex(t *testing.T) {
	if !constantTimeEqualHex("ABCD", "abcd") {
		t.Fatal("expected case-insensitive match")
	}
	if constantTimeEqualHex("ABCD", "ABCE") {
		t.Fatal("expected mismatch")
	}
	if constantTimeEqualHex("ABCD", "ABCDE") {
		t.Fatal("expected length mismatch to fail")
	}
}
