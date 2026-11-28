package newebpayadapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// TestSecretsNeverPrint exercises every print, marshal and log path this
// package's HashKey/HashIV expose, and asserts the raw key/IV bytes never
// appear in the output. A leak here would mean a store's NewebPay
// credential ending up in a log line or an error message somewhere this
// package is used.
func TestSecretsNeverPrint(t *testing.T) {
	key := vecKey(t)
	iv := vecIV(t)
	creds := vecCreds(t)

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%v %+v %#v %s\n", key, key, key, key)
	fmt.Fprintf(&buf, "%v %+v %#v %s\n", iv, iv, iv, iv)
	fmt.Fprintf(&buf, "%v %+v %#v %s\n", creds, creds, creds, creds)

	if b, err := json.Marshal(key); err != nil {
		t.Fatalf("json.Marshal(key): %v", err)
	} else {
		buf.Write(b)
	}
	if b, err := json.Marshal(iv); err != nil {
		t.Fatalf("json.Marshal(iv): %v", err)
	} else {
		buf.Write(b)
	}
	if b, err := json.Marshal(creds); err != nil {
		t.Fatalf("json.Marshal(creds): %v", err)
	} else {
		buf.Write(b)
	}

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	logger.Info("notice verified", "key", key, "iv", iv, "creds", creds)
	buf.Write(logBuf.Bytes())

	output := buf.String()
	for _, secret := range []string{vecKeyRaw, vecIVRaw} {
		if strings.Contains(output, secret) {
			t.Fatalf("secret bytes %q leaked into print/marshal/log output:\n%s", secret, output)
		}
	}
	// Reveal is the one deliberate exception, and this test proves it is
	// the only one: every OTHER path above must redact.
	if !bytes.Equal(key.Reveal(), []byte(vecKeyRaw)) {
		t.Fatal("Reveal() no longer returns the raw key — vector or type changed out from under this test")
	}
}
