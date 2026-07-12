package awslambdainv

import "testing"

func TestParseSSRFunctionName_ValidCase(t *testing.T) {
	appID := "3fa85f64-5717-4562-b3fc-2c963f66afa6"
	name := "ms-apphost-" + appID + "-prod"

	gotID, gotEnv, ok := ParseSSRFunctionName(name)
	if !ok {
		t.Fatalf("ParseSSRFunctionName(%q) ok = false, want true", name)
	}
	if gotID.String() != appID {
		t.Errorf("appID = %q, want %q", gotID.String(), appID)
	}
	if gotEnv != "prod" {
		t.Errorf("env = %q, want %q", gotEnv, "prod")
	}
}

func TestParseSSRFunctionName_HyphenatedEnvDoesNotBreakFixedLengthParse(t *testing.T) {
	// A naive strings.Split(name, "-") or
	// TrimSuffix(TrimPrefix(name, prefix), "-"+env) approach would either
	// misindex fields (the UUID's own 4 internal hyphens) or require knowing
	// the env value up front to trim it. Fixed-length UUID slicing sidesteps
	// both: it only needs the FIRST 36 characters after the prefix, so a
	// hyphenated env value like "us-east-1-canary" parses correctly as the
	// entire remainder.
	appID := "9c858901-8a57-4791-81fe-4c455b099bc9"
	name := "ms-apphost-" + appID + "-us-east-1-canary"

	gotID, gotEnv, ok := ParseSSRFunctionName(name)
	if !ok {
		t.Fatalf("ParseSSRFunctionName(%q) ok = false, want true", name)
	}
	if gotID.String() != appID {
		t.Errorf("appID = %q, want %q", gotID.String(), appID)
	}
	if gotEnv != "us-east-1-canary" {
		t.Errorf("env = %q, want %q (naive hyphen-splitting would corrupt this)", gotEnv, "us-east-1-canary")
	}
}

func TestParseSSRFunctionName_RejectsWrongPrefix(t *testing.T) {
	if _, _, ok := ParseSSRFunctionName("some-other-function-name"); ok {
		t.Errorf("ok = true, want false for a name without the ms-apphost- prefix")
	}
}

func TestParseSSRFunctionName_RejectsGarbageUUID(t *testing.T) {
	name := "ms-apphost-not-a-real-uuid-at-all-here-prod"
	if _, _, ok := ParseSSRFunctionName(name); ok {
		t.Errorf("ok = true, want false for a non-UUID app_id segment")
	}
}

func TestParseSSRFunctionName_RejectsMissingEnv(t *testing.T) {
	appID := "3fa85f64-5717-4562-b3fc-2c963f66afa6"
	name := "ms-apphost-" + appID // no trailing "-env" at all
	if _, _, ok := ParseSSRFunctionName(name); ok {
		t.Errorf("ok = true, want false when there is no env suffix")
	}
}

func TestParseSSRFunctionName_RejectsMissingSeparator(t *testing.T) {
	// 36 chars right after the prefix but the 37th char isn't "-".
	name := "ms-apphost-3fa85f645717456ab3fc2c963f66afa6Xprod"
	if _, _, ok := ParseSSRFunctionName(name); ok {
		t.Errorf("ok = true, want false when the char after the 36-byte slice isn't '-'")
	}
}

func TestParseSSRFunctionName_RejectsTooShort(t *testing.T) {
	if _, _, ok := ParseSSRFunctionName("ms-apphost-short"); ok {
		t.Errorf("ok = true, want false for a name too short to contain a UUID")
	}
}
