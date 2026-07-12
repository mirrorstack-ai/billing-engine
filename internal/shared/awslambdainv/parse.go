package awslambdainv

import (
	"github.com/google/uuid"
)

// ssrFunctionPrefix is the fixed prefix every app-hosting SSR Lambda function
// name carries (api-platform's internal/applications/deploy/apphost/
// apphost.go:82-84: FunctionName(appID, env) = "ms-apphost-" + appID + "-" +
// env). It is also the client-side ListFunctions filter (§2.1 of the design
// doc — lambda:ListFunctions has no server-side prefix filter parameter).
const ssrFunctionPrefix = "ms-apphost-"

// uuidStringLen is the length of a canonical 8-4-4-4-12 hex-with-hyphens
// UUID string (36 characters). appID is always minted as a real UUID, so this
// length is fixed and safe to assume.
const uuidStringLen = 36

// ParseSSRFunctionName reverse-parses an "ms-apphost-<app_id>-<env>" Lambda
// function name into (appID, env) using FIXED-LENGTH UUID slicing, NOT a
// naive strings.Split/TrimPrefix+TrimSuffix on "-".
//
// Why fixed-length slicing and not a hyphen split: appID itself is a
// canonical UUID and ALREADY contains 4 internal hyphens
// (8-4-4-4-12), so a plain split-on-"-" can't even find the app_id/env
// boundary unambiguously by counting fields. And env is not guaranteed to be
// hyphen-free forever (e.g. a future "us-east-1-canary" style env value) —
// design doc Open Question 2. The only boundary that is ALWAYS safe is: the
// prefix is exactly 11 characters, the app_id is exactly the next 36
// characters (it is always a canonical UUID string), and the very next
// character after that is the "-" separator; everything after IS the env,
// hyphens and all.
//
// Returns ok=false (never panics) when the name doesn't carry the prefix,
// isn't long enough, the expected separator is missing, or the sliced app_id
// segment doesn't parse as a real UUID — the caller skips + logs, exactly
// like infra-egress-sync's existing unparseable-app_id skip.
//
// NOTE (matches the design doc's own Open Question 2): this name-parsing
// approach is a deliberate, load-bearing simplification that works ONLY
// because app_id is always a canonical 36-char UUID today. Once
// api-platform's function-tagging PR (parallel work) lands and tags every
// ms-apphost-* function with {"app_id": ..., "env": ...} at CreateFunction
// time, a tag-based attribution fast-follow should replace this — but this
// PR does not block on that landing first.
func ParseSSRFunctionName(name string) (appID uuid.UUID, env string, ok bool) {
	if len(name) <= len(ssrFunctionPrefix) {
		return uuid.Nil, "", false
	}
	if name[:len(ssrFunctionPrefix)] != ssrFunctionPrefix {
		return uuid.Nil, "", false
	}
	rest := name[len(ssrFunctionPrefix):]
	// Need at least uuidStringLen (the app_id) + 1 (the "-" separator) + 1
	// (a non-empty env) more characters.
	if len(rest) < uuidStringLen+2 {
		return uuid.Nil, "", false
	}
	if rest[uuidStringLen] != '-' {
		return uuid.Nil, "", false
	}
	idPart := rest[:uuidStringLen]
	envPart := rest[uuidStringLen+1:]
	if envPart == "" {
		return uuid.Nil, "", false
	}
	id, err := uuid.Parse(idPart)
	if err != nil {
		return uuid.Nil, "", false
	}
	return id, envPart, true
}
