package httputil_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/httputil"
)

func TestJSON_WritesStatusAndBody(t *testing.T) {
	rr := httptest.NewRecorder()
	payload := map[string]string{"status": "ok"}

	httputil.JSON(rr, http.StatusCreated, payload)

	if rr.Code != http.StatusCreated {
		t.Errorf("status: got %d, want %d", rr.Code, http.StatusCreated)
	}
	if got := rr.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type: got %q, want %q", got, "application/json")
	}

	var decoded map[string]string
	if err := json.NewDecoder(strings.NewReader(rr.Body.String())).Decode(&decoded); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if decoded["status"] != "ok" {
		t.Errorf("body status: got %q, want %q", decoded["status"], "ok")
	}
}

func TestJSON_AcceptsStruct(t *testing.T) {
	type response struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	}

	rr := httptest.NewRecorder()
	httputil.JSON(rr, http.StatusOK, response{Message: "hello", Code: 42})

	if rr.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", rr.Code)
	}
	var decoded response
	if err := json.NewDecoder(strings.NewReader(rr.Body.String())).Decode(&decoded); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if decoded.Message != "hello" || decoded.Code != 42 {
		t.Errorf("decoded = %+v; want {Message:hello Code:42}", decoded)
	}
}
