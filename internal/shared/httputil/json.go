// Package httputil provides thin HTTP helpers shared across cmd/.
package httputil

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// JSON writes a JSON response with the given status code. The payload
// is marshaled via json.Marshal — no trailing newline, matching the
// byte output of the Lambda-transport proxyResponse paths. On marshal
// failure the status header has not yet been written, so a 500 is
// emitted with no body; the caller observes a logged error.
func JSON(w http.ResponseWriter, status int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		slog.Error("httputil.JSON marshal failed", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
